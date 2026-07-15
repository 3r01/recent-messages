#![type_length_limit = "99999999"]

mod config;
mod coverage;
mod db;
mod irc_listener;
mod message_export;
mod monitoring;
mod shutdown;
mod web;

use crate::config::{Args, Config};
use crate::db::ControlStore;
use futures::future::FusedFuture;
use futures::prelude::*;
use recent_messages2::storage::{
    AsyncSqliteBlockStore, DurableIngest, IngestRuntimeConfig, OpenBlockLimits, RawFirehoseConfig,
    RawFirehoseSource, RawIngestRuntime, StorageBudget, StorageBudgetEnforcer,
};
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    tracing_subscriber::fmt::init();

    // args and config parsing
    let args = Args::from_args();
    tracing::debug!("Parsed args: {:#?}", args);

    let config = config::load_config(&args).await;
    let config = match config {
        Ok(config) => config,
        Err(e) => {
            tracing::error!(
                "Failed to load config from `{}`: {}",
                args.config_path.display(),
                e,
            );
            std::process::exit(1);
        }
    };
    let config: &'static Config = Box::leak(Box::new(config));

    tracing::debug!(path = %args.config_path.display(), "Configuration loaded");

    #[cfg(unix)]
    increase_nofile_rlimit();
    let shutdown_signal = CancellationToken::new();

    let process_monitoring_join_handle =
        tokio::spawn(monitoring::run_process_monitoring(shutdown_signal.clone()));

    let control_store = match ControlStore::open(&config.control_store.path) {
        Ok(store) => Box::leak(Box::new(store)),
        Err(error) => {
            tracing::error!("Failed to open SQLite control store: {error}");
            std::process::exit(1);
        }
    };
    let block_store = match AsyncSqliteBlockStore::open(
        &config.block_store.path,
        config.block_store.block_messages,
        config.block_store.max_messages_per_channel,
        config.block_store.writer_queue_batches,
        config.block_store.read_connections,
    ) {
        Ok(store) => Box::leak(Box::new(store)) as &'static AsyncSqliteBlockStore,
        Err(error) => {
            tracing::error!("Failed to open local block store: {error}");
            std::process::exit(1);
        }
    };
    let mut bootstrap_channels = config
        .irc
        .always_join_channels
        .iter()
        .map(|channel| channel.to_ascii_lowercase())
        .collect::<Vec<_>>();
    bootstrap_channels.sort_unstable();
    bootstrap_channels.dedup();
    if bootstrap_channels.len() > config.admin.max_always_join_channels {
        tracing::error!(
            count = bootstrap_channels.len(),
            maximum = config.admin.max_always_join_channels,
            "Always-join bootstrap exceeds configured maximum"
        );
        std::process::exit(1);
    }
    for channel in &bootstrap_channels {
        if let Err(error) = twitch_irc::validate::validate_login(channel) {
            tracing::error!(%error, %channel, "Invalid always-join bootstrap channel");
            std::process::exit(1);
        }
    }
    control_store
        .bootstrap_always_join_channels(&bootstrap_channels)
        .await
        .unwrap_or_else(|error| {
            tracing::error!(%error, "Failed to apply always-join bootstrap");
            std::process::exit(1);
        });
    let always_join_channels = control_store
        .get_always_join_channels()
        .await
        .unwrap_or_else(|error| {
            tracing::error!(%error, "Failed to load always-join channels");
            std::process::exit(1);
        });
    block_store
        .sync_always_join_channels(always_join_channels)
        .await
        .unwrap_or_else(|error| {
            tracing::error!(%error, "Failed to synchronize always-join storage priority");
            std::process::exit(1);
        });
    let ignored_channels = control_store
        .get_ignored_channels()
        .await
        .unwrap_or_else(|error| {
            tracing::error!("Failed to load ignored channels from SQLite control store: {error}");
            std::process::exit(1);
        });
    block_store
        .sync_blocked_channels(ignored_channels.into_iter().collect())
        .await
        .unwrap_or_else(|error| {
            tracing::error!("Failed to synchronize ignored channels to block store: {error}");
            std::process::exit(1);
        });

    if config.ingest.enabled
        && (config.ingest.queue_batches == 0 || config.ingest.batch_messages == 0)
    {
        tracing::error!("ingest.enabled requires non-zero queue/batch bounds");
        std::process::exit(1);
    }
    let (direct_ingest, direct_input) = if config.ingest.enabled {
        let (output, input) = mpsc::channel(config.ingest.queue_batches);
        (Some(output), Some(input))
    } else {
        (None, None)
    };

    let (irc_listener, forward_worker_join_handle, channel_jp_join_handle) =
        irc_listener::IrcListener::start_with_direct_ingest(
            control_store,
            config,
            direct_ingest,
            shutdown_signal.clone(),
        );
    let irc_listener = Box::leak(Box::new(irc_listener));

    let mut shared_open_blocks = None;
    let ingest_join_handle = direct_input.map(|input| {
        let store = block_store.clone();
        let ingest = DurableIngest::new(
            store.clone(),
            OpenBlockLimits {
                block_messages: config.block_store.block_messages,
                max_open_channels: config.ingest.max_open_channels,
                max_open_bytes: config.ingest.max_open_bytes,
                idle_seal_after_ms: duration_millis(config.ingest.idle_seal_after),
                max_open_age_ms: duration_millis(config.ingest.max_open_age),
            },
        )
        .unwrap_or_else(|error| {
            tracing::error!("Invalid durable ingest configuration: {error}");
            std::process::exit(1);
        });
        shared_open_blocks = Some(ingest.open_blocks_handle());
        let sources = config
            .ingest
            .raw_firehoses
            .iter()
            .enumerate()
            .map(|(index, url)| {
                RawFirehoseSource::new(RawFirehoseConfig {
                    source_id: format!("raw-firehose-{index}"),
                    url: url.clone(),
                    origin: config.ingest.origin.clone(),
                    batch_messages: config.ingest.batch_messages,
                    batch_max_delay: config.ingest.batch_max_delay,
                    reconnect_min_delay: config.ingest.reconnect_min_delay,
                    reconnect_max_delay: config.ingest.reconnect_max_delay,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|error| {
                tracing::error!("Invalid raw firehose configuration: {error}");
                std::process::exit(1);
            });
        let budget = StorageBudgetEnforcer::new(
            store,
            StorageBudget {
                max_bytes: config.storage_budget.max_bytes,
                max_filesystem_percent: config.storage_budget.max_filesystem_percent,
                min_free_bytes: config.storage_budget.min_free_bytes,
                wal_reserve_bytes: config.storage_budget.wal_reserve_bytes,
                high_water_ratio: config.storage_budget.high_water_ratio,
                target_ratio: config.storage_budget.target_ratio,
                emergency_ratio: config.storage_budget.emergency_ratio,
                pressure_floor_messages_per_channel: config
                    .storage_budget
                    .pressure_floor_messages_per_channel,
                requested_channel_protect_for: config.app.channels_expire_after,
                max_enforcement_passes: config.storage_budget.max_enforcement_passes,
            },
        )
        .unwrap_or_else(|error| {
            tracing::error!("Invalid storage budget configuration: {error}");
            std::process::exit(1);
        });
        let runtime = RawIngestRuntime::new_with_external_input(
            ingest,
            sources,
            IngestRuntimeConfig {
                queue_batches: config.ingest.queue_batches,
                seal_interval: config.ingest.seal_interval,
                checkpoint_interval: config.ingest.checkpoint_interval,
                checkpoint_poll_interval: config.ingest.checkpoint_poll_interval,
                checkpoint_journal_bytes: config.ingest.checkpoint_journal_bytes,
                message_ttl: config.app.message_ttl,
            },
            input,
        )
        .unwrap_or_else(|error| {
            tracing::error!("Invalid ingest runtime configuration: {error}");
            std::process::exit(1);
        })
        .with_budget_enforcer(budget);
        let cancellation = shutdown_signal.clone();
        tokio::spawn(async move {
            if let Err(error) = runtime.run(cancellation).await {
                panic!("ingest runtime failed: {error}");
            }
        })
    });

    let webserver = match web::run(
        control_store,
        irc_listener,
        config,
        block_store,
        shared_open_blocks,
        shutdown_signal.clone(),
    )
    .await
    {
        Ok(webserver) => webserver,
        Err(bind_error) => {
            tracing::error!("{}", bind_error);
            std::process::exit(1);
        }
    };
    let webserver_join_handle = tokio::spawn(webserver);

    // await termination.
    let os_shutdown_signal = shutdown::shutdown_signal().fuse();
    futures::pin_mut!(os_shutdown_signal);

    let with_name = move |fut: JoinHandle<()>, name| fut.map(move |x| (x, name));
    let mut simple_workers = vec![
        with_name(process_monitoring_join_handle, "Process Monitoring task").fuse(),
        with_name(
            forward_worker_join_handle,
            "IRC message forwarder (preprocessor)",
        )
        .fuse(),
        with_name(channel_jp_join_handle, "IRC channel join/part task").fuse(),
    ];
    if let Some(handle) = ingest_join_handle {
        simple_workers.push(with_name(handle, "Durable ingest runtime").fuse());
    }

    let mut webserver_join_handle = webserver_join_handle.fuse();
    let mut exit_code: i32 = 0;
    loop {
        let all_simple_workers_terminated = simple_workers.iter().all(FusedFuture::is_terminated);
        if all_simple_workers_terminated && webserver_join_handle.is_terminated() {
            tracing::info!("Everything shut down successfully, ending");
            break;
        }

        let any_simple_worker = futures::future::select_all(simple_workers.iter_mut());

        tokio::select! {
            () = &mut os_shutdown_signal, if !os_shutdown_signal.is_terminated() => {
                tracing::debug!("Received shutdown signal");
                shutdown_signal.cancel();
            },
            fut_output = any_simple_worker, if !all_simple_workers_terminated => {
                let ((worker_result, name), _, _) = fut_output;
                match worker_result {
                    Ok(()) => {
                        if shutdown_signal.is_cancelled() {
                            // regular end after graceful shutdown request
                            tracing::info!("{} has successfully shut down gracefully", name);
                        } else {
                            tracing::error!("{} ended without error even though no shutdown was requested (shutting down other parts of application gracefully)", name);
                            shutdown_signal.cancel();
                            exit_code = 1;
                        }
                    }
                    Err(join_error) => {
                        tracing::error!(
                            "{} ended abnormally (shutting down other parts of application gracefully): {}",
                            name,
                            join_error
                        );
                        shutdown_signal.cancel();
                        exit_code = 1;
                    }
                }
            }
            webserver_result = (&mut webserver_join_handle), if !webserver_join_handle.is_terminated() => {
                // two cases:
                // - webserver ends on its own WITHOUT us sending the
                //   shutdown signal first (fatal error probably)
                //   ctrl_c_event.is_terminated() will be FALSE
                // - webserver ends after Ctrl-C shutdown request
                //   ctrl_c_event.is_terminated() will be TRUE
                match webserver_result {
                    Ok(Ok(())) => {
                        if shutdown_signal.is_cancelled() {
                            // regular end after graceful shutdown request
                            tracing::info!("Webserver has successfully shut down gracefully");
                        } else {
                            tracing::error!("Webserver ended without error even though no shutdown was requested (shutting down other parts of application gracefully)");
                            shutdown_signal.cancel();
                            exit_code = 1;
                        }
                    },
                    Ok(Err(tower_error)) => {
                        tracing::error!("Webserver encountered fatal error (shutting down other parts of application gracefully): {}", tower_error);
                        shutdown_signal.cancel();
                        exit_code = 1;
                    },
                    Err(join_error) => {
                        tracing::error!("Webserver tokio task ended abnormally (shutting down other parts of application gracefully): {}", join_error);
                        shutdown_signal.cancel();
                        exit_code = 1;
                    }
                }
            }
        }
    }

    std::process::exit(exit_code);
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(unix)]
fn increase_nofile_rlimit() {
    use rlimit::Resource;
    let (soft, hard) = match Resource::NOFILE.get() {
        Ok((soft, hard)) => (soft, hard),
        Err(e) => {
            tracing::error!(
                "Failed to get NOFILE rlimit, will not attempt to increase rlimit: {}",
                e
            );
            return;
        }
    };
    tracing::debug!(
        "NOFILE rlimit: process was started with limits set to {} soft, {} hard",
        soft,
        hard
    );

    if soft < hard {
        match Resource::NOFILE.set(hard, hard) {
            Ok(()) => tracing::info!(
                "Successfully increased NOFILE rlimit to {}, was at {}",
                hard,
                soft
            ),
            Err(e) => tracing::error!("Failed to increase NOFILE rlimit to {}: {}", hard, e),
        }
    } else {
        tracing::debug!("NOFILE rlimit: no need to increase (soft limit is not below hard limit)");
    }
}
