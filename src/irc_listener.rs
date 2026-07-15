use crate::config::Config;
use crate::db::ControlStore;
use chrono::Utc;
use recent_messages2::storage::{DirectIrcBatcher, RawIrcError, RawSourceBatch};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use twitch_irc::login::StaticLoginCredentials;
use twitch_irc::message::{AsRawIRC, ServerMessage};
use twitch_irc::{ClientConfig, SecureTCPTransport, TwitchIRCClient};

#[derive(Debug, Clone)]
pub struct IrcListener {
    pub irc_client: TwitchIRCClient<SecureTCPTransport, StaticLoginCredentials>,
    control_store: &'static ControlStore,
    config: &'static Config,
    wanted_channels_gate: Arc<Mutex<()>>,
    reconcile_notify: Arc<Notify>,
}

impl IrcListener {
    pub fn start_with_direct_ingest(
        control_store: &'static ControlStore,
        config: &'static Config,
        direct_ingest: Option<mpsc::Sender<RawSourceBatch>>,
        shutdown_signal: CancellationToken,
    ) -> (IrcListener, JoinHandle<()>, JoinHandle<()>) {
        let (incoming_messages, client) = TwitchIRCClient::new(ClientConfig {
            new_connection_every: config.irc.new_connection_every,
            ..ClientConfig::default()
        });

        let forward_worker_join_handle = IrcListener::run_forwarder(
            incoming_messages,
            client.clone(),
            config,
            control_store,
            direct_ingest,
            &shutdown_signal,
        );

        let wanted_channels_gate = Arc::new(Mutex::new(()));
        let reconcile_notify = Arc::new(Notify::new());

        let channel_jp_join_handle = tokio::spawn(IrcListener::run_channel_join_parter(
            client.clone(),
            config,
            control_store,
            Arc::clone(&wanted_channels_gate),
            Arc::clone(&reconcile_notify),
            shutdown_signal,
        ));

        (
            IrcListener {
                irc_client: client,
                control_store,
                config,
                wanted_channels_gate,
                reconcile_notify,
            },
            forward_worker_join_handle,
            channel_jp_join_handle,
        )
    }

    fn run_forwarder(
        mut incoming_messages: mpsc::UnboundedReceiver<ServerMessage>,
        irc_client: TwitchIRCClient<SecureTCPTransport, StaticLoginCredentials>,
        config: &'static Config,
        control_store: &'static ControlStore,
        direct_ingest: Option<mpsc::Sender<RawSourceBatch>>,
        shutdown_signal: &CancellationToken,
    ) -> JoinHandle<()> {
        let forward_shutdown = shutdown_signal.clone();
        let forward_worker = async move {
            let mut direct_batcher = direct_ingest.as_ref().map(|_| {
                DirectIrcBatcher::new(
                    "owned-irc",
                    format!(
                        "owned-irc-{}-{}",
                        std::process::id(),
                        Utc::now().timestamp_millis()
                    ),
                    config.ingest.batch_messages,
                )
                .expect("static direct IRC batcher configuration is valid")
            });
            loop {
                let message = tokio::select! {
                    () = forward_shutdown.cancelled() => {
                        let mut direct_flush_succeeded = true;
                        if let (Some(output), Some(batcher)) =
                            (direct_ingest.as_ref(), direct_batcher.as_mut())
                            && let Some(batch) = batcher.flush()
                        {
                            direct_flush_succeeded = output.send(batch).await.is_ok();
                        }
                        let coverage_result = if direct_flush_succeeded {
                            control_store
                                .close_all_live_coverage(Utc::now().timestamp_millis())
                                .await
                        } else {
                            control_store.discard_all_live_coverage().await
                        };
                        if let Err(error) = coverage_result {
                            tracing::error!(%error, "Failed to finalize direct-IRC coverage on shutdown");
                        }
                        return;
                    }
                    message = incoming_messages.recv() => {
                        let Some(message) = message else { return; };
                        message
                    }
                };
                let received_at_ms = Utc::now().timestamp_millis();
                let unavailable_notice = if let ServerMessage::Notice(notice) = &message
                    && let (Some(channel_login), Some(message_id)) =
                        (&notice.channel_login, &notice.message_id)
                {
                    let prevents_membership =
                        matches!(message_id.as_str(), "msg_channel_suspended" | "tos_ban")
                            || (message_id == "msg_banned"
                                && irc_client.get_channel_status(channel_login.clone()).await
                                    != (true, true));
                    prevents_membership.then(|| (channel_login.clone(), message_id.clone()))
                } else {
                    None
                };
                let join_health_result = match &message {
                    ServerMessage::Join(message) => {
                        control_store
                            .mark_join_confirmed(&message.channel_login)
                            .await
                    }
                    ServerMessage::Part(message) => {
                        control_store
                            .mark_join_unavailable(
                                &message.channel_login,
                                "parted",
                                config.irc.join_retry_min,
                            )
                            .await
                    }
                    _ if unavailable_notice.is_some() => {
                        let (channel_login, failure_kind) =
                            unavailable_notice.as_ref().expect("checked above");
                        let result = control_store
                            .mark_join_unavailable(
                                channel_login,
                                failure_kind,
                                config.irc.unavailable_retry_after,
                            )
                            .await;
                        irc_client.part(channel_login.clone());
                        result
                    }
                    _ => Ok(()),
                };
                if let Err(error) = join_health_result {
                    tracing::error!(%error, "Failed to update IRC join health");
                }
                let coverage_result = match (&message, direct_batcher.is_some()) {
                    (ServerMessage::Join(message), true) => {
                        control_store
                            .begin_live_coverage(
                                &message.channel_login,
                                "direct-irc",
                                received_at_ms,
                            )
                            .await
                    }
                    (ServerMessage::Part(message), true) => {
                        control_store
                            .end_live_coverage(&message.channel_login, received_at_ms)
                            .await
                    }
                    (_, true) if unavailable_notice.is_some() => {
                        control_store
                            .end_live_coverage(
                                &unavailable_notice.as_ref().expect("checked above").0,
                                received_at_ms,
                            )
                            .await
                    }
                    _ => Ok(()),
                };
                if let Err(error) = coverage_result {
                    tracing::error!(%error, "Failed to update direct-IRC coverage");
                }
                if message.channel_login().is_some() {
                    let message_source = message.source().as_raw_irc();
                    if let (Some(output), Some(batcher)) =
                        (direct_ingest.as_ref(), direct_batcher.as_mut())
                    {
                        match batcher.push_raw(&message_source, received_at_ms) {
                            Ok(Some(batch)) => {
                                if output.send(batch).await.is_err() {
                                    tracing::error!("Direct IRC ingest coordinator stopped");
                                    direct_batcher = None;
                                    if let Err(error) =
                                        control_store.discard_all_live_coverage().await
                                    {
                                        tracing::error!(%error, "Failed to discard interrupted direct-IRC coverage");
                                    }
                                }
                            }
                            Ok(None) | Err(RawIrcError::UnsupportedCommand(_)) => {}
                            Err(error) => {
                                tracing::warn!(%error, "Failed to canonicalize owned IRC event");
                                if let Some(channel_login) = message.channel_login()
                                    && let Err(error) =
                                        control_store.discard_live_coverage(channel_login).await
                                {
                                    tracing::error!(%error, %channel_login, "Failed to discard incomplete direct-IRC coverage");
                                }
                            }
                        }
                    }
                }
            }
        };

        let shutdown_signal_1 = shutdown_signal.clone();
        tokio::spawn(async move {
            forward_worker.await;
            assert!(
                shutdown_signal_1.is_cancelled(),
                "forward worker should never end"
            );
        })
    }

    /// Start background loop to vacuum/part channels that are not used.
    pub async fn run_channel_join_parter(
        irc_client: TwitchIRCClient<SecureTCPTransport, StaticLoginCredentials>,
        config: &'static Config,
        control_store: &'static ControlStore,
        wanted_channels_gate: Arc<Mutex<()>>,
        reconcile_notify: Arc<Notify>,
        shutdown_signal: CancellationToken,
    ) {
        let mut check_interval = tokio::time::interval(config.app.vacuum_channels_every);
        let mut retry_interval = tokio::time::interval(
            config
                .irc
                .join_retry_poll
                .max(std::time::Duration::from_secs(1)),
        );

        let worker = async move {
            loop {
                tokio::select! {
                    _ = retry_interval.tick() => {
                        match control_store
                            .fail_stale_join_attempts(
                                config.irc.join_retry_min,
                                config.irc.join_retry_max,
                            )
                            .await
                        {
                            Ok(0) => {}
                            Ok(count) => {
                                tracing::warn!(count, "IRC joins timed out; retry backoff applied");
                                let _wanted_channels_guard = wanted_channels_gate.lock().await;
                                match control_store
                                    .get_channel_logins_to_join(config.app.channels_expire_after)
                                    .await
                                {
                                    Ok(channels) => {
                                        if let Err(error) = irc_client.set_wanted_channels(channels) {
                                            tracing::error!(%error, "Failed to remove backed-off IRC joins from wanted set");
                                        }
                                    }
                                    Err(error) => tracing::error!(%error, "Failed to refresh IRC wanted set after join timeout"),
                                }
                            }
                            Err(error) => tracing::error!(%error, "Failed to reconcile pending IRC joins"),
                        }
                        continue;
                    }
                    _ = check_interval.tick() => {}
                    _ = reconcile_notify.notified() => {}
                }

                let _wanted_channels_guard = wanted_channels_gate.lock().await;

                let res = control_store
                    .get_channel_logins_to_join(config.app.channels_expire_after)
                    .await;
                let channels = match res {
                    Ok(channels_to_part) => channels_to_part,
                    Err(e) => {
                        tracing::error!(
                            "Failed to query the DB for a list of channels that should be joined. This iteration will be skipped. Cause: {}",
                            e
                        );
                        continue;
                    }
                };

                tracing::info!(
                    "Checked database for channels that should be joined, now at {} channels",
                    channels.len()
                );
                for channel_login in &channels {
                    if irc_client.get_channel_status(channel_login.clone()).await == (true, true) {
                        if let Err(error) = control_store.mark_join_confirmed(channel_login).await {
                            tracing::error!(%error, %channel_login, "Failed to persist confirmed IRC join");
                        }
                    } else if let Err(error) = control_store
                        .begin_join_attempt_if_due(
                            channel_login,
                            config.irc.join_confirmation_timeout,
                            config.irc.join_retry_min,
                            config.irc.join_retry_max,
                        )
                        .await
                    {
                        tracing::error!(%error, %channel_login, "Failed to persist IRC join attempt");
                    }
                }
                let coverage_cutoff = Utc::now().timestamp_millis().saturating_sub(
                    i64::try_from(config.app.message_ttl.as_millis()).unwrap_or(i64::MAX),
                );
                if let Err(error) = control_store
                    .prune_coverage_ended_before(coverage_cutoff)
                    .await
                {
                    tracing::error!(%error, "Failed to prune expired channel coverage");
                }
                let demand_cutoff = Utc::now().timestamp_millis().saturating_sub(
                    i64::try_from(config.app.channels_expire_after.as_millis()).unwrap_or(i64::MAX),
                );
                if let Err(error) = control_store.prune_inactive_channels(demand_cutoff).await {
                    tracing::error!(%error, "Failed to prune inactive channel control state");
                }
                irc_client.set_wanted_channels(channels).unwrap();
            }
        };

        tokio::select! {
            _ = worker => {},
            () = shutdown_signal.cancelled() => {}
        }
    }

    pub async fn join_if_needed(
        &self,
        channel_login: String,
    ) -> Result<(), crate::db::StorageError> {
        let _wanted_channels_guard = self.wanted_channels_gate.lock().await;
        self.control_store
            .touch_or_add_channel(&channel_login)
            .await?;
        let joined = self
            .irc_client
            .get_channel_status(channel_login.clone())
            .await
            == (true, true);
        if !joined
            && self
                .control_store
                .begin_join_attempt_if_due(
                    &channel_login,
                    self.config.irc.join_confirmation_timeout,
                    self.config.irc.join_retry_min,
                    self.config.irc.join_retry_max,
                )
                .await?
        {
            self.irc_client.join(channel_login).unwrap();
        }
        Ok(())
    }

    pub fn request_reconcile(&self) {
        self.reconcile_notify.notify_one();
    }

    pub async fn is_join_confirmed(&self, channel_login: String) -> bool {
        self.irc_client.get_channel_status(channel_login).await == (true, true)
    }
}

trait ServerMessageExt {
    /// Get the channel login if this message was sent to a channel.
    fn channel_login(&self) -> Option<&str>;
}

impl ServerMessageExt for ServerMessage {
    fn channel_login(&self) -> Option<&str> {
        match self {
            ServerMessage::ClearChat(m) => Some(&m.channel_login),
            ServerMessage::ClearMsg(m) => Some(&m.channel_login),
            ServerMessage::Join(m) => Some(&m.channel_login),
            ServerMessage::Notice(m) => m.channel_login.as_deref(),
            ServerMessage::Part(m) => Some(&m.channel_login),
            ServerMessage::Privmsg(m) => Some(&m.channel_login),
            ServerMessage::RoomState(m) => Some(&m.channel_login),
            ServerMessage::UserNotice(m) => Some(&m.channel_login),
            ServerMessage::UserState(m) => Some(&m.channel_login),
            _ => None,
        }
    }
}
