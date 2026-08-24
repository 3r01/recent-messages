use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prometheus::{
    Histogram, HistogramVec, IntCounter, IntGauge, exponential_buckets, register_histogram,
    register_histogram_vec, register_int_counter, register_int_gauge,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{
    AsyncSqliteBlockStore, DurableIngest, IngestError, MessageStore, RawFirehoseSource,
    RawSourceBatch, StorageBudgetEnforcer, StorageBudgetError,
};

static QUEUE_DEPTH: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_ingest_queue_depth",
        "Raw source batches waiting for the durable ingest coordinator"
    )
    .unwrap()
});
static ACCEPTED_BATCHES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "recentmessages_ingest_accepted_batches_total",
        "Raw source batches accepted by the durable journal"
    )
    .unwrap()
});
static ACCEPTED_RECORDS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "recentmessages_ingest_accepted_records_total",
        "Records presented in durably accepted raw source batches"
    )
    .unwrap()
});
static ACCEPT_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "recentmessages_ingest_accept_seconds",
        "Time to journal and materialize one raw source batch"
    )
    .unwrap()
});
static CHECKPOINTS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "recentmessages_ingest_checkpoints_total",
        "Completed all-tail durable ingest checkpoints"
    )
    .unwrap()
});
static CHECKPOINT_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "recentmessages_ingest_checkpoint_seconds",
        "Total adaptive checkpoint maintenance-cycle duration"
    )
    .unwrap()
});
static CHECKPOINT_STAGE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "recentmessages_ingest_checkpoint_stage_seconds",
        "Adaptive checkpoint maintenance duration by bounded stage",
        &["stage"],
        exponential_buckets(0.001, 2.0, 17).unwrap()
    )
    .unwrap()
});
static LAST_ACCEPT_TIMESTAMP_SECONDS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_ingest_last_accept_timestamp_seconds",
        "Unix timestamp of the last batch durably accepted by ingest"
    )
    .unwrap()
});
static LAST_CHECKPOINT_TIMESTAMP_SECONDS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_ingest_last_checkpoint_timestamp_seconds",
        "Unix timestamp of the last completed durable ingest checkpoint"
    )
    .unwrap()
});
static STORE_CHANNELS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_store_channels",
        "Channels represented in the block store"
    )
    .unwrap()
});
static STORE_BLOCKS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_store_blocks",
        "Compressed blocks in the block store"
    )
    .unwrap()
});
static STORE_MESSAGES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_store_messages",
        "Canonical records retained in blocks"
    )
    .unwrap()
});
static STORE_COMPRESSED_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_store_compressed_payload_bytes",
        "Compressed block payload bytes"
    )
    .unwrap()
});
static STORE_UNCOMPRESSED_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_store_uncompressed_bytes",
        "Encoded bytes before block compression"
    )
    .unwrap()
});
static JOURNAL_BATCHES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_store_journal_batches",
        "Pending durable journal batches"
    )
    .unwrap()
});
static JOURNAL_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_store_journal_bytes",
        "Pending durable journal payload bytes"
    )
    .unwrap()
});
static EXPIRED_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "recentmessages_storage_expired_blocks_total",
        "Whole blocks removed after their newest event passed the retention TTL"
    )
    .unwrap()
});
static EXPIRED_MESSAGES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "recentmessages_storage_expired_messages_total",
        "Canonical records removed in TTL-expired blocks"
    )
    .unwrap()
});

#[derive(Clone, Copy, Debug)]
pub struct IngestRuntimeConfig {
    pub queue_batches: usize,
    pub seal_interval: Duration,
    pub checkpoint_interval: Duration,
    pub checkpoint_poll_interval: Duration,
    pub checkpoint_journal_bytes: u64,
    pub message_ttl: Duration,
}

#[derive(Debug, Error)]
pub enum IngestRuntimeError {
    #[error("ingest runtime requires sources and non-zero queue/timer bounds")]
    InvalidConfig,
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Budget(#[from] StorageBudgetError),
    #[error("raw source task failed: {0}")]
    SourceTask(String),
    #[error("all raw source tasks stopped unexpectedly")]
    AllSourcesStopped,
}

pub struct RawIngestRuntime {
    ingest: DurableIngest,
    sources: Vec<RawFirehoseSource>,
    external_input: Option<mpsc::Receiver<RawSourceBatch>>,
    config: IngestRuntimeConfig,
    budget: Option<StorageBudgetEnforcer>,
}

impl RawIngestRuntime {
    pub fn new(
        ingest: DurableIngest,
        sources: Vec<RawFirehoseSource>,
        config: IngestRuntimeConfig,
    ) -> Result<Self, IngestRuntimeError> {
        if sources.is_empty() || invalid_runtime_config(config) {
            return Err(IngestRuntimeError::InvalidConfig);
        }
        Ok(Self {
            ingest,
            sources,
            external_input: None,
            config,
            budget: None,
        })
    }

    pub fn new_with_external_input(
        ingest: DurableIngest,
        sources: Vec<RawFirehoseSource>,
        config: IngestRuntimeConfig,
        input: mpsc::Receiver<RawSourceBatch>,
    ) -> Result<Self, IngestRuntimeError> {
        if invalid_runtime_config(config) {
            return Err(IngestRuntimeError::InvalidConfig);
        }
        Ok(Self {
            ingest,
            sources,
            external_input: Some(input),
            config,
            budget: None,
        })
    }

    #[must_use]
    pub fn with_external_input(mut self, input: mpsc::Receiver<RawSourceBatch>) -> Self {
        self.external_input = Some(input);
        self
    }

    #[must_use]
    pub fn with_budget_enforcer(mut self, budget: StorageBudgetEnforcer) -> Self {
        self.budget = Some(budget);
        self
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<(), IngestRuntimeError> {
        let recovery_started = Instant::now();
        let recovered_batches = self.ingest.recover_pending(unix_millis()).await?;
        tracing::info!(
            recovered_batches,
            elapsed_ms = recovery_started.elapsed().as_millis(),
            "Recovered durable ingest journal"
        );
        self.record_store_metrics().await?;
        let (output, mut input) = mpsc::channel(self.config.queue_batches);
        let mut tasks = JoinSet::new();
        for source in std::mem::take(&mut self.sources) {
            tasks.spawn(source.run(output.clone(), cancellation.clone()));
        }
        drop(output);
        let mut source_open = true;
        let mut external_input = self.external_input.take();
        let mut external_open = external_input.is_some();

        let mut seal = tokio::time::interval(self.config.seal_interval);
        seal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut checkpoint_poll = tokio::time::interval(self.config.checkpoint_poll_interval);
        checkpoint_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        seal.tick().await;
        checkpoint_poll.tick().await;
        let mut last_checkpoint = Instant::now();

        loop {
            QUEUE_DEPTH.set(i64::try_from(input.len()).unwrap_or(i64::MAX));
            tokio::select! {
                () = cancellation.cancelled() => {
                    while !tasks.is_empty() {
                        tokio::select! {
                            batch = input.recv() => {
                                if let Some(batch) = batch {
                                    self.accept(batch).await?;
                                }
                            }
                            result = tasks.join_next() => {
                                if let Some(result) = result {
                                    result
                                        .map_err(|error| IngestRuntimeError::SourceTask(error.to_string()))?
                                        .map_err(|error| IngestRuntimeError::SourceTask(error.to_string()))?;
                                }
                            }
                        }
                    }
                    while let Ok(batch) = input.try_recv() {
                        self.accept(batch).await?;
                    }
                    if let Some(external) = external_input.as_mut() {
                        while let Some(batch) = external.recv().await {
                            self.accept(batch).await?;
                        }
                    }
                    let checkpoint_started = Instant::now();
                    let sealed_records = self.ingest.checkpoint().await?;
                    tracing::info!(
                        sealed_records,
                        elapsed_ms = checkpoint_started.elapsed().as_millis(),
                        "Completed shutdown ingest checkpoint"
                    );
                    QUEUE_DEPTH.set(0);
                    return Ok(());
                }
                batch = input.recv(), if source_open => match batch {
                    Some(batch) => self.accept(batch).await?,
                    None if !external_open => return Err(IngestRuntimeError::AllSourcesStopped),
                    None => source_open = false,
                },
                batch = async {
                    match external_input.as_mut() {
                        Some(input) => input.recv().await,
                        None => std::future::pending().await,
                    }
                }, if external_open => if let Some(batch) = batch {
                    self.accept(batch).await?;
                } else {
                    external_open = false;
                    if !source_open {
                        return Err(IngestRuntimeError::AllSourcesStopped);
                    }
                },
                _ = seal.tick() => {
                    self.ingest.seal_due(unix_millis()).await?;
                }
                _ = checkpoint_poll.tick() => {
                    let journal = self.ingest.journal_stats().await?;
                    record_journal_metrics(journal);
                    if !checkpoint_due(journal.bytes, last_checkpoint.elapsed(), self.config) {
                        continue;
                    }
                    let started = Instant::now();
                    let stage_started = Instant::now();
                    self.ingest.checkpoint().await?;
                    CHECKPOINT_STAGE_LATENCY
                        .with_label_values(&["seal_and_retire"])
                        .observe(stage_started.elapsed().as_secs_f64());
                    let stage_started = Instant::now();
                    self.expire_retention().await?;
                    CHECKPOINT_STAGE_LATENCY
                        .with_label_values(&["ttl_expiry"])
                        .observe(stage_started.elapsed().as_secs_f64());
                    let stage_started = Instant::now();
                    self.enforce_budget().await?;
                    CHECKPOINT_STAGE_LATENCY
                        .with_label_values(&["budget_enforcement"])
                        .observe(stage_started.elapsed().as_secs_f64());
                    let stage_started = Instant::now();
                    let metrics_store = self.ingest.store_handle();
                    tokio::spawn(async move {
                        if let Err(error) = record_store_metrics(&metrics_store).await {
                            tracing::warn!(%error, "Failed to refresh aggregate store metrics");
                        }
                        CHECKPOINT_STAGE_LATENCY
                            .with_label_values(&["metrics_refresh"])
                            .observe(stage_started.elapsed().as_secs_f64());
                    });
                    CHECKPOINT_LATENCY.observe(started.elapsed().as_secs_f64());
                    CHECKPOINTS.inc();
                    LAST_CHECKPOINT_TIMESTAMP_SECONDS.set(unix_seconds());
                    last_checkpoint = Instant::now();
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(result) = result {
                        result
                            .map_err(|error| IngestRuntimeError::SourceTask(error.to_string()))?
                            .map_err(|error| IngestRuntimeError::SourceTask(error.to_string()))?;
                    }
                }
            }
        }
    }

    async fn accept(&mut self, batch: RawSourceBatch) -> Result<(), IngestRuntimeError> {
        let records = batch.records.len();
        let started = Instant::now();
        self.ingest
            .accept_source_batch(batch, unix_millis())
            .await?;
        ACCEPT_LATENCY.observe(started.elapsed().as_secs_f64());
        ACCEPTED_BATCHES.inc();
        ACCEPTED_RECORDS.inc_by(u64::try_from(records).unwrap_or(u64::MAX));
        LAST_ACCEPT_TIMESTAMP_SECONDS.set(unix_seconds());
        Ok(())
    }

    async fn record_store_metrics(&self) -> Result<(), IngestRuntimeError> {
        record_store_metrics(&self.ingest.store_handle()).await
    }

    async fn enforce_budget(&self) -> Result<(), IngestRuntimeError> {
        if let Some(budget) = &self.budget {
            budget.enforce().await?;
        }
        Ok(())
    }

    async fn expire_retention(&self) -> Result<(), IngestRuntimeError> {
        let cutoff = unix_millis()
            .saturating_sub(u64::try_from(self.config.message_ttl.as_millis()).unwrap_or(u64::MAX));
        let expired = self
            .ingest
            .expire_blocks_before(i64::try_from(cutoff).unwrap_or(i64::MAX))
            .await?;
        EXPIRED_BLOCKS.inc_by(u64::try_from(expired.blocks).unwrap_or(u64::MAX));
        EXPIRED_MESSAGES.inc_by(u64::try_from(expired.messages).unwrap_or(u64::MAX));
        Ok(())
    }
}

async fn record_store_metrics(store: &AsyncSqliteBlockStore) -> Result<(), IngestRuntimeError> {
    let stats = store.stats().await.map_err(IngestError::from)?;
    let journal = store.journal_stats().await.map_err(IngestError::from)?;
    STORE_CHANNELS.set(i64::try_from(stats.channels).unwrap_or(i64::MAX));
    STORE_BLOCKS.set(i64::try_from(stats.blocks).unwrap_or(i64::MAX));
    STORE_MESSAGES.set(i64::try_from(stats.messages).unwrap_or(i64::MAX));
    STORE_COMPRESSED_BYTES.set(i64::try_from(stats.compressed_bytes).unwrap_or(i64::MAX));
    STORE_UNCOMPRESSED_BYTES.set(i64::try_from(stats.uncompressed_bytes).unwrap_or(i64::MAX));
    record_journal_metrics(journal);
    Ok(())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn unix_seconds() -> i64 {
    i64::try_from(unix_millis() / 1_000).unwrap_or(i64::MAX)
}

#[must_use]
pub fn ingest_is_fresh(now_seconds: i64, max_age: Duration, max_queue_batches: usize) -> bool {
    let last_accept = LAST_ACCEPT_TIMESTAMP_SECONDS.get();
    last_accept > 0
        && now_seconds.saturating_sub(last_accept)
            <= i64::try_from(max_age.as_secs()).unwrap_or(i64::MAX)
        && usize::try_from(QUEUE_DEPTH.get()).unwrap_or(usize::MAX) <= max_queue_batches
}

fn invalid_runtime_config(config: IngestRuntimeConfig) -> bool {
    config.queue_batches == 0
        || config.seal_interval.is_zero()
        || config.checkpoint_interval.is_zero()
        || config.checkpoint_poll_interval.is_zero()
        || config.checkpoint_journal_bytes == 0
        || config.message_ttl.is_zero()
}

fn checkpoint_due(journal_bytes: u64, elapsed: Duration, config: IngestRuntimeConfig) -> bool {
    journal_bytes >= config.checkpoint_journal_bytes || elapsed >= config.checkpoint_interval
}

fn record_journal_metrics(journal: super::JournalStats) {
    JOURNAL_BATCHES.set(i64::try_from(journal.batches).unwrap_or(i64::MAX));
    JOURNAL_BYTES.set(i64::try_from(journal.bytes).unwrap_or(i64::MAX));
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::SinkExt;
    use tokio::net::TcpListener;
    use tokio::process::Command;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::storage::{
        AsyncSqliteBlockStore, DirectIrcBatcher, MessageStore, OpenBlockLimits, QueryRequest,
        RawFirehoseConfig, SourceFidelity,
    };

    #[test]
    fn adaptive_checkpoint_uses_byte_or_age_bound() {
        let config = IngestRuntimeConfig {
            queue_batches: 1,
            seal_interval: Duration::from_secs(1),
            checkpoint_interval: Duration::from_mins(10),
            checkpoint_poll_interval: Duration::from_secs(30),
            checkpoint_journal_bytes: 256 * 1024 * 1024,
            message_ttl: Duration::from_hours(24),
        };
        assert!(!checkpoint_due(
            config.checkpoint_journal_bytes - 1,
            config
                .checkpoint_interval
                .checked_sub(Duration::from_millis(1))
                .unwrap(),
            config,
        ));
        assert!(checkpoint_due(
            config.checkpoint_journal_bytes,
            Duration::ZERO,
            config,
        ));
        assert!(checkpoint_due(0, config.checkpoint_interval, config));
    }

    #[tokio::test]
    async fn drains_owned_irc_input_into_the_shared_store() {
        let path = std::env::temp_dir().join(format!(
            "recent-messages-owned-irc-runtime-{}.sqlite",
            unix_millis()
        ));
        let store = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
        let ingest = DurableIngest::new(store.clone(), test_limits()).unwrap();
        let (output, input) = mpsc::channel(2);
        let mut batcher = DirectIrcBatcher::new("owned-irc", "startup-1", 10).unwrap();
        batcher
            .push_raw(
                "@id=direct;tmi-sent-ts=1 :u!u@u.tmi.twitch.tv PRIVMSG #channel :hello",
                2,
            )
            .unwrap();
        output.send(batcher.flush().unwrap()).await.unwrap();
        drop(output);

        let runtime = RawIngestRuntime::new_with_external_input(
            ingest,
            Vec::new(),
            IngestRuntimeConfig {
                queue_batches: 2,
                seal_interval: Duration::from_secs(1),
                checkpoint_interval: Duration::from_secs(1),
                checkpoint_poll_interval: Duration::from_millis(100),
                checkpoint_journal_bytes: 1024 * 1024,
                message_ttl: Duration::MAX,
            },
            input,
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        runtime.run(cancellation).await.unwrap();

        let result = store
            .query(QueryRequest {
                channel_key: "channel".to_owned(),
                after_ms: None,
                before_ms: None,
                limit: 800,
            })
            .await
            .unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].fidelity, SourceFidelity::DirectIrc);
        assert!(store.pending_journal_batches().await.unwrap().is_empty());
        drop(store);
        cleanup(&path);
    }

    #[tokio::test]
    #[ignore = "connects to a public live service"]
    async fn live_receiver_journals_seals_and_shuts_down_cleanly() {
        let path = std::env::temp_dir().join(format!(
            "recent-messages-live-runtime-{}.sqlite",
            unix_millis()
        ));
        let store = AsyncSqliteBlockStore::open(&path, 100, 800, 16, 2).unwrap();
        let ingest = DurableIngest::new(
            store.clone(),
            OpenBlockLimits {
                block_messages: 100,
                max_open_channels: 2_000,
                max_open_bytes: 16 * 1024 * 1024,
                idle_seal_after_ms: 500,
                max_open_age_ms: 1_000,
            },
        )
        .unwrap();
        let sources = [
            ("spanix", "wss://logs.spanix.team/firehose"),
            ("supa", "wss://logs.supa.codes/firehose"),
            ("susgee", "wss://logs.susgee.dev/firehose"),
            ("logxx", "wss://logxx.dev/firehose"),
        ]
        .into_iter()
        .map(|(source_id, url)| {
            RawFirehoseSource::new(RawFirehoseConfig {
                source_id: format!("{source_id}-live-test"),
                url: url.to_owned(),
                origin: "https://tv.supa.sh".to_owned(),
                batch_messages: 64,
                batch_max_delay: Duration::from_millis(200),
                reconnect_min_delay: Duration::from_millis(100),
                reconnect_max_delay: Duration::from_secs(1),
            })
            .unwrap()
        })
        .collect();
        let runtime = RawIngestRuntime::new(
            ingest,
            sources,
            IngestRuntimeConfig {
                queue_batches: 8,
                seal_interval: Duration::from_millis(200),
                checkpoint_interval: Duration::from_secs(1),
                checkpoint_poll_interval: Duration::from_millis(100),
                checkpoint_journal_bytes: 1024 * 1024,
                message_ttl: Duration::MAX,
            },
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            cancel.cancel();
        });

        tokio::time::timeout(Duration::from_secs(20), runtime.run(cancellation))
            .await
            .expect("runtime shutdown timed out")
            .unwrap();
        assert!(store.stats().await.unwrap().messages > 0);
        assert!(store.pending_journal_batches().await.unwrap().is_empty());
        drop(store);
        cleanup(&path);
    }

    #[tokio::test]
    #[ignore = "spawns a child process and binds a loopback WebSocket server"]
    async fn acknowledged_runtime_batch_survives_process_kill() {
        if let (Ok(path), Ok(url)) = (
            std::env::var("RM_RUNTIME_CRASH_CHILD_PATH"),
            std::env::var("RM_RUNTIME_CRASH_CHILD_URL"),
        ) {
            run_crash_child(Path::new(&path), &url).await;
        }

        let path = std::env::temp_dir().join(format!(
            "recent-messages-runtime-crash-{}.sqlite",
            unix_millis()
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("storage::ingest_runtime::tests::acknowledged_runtime_batch_survives_process_kill")
            .arg("--ignored")
            .arg("--exact")
            .env("RM_RUNTIME_CRASH_CHILD_PATH", &path)
            .env("RM_RUNTIME_CRASH_CHILD_URL", &url)
            .spawn()
            .unwrap();
        let (connection, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = accept_async(connection).await.unwrap();
        socket
            .send(Message::Text(
                "@id=crash;tmi-sent-ts=1 :u!u@u.tmi.twitch.tv PRIVMSG #channel :hello".into(),
            ))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if pending_journal_rows(&path) > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child did not durably acknowledge a batch");
        child.kill().await.unwrap();
        let _ = child.wait().await;
        drop(socket);

        let store = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
        let mut ingest = DurableIngest::new(store.clone(), test_limits()).unwrap();
        assert_eq!(ingest.recover_pending(unix_millis()).await.unwrap(), 1);
        assert_eq!(ingest.checkpoint().await.unwrap(), 1);
        assert!(store.pending_journal_batches().await.unwrap().is_empty());
        let result = store
            .query(crate::storage::QueryRequest {
                channel_key: "channel".to_owned(),
                after_ms: None,
                before_ms: None,
                limit: 800,
            })
            .await
            .unwrap();
        assert_eq!(result.records.len(), 1);
        drop(ingest);
        drop(store);
        cleanup(&path);
    }

    async fn run_crash_child(path: &Path, url: &str) -> ! {
        let store = AsyncSqliteBlockStore::open(path, 100, 800, 8, 1).unwrap();
        let ingest = DurableIngest::new(store, test_limits()).unwrap();
        let source = RawFirehoseSource::new(RawFirehoseConfig {
            source_id: "runtime-crash".to_owned(),
            url: url.to_owned(),
            origin: "https://localhost.invalid".to_owned(),
            batch_messages: 1,
            batch_max_delay: Duration::from_secs(1),
            reconnect_min_delay: Duration::from_millis(10),
            reconnect_max_delay: Duration::from_millis(100),
        })
        .unwrap();
        RawIngestRuntime::new(
            ingest,
            vec![source],
            IngestRuntimeConfig {
                queue_batches: 2,
                seal_interval: Duration::from_mins(1),
                checkpoint_interval: Duration::from_mins(1),
                checkpoint_poll_interval: Duration::from_secs(1),
                checkpoint_journal_bytes: 1024 * 1024,
                message_ttl: Duration::MAX,
            },
        )
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();
        unreachable!()
    }

    fn pending_journal_rows(path: &Path) -> i64 {
        let Ok(connection) = rusqlite::Connection::open(path) else {
            return 0;
        };
        connection
            .query_row("SELECT COUNT(*) FROM ingest_journal", [], |row| row.get(0))
            .unwrap_or(0)
    }

    fn test_limits() -> OpenBlockLimits {
        OpenBlockLimits {
            block_messages: 100,
            max_open_channels: 128,
            max_open_bytes: 1024 * 1024,
            idle_seal_after_ms: 30_000,
            max_open_age_ms: 60_000,
        }
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
