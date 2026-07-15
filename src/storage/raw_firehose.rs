use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{SinkExt, StreamExt};
use prometheus::{
    HistogramVec, IntCounterVec, IntGaugeVec, register_histogram_vec, register_int_counter_vec,
    register_int_gauge_vec,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{CanonicalRecord, RawIrcError, SourceFidelity, canonicalize_raw_irc_from};

static CONNECTION_ATTEMPTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_raw_firehose_connection_attempts_total",
        "Raw firehose WebSocket connection attempts",
        &["source"]
    )
    .unwrap()
});
static RECONNECTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_raw_firehose_reconnects_total",
        "Raw firehose reconnect waits entered after a connection ended",
        &["source"]
    )
    .unwrap()
});
static CONNECTED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "recentmessages_raw_firehose_connected",
        "Whether a raw firehose is currently connected",
        &["source"]
    )
    .unwrap()
});
static EVENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_raw_firehose_events_total",
        "Raw firehose lines by parsing outcome",
        &["source", "outcome"]
    )
    .unwrap()
});
static LAST_EVENT_TIMESTAMP_SECONDS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "recentmessages_raw_firehose_last_event_timestamp_seconds",
        "Unix timestamp of the last non-empty line received from each raw firehose",
        &["source"]
    )
    .unwrap()
});
static BATCHES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_raw_firehose_batches_total",
        "Batches submitted by each raw firehose",
        &["source"]
    )
    .unwrap()
});
static OUTPUT_WAIT: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "recentmessages_raw_firehose_output_wait_seconds",
        "Time a raw source waits for bounded ingest queue capacity",
        &["source"]
    )
    .unwrap()
});

#[derive(Clone, Debug)]
pub struct RawFirehoseConfig {
    pub source_id: String,
    pub url: String,
    pub origin: String,
    pub batch_messages: usize,
    pub batch_max_delay: Duration,
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
}

#[derive(Clone, Debug)]
pub struct RawSourceBatch {
    pub source_id: String,
    pub stream_id: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub records: Vec<CanonicalRecord>,
}

#[derive(Debug, Error)]
pub enum RawFirehoseError {
    #[error("invalid raw firehose configuration: {0}")]
    InvalidConfig(String),
    #[error("raw firehose connection error: {0}")]
    Connection(String),
    #[error("raw firehose output queue closed")]
    OutputClosed,
}

pub struct RawFirehoseSource {
    config: RawFirehoseConfig,
}

impl RawFirehoseSource {
    pub fn new(config: RawFirehoseConfig) -> Result<Self, RawFirehoseError> {
        if config.source_id.is_empty()
            || config.url.is_empty()
            || config.batch_messages == 0
            || config.batch_max_delay.is_zero()
            || config.reconnect_min_delay.is_zero()
            || config.reconnect_max_delay < config.reconnect_min_delay
        {
            return Err(RawFirehoseError::InvalidConfig(
                "IDs, URL, batch bounds, and reconnect bounds must be valid".to_owned(),
            ));
        }
        Ok(Self { config })
    }

    pub async fn run(
        self,
        output: mpsc::Sender<RawSourceBatch>,
        cancellation: CancellationToken,
    ) -> Result<(), RawFirehoseError> {
        let mut generation = 0_u64;
        let mut backoff = self.config.reconnect_min_delay;
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            generation = generation.saturating_add(1);
            CONNECTION_ATTEMPTS
                .with_label_values(&[&self.config.source_id])
                .inc();
            let started = Instant::now();
            match self
                .run_connection(generation, &output, &cancellation)
                .await
            {
                Ok(()) if cancellation.is_cancelled() => return Ok(()),
                Ok(()) => warn!(source = %self.config.source_id, "raw firehose connection closed"),
                Err(RawFirehoseError::OutputClosed) => {
                    return Err(RawFirehoseError::OutputClosed);
                }
                Err(error) => {
                    warn!(source = %self.config.source_id, %error, "raw firehose connection failed");
                }
            }
            RECONNECTS
                .with_label_values(&[&self.config.source_id])
                .inc();
            if started.elapsed() >= Duration::from_secs(30) {
                backoff = self.config.reconnect_min_delay;
            } else {
                backoff = backoff
                    .saturating_mul(2)
                    .min(self.config.reconnect_max_delay);
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(backoff) => {}
            }
        }
    }

    async fn run_connection(
        &self,
        generation: u64,
        output: &mpsc::Sender<RawSourceBatch>,
        cancellation: &CancellationToken,
    ) -> Result<(), RawFirehoseError> {
        let mut request = self
            .config
            .url
            .as_str()
            .into_client_request()
            .map_err(|error| RawFirehoseError::InvalidConfig(error.to_string()))?;
        let origin = HeaderValue::from_str(&self.config.origin)
            .map_err(|error| RawFirehoseError::InvalidConfig(error.to_string()))?;
        request.headers_mut().insert("Origin", origin);
        request.headers_mut().insert(
            "User-Agent",
            HeaderValue::from_static("recent-messages-raw-ingest/0.1"),
        );
        let (mut socket, _) = connect_async(request)
            .await
            .map_err(|error| RawFirehoseError::Connection(error.to_string()))?;
        info!(source = %self.config.source_id, generation, "raw firehose connected");
        CONNECTED
            .with_label_values(&[&self.config.source_id])
            .set(1);
        LAST_EVENT_TIMESTAMP_SECONDS
            .with_label_values(&[&self.config.source_id])
            .set(0);
        let _connected = ConnectedGuard(&self.config.source_id);
        let connected_at = unix_millis();
        let stream_id = format!("{}-{connected_at}-{generation}", self.config.source_id);
        let mut batcher = RawBatcher::new(
            self.config.source_id.clone(),
            stream_id,
            self.config.batch_messages,
        );
        let mut flush = tokio::time::interval(self.config.batch_max_delay);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => {
                    send_if_some(output, batcher.flush()).await?;
                    let _ = socket.close(None).await;
                    return Ok(());
                }
                _ = flush.tick() => send_if_some(output, batcher.flush()).await?,
                incoming = socket.next() => match incoming {
                    Some(Ok(Message::Text(text))) => {
                        accept_lines(text.as_str(), &mut batcher, output).await?;
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Ok(text) = std::str::from_utf8(&bytes) {
                            accept_lines(text, &mut batcher, output).await?;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            send_if_some(output, batcher.flush()).await?;
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Close(_)) | Err(_)) | None => {
                        send_if_some(output, batcher.flush()).await?;
                        return Ok(());
                    }
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

struct RawBatcher {
    source_id: String,
    stream_id: String,
    batch_messages: usize,
    next_sequence: u64,
    first_sequence: u64,
    records: Vec<CanonicalRecord>,
}

impl RawBatcher {
    fn new(source_id: String, stream_id: String, batch_messages: usize) -> Self {
        Self {
            source_id,
            stream_id,
            batch_messages,
            next_sequence: 0,
            first_sequence: 0,
            records: Vec::with_capacity(batch_messages),
        }
    }

    fn push(&mut self, record: CanonicalRecord) -> Option<RawSourceBatch> {
        if self.records.is_empty() {
            self.first_sequence = self.next_sequence;
        }
        self.records.push(record);
        self.next_sequence = self.next_sequence.saturating_add(1);
        (self.records.len() >= self.batch_messages).then(|| self.take_batch())
    }

    fn flush(&mut self) -> Option<RawSourceBatch> {
        (!self.records.is_empty()).then(|| self.take_batch())
    }

    fn take_batch(&mut self) -> RawSourceBatch {
        let records = std::mem::replace(&mut self.records, Vec::with_capacity(self.batch_messages));
        RawSourceBatch {
            source_id: self.source_id.clone(),
            stream_id: self.stream_id.clone(),
            first_sequence: self.first_sequence,
            last_sequence: self.next_sequence.saturating_sub(1),
            records,
        }
    }
}

async fn accept_lines(
    text: &str,
    batcher: &mut RawBatcher,
    output: &mpsc::Sender<RawSourceBatch>,
) -> Result<(), RawFirehoseError> {
    let now_ms = i64::try_from(unix_millis()).unwrap_or(i64::MAX);
    for line in text
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
    {
        LAST_EVENT_TIMESTAMP_SECONDS
            .with_label_values(&[&batcher.source_id])
            .set(i64::try_from(unix_millis() / 1_000).unwrap_or(i64::MAX));
        match canonicalize_raw_irc_from(
            line,
            now_ms,
            &batcher.source_id,
            SourceFidelity::Reconstructed,
        ) {
            Ok(record) => {
                EVENTS
                    .with_label_values(&[&batcher.source_id, "accepted"])
                    .inc();
                send_if_some(output, batcher.push(record)).await?;
            }
            Err(RawIrcError::UnsupportedCommand(_)) => EVENTS
                .with_label_values(&[&batcher.source_id, "unsupported"])
                .inc(),
            Err(RawIrcError::MissingIdentity { .. }) => EVENTS
                .with_label_values(&[&batcher.source_id, "missing_identity"])
                .inc(),
            Err(_) => EVENTS
                .with_label_values(&[&batcher.source_id, "invalid"])
                .inc(),
        }
    }
    Ok(())
}

async fn send_if_some(
    output: &mpsc::Sender<RawSourceBatch>,
    batch: Option<RawSourceBatch>,
) -> Result<(), RawFirehoseError> {
    if let Some(batch) = batch {
        let source = batch.source_id.clone();
        let started = Instant::now();
        output
            .send(batch)
            .await
            .map_err(|_| RawFirehoseError::OutputClosed)?;
        OUTPUT_WAIT
            .with_label_values(&[&source])
            .observe(started.elapsed().as_secs_f64());
        BATCHES.with_label_values(&[&source]).inc();
    }
    Ok(())
}

struct ConnectedGuard<'a>(&'a str);

impl Drop for ConnectedGuard<'_> {
    fn drop(&mut self) {
        CONNECTED.with_label_values(&[self.0]).set(0);
    }
}

#[must_use]
pub fn source_is_fresh(source_id: &str, now_seconds: i64, max_age: Duration) -> bool {
    let Ok(connected) = CONNECTED.get_metric_with_label_values(&[source_id]) else {
        return false;
    };
    let Ok(last_event) = LAST_EVENT_TIMESTAMP_SECONDS.get_metric_with_label_values(&[source_id])
    else {
        return false;
    };
    connected.get() == 1
        && last_event.get() > 0
        && now_seconds.saturating_sub(last_event.get())
            <= i64::try_from(max_age.as_secs()).unwrap_or(i64::MAX)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    use super::*;

    fn record(sequence: usize) -> CanonicalRecord {
        canonicalize_raw_irc_from(
            &format!(
                "@id={sequence};tmi-sent-ts={sequence} :u!u@u.tmi.twitch.tv PRIVMSG #channel :hello"
            ),
            sequence as i64,
            "test-firehose",
            SourceFidelity::Reconstructed,
        )
        .unwrap()
    }

    #[test]
    fn batches_have_contiguous_local_sequences() {
        let mut batcher = RawBatcher::new("source".to_owned(), "stream".to_owned(), 2);
        assert!(batcher.push(record(0)).is_none());
        let first = batcher.push(record(1)).unwrap();
        assert_eq!((first.first_sequence, first.last_sequence), (0, 1));
        assert_eq!(first.records.len(), 2);
        batcher.push(record(2));
        let second = batcher.flush().unwrap();
        assert_eq!((second.first_sequence, second.last_sequence), (2, 2));
    }

    #[test]
    fn validates_limits() {
        let error = RawFirehoseSource::new(RawFirehoseConfig {
            source_id: String::new(),
            url: String::new(),
            origin: String::new(),
            batch_messages: 0,
            batch_max_delay: Duration::ZERO,
            reconnect_min_delay: Duration::ZERO,
            reconnect_max_delay: Duration::ZERO,
        });
        assert!(matches!(error, Err(RawFirehoseError::InvalidConfig(_))));
    }

    #[tokio::test]
    #[ignore = "requires binding a loopback WebSocket server"]
    async fn reconnects_with_a_new_stream_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for generation in 0..2 {
                let (connection, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(connection).await.unwrap();
                let line = format!(
                    "@id={generation};tmi-sent-ts={generation} :u!u@u.tmi.twitch.tv PRIVMSG #channel :hello"
                );
                socket.send(Message::Text(line.into())).await.unwrap();
                socket.close(None).await.unwrap();
            }
        });
        let source = RawFirehoseSource::new(RawFirehoseConfig {
            source_id: "reconnect-test".to_owned(),
            url: format!("ws://{address}"),
            origin: "https://localhost.invalid".to_owned(),
            batch_messages: 1,
            batch_max_delay: Duration::from_secs(1),
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(20),
        })
        .unwrap();
        let cancellation = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel(2);
        let task = tokio::spawn(source.run(tx, cancellation.clone()));

        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.stream_id, second.stream_id);
        assert_eq!((first.first_sequence, first.last_sequence), (0, 0));
        assert_eq!((second.first_sequence, second.last_sequence), (0, 0));

        cancellation.cancel();
        task.await.unwrap().unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "connects to a public live service"]
    async fn receives_raw_spanix_batch() {
        let source = RawFirehoseSource::new(RawFirehoseConfig {
            source_id: "spanix".to_owned(),
            url: "wss://logs.spanix.team/firehose".to_owned(),
            origin: "https://tv.supa.sh".to_owned(),
            batch_messages: 8,
            batch_max_delay: Duration::from_secs(2),
            reconnect_min_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_secs(1),
        })
        .unwrap();
        let cancellation = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel(2);
        let task = tokio::spawn(source.run(tx, cancellation.clone()));
        let batch = tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("raw firehose timed out")
            .expect("raw firehose closed its output");
        assert_eq!(batch.source_id, "spanix");
        assert!(!batch.stream_id.is_empty());
        assert_eq!(
            batch.last_sequence - batch.first_sequence + 1,
            batch.records.len() as u64
        );
        assert!(!batch.records.is_empty());
        assert!(
            batch
                .records
                .iter()
                .all(|record| !record.raw_irc.is_empty())
        );
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }
}
