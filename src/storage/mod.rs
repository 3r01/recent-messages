mod block;
mod budget;
mod direct_irc;
mod eviction;
mod ingest;
mod ingest_runtime;
mod memory;
mod open_blocks;
mod raw_firehose;
mod raw_irc;
mod segment;
mod sqlite;
mod sqlite_async;

use std::sync::LazyLock;

use prometheus::{IntCounterVec, register_int_counter_vec};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type EventKey = [u8; 32];

static FIDELITY_UPGRADES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_fidelity_upgrades_total",
        "Higher-fidelity duplicate representations retained by storage location",
        &["location"]
    )
    .unwrap()
});

fn record_fidelity_upgrade(location: &str) {
    FIDELITY_UPGRADES.with_label_values(&[location]).inc();
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFidelity {
    #[default]
    Unknown,
    Reconstructed,
    DirectIrc,
}

pub use block::{BlockError, EncodedBlock};
pub use budget::{
    BudgetEnforcement, FilesystemSpace, StorageBudget, StorageBudgetEnforcer, StorageBudgetError,
    effective_max_bytes, filesystem_space,
};
pub use direct_irc::{DirectIrcBatcher, DirectIrcBatcherError};
pub use eviction::{EvictionError, PressureMode};
pub use ingest::{DurableIngest, IngestError};
pub use ingest_runtime::{
    IngestRuntimeConfig, IngestRuntimeError, RawIngestRuntime, ingest_is_fresh,
};
pub use open_blocks::{
    OpenBlockError, OpenBlockLimits, OpenBlockManager, OpenBlockStats, SharedOpenBlocks,
    SharedOpenBlocksHandle,
};
pub use raw_firehose::{
    RawFirehoseConfig, RawFirehoseError, RawFirehoseSource, RawSourceBatch, source_is_fresh,
};
pub use raw_irc::{RawIrcError, canonicalize_raw_irc, canonicalize_raw_irc_from};
pub use segment::SegmentBlockStore;
pub use sqlite::SqliteBlockStore;
pub use sqlite_async::AsyncSqliteBlockStore;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalRecord {
    pub channel_key: String,
    pub event_at_ms: i64,
    pub received_at_ms: i64,
    pub event_key: EventKey,
    #[serde(default)]
    pub source_id: String,
    #[serde(default)]
    pub fidelity: SourceFidelity,
    pub raw_irc: Vec<u8>,
}

impl CanonicalRecord {
    pub fn derive_event_key(channel_key: &str, raw_irc: &[u8]) -> EventKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(channel_key.as_bytes());
        hasher.update(&[0]);
        hasher.update(raw_irc);
        *hasher.finalize().as_bytes()
    }

    #[must_use]
    pub fn should_replace(&self, retained: &Self) -> bool {
        self.event_key == retained.event_key && self.fidelity > retained.fidelity
    }
}

#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub channel_key: String,
    pub after_ms: Option<i64>,
    pub before_ms: Option<i64>,
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalResult {
    pub records: Vec<CanonicalRecord>,
    pub oldest_retained_at_ms: Option<i64>,
    pub newest_retained_at_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreStats {
    pub channels: usize,
    pub blocks: usize,
    pub messages: usize,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JournalStats {
    pub batches: usize,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExpireStats {
    pub blocks: usize,
    pub messages: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalBatch {
    pub source_id: String,
    pub stream_id: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub checksum: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("block codec error: {0}")]
    Block(#[from] block::BlockError),
    #[error("store lock poisoned")]
    LockPoisoned,
    #[error("batch contains records for multiple channels")]
    MixedChannelBatch,
    #[error("query limit must be greater than zero")]
    InvalidLimit,
    #[error("storage backend error: {0}")]
    Backend(String),
}

#[allow(async_fn_in_trait)]
pub trait MessageStore: Send + Sync {
    async fn append_batch(&self, records: Vec<CanonicalRecord>) -> Result<(), StoreError>;
    async fn query(&self, request: QueryRequest) -> Result<LocalResult, StoreError>;
    async fn stats(&self) -> Result<StoreStats, StoreError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct LegacyRecord<'a> {
        channel_key: &'a str,
        event_at_ms: i64,
        received_at_ms: i64,
        event_key: EventKey,
        raw_irc: &'a [u8],
    }

    #[test]
    fn legacy_records_default_to_unknown_fidelity() {
        let encoded = rmp_serde::to_vec_named(&LegacyRecord {
            channel_key: "channel",
            event_at_ms: 1,
            received_at_ms: 2,
            event_key: [3; 32],
            raw_irc: b"PRIVMSG #channel :hello",
        })
        .unwrap();
        let decoded: CanonicalRecord = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.source_id, "");
        assert_eq!(decoded.fidelity, SourceFidelity::Unknown);
    }
}
