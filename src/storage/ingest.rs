use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    AsyncSqliteBlockStore, CanonicalRecord, ExpireStats, JournalStats, MessageStore,
    OpenBlockError, OpenBlockLimits, RawSourceBatch, SharedOpenBlocks, SharedOpenBlocksHandle,
    StoreError, StoreStats,
};

#[derive(Debug, Error)]
pub enum IngestError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    OpenBlock(#[from] OpenBlockError),
    #[error("journal payload codec error: {0}")]
    Codec(String),
    #[error("journal payload checksum does not match")]
    Checksum,
    #[error("raw source batch has an invalid sequence range")]
    InvalidSourceBatch,
}

pub struct DurableIngest {
    store: AsyncSqliteBlockStore,
    open: SharedOpenBlocksHandle,
    materialized_through: HashMap<(String, String), u64>,
}

#[derive(Serialize)]
struct JournalEnvelopeRef<'a> {
    format_version: u16,
    records: &'a [CanonicalRecord],
}

#[derive(Deserialize)]
struct JournalEnvelope {
    format_version: u16,
    records: Vec<CanonicalRecord>,
}

const JOURNAL_FORMAT_VERSION: u16 = 1;
const JOURNAL_COMPRESSION_LEVEL: i32 = 1;
const RECOVERY_APPEND_BATCHES: usize = 1024;

impl DurableIngest {
    pub fn new(store: AsyncSqliteBlockStore, limits: OpenBlockLimits) -> Result<Self, IngestError> {
        Ok(Self {
            store,
            open: Arc::new(SharedOpenBlocks::new(limits)?),
            materialized_through: HashMap::new(),
        })
    }

    pub fn open_blocks_handle(&self) -> SharedOpenBlocksHandle {
        self.open.clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn accept_batch(
        &mut self,
        source_id: &str,
        stream_id: &str,
        first_sequence: u64,
        last_sequence: u64,
        records: Vec<CanonicalRecord>,
        now_ms: u64,
    ) -> Result<u64, IngestError> {
        let payload = encode_journal(&records)?;
        let checksum = *blake3::hash(&payload).as_bytes();
        let acknowledged = self
            .store
            .accept_journal_batch(
                source_id.to_owned(),
                stream_id.to_owned(),
                first_sequence,
                last_sequence,
                checksum,
                payload,
                now_ms,
            )
            .await?;
        self.materialize(source_id, stream_id, last_sequence, records, now_ms)
            .await?;
        Ok(acknowledged)
    }

    pub async fn accept_source_batch(
        &mut self,
        batch: RawSourceBatch,
        now_ms: u64,
    ) -> Result<u64, IngestError> {
        let expected_records = batch
            .last_sequence
            .checked_sub(batch.first_sequence)
            .and_then(|difference| difference.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or(IngestError::InvalidSourceBatch)?;
        if batch.records.is_empty() || batch.records.len() != expected_records {
            return Err(IngestError::InvalidSourceBatch);
        }
        self.accept_batch(
            &batch.source_id,
            &batch.stream_id,
            batch.first_sequence,
            batch.last_sequence,
            batch.records,
            now_ms,
        )
        .await
    }

    pub async fn recover_pending(&mut self, now_ms: u64) -> Result<usize, IngestError> {
        let pending = self.store.pending_journal_batches().await?;
        let count = pending.len();
        let mut sealed = Vec::new();
        for batch in pending {
            if *blake3::hash(&batch.payload).as_bytes() != batch.checksum {
                return Err(IngestError::Checksum);
            }
            let records = decode_journal(&batch.payload)?;
            for record in records {
                sealed.extend(self.open.append(record, now_ms)?);
                if sealed.len() >= RECOVERY_APPEND_BATCHES {
                    self.store
                        .append_channel_batches(std::mem::take(&mut sealed))
                        .await?;
                }
            }
            self.materialized_through
                .entry((batch.source_id, batch.stream_id))
                .and_modify(|through| *through = (*through).max(batch.last_sequence))
                .or_insert(batch.last_sequence);
        }
        if !sealed.is_empty() {
            self.store.append_channel_batches(sealed).await?;
        }
        Ok(count)
    }

    pub async fn seal_due(&mut self, now_ms: u64) -> Result<usize, IngestError> {
        let sealed = self.open.seal_due(now_ms)?;
        let count = sealed.iter().map(Vec::len).sum();
        if !sealed.is_empty() {
            self.store.append_channel_batches(sealed).await?;
        }
        Ok(count)
    }

    pub async fn store_stats(&self) -> Result<StoreStats, IngestError> {
        Ok(self.store.stats().await?)
    }

    pub(crate) fn store_handle(&self) -> AsyncSqliteBlockStore {
        self.store.clone()
    }

    pub async fn journal_stats(&self) -> Result<JournalStats, IngestError> {
        Ok(self.store.journal_stats().await?)
    }

    pub async fn expire_blocks_before(&self, cutoff_ms: i64) -> Result<ExpireStats, IngestError> {
        Ok(self.store.expire_blocks_before(cutoff_ms).await?)
    }

    pub async fn checkpoint(&mut self) -> Result<usize, IngestError> {
        let sealed = self.open.seal_all()?;
        let count = sealed.iter().map(Vec::len).sum();
        if !sealed.is_empty() {
            self.store.append_channel_batches(sealed).await?;
        }
        let retirements = self
            .materialized_through
            .iter()
            .map(|(key, through)| (key.clone(), *through))
            .collect::<Vec<_>>();
        for ((source_id, stream_id), processed_through) in retirements {
            self.store
                .retire_journal_through(source_id.clone(), stream_id.clone(), processed_through)
                .await?;
            self.materialized_through.remove(&(source_id, stream_id));
        }
        Ok(count)
    }

    async fn materialize(
        &mut self,
        source_id: &str,
        stream_id: &str,
        last_sequence: u64,
        records: Vec<CanonicalRecord>,
        now_ms: u64,
    ) -> Result<(), IngestError> {
        let mut sealed = Vec::new();
        for record in records {
            sealed.extend(self.open.append(record, now_ms)?);
        }
        if !sealed.is_empty() {
            self.store.append_channel_batches(sealed).await?;
        }
        self.materialized_through
            .entry((source_id.to_owned(), stream_id.to_owned()))
            .and_modify(|through| *through = (*through).max(last_sequence))
            .or_insert(last_sequence);
        Ok(())
    }
}

fn encode_journal(records: &[CanonicalRecord]) -> Result<Vec<u8>, IngestError> {
    let encoded = rmp_serde::to_vec_named(&JournalEnvelopeRef {
        format_version: JOURNAL_FORMAT_VERSION,
        records,
    })
    .map_err(|error| IngestError::Codec(error.to_string()))?;
    zstd::stream::encode_all(Cursor::new(encoded), JOURNAL_COMPRESSION_LEVEL)
        .map_err(|error| IngestError::Codec(error.to_string()))
}

fn decode_journal(payload: &[u8]) -> Result<Vec<CanonicalRecord>, IngestError> {
    let decoded = zstd::stream::decode_all(Cursor::new(payload))
        .map_err(|error| IngestError::Codec(error.to_string()))?;
    let envelope: JournalEnvelope =
        rmp_serde::from_slice(&decoded).map_err(|error| IngestError::Codec(error.to_string()))?;
    if envelope.format_version != JOURNAL_FORMAT_VERSION {
        return Err(IngestError::Codec(format!(
            "unsupported journal format version {}",
            envelope.format_version
        )));
    }
    Ok(envelope.records)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::storage::{MessageStore, QueryRequest};

    fn path() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rm-durable-ingest-{unique}.sqlite"))
    }

    fn limits() -> OpenBlockLimits {
        OpenBlockLimits {
            block_messages: 100,
            max_open_channels: 128,
            max_open_bytes: 1024 * 1024,
            idle_seal_after_ms: 30_000,
            max_open_age_ms: 60_000,
        }
    }

    fn record(sequence: usize) -> CanonicalRecord {
        let channel_key = "channel:quiet".to_owned();
        let raw_irc = format!("@id={sequence} PRIVMSG #quiet :hello").into_bytes();
        CanonicalRecord {
            channel_key: channel_key.clone(),
            event_at_ms: sequence as i64,
            received_at_ms: sequence as i64,
            event_key: CanonicalRecord::derive_event_key(&channel_key, &raw_irc),
            source_id: String::new(),
            fidelity: Default::default(),
            raw_irc,
        }
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn replays_acknowledged_quiet_tail_after_restart() {
        let path = path();
        {
            let store = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
            let mut ingest = DurableIngest::new(store.clone(), limits()).unwrap();
            assert_eq!(
                ingest
                    .accept_batch("source", "stream", 1, 2, vec![record(1), record(2)], 10)
                    .await
                    .unwrap(),
                2
            );
            assert_eq!(store.stats().await.unwrap().messages, 0);
            assert_eq!(store.pending_journal_batches().await.unwrap().len(), 1);
        }
        {
            let store = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
            let mut ingest = DurableIngest::new(store.clone(), limits()).unwrap();
            assert_eq!(ingest.recover_pending(20).await.unwrap(), 1);
            assert_eq!(ingest.checkpoint().await.unwrap(), 2);
            assert!(store.pending_journal_batches().await.unwrap().is_empty());
            let result = store
                .query(QueryRequest {
                    channel_key: "channel:quiet".to_owned(),
                    after_ms: None,
                    before_ms: None,
                    limit: 800,
                })
                .await
                .unwrap();
            assert_eq!(result.records, vec![record(1), record(2)]);
        }
        cleanup(&path);
    }

    #[tokio::test]
    async fn cross_source_duplicate_is_retained_once() {
        let path = path();
        let store = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
        let mut ingest = DurableIngest::new(store.clone(), limits()).unwrap();
        ingest
            .accept_batch("one", "stream", 1, 1, vec![record(1)], 10)
            .await
            .unwrap();
        ingest
            .accept_batch("two", "stream", 1, 1, vec![record(1)], 11)
            .await
            .unwrap();
        assert_eq!(ingest.checkpoint().await.unwrap(), 1);
        assert_eq!(store.stats().await.unwrap().messages, 1);
        assert!(store.pending_journal_batches().await.unwrap().is_empty());
        drop(ingest);
        drop(store);
        cleanup(&path);
    }

    #[tokio::test]
    async fn accepts_typed_raw_source_batch() {
        let path = path();
        let store = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
        let mut ingest = DurableIngest::new(store.clone(), limits()).unwrap();
        let batch = RawSourceBatch {
            source_id: "raw".to_owned(),
            stream_id: "connection-1".to_owned(),
            first_sequence: 40,
            last_sequence: 41,
            records: vec![record(40), record(41)],
        };
        assert_eq!(ingest.accept_source_batch(batch, 50).await.unwrap(), 41);
        assert_eq!(ingest.checkpoint().await.unwrap(), 2);
        assert_eq!(store.stats().await.unwrap().messages, 2);
        drop(ingest);
        drop(store);
        cleanup(&path);
    }
}
