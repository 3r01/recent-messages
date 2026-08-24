use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use super::block::EncodedBlock;
use super::{CanonicalRecord, LocalResult, MessageStore, QueryRequest, StoreError, StoreStats};

struct SegmentState {
    data: File,
    index: Connection,
}

pub struct SegmentBlockStore {
    state: Mutex<SegmentState>,
    data_path: PathBuf,
    index_path: PathBuf,
    block_messages: usize,
    target_messages_per_channel: usize,
    compression_level: i32,
}

impl SegmentBlockStore {
    pub fn open(
        base_path: impl AsRef<Path>,
        block_messages: usize,
        target_messages_per_channel: usize,
    ) -> Result<Self, StoreError> {
        if block_messages == 0 || target_messages_per_channel == 0 {
            return Err(backend(
                "block and retention sizes must be greater than zero",
            ));
        }
        let base = base_path.as_ref();
        let data_path = PathBuf::from(format!("{}.blocks", base.display()));
        let index_path = PathBuf::from(format!("{}.index.sqlite", base.display()));
        let data = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&data_path)
            .map_err(backend)?;
        let index = Connection::open(&index_path).map_err(backend)?;
        index
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(backend)?;
        index
            .pragma_update(None, "synchronous", "FULL")
            .map_err(backend)?;
        index
            .execute_batch(
                r"
CREATE TABLE IF NOT EXISTS channels (
    channel_key TEXT PRIMARY KEY,
    next_block_sequence INTEGER NOT NULL DEFAULT 0,
    retained_messages INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS segment_blocks (
    id INTEGER PRIMARY KEY,
    channel_key TEXT NOT NULL REFERENCES channels(channel_key) ON DELETE CASCADE,
    block_sequence INTEGER NOT NULL,
    format_version INTEGER NOT NULL,
    first_event_at_ms INTEGER NOT NULL,
    last_event_at_ms INTEGER NOT NULL,
    message_count INTEGER NOT NULL,
    uncompressed_bytes INTEGER NOT NULL,
    checksum BLOB NOT NULL,
    file_offset INTEGER NOT NULL,
    payload_bytes INTEGER NOT NULL,
    UNIQUE(channel_key, block_sequence)
);
CREATE INDEX IF NOT EXISTS segment_blocks_query
    ON segment_blocks(channel_key, last_event_at_ms, first_event_at_ms);
",
            )
            .map_err(backend)?;
        let indexed_end: i64 = index
            .query_row(
                "SELECT COALESCE(MAX(file_offset + payload_bytes), 0) FROM segment_blocks",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        let indexed_end = u64::try_from(indexed_end).map_err(backend)?;
        let physical_end = data.metadata().map_err(backend)?.len();
        if physical_end < indexed_end {
            return Err(backend("segment file is shorter than its committed index"));
        }
        if physical_end > indexed_end {
            data.set_len(indexed_end).map_err(backend)?;
            data.sync_data().map_err(backend)?;
        }
        Ok(Self {
            state: Mutex::new(SegmentState { data, index }),
            data_path,
            index_path,
            block_messages,
            target_messages_per_channel,
            compression_level: 3,
        })
    }

    fn allowed_messages(&self) -> usize {
        self.target_messages_per_channel
            .saturating_add(self.block_messages.saturating_sub(1))
    }

    pub fn append_channel_batches(
        &self,
        batches: Vec<Vec<CanonicalRecord>>,
    ) -> Result<(), StoreError> {
        for records in &batches {
            let Some(first) = records.first() else {
                continue;
            };
            if records
                .iter()
                .any(|record| record.channel_key != first.channel_key)
            {
                return Err(StoreError::MixedChannelBatch);
            }
        }
        let mut state = self.state.lock().map_err(|_| StoreError::LockPoisoned)?;
        let SegmentState { data, index } = &mut *state;
        let uncommitted_start = data.seek(SeekFrom::End(0)).map_err(backend)?;
        let tx = index.transaction().map_err(backend)?;
        let result = (|| {
            for records in batches {
                let Some(first) = records.first() else {
                    continue;
                };
                let channel_key = first.channel_key.clone();
                self.append_transaction(data, &tx, &channel_key, records)?;
            }
            data.sync_data().map_err(backend)?;
            tx.commit().map_err(backend)
        })();
        if result.is_err() {
            data.set_len(uncommitted_start).map_err(backend)?;
            data.seek(SeekFrom::End(0)).map_err(backend)?;
            data.sync_data().map_err(backend)?;
        }
        result
    }

    fn append_transaction(
        &self,
        data: &mut File,
        tx: &Transaction<'_>,
        channel_key: &str,
        mut records: Vec<CanonicalRecord>,
    ) -> Result<(), StoreError> {
        tx.execute(
            "INSERT OR IGNORE INTO channels(channel_key) VALUES (?1)",
            [channel_key],
        )
        .map_err(backend)?;
        let mut retained_keys = HashSet::new();
        for block in load_channel_blocks(data, tx, channel_key)? {
            for record in block.decode()? {
                retained_keys.insert(record.event_key);
            }
        }
        records.sort_by_key(|record| (record.event_at_ms, record.received_at_ms, record.event_key));
        records.retain(|record| retained_keys.insert(record.event_key));
        if records.is_empty() {
            return Ok(());
        }
        let (mut sequence, mut retained): (i64, i64) = tx
            .query_row(
                "SELECT next_block_sequence, retained_messages FROM channels WHERE channel_key=?1",
                [channel_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(backend)?;
        for chunk in records.chunks(self.block_messages) {
            let block = EncodedBlock::encode(chunk, self.compression_level)?;
            let offset = data.seek(SeekFrom::End(0)).map_err(backend)?;
            data.write_all(&block.payload).map_err(backend)?;
            tx.execute(
                "INSERT INTO segment_blocks(
                    channel_key, block_sequence, format_version, first_event_at_ms,
                    last_event_at_ms, message_count, uncompressed_bytes, checksum,
                    file_offset, payload_bytes
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    channel_key,
                    sequence,
                    i64::from(block.format_version),
                    block.first_event_at_ms,
                    block.last_event_at_ms,
                    i64::from(block.message_count),
                    i64::from(block.uncompressed_bytes),
                    block.checksum.as_slice(),
                    i64::try_from(offset).map_err(backend)?,
                    i64::try_from(block.payload.len()).map_err(backend)?,
                ],
            )
            .map_err(backend)?;
            sequence += 1;
            retained += i64::from(block.message_count);
        }
        while retained > i64::try_from(self.allowed_messages()).unwrap_or(i64::MAX) {
            let oldest: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT id,message_count FROM segment_blocks
                     WHERE channel_key=?1 ORDER BY block_sequence LIMIT 1",
                    [channel_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(backend)?;
            let Some((id, count)) = oldest else { break };
            tx.execute("DELETE FROM segment_blocks WHERE id=?1", [id])
                .map_err(backend)?;
            retained = retained.saturating_sub(count);
        }
        tx.execute(
            "UPDATE channels SET next_block_sequence=?2,retained_messages=?3 WHERE channel_key=?1",
            params![channel_key, sequence, retained],
        )
        .map_err(backend)?;
        Ok(())
    }

    pub fn physical_bytes(&self) -> Result<u64, StoreError> {
        Ok(file_len(&self.data_path)?
            + file_len(&self.index_path)?
            + file_len(&PathBuf::from(format!("{}-wal", self.index_path.display())))?
            + file_len(&PathBuf::from(format!("{}-shm", self.index_path.display())))?)
    }
}

impl MessageStore for SegmentBlockStore {
    async fn append_batch(&self, records: Vec<CanonicalRecord>) -> Result<(), StoreError> {
        self.append_channel_batches(vec![records])
    }

    async fn query(&self, request: QueryRequest) -> Result<LocalResult, StoreError> {
        if request.limit == 0 {
            return Err(StoreError::InvalidLimit);
        }
        let mut state = self.state.lock().map_err(|_| StoreError::LockPoisoned)?;
        let SegmentState { data, index } = &mut *state;
        let mut statement = index
            .prepare(
                "SELECT format_version,first_event_at_ms,last_event_at_ms,message_count,
                        uncompressed_bytes,checksum,file_offset,payload_bytes
                 FROM segment_blocks WHERE channel_key=?1
                 ORDER BY block_sequence",
            )
            .map_err(backend)?;
        let descriptors = statement
            .query_map(params![request.channel_key], descriptor_row)
            .map_err(backend)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(backend)?;
        drop(statement);
        let mut records = Vec::new();
        for descriptor in descriptors {
            records.extend(read_block(data, &request.channel_key, descriptor)?.decode()?);
        }
        records.sort_by_key(|record| (record.received_at_ms, record.event_at_ms, record.event_key));
        let oldest_retained_at_ms = records.first().map(|record| record.received_at_ms);
        let newest_retained_at_ms = records.last().map(|record| record.received_at_ms);
        records.retain(|record| {
            request
                .after_ms
                .is_none_or(|after| record.received_at_ms > after)
                && request
                    .before_ms
                    .is_none_or(|before| record.received_at_ms < before)
        });
        if records.len() > request.limit {
            records.drain(..records.len() - request.limit);
        }
        Ok(LocalResult {
            records,
            oldest_retained_at_ms,
            newest_retained_at_ms,
        })
    }

    async fn stats(&self) -> Result<StoreStats, StoreError> {
        let state = self.state.lock().map_err(|_| StoreError::LockPoisoned)?;
        let values: (i64, i64, i64, i64, i64) = state
            .index
            .query_row(
                "SELECT (SELECT COUNT(*) FROM channels),COUNT(*),
                        COALESCE(SUM(message_count),0),COALESCE(SUM(payload_bytes),0),
                        COALESCE(SUM(uncompressed_bytes),0) FROM segment_blocks",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .map_err(backend)?;
        Ok(StoreStats {
            channels: usize::try_from(values.0).map_err(backend)?,
            blocks: usize::try_from(values.1).map_err(backend)?,
            messages: usize::try_from(values.2).map_err(backend)?,
            compressed_bytes: u64::try_from(values.3).map_err(backend)?,
            uncompressed_bytes: u64::try_from(values.4).map_err(backend)?,
        })
    }
}

type Descriptor = (i64, i64, i64, i64, i64, [u8; 32], i64, i64);

fn descriptor_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Descriptor> {
    let checksum: Vec<u8> = row.get(5)?;
    let checksum = checksum
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        checksum,
        row.get(6)?,
        row.get(7)?,
    ))
}

fn read_block(data: &mut File, channel: &str, d: Descriptor) -> Result<EncodedBlock, StoreError> {
    let payload_bytes = usize::try_from(d.7).map_err(backend)?;
    let file_offset = u64::try_from(d.6).map_err(backend)?;
    let mut payload = vec![0; payload_bytes];
    data.seek(SeekFrom::Start(file_offset)).map_err(backend)?;
    data.read_exact(&mut payload).map_err(backend)?;
    Ok(EncodedBlock {
        format_version: u16::try_from(d.0).map_err(backend)?,
        channel_key: channel.to_owned(),
        first_event_at_ms: d.1,
        last_event_at_ms: d.2,
        message_count: u32::try_from(d.3).map_err(backend)?,
        uncompressed_bytes: u32::try_from(d.4).map_err(backend)?,
        checksum: d.5,
        payload,
    })
}

fn load_channel_blocks(
    data: &mut File,
    tx: &Transaction<'_>,
    channel: &str,
) -> Result<Vec<EncodedBlock>, StoreError> {
    let mut statement = tx
        .prepare(
            "SELECT format_version,first_event_at_ms,last_event_at_ms,message_count,
                    uncompressed_bytes,checksum,file_offset,payload_bytes
             FROM segment_blocks WHERE channel_key=?1 ORDER BY block_sequence",
        )
        .map_err(backend)?;
    let descriptors = statement
        .query_map([channel], descriptor_row)
        .map_err(backend)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(backend)?;
    descriptors
        .into_iter()
        .map(|descriptor| read_block(data, channel, descriptor))
        .collect()
}

fn backend(error: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(error.to_string())
}

fn file_len(path: &Path) -> Result<u64, StoreError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(backend(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn record(sequence: u16) -> CanonicalRecord {
        let channel_key = "channel:1".to_owned();
        let raw_irc = format!("@id={sequence} PRIVMSG #channel :hello {sequence}").into_bytes();
        CanonicalRecord {
            event_at_ms: i64::from(sequence),
            received_at_ms: i64::from(sequence),
            event_key: CanonicalRecord::derive_event_key(&channel_key, &raw_irc),
            source_id: String::new(),
            fidelity: Default::default(),
            channel_key,
            raw_irc,
        }
    }

    fn base() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rm-segment-{unique}"))
    }

    fn cleanup(base: &Path) {
        for suffix in [
            ".blocks",
            ".index.sqlite",
            ".index.sqlite-wal",
            ".index.sqlite-shm",
        ] {
            let _ = std::fs::remove_file(format!("{}{}", base.display(), suffix));
        }
    }

    #[tokio::test]
    async fn appends_queries_evicts_and_reopens() {
        let base = base();
        {
            let store = SegmentBlockStore::open(&base, 2, 3).unwrap();
            store
                .append_batch((0..4).map(record).collect())
                .await
                .unwrap();
            store.append_batch(vec![record(4)]).await.unwrap();
            let result = store
                .query(QueryRequest {
                    channel_key: "channel:1".to_owned(),
                    after_ms: None,
                    before_ms: None,
                    limit: 10,
                })
                .await
                .unwrap();
            assert_eq!(result.records.len(), 3);
            assert!(store.physical_bytes().unwrap() > 0);
        }
        {
            let store = SegmentBlockStore::open(&base, 2, 3).unwrap();
            assert_eq!(store.stats().await.unwrap().messages, 3);
        }
        cleanup(&base);
    }

    #[test]
    fn truncates_an_unindexed_orphan_tail_on_reopen() {
        let base = base();
        let store = SegmentBlockStore::open(&base, 100, 800).unwrap();
        drop(store);
        let data_path = PathBuf::from(format!("{}.blocks", base.display()));
        let mut file = OpenOptions::new().append(true).open(&data_path).unwrap();
        file.write_all(b"orphan").unwrap();
        drop(file);
        assert_eq!(std::fs::metadata(&data_path).unwrap().len(), 6);
        let store = SegmentBlockStore::open(&base, 100, 800).unwrap();
        assert_eq!(std::fs::metadata(&data_path).unwrap().len(), 0);
        drop(store);
        cleanup(&base);
    }

    #[tokio::test]
    async fn rejects_mixed_channel_batch_before_writing() {
        let base = base();
        let store = SegmentBlockStore::open(&base, 2, 4).unwrap();
        let mut second = record(2);
        second.channel_key = "channel:2".to_owned();
        assert!(matches!(
            store.append_channel_batches(vec![vec![record(1), second]]),
            Err(StoreError::MixedChannelBatch)
        ));
        assert_eq!(store.stats().await.unwrap().messages, 0);
        assert_eq!(
            std::fs::metadata(format!("{}.blocks", base.display()))
                .unwrap()
                .len(),
            0
        );
        drop(store);
        cleanup(&base);
    }
}
