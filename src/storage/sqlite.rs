use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::block::EncodedBlock;
use super::eviction::BlockMeta;
use super::{
    CanonicalRecord, ExpireStats, JournalBatch, JournalStats, LocalResult, MessageStore,
    QueryRequest, StoreError, StoreStats, record_fidelity_upgrade,
};

pub struct SqliteBlockStore {
    connection: Mutex<Connection>,
    path: Option<PathBuf>,
    block_messages: usize,
    target_messages_per_channel: usize,
    compression_level: i32,
}

impl SqliteBlockStore {
    pub fn open(
        path: impl AsRef<Path>,
        block_messages: usize,
        target_messages_per_channel: usize,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let connection = Connection::open(&path).map_err(backend)?;
        Self::from_connection(
            connection,
            Some(path),
            block_messages,
            target_messages_per_channel,
        )
    }

    pub fn open_in_memory(
        block_messages: usize,
        target_messages_per_channel: usize,
    ) -> Result<Self, StoreError> {
        Self::from_connection(
            Connection::open_in_memory().map_err(backend)?,
            None,
            block_messages,
            target_messages_per_channel,
        )
    }

    fn from_connection(
        connection: Connection,
        path: Option<PathBuf>,
        block_messages: usize,
        target_messages_per_channel: usize,
    ) -> Result<Self, StoreError> {
        if block_messages == 0 || target_messages_per_channel == 0 {
            return Err(StoreError::Backend(
                "block and retention sizes must be greater than zero".to_owned(),
            ));
        }
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(backend)?;
        connection
            .pragma_update(None, "auto_vacuum", 2)
            .map_err(backend)?;
        let auto_vacuum: i64 = connection
            .pragma_query_value(None, "auto_vacuum", |row| row.get(0))
            .map_err(backend)?;
        if auto_vacuum != 2 {
            return Err(StoreError::Backend(
                "existing SQLite store does not use incremental auto-vacuum; migrate it offline before opening"
                    .to_owned(),
            ));
        }
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(backend)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(backend)?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(backend)?;
        connection
            .execute_batch(
                r"
CREATE TABLE IF NOT EXISTS channels (
    channel_key TEXT PRIMARY KEY,
    next_block_sequence INTEGER NOT NULL DEFAULT 0,
    retained_messages INTEGER NOT NULL DEFAULT 0,
    last_requested_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS blocked_channels (
    channel_key TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS always_join_channels (
    channel_key TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS channel_purge_cutoffs (
    channel_key TEXT PRIMARY KEY,
    received_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS message_blocks (
    id INTEGER PRIMARY KEY,
    channel_key TEXT NOT NULL REFERENCES channels(channel_key) ON DELETE CASCADE,
    block_sequence INTEGER NOT NULL,
    format_version INTEGER NOT NULL,
    first_event_at_ms INTEGER NOT NULL,
    last_event_at_ms INTEGER NOT NULL,
    message_count INTEGER NOT NULL,
    uncompressed_bytes INTEGER NOT NULL,
    checksum BLOB NOT NULL,
    payload BLOB NOT NULL,
    UNIQUE(channel_key, block_sequence)
);
CREATE INDEX IF NOT EXISTS message_blocks_query
    ON message_blocks(channel_key, last_event_at_ms, first_event_at_ms);
CREATE INDEX IF NOT EXISTS message_blocks_expiry
    ON message_blocks(last_event_at_ms);

CREATE TABLE IF NOT EXISTS store_totals (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    blocks INTEGER NOT NULL,
    messages INTEGER NOT NULL,
    compressed_bytes INTEGER NOT NULL,
    uncompressed_bytes INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_journal (
    source_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    first_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    checksum BLOB NOT NULL,
    payload BLOB NOT NULL,
    processing_state INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(source_id, stream_id, first_sequence)
);

CREATE TABLE IF NOT EXISTS source_offsets (
    source_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    committed_through INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(source_id, stream_id)
);
",
            )
            .map_err(backend)?;
        initialize_store_totals(&connection)?;
        ensure_channels_last_requested_column(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            path,
            block_messages,
            target_messages_per_channel,
            compression_level: 3,
        })
    }

    fn allowed_messages(&self) -> usize {
        self.target_messages_per_channel
            .saturating_add(self.block_messages.saturating_sub(1))
    }

    pub fn physical_bytes(&self) -> Result<u64, StoreError> {
        let Some(path) = &self.path else {
            return Ok(0);
        };
        let mut bytes = file_len(path)?;
        bytes = bytes.saturating_add(file_len(&PathBuf::from(format!("{}-wal", path.display())))?);
        bytes = bytes.saturating_add(file_len(&PathBuf::from(format!("{}-shm", path.display())))?);
        Ok(bytes)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn accept_journal_batch(
        &self,
        source_id: &str,
        stream_id: &str,
        first_sequence: u64,
        last_sequence: u64,
        checksum: [u8; 32],
        payload: &[u8],
        now_ms: u64,
    ) -> Result<u64, StoreError> {
        if first_sequence > last_sequence {
            return Err(StoreError::Backend(
                "journal batch sequence range is invalid".to_owned(),
            ));
        }
        let first = i64::try_from(first_sequence).map_err(backend)?;
        let last = i64::try_from(last_sequence).map_err(backend)?;
        let now = i64::try_from(now_ms).map_err(backend)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let existing_offset: Option<i64> = tx
            .query_row(
                "SELECT committed_through FROM source_offsets
                 WHERE source_id = ?1 AND stream_id = ?2",
                params![source_id, stream_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        let initial_offset = existing_offset.unwrap_or(first.saturating_sub(1));
        tx.execute(
            "INSERT OR IGNORE INTO source_offsets(
                source_id, stream_id, committed_through, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4)",
            params![source_id, stream_id, initial_offset, now],
        )
        .map_err(backend)?;
        let existing_batch: Option<(i64, Vec<u8>)> = tx
            .query_row(
                "SELECT last_sequence, checksum FROM ingest_journal
                 WHERE source_id = ?1 AND stream_id = ?2 AND first_sequence = ?3",
                params![source_id, stream_id, first],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(backend)?;
        if let Some((existing_last, existing_checksum)) = existing_batch {
            if existing_last != last || existing_checksum.as_slice() != checksum {
                return Err(StoreError::Backend(
                    "conflicting replay for an existing journal sequence".to_owned(),
                ));
            }
        } else {
            tx.execute(
                "INSERT INTO ingest_journal(
                    source_id, stream_id, first_sequence, last_sequence, checksum, payload
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    source_id,
                    stream_id,
                    first,
                    last,
                    checksum.as_slice(),
                    payload
                ],
            )
            .map_err(backend)?;
        }

        let mut committed = initial_offset;
        loop {
            let next: Option<i64> = tx
                .query_row(
                    "SELECT last_sequence FROM ingest_journal
                     WHERE source_id = ?1 AND stream_id = ?2 AND first_sequence = ?3",
                    params![source_id, stream_id, committed.saturating_add(1)],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            let Some(next) = next else { break };
            if next <= committed {
                return Err(StoreError::Backend(
                    "journal contains a non-advancing sequence range".to_owned(),
                ));
            }
            committed = next;
        }
        tx.execute(
            "UPDATE source_offsets SET committed_through = ?3, updated_at_ms = ?4
             WHERE source_id = ?1 AND stream_id = ?2",
            params![source_id, stream_id, committed, now],
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)?;
        u64::try_from(committed).map_err(backend)
    }

    pub fn committed_through(
        &self,
        source_id: &str,
        stream_id: &str,
    ) -> Result<Option<u64>, StoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let value: Option<i64> = connection
            .query_row(
                "SELECT committed_through FROM source_offsets
                 WHERE source_id = ?1 AND stream_id = ?2",
                params![source_id, stream_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        value
            .map(|value| u64::try_from(value).map_err(backend))
            .transpose()
    }

    pub fn retire_journal_through(
        &self,
        source_id: &str,
        stream_id: &str,
        processed_through: u64,
    ) -> Result<usize, StoreError> {
        let processed = i64::try_from(processed_through).map_err(backend)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        connection
            .execute(
                "DELETE FROM ingest_journal
                 WHERE source_id = ?1 AND stream_id = ?2 AND last_sequence <= ?3",
                params![source_id, stream_id, processed],
            )
            .map_err(backend)
    }

    pub fn pending_journal_batches(&self) -> Result<Vec<JournalBatch>, StoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let mut statement = connection
            .prepare(
                "SELECT source_id,stream_id,first_sequence,last_sequence,checksum,payload
                 FROM ingest_journal ORDER BY source_id,stream_id,first_sequence",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map([], |row| {
                let first: i64 = row.get(2)?;
                let last: i64 = row.get(3)?;
                let checksum: Vec<u8> = row.get(4)?;
                let checksum = checksum
                    .try_into()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?;
                Ok((row.get(0)?, row.get(1)?, first, last, checksum, row.get(5)?))
            })
            .map_err(backend)?;
        rows.map(|row| {
            let (source_id, stream_id, first, last, checksum, payload) = row.map_err(backend)?;
            Ok(JournalBatch {
                source_id,
                stream_id,
                first_sequence: u64::try_from(first).map_err(backend)?,
                last_sequence: u64::try_from(last).map_err(backend)?,
                checksum,
                payload,
            })
        })
        .collect()
    }

    pub fn journal_stats(&self) -> Result<JournalStats, StoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let (batches, bytes): (i64, i64) = connection
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) FROM ingest_journal",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(backend)?;
        Ok(JournalStats {
            batches: usize::try_from(batches).map_err(backend)?,
            bytes: u64::try_from(bytes).map_err(backend)?,
        })
    }

    pub fn append_channel_batches(
        &self,
        batches: Vec<Vec<CanonicalRecord>>,
    ) -> Result<(), StoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        for records in batches {
            let Some(first) = records.first() else {
                continue;
            };
            if records
                .iter()
                .any(|record| record.channel_key != first.channel_key)
            {
                return Err(StoreError::MixedChannelBatch);
            }
            let channel_key = first.channel_key.clone();
            self.append_transaction(&tx, &channel_key, records)?;
        }
        tx.commit().map_err(backend)
    }

    pub fn checkpoint_wal(&self) -> Result<(), StoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .map_err(backend)
    }

    pub(crate) fn check_readable(&self) -> Result<(), StoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        connection
            .query_row("SELECT COUNT(*) FROM sqlite_schema", [], |_| Ok(()))
            .map_err(backend)
    }

    pub(crate) fn touch_channel_requested(
        &self,
        channel_key: &str,
        requested_at_ms: u64,
        coalesce_before_ms: u64,
    ) -> Result<bool, StoreError> {
        let requested_at_ms = i64::try_from(requested_at_ms).map_err(backend)?;
        let coalesce_before_ms = i64::try_from(coalesce_before_ms).map_err(backend)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let changed = connection
            .execute(
                "INSERT INTO channels(channel_key, last_requested_at_ms) VALUES (?1, ?2)
                 ON CONFLICT(channel_key) DO UPDATE SET last_requested_at_ms = excluded.last_requested_at_ms
                 WHERE channels.last_requested_at_ms < ?3",
                params![channel_key, requested_at_ms, coalesce_before_ms],
            )
            .map_err(backend)?;
        Ok(changed > 0)
    }

    pub(crate) fn pressure_blocks(&self) -> Result<Vec<BlockMeta>, StoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let mut statement = connection
            .prepare(
                "SELECT b.id, b.channel_key, b.message_count, LENGTH(b.payload),
                        b.last_event_at_ms, c.last_requested_at_ms,
                        p.channel_key IS NOT NULL
                 FROM message_blocks b
                 JOIN channels c ON c.channel_key = b.channel_key
                 LEFT JOIN always_join_channels p ON p.channel_key = b.channel_key",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let message_count: i64 = row.get(2)?;
                let payload_bytes: i64 = row.get(3)?;
                let last_event_at_ms: i64 = row.get(4)?;
                let last_requested_at_ms: i64 = row.get(5)?;
                let always_join: bool = row.get(6)?;
                Ok(BlockMeta {
                    id: u64::try_from(id)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, id))?,
                    channel_key: row.get(1)?,
                    message_count: usize::try_from(message_count)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, message_count))?,
                    bytes: u64::try_from(payload_bytes)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, payload_bytes))?
                        .saturating_add(384),
                    last_requested_at_ms: u64::try_from(last_requested_at_ms).unwrap_or(0),
                    last_event_at_ms: u64::try_from(last_event_at_ms).unwrap_or(0),
                    always_join,
                    journal_protected: false,
                })
            })
            .map_err(backend)?;
        rows.map(|row| row.map_err(backend)).collect()
    }

    pub(crate) fn evict_block_ids(&self, ids: &[u64]) -> Result<usize, StoreError> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let mut deleted = 0;
        for id in ids {
            let id = i64::try_from(*id).map_err(backend)?;
            let block: Option<(String, i64)> = tx
                .query_row(
                    "SELECT channel_key, message_count FROM message_blocks WHERE id = ?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(backend)?;
            let Some((channel_key, messages)) = block else {
                continue;
            };
            deleted += tx
                .execute("DELETE FROM message_blocks WHERE id = ?1", [id])
                .map_err(backend)?;
            tx.execute(
                "UPDATE channels
                 SET retained_messages = MAX(0, retained_messages - ?2)
                 WHERE channel_key = ?1",
                params![channel_key, messages],
            )
            .map_err(backend)?;
        }
        tx.commit().map_err(backend)?;
        reclaim_free_pages(&connection, true)?;
        Ok(deleted)
    }

    pub(crate) fn expire_blocks_before(&self, cutoff_ms: i64) -> Result<ExpireStats, StoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let expired = {
            let mut statement = tx
                .prepare(
                    "SELECT channel_key, COUNT(*), SUM(message_count)
                     FROM message_blocks INDEXED BY message_blocks_expiry
                     WHERE last_event_at_ms < ?1
                     GROUP BY channel_key",
                )
                .map_err(backend)?;
            let rows = statement
                .query_map([cutoff_ms], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(backend)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(backend)?
        };
        let mut blocks = 0_usize;
        let mut messages = 0_usize;
        for (channel, channel_blocks, channel_messages) in &expired {
            blocks = blocks.saturating_add(usize::try_from(*channel_blocks).map_err(backend)?);
            messages =
                messages.saturating_add(usize::try_from(*channel_messages).map_err(backend)?);
            tx.execute(
                "UPDATE channels
                 SET retained_messages = MAX(0, retained_messages - ?2)
                 WHERE channel_key = ?1",
                params![channel, channel_messages],
            )
            .map_err(backend)?;
        }
        tx.execute(
            "DELETE FROM message_blocks WHERE last_event_at_ms < ?1",
            [cutoff_ms],
        )
        .map_err(backend)?;
        tx.execute(
            "DELETE FROM channels
             WHERE retained_messages = 0
               AND (last_requested_at_ms = 0 OR last_requested_at_ms < ?1)",
            [cutoff_ms],
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)?;
        if blocks > 0 {
            reclaim_free_pages(&connection, false)?;
        }
        Ok(ExpireStats { blocks, messages })
    }

    pub(crate) fn purge_channel(
        &self,
        channel_key: &str,
        received_at_ms: i64,
    ) -> Result<usize, StoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        tx.execute(
            "INSERT INTO channel_purge_cutoffs(channel_key, received_at_ms) VALUES (?1, ?2)
             ON CONFLICT(channel_key) DO UPDATE SET received_at_ms=MAX(received_at_ms, excluded.received_at_ms)",
            params![channel_key, received_at_ms],
        )
        .map_err(backend)?;
        let removed = tx
            .execute(
                "DELETE FROM message_blocks WHERE channel_key=?1",
                [channel_key],
            )
            .map_err(backend)?;
        tx.execute("DELETE FROM channels WHERE channel_key=?1", [channel_key])
            .map_err(backend)?;
        tx.commit().map_err(backend)?;
        Ok(removed)
    }

    pub(crate) fn set_channel_blocked(
        &self,
        channel_key: &str,
        blocked: bool,
    ) -> Result<usize, StoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let removed = if blocked {
            tx.execute(
                "INSERT OR IGNORE INTO blocked_channels(channel_key) VALUES (?1)",
                [channel_key],
            )
            .map_err(backend)?;
            let removed = tx
                .execute(
                    "DELETE FROM message_blocks WHERE channel_key=?1",
                    [channel_key],
                )
                .map_err(backend)?;
            tx.execute("DELETE FROM channels WHERE channel_key=?1", [channel_key])
                .map_err(backend)?;
            removed
        } else {
            tx.execute(
                "DELETE FROM blocked_channels WHERE channel_key=?1",
                [channel_key],
            )
            .map_err(backend)?;
            0
        };
        tx.commit().map_err(backend)?;
        Ok(removed)
    }

    pub(crate) fn sync_blocked_channels(&self, channels: &[String]) -> Result<(), StoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        tx.execute("DELETE FROM blocked_channels", [])
            .map_err(backend)?;
        for channel in channels {
            tx.execute(
                "INSERT INTO blocked_channels(channel_key) VALUES (?1)",
                [channel],
            )
            .map_err(backend)?;
            tx.execute("DELETE FROM message_blocks WHERE channel_key=?1", [channel])
                .map_err(backend)?;
            tx.execute("DELETE FROM channels WHERE channel_key=?1", [channel])
                .map_err(backend)?;
        }
        tx.commit().map_err(backend)
    }

    pub(crate) fn sync_always_join_channels(&self, channels: &[String]) -> Result<(), StoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        tx.execute("DELETE FROM always_join_channels", [])
            .map_err(backend)?;
        for channel in channels {
            tx.execute(
                "INSERT INTO always_join_channels(channel_key) VALUES (?1)",
                [channel],
            )
            .map_err(backend)?;
        }
        tx.commit().map_err(backend)
    }

    fn append_transaction(
        &self,
        tx: &Transaction<'_>,
        channel_key: &str,
        mut records: Vec<CanonicalRecord>,
    ) -> Result<(), StoreError> {
        let blocked: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM blocked_channels WHERE channel_key=?1)",
                [channel_key],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if blocked {
            return Ok(());
        }
        let purge_cutoff: Option<i64> = tx
            .query_row(
                "SELECT received_at_ms FROM channel_purge_cutoffs WHERE channel_key=?1",
                [channel_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        if let Some(cutoff) = purge_cutoff {
            records.retain(|record| record.received_at_ms > cutoff);
            if records.is_empty() {
                return Ok(());
            }
        }
        tx.execute(
            "INSERT OR IGNORE INTO channels(channel_key) VALUES (?1)",
            [channel_key],
        )
        .map_err(backend)?;

        let first_incoming_event_at_ms = records
            .iter()
            .map(|record| record.event_at_ms)
            .min()
            .expect("records are non-empty after purge filtering");
        let last_incoming_event_at_ms = records
            .iter()
            .map(|record| record.event_at_ms)
            .max()
            .expect("records are non-empty after purge filtering");

        let mut retained = HashMap::new();
        let mut retained_blocks = Vec::new();
        {
            let mut statement = tx
                .prepare(
                    "SELECT format_version, first_event_at_ms, last_event_at_ms,
                            message_count, uncompressed_bytes, checksum, payload, id
                     FROM message_blocks
                     WHERE channel_key = ?1
                       AND last_event_at_ms >= ?2
                       AND first_event_at_ms <= ?3
                     ORDER BY block_sequence",
                )
                .map_err(backend)?;
            let rows = statement
                .query_map(
                    params![
                        channel_key,
                        first_incoming_event_at_ms,
                        last_incoming_event_at_ms
                    ],
                    |row| Ok((row.get::<_, i64>(7)?, decode_row(channel_key, row)?)),
                )
                .map_err(backend)?;
            for row in rows {
                let (id, block) = row.map_err(backend)?;
                let block_index = retained_blocks.len();
                let decoded = block.decode()?;
                for (record_index, record) in decoded.iter().enumerate() {
                    retained.insert(record.event_key, (block_index, record_index));
                }
                retained_blocks.push((id, decoded, false));
            }
        }

        records.sort_by_key(|record| (record.event_at_ms, record.received_at_ms, record.event_key));
        let mut new_indices = HashMap::new();
        let mut new_records: Vec<CanonicalRecord> = Vec::new();
        for record in records {
            if let Some(&(block_index, record_index)) = retained.get(&record.event_key) {
                let retained_record = &mut retained_blocks[block_index].1[record_index];
                if record.should_replace(retained_record) {
                    *retained_record = record;
                    retained_blocks[block_index].2 = true;
                    record_fidelity_upgrade("sealed_block");
                }
            } else if let Some(&new_index) = new_indices.get(&record.event_key) {
                if record.should_replace(&new_records[new_index]) {
                    new_records[new_index] = record;
                }
            } else {
                new_indices.insert(record.event_key, new_records.len());
                new_records.push(record);
            }
        }
        for (id, upgraded_records, dirty) in retained_blocks {
            if !dirty {
                continue;
            }
            let block = EncodedBlock::encode(&upgraded_records, self.compression_level)?;
            tx.execute(
                "UPDATE message_blocks SET
                    format_version = ?2, first_event_at_ms = ?3, last_event_at_ms = ?4,
                    message_count = ?5, uncompressed_bytes = ?6, checksum = ?7, payload = ?8
                 WHERE id = ?1",
                params![
                    id,
                    i64::from(block.format_version),
                    block.first_event_at_ms,
                    block.last_event_at_ms,
                    i64::from(block.message_count),
                    i64::from(block.uncompressed_bytes),
                    block.checksum.as_slice(),
                    block.payload,
                ],
            )
            .map_err(backend)?;
        }
        let records = new_records;
        if records.is_empty() {
            return Ok(());
        }

        let (mut next_sequence, mut retained_messages): (i64, i64) = tx
            .query_row(
                "SELECT next_block_sequence, retained_messages FROM channels WHERE channel_key = ?1",
                [channel_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(backend)?;
        for chunk in records.chunks(self.block_messages) {
            let block = EncodedBlock::encode(chunk, self.compression_level)?;
            tx.execute(
                "INSERT INTO message_blocks(
                    channel_key, block_sequence, format_version, first_event_at_ms,
                    last_event_at_ms, message_count, uncompressed_bytes, checksum, payload
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    channel_key,
                    next_sequence,
                    i64::from(block.format_version),
                    block.first_event_at_ms,
                    block.last_event_at_ms,
                    i64::from(block.message_count),
                    i64::from(block.uncompressed_bytes),
                    block.checksum.as_slice(),
                    block.payload,
                ],
            )
            .map_err(backend)?;
            next_sequence += 1;
            retained_messages += i64::from(block.message_count);
        }

        let allowed = i64::try_from(self.allowed_messages()).unwrap_or(i64::MAX);
        while retained_messages > allowed {
            let oldest: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT id, message_count FROM message_blocks
                     WHERE channel_key = ?1 ORDER BY block_sequence LIMIT 1",
                    [channel_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(backend)?;
            let Some((id, count)) = oldest else { break };
            tx.execute("DELETE FROM message_blocks WHERE id = ?1", [id])
                .map_err(backend)?;
            retained_messages = retained_messages.saturating_sub(count);
        }
        tx.execute(
            "UPDATE channels SET next_block_sequence = ?2, retained_messages = ?3
             WHERE channel_key = ?1",
            params![channel_key, next_sequence, retained_messages],
        )
        .map_err(backend)?;
        Ok(())
    }
}

fn ensure_channels_last_requested_column(connection: &Connection) -> Result<(), StoreError> {
    let mut statement = connection
        .prepare("PRAGMA table_info(channels)")
        .map_err(backend)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(backend)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(backend)?;
    if !columns
        .iter()
        .any(|column| column == "last_requested_at_ms")
    {
        connection
            .execute(
                "ALTER TABLE channels ADD COLUMN last_requested_at_ms INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(backend)?;
    }
    Ok(())
}

impl MessageStore for SqliteBlockStore {
    async fn append_batch(&self, records: Vec<CanonicalRecord>) -> Result<(), StoreError> {
        let Some(first) = records.first() else {
            return Ok(());
        };
        if records
            .iter()
            .any(|record| record.channel_key != first.channel_key)
        {
            return Err(StoreError::MixedChannelBatch);
        }
        let channel_key = first.channel_key.clone();
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        self.append_transaction(&tx, &channel_key, records)?;
        tx.commit().map_err(backend)
    }

    async fn query(&self, request: QueryRequest) -> Result<LocalResult, StoreError> {
        if request.limit == 0 {
            return Err(StoreError::InvalidLimit);
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let mut statement = connection
            .prepare(
                "SELECT format_version, first_event_at_ms, last_event_at_ms,
                        message_count, uncompressed_bytes, checksum, payload
                 FROM message_blocks
                 WHERE channel_key = ?1
                 ORDER BY block_sequence",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map(params![request.channel_key], |row| {
                decode_row(&request.channel_key, row)
            })
            .map_err(backend)?;
        let mut records = Vec::new();
        for row in rows {
            records.extend(row.map_err(backend)?.decode()?);
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
        let connection = self
            .connection
            .lock()
            .map_err(|_| StoreError::LockPoisoned)?;
        let values: (i64, i64, i64, i64, i64) = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM channels),
                    blocks,
                    messages,
                    compressed_bytes,
                    uncompressed_bytes
                 FROM store_totals WHERE singleton = 1",
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

fn initialize_store_totals(connection: &Connection) -> Result<(), StoreError> {
    let initialized: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM store_totals WHERE singleton = 1)",
            [],
            |row| row.get(0),
        )
        .map_err(backend)?;
    if !initialized {
        connection
            .execute(
                "INSERT INTO store_totals(
                    singleton, blocks, messages, compressed_bytes, uncompressed_bytes
                 )
                 SELECT 1, COUNT(*), COALESCE(SUM(message_count), 0),
                        COALESCE(SUM(LENGTH(payload)), 0),
                        COALESCE(SUM(uncompressed_bytes), 0)
                 FROM message_blocks",
                [],
            )
            .map_err(backend)?;
    }
    connection
        .execute_batch(
            r"
CREATE TRIGGER IF NOT EXISTS message_blocks_totals_insert
AFTER INSERT ON message_blocks
BEGIN
    UPDATE store_totals
    SET blocks = blocks + 1,
        messages = messages + NEW.message_count,
        compressed_bytes = compressed_bytes + LENGTH(NEW.payload),
        uncompressed_bytes = uncompressed_bytes + NEW.uncompressed_bytes
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS message_blocks_totals_update
AFTER UPDATE ON message_blocks
BEGIN
    UPDATE store_totals
    SET messages = messages + NEW.message_count - OLD.message_count,
        compressed_bytes = compressed_bytes + LENGTH(NEW.payload) - LENGTH(OLD.payload),
        uncompressed_bytes = uncompressed_bytes + NEW.uncompressed_bytes - OLD.uncompressed_bytes
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS message_blocks_totals_delete
AFTER DELETE ON message_blocks
BEGIN
    UPDATE store_totals
    SET blocks = MAX(0, blocks - 1),
        messages = MAX(0, messages - OLD.message_count),
        compressed_bytes = MAX(0, compressed_bytes - LENGTH(OLD.payload)),
        uncompressed_bytes = MAX(0, uncompressed_bytes - OLD.uncompressed_bytes)
    WHERE singleton = 1;
END;
",
        )
        .map_err(backend)
}

fn decode_row(channel_key: &str, row: &rusqlite::Row<'_>) -> rusqlite::Result<EncodedBlock> {
    let checksum: Vec<u8> = row.get(5)?;
    let checksum: [u8; 32] = checksum.try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            32,
            rusqlite::types::Type::Blob,
            "invalid BLAKE3 checksum length".into(),
        )
    })?;
    Ok(EncodedBlock {
        format_version: row.get::<_, u16>(0)?,
        channel_key: channel_key.to_owned(),
        first_event_at_ms: row.get(1)?,
        last_event_at_ms: row.get(2)?,
        message_count: row.get(3)?,
        uncompressed_bytes: row.get(4)?,
        checksum,
        payload: row.get(6)?,
    })
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

fn reclaim_free_pages(connection: &Connection, drain: bool) -> Result<(), StoreError> {
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(backend)?;
    let mut remaining: i64 = connection
        .pragma_query_value(None, "freelist_count", |row| row.get(0))
        .map_err(backend)?;
    while remaining > 0 {
        connection
            .execute_batch("PRAGMA incremental_vacuum(10000)")
            .map_err(backend)?;
        if !drain {
            break;
        }
        let next: i64 = connection
            .pragma_query_value(None, "freelist_count", |row| row.get(0))
            .map_err(backend)?;
        if next >= remaining {
            break;
        }
        remaining = next;
    }
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(backend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn record(sequence: u16) -> CanonicalRecord {
        let channel_key = "channel:1".to_owned();
        let raw_irc = format!("@id={sequence} PRIVMSG #channel :hello {sequence}").into_bytes();
        CanonicalRecord {
            event_at_ms: 1_700_000_000_000 + i64::from(sequence),
            received_at_ms: 1_700_000_000_100 + i64::from(sequence),
            event_key: CanonicalRecord::derive_event_key(&channel_key, &raw_irc),
            source_id: String::new(),
            fidelity: Default::default(),
            channel_key,
            raw_irc,
        }
    }

    #[test]
    fn configures_busy_timeout() {
        let store = SqliteBlockStore::open_in_memory(100, 800).unwrap();
        let timeout_ms: i64 = store
            .connection
            .lock()
            .unwrap()
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert_eq!(timeout_ms, 5_000);
    }

    #[test]
    fn waits_for_a_brief_external_writer() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rm-busy-timeout-{unique}.sqlite"));
        let store = SqliteBlockStore::open(&path, 100, 800).unwrap();
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let writer =
            std::thread::spawn(move || store.append_channel_batches(vec![vec![record(1)]]));
        std::thread::sleep(Duration::from_millis(100));
        blocker.execute_batch("COMMIT").unwrap();
        writer.join().unwrap().unwrap();

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn persists_queries_deduplicates_and_evicts_whole_blocks() {
        let store = SqliteBlockStore::open_in_memory(2, 3).unwrap();
        store
            .append_batch(vec![record(0), record(1), record(2), record(3)])
            .await
            .unwrap();
        assert_eq!(store.stats().await.unwrap().messages, 4);

        store
            .append_batch(vec![record(3), record(4)])
            .await
            .unwrap();
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
        assert_eq!(result.records[0].event_at_ms, 1_700_000_000_002);
        assert_eq!(result.records[2].event_at_ms, 1_700_000_000_004);
    }

    #[tokio::test]
    async fn expires_only_whole_blocks_older_than_the_ttl_boundary() {
        let store = SqliteBlockStore::open_in_memory(2, 800).unwrap();
        store
            .append_batch(vec![record(0), record(1), record(2), record(3)])
            .await
            .unwrap();

        let expired = store.expire_blocks_before(1_700_000_000_002).unwrap();
        assert_eq!(
            expired,
            ExpireStats {
                blocks: 1,
                messages: 2
            }
        );
        assert_eq!(store.stats().await.unwrap().messages, 2);
        let result = store
            .query(QueryRequest {
                channel_key: "channel:1".to_owned(),
                after_ms: None,
                before_ms: None,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(
            result
                .records
                .iter()
                .map(|record| record.event_at_ms)
                .collect::<Vec<_>>(),
            vec![1_700_000_000_002, 1_700_000_000_003]
        );
    }

    #[tokio::test]
    async fn upgrades_a_sealed_record_without_adding_a_message() {
        let store = SqliteBlockStore::open_in_memory(2, 800).unwrap();
        let mut reconstructed = record(1);
        reconstructed.source_id = "firehose".to_owned();
        reconstructed.fidelity = crate::storage::SourceFidelity::Reconstructed;
        let mut direct = reconstructed.clone();
        direct.source_id = "owned-irc".to_owned();
        direct.fidelity = crate::storage::SourceFidelity::DirectIrc;
        direct.raw_irc.extend_from_slice(b";first-msg=1");

        store
            .append_batch(vec![reconstructed.clone()])
            .await
            .unwrap();
        store.append_batch(vec![direct.clone()]).await.unwrap();
        store.append_batch(vec![reconstructed]).await.unwrap();

        let result = store
            .query(QueryRequest {
                channel_key: "channel:1".to_owned(),
                after_ms: None,
                before_ms: None,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(result.records, vec![direct]);
        assert_eq!(store.stats().await.unwrap().messages, 1);
    }

    #[tokio::test]
    async fn appending_a_newer_time_range_does_not_decode_disjoint_blocks() {
        let store = SqliteBlockStore::open_in_memory(2, 800).unwrap();
        store
            .append_batch(vec![record(0), record(1)])
            .await
            .unwrap();
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute("UPDATE message_blocks SET payload = X'00'", [])
                .unwrap();
        }

        store.append_batch(vec![record(2)]).await.unwrap();
        assert_eq!(store.stats().await.unwrap().messages, 3);
    }

    #[tokio::test]
    async fn survives_close_and_reopen_with_wal_accounting() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("federated-rm-{unique}.sqlite"));
        {
            let store = SqliteBlockStore::open(&path, 100, 800).unwrap();
            store.append_batch(vec![record(1)]).await.unwrap();
            let checksum = *blake3::hash(b"durable").as_bytes();
            assert_eq!(
                store
                    .accept_journal_batch("source", "stream", 1, 10, checksum, b"batch", 1)
                    .unwrap(),
                10
            );
            assert!(store.physical_bytes().unwrap() > 0);
        }
        {
            let store = SqliteBlockStore::open(&path, 100, 800).unwrap();
            assert_eq!(store.stats().await.unwrap().messages, 1);
            let result = store
                .query(QueryRequest {
                    channel_key: "channel:1".to_owned(),
                    after_ms: None,
                    before_ms: None,
                    limit: 800,
                })
                .await
                .unwrap();
            assert_eq!(result.records, vec![record(1)]);
            assert_eq!(
                store.committed_through("source", "stream").unwrap(),
                Some(10)
            );
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn initializes_totals_for_an_existing_store() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rm-store-totals-{unique}.sqlite"));
        let expected = {
            let store = SqliteBlockStore::open(&path, 2, 800).unwrap();
            store
                .append_batch(vec![record(0), record(1), record(2)])
                .await
                .unwrap();
            store.stats().await.unwrap()
        };
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "DROP TRIGGER message_blocks_totals_insert;
                 DROP TRIGGER message_blocks_totals_update;
                 DROP TRIGGER message_blocks_totals_delete;
                 DROP TABLE store_totals;",
            )
            .unwrap();

        let reopened = SqliteBlockStore::open(&path, 2, 800).unwrap();
        assert_eq!(reopened.stats().await.unwrap(), expected);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[test]
    fn journal_acknowledgement_advances_only_over_contiguous_ranges() {
        let store = SqliteBlockStore::open_in_memory(100, 800).unwrap();
        let checksum = *blake3::hash(b"batch").as_bytes();

        assert_eq!(
            store
                .accept_journal_batch("source", "stream", 10, 19, checksum, b"first", 1)
                .unwrap(),
            19
        );
        assert_eq!(
            store
                .accept_journal_batch("source", "stream", 30, 39, checksum, b"third", 2)
                .unwrap(),
            19
        );
        assert_eq!(
            store
                .accept_journal_batch("source", "stream", 20, 29, checksum, b"second", 3)
                .unwrap(),
            39
        );
        assert_eq!(
            store.committed_through("source", "stream").unwrap(),
            Some(39)
        );
        assert!(
            store
                .accept_journal_batch(
                    "source",
                    "stream",
                    20,
                    29,
                    *blake3::hash(b"different").as_bytes(),
                    b"conflict",
                    4,
                )
                .is_err()
        );
        assert_eq!(
            store
                .retire_journal_through("source", "stream", 29)
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn appends_multiple_channels_in_one_transaction() {
        let store = SqliteBlockStore::open_in_memory(100, 800).unwrap();
        let mut second = record(2);
        second.channel_key = "channel:2".to_owned();
        second.event_key = CanonicalRecord::derive_event_key(&second.channel_key, &second.raw_irc);
        store
            .append_channel_batches(vec![vec![record(1)], vec![second]])
            .unwrap();

        let stats = store.stats().await.unwrap();
        assert_eq!(stats.channels, 2);
        assert_eq!(stats.messages, 2);
        assert_eq!(stats.blocks, 2);
    }

    #[tokio::test]
    async fn coalesces_request_priority_and_exposes_it_to_eviction() {
        let store = SqliteBlockStore::open_in_memory(100, 800).unwrap();
        store.append_batch(vec![record(1)]).await.unwrap();

        assert!(
            store
                .touch_channel_requested("channel:1", 1_000, 500)
                .unwrap()
        );
        assert!(
            !store
                .touch_channel_requested("channel:1", 1_100, 900)
                .unwrap()
        );
        assert_eq!(
            store.pressure_blocks().unwrap()[0].last_requested_at_ms,
            1_000
        );
        assert!(
            store
                .touch_channel_requested("channel:1", 2_000, 1_500)
                .unwrap()
        );
        assert_eq!(
            store.pressure_blocks().unwrap()[0].last_requested_at_ms,
            2_000
        );

        store
            .sync_always_join_channels(&["channel:1".to_owned()])
            .unwrap();
        assert!(store.pressure_blocks().unwrap()[0].always_join);
        store.sync_always_join_channels(&[]).unwrap();
        assert!(!store.pressure_blocks().unwrap()[0].always_join);
    }

    #[test]
    fn migrates_existing_channels_table_with_request_priority() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rm-priority-migration-{unique}.sqlite"));
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "auto_vacuum", 2).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE channels (
                    channel_key TEXT PRIMARY KEY,
                    next_block_sequence INTEGER NOT NULL DEFAULT 0,
                    retained_messages INTEGER NOT NULL DEFAULT 0
                );",
            )
            .unwrap();
        drop(connection);

        let store = SqliteBlockStore::open(&path, 100, 800).unwrap();
        assert!(
            store
                .touch_channel_requested("channel:1", 1_000, 500)
                .unwrap()
        );
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
