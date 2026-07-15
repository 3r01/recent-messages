use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{mpsc, oneshot};

use super::eviction::BlockMeta;
use super::{
    CanonicalRecord, ExpireStats, JournalBatch, JournalStats, LocalResult, MessageStore,
    QueryRequest, SqliteBlockStore, StoreError, StoreStats,
};

enum WriterCommand {
    Append {
        batches: Vec<Vec<CanonicalRecord>>,
        response: oneshot::Sender<Result<(), StoreError>>,
    },
    Checkpoint {
        response: oneshot::Sender<Result<(), StoreError>>,
    },
    TouchChannelRequested {
        channel_key: String,
        requested_at_ms: u64,
        coalesce_before_ms: u64,
        response: oneshot::Sender<Result<bool, StoreError>>,
    },
    AcceptJournal {
        source_id: String,
        stream_id: String,
        first_sequence: u64,
        last_sequence: u64,
        checksum: [u8; 32],
        payload: Vec<u8>,
        now_ms: u64,
        response: oneshot::Sender<Result<u64, StoreError>>,
    },
    RetireJournal {
        source_id: String,
        stream_id: String,
        processed_through: u64,
        response: oneshot::Sender<Result<usize, StoreError>>,
    },
    PendingJournal {
        response: oneshot::Sender<Result<Vec<JournalBatch>, StoreError>>,
    },
    EvictBlocks {
        ids: Vec<u64>,
        response: oneshot::Sender<Result<usize, StoreError>>,
    },
    ExpireBlocks {
        cutoff_ms: i64,
        response: oneshot::Sender<Result<ExpireStats, StoreError>>,
    },
    PurgeChannel {
        channel_key: String,
        received_at_ms: i64,
        response: oneshot::Sender<Result<usize, StoreError>>,
    },
    SetChannelBlocked {
        channel_key: String,
        blocked: bool,
        response: oneshot::Sender<Result<usize, StoreError>>,
    },
    SyncBlockedChannels {
        channels: Vec<String>,
        response: oneshot::Sender<Result<(), StoreError>>,
    },
    SyncAlwaysJoinChannels {
        channels: Vec<String>,
        response: oneshot::Sender<Result<(), StoreError>>,
    },
}

struct AsyncSqliteInner {
    writer: mpsc::Sender<WriterCommand>,
    readers: Vec<Arc<SqliteBlockStore>>,
    maintenance_reader: Arc<SqliteBlockStore>,
    next_reader: AtomicUsize,
    path: PathBuf,
}

#[derive(Clone)]
pub struct AsyncSqliteBlockStore {
    inner: Arc<AsyncSqliteInner>,
}

impl AsyncSqliteBlockStore {
    pub fn open(
        path: impl AsRef<Path>,
        block_messages: usize,
        target_messages_per_channel: usize,
        writer_queue_capacity: usize,
        read_connections: usize,
    ) -> Result<Self, StoreError> {
        if writer_queue_capacity == 0 || read_connections == 0 {
            return Err(StoreError::Backend(
                "writer queue capacity and read connection count must be greater than zero"
                    .to_owned(),
            ));
        }
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                StoreError::Backend(format!(
                    "failed to create SQLite parent directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let writer_store =
            SqliteBlockStore::open(&path, block_messages, target_messages_per_channel)?;
        let mut readers = Vec::with_capacity(read_connections);
        for _ in 0..read_connections {
            readers.push(Arc::new(SqliteBlockStore::open(
                &path,
                block_messages,
                target_messages_per_channel,
            )?));
        }
        let maintenance_reader = Arc::new(SqliteBlockStore::open(
            &path,
            block_messages,
            target_messages_per_channel,
        )?);
        let (writer, mut receiver) = mpsc::channel(writer_queue_capacity);
        std::thread::Builder::new()
            .name("recent-messages-sqlite-writer".to_owned())
            .spawn(move || {
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        WriterCommand::Append { batches, response } => {
                            let _ = response.send(writer_store.append_channel_batches(batches));
                        }
                        WriterCommand::Checkpoint { response } => {
                            let _ = response.send(writer_store.checkpoint_wal());
                        }
                        WriterCommand::TouchChannelRequested {
                            channel_key,
                            requested_at_ms,
                            coalesce_before_ms,
                            response,
                        } => {
                            let _ = response.send(writer_store.touch_channel_requested(
                                &channel_key,
                                requested_at_ms,
                                coalesce_before_ms,
                            ));
                        }
                        WriterCommand::AcceptJournal {
                            source_id,
                            stream_id,
                            first_sequence,
                            last_sequence,
                            checksum,
                            payload,
                            now_ms,
                            response,
                        } => {
                            let result = writer_store.accept_journal_batch(
                                &source_id,
                                &stream_id,
                                first_sequence,
                                last_sequence,
                                checksum,
                                &payload,
                                now_ms,
                            );
                            let _ = response.send(result);
                        }
                        WriterCommand::RetireJournal {
                            source_id,
                            stream_id,
                            processed_through,
                            response,
                        } => {
                            let _ = response.send(writer_store.retire_journal_through(
                                &source_id,
                                &stream_id,
                                processed_through,
                            ));
                        }
                        WriterCommand::PendingJournal { response } => {
                            let _ = response.send(writer_store.pending_journal_batches());
                        }
                        WriterCommand::EvictBlocks { ids, response } => {
                            let _ = response.send(writer_store.evict_block_ids(&ids));
                        }
                        WriterCommand::ExpireBlocks {
                            cutoff_ms,
                            response,
                        } => {
                            let _ = response.send(writer_store.expire_blocks_before(cutoff_ms));
                        }
                        WriterCommand::PurgeChannel {
                            channel_key,
                            received_at_ms,
                            response,
                        } => {
                            let _ = response
                                .send(writer_store.purge_channel(&channel_key, received_at_ms));
                        }
                        WriterCommand::SetChannelBlocked {
                            channel_key,
                            blocked,
                            response,
                        } => {
                            let _ = response
                                .send(writer_store.set_channel_blocked(&channel_key, blocked));
                        }
                        WriterCommand::SyncBlockedChannels { channels, response } => {
                            let _ = response.send(writer_store.sync_blocked_channels(&channels));
                        }
                        WriterCommand::SyncAlwaysJoinChannels { channels, response } => {
                            let _ =
                                response.send(writer_store.sync_always_join_channels(&channels));
                        }
                    }
                }
            })
            .map_err(|error| StoreError::Backend(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(AsyncSqliteInner {
                writer,
                readers,
                maintenance_reader,
                next_reader: AtomicUsize::new(0),
                path,
            }),
        })
    }

    pub async fn append_channel_batches(
        &self,
        batches: Vec<Vec<CanonicalRecord>>,
    ) -> Result<(), StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::Append { batches, response })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn checkpoint_wal(&self) -> Result<(), StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::Checkpoint { response })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn check_readable(&self) -> Result<(), StoreError> {
        let reader = self.reader();
        tokio::task::spawn_blocking(move || reader.check_readable())
            .await
            .map_err(join_error)?
    }

    pub async fn touch_channel_requested(
        &self,
        channel_key: String,
        requested_at_ms: u64,
        coalesce_before_ms: u64,
    ) -> Result<bool, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::TouchChannelRequested {
                channel_key,
                requested_at_ms,
                coalesce_before_ms,
                response,
            })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn accept_journal_batch(
        &self,
        source_id: String,
        stream_id: String,
        first_sequence: u64,
        last_sequence: u64,
        checksum: [u8; 32],
        payload: Vec<u8>,
        now_ms: u64,
    ) -> Result<u64, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::AcceptJournal {
                source_id,
                stream_id,
                first_sequence,
                last_sequence,
                checksum,
                payload,
                now_ms,
                response,
            })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn retire_journal_through(
        &self,
        source_id: String,
        stream_id: String,
        processed_through: u64,
    ) -> Result<usize, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::RetireJournal {
                source_id,
                stream_id,
                processed_through,
                response,
            })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn pending_journal_batches(&self) -> Result<Vec<JournalBatch>, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::PendingJournal { response })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn journal_stats(&self) -> Result<JournalStats, StoreError> {
        let reader = self.maintenance_reader();
        tokio::task::spawn_blocking(move || reader.journal_stats())
            .await
            .map_err(join_error)?
    }

    pub async fn physical_bytes(&self) -> Result<u64, StoreError> {
        let reader = self.maintenance_reader();
        tokio::task::spawn_blocking(move || reader.physical_bytes())
            .await
            .map_err(join_error)?
    }

    pub(crate) async fn pressure_blocks(&self) -> Result<Vec<BlockMeta>, StoreError> {
        let reader = self.maintenance_reader();
        tokio::task::spawn_blocking(move || reader.pressure_blocks())
            .await
            .map_err(join_error)?
    }

    pub(crate) async fn evict_block_ids(&self, ids: Vec<u64>) -> Result<usize, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::EvictBlocks { ids, response })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn expire_blocks_before(&self, cutoff_ms: i64) -> Result<ExpireStats, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::ExpireBlocks {
                cutoff_ms,
                response,
            })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn purge_channel(
        &self,
        channel_key: String,
        received_at_ms: i64,
    ) -> Result<usize, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::PurgeChannel {
                channel_key,
                received_at_ms,
                response,
            })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn set_channel_blocked(
        &self,
        channel_key: String,
        blocked: bool,
    ) -> Result<usize, StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::SetChannelBlocked {
                channel_key,
                blocked,
                response,
            })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn sync_blocked_channels(&self, channels: Vec<String>) -> Result<(), StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::SyncBlockedChannels { channels, response })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub async fn sync_always_join_channels(&self, channels: Vec<String>) -> Result<(), StoreError> {
        let (response, result) = oneshot::channel();
        self.inner
            .writer
            .send(WriterCommand::SyncAlwaysJoinChannels { channels, response })
            .await
            .map_err(|_| writer_closed())?;
        result.await.map_err(|_| writer_closed())?
    }

    pub(crate) fn path(&self) -> &Path {
        &self.inner.path
    }

    fn reader(&self) -> Arc<SqliteBlockStore> {
        let index = self.inner.next_reader.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&self.inner.readers[index % self.inner.readers.len()])
    }

    fn maintenance_reader(&self) -> Arc<SqliteBlockStore> {
        Arc::clone(&self.inner.maintenance_reader)
    }
}

impl MessageStore for AsyncSqliteBlockStore {
    async fn append_batch(&self, records: Vec<CanonicalRecord>) -> Result<(), StoreError> {
        self.append_channel_batches(vec![records]).await
    }

    async fn query(&self, request: QueryRequest) -> Result<LocalResult, StoreError> {
        let reader = self.reader();
        tokio::task::spawn_blocking(move || futures::executor::block_on(reader.query(request)))
            .await
            .map_err(join_error)?
    }

    async fn stats(&self) -> Result<StoreStats, StoreError> {
        let reader = self.maintenance_reader();
        tokio::task::spawn_blocking(move || futures::executor::block_on(reader.stats()))
            .await
            .map_err(join_error)?
    }
}

fn writer_closed() -> StoreError {
    StoreError::Backend("SQLite writer thread closed unexpectedly".to_owned())
}

fn join_error(error: tokio::task::JoinError) -> StoreError {
    StoreError::Backend(format!("SQLite read worker failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn path() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rm-async-sqlite-{unique}.sqlite"))
    }

    fn record(channel: usize, sequence: usize) -> CanonicalRecord {
        let channel_key = format!("channel:{channel}");
        let raw_irc = format!("@id={sequence} PRIVMSG #channel{channel} :hello").into_bytes();
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

    #[test]
    fn creates_missing_parent_directories() {
        let base = path().with_extension("directory");
        let nested = base.join("nested/store.sqlite");
        let store = AsyncSqliteBlockStore::open(&nested, 2, 4, 2, 1).unwrap();
        assert!(nested.exists());
        drop(store);
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn bounded_writer_and_read_pool_round_trip() {
        let path = path();
        let store = AsyncSqliteBlockStore::open(&path, 2, 4, 2, 2).unwrap();
        let writes = (0..8).map(|channel| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append_batch(vec![record(channel, 1), record(channel, 2)])
                    .await
            })
        });
        for write in writes {
            write.await.unwrap().unwrap();
        }
        let queries = (0..8).map(|channel| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .query(QueryRequest {
                        channel_key: format!("channel:{channel}"),
                        after_ms: None,
                        before_ms: None,
                        limit: 4,
                    })
                    .await
            })
        });
        for query in queries {
            assert_eq!(query.await.unwrap().unwrap().records.len(), 2);
        }
        assert_eq!(store.stats().await.unwrap().messages, 16);
        store.checkpoint_wal().await.unwrap();
        drop(store);
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_priority_uses_the_bounded_writer() {
        let path = path();
        let store = AsyncSqliteBlockStore::open(&path, 2, 4, 2, 1).unwrap();
        store.append_batch(vec![record(0, 1)]).await.unwrap();

        assert!(
            store
                .touch_channel_requested("channel:0".to_owned(), 1_000, 500)
                .await
                .unwrap()
        );
        assert_eq!(
            store.pressure_blocks().await.unwrap()[0].last_requested_at_ms,
            1_000
        );
        store
            .sync_always_join_channels(vec!["channel:0".to_owned()])
            .await
            .unwrap();
        assert!(store.pressure_blocks().await.unwrap()[0].always_join);
        drop(store);
        cleanup(&path);
    }

    #[tokio::test]
    async fn blocked_channel_is_purged_and_rejects_future_batches() {
        let path = path();
        let store = AsyncSqliteBlockStore::open(&path, 2, 4, 4, 1).unwrap();
        store
            .append_batch(vec![record(0, 1), record(0, 2)])
            .await
            .unwrap();
        assert_eq!(
            store
                .set_channel_blocked("channel:0".to_owned(), true)
                .await
                .unwrap(),
            1
        );
        store.append_batch(vec![record(0, 3)]).await.unwrap();
        let query = || QueryRequest {
            channel_key: "channel:0".to_owned(),
            after_ms: None,
            before_ms: None,
            limit: 4,
        };
        assert!(store.query(query()).await.unwrap().records.is_empty());

        store
            .set_channel_blocked("channel:0".to_owned(), false)
            .await
            .unwrap();
        store.append_batch(vec![record(0, 4)]).await.unwrap();
        assert_eq!(store.query(query()).await.unwrap().records.len(), 1);
        drop(store);
        cleanup(&path);
    }

    #[tokio::test]
    async fn purge_cutoff_rejects_pre_purge_open_tail_records() {
        let path = path();
        let store = AsyncSqliteBlockStore::open(&path, 2, 4, 4, 1).unwrap();
        store
            .append_batch(vec![record(0, 1), record(0, 2)])
            .await
            .unwrap();
        store
            .purge_channel("channel:0".to_owned(), 10)
            .await
            .unwrap();
        store
            .append_batch(vec![record(0, 3), record(0, 11)])
            .await
            .unwrap();
        let result = store
            .query(QueryRequest {
                channel_key: "channel:0".to_owned(),
                after_ms: None,
                before_ms: None,
                limit: 4,
            })
            .await
            .unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].received_at_ms, 11);
        drop(store);
        cleanup(&path);
    }
}
