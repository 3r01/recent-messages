use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::RwLock;

use super::block::EncodedBlock;
use super::{CanonicalRecord, LocalResult, MessageStore, QueryRequest, StoreError, StoreStats};

#[derive(Debug, Default)]
struct ChannelBlocks {
    blocks: VecDeque<EncodedBlock>,
    seen_event_keys: HashSet<[u8; 32]>,
    message_count: usize,
}

#[derive(Debug)]
pub struct MemoryBlockStore {
    block_messages: usize,
    target_messages_per_channel: usize,
    compression_level: i32,
    channels: RwLock<HashMap<String, ChannelBlocks>>,
}

impl MemoryBlockStore {
    pub fn new(block_messages: usize, target_messages_per_channel: usize) -> Self {
        assert!(block_messages > 0);
        assert!(target_messages_per_channel > 0);
        Self {
            block_messages,
            target_messages_per_channel,
            compression_level: 3,
            channels: RwLock::new(HashMap::new()),
        }
    }

    fn allowed_messages(&self) -> usize {
        self.target_messages_per_channel
            .saturating_add(self.block_messages.saturating_sub(1))
    }

    fn decode_blocks(blocks: &VecDeque<EncodedBlock>) -> Result<Vec<CanonicalRecord>, StoreError> {
        let mut records = Vec::new();
        for block in blocks {
            records.extend(block.decode()?);
        }
        Ok(records)
    }
}

impl MessageStore for MemoryBlockStore {
    async fn append_batch(&self, mut records: Vec<CanonicalRecord>) -> Result<(), StoreError> {
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
        records.sort_by_key(|record| (record.event_at_ms, record.received_at_ms, record.event_key));
        let mut channels = self
            .channels
            .write()
            .map_err(|_| StoreError::LockPoisoned)?;
        let channel = channels.entry(channel_key).or_default();
        records.retain(|record| channel.seen_event_keys.insert(record.event_key));

        for chunk in records.chunks(self.block_messages) {
            let block = EncodedBlock::encode(chunk, self.compression_level)?;
            channel.message_count += chunk.len();
            channel.blocks.push_back(block);
        }

        while channel.message_count > self.allowed_messages() {
            let Some(evicted) = channel.blocks.pop_front() else {
                break;
            };
            for record in evicted.decode()? {
                channel.seen_event_keys.remove(&record.event_key);
                channel.message_count = channel.message_count.saturating_sub(1);
            }
        }
        Ok(())
    }

    async fn query(&self, request: QueryRequest) -> Result<LocalResult, StoreError> {
        if request.limit == 0 {
            return Err(StoreError::InvalidLimit);
        }
        let channels = self.channels.read().map_err(|_| StoreError::LockPoisoned)?;
        let mut all_records = channels.get(&request.channel_key).map_or_else(
            || Ok(Vec::new()),
            |channel| Self::decode_blocks(&channel.blocks),
        )?;
        all_records
            .sort_by_key(|record| (record.received_at_ms, record.event_at_ms, record.event_key));
        let oldest_retained_at_ms = all_records.first().map(|record| record.received_at_ms);
        let newest_retained_at_ms = all_records.last().map(|record| record.received_at_ms);
        let mut records = all_records
            .into_iter()
            .filter(|record| {
                request
                    .after_ms
                    .is_none_or(|after| record.received_at_ms > after)
                    && request
                        .before_ms
                        .is_none_or(|before| record.received_at_ms < before)
            })
            .collect::<Vec<_>>();
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
        let channels = self.channels.read().map_err(|_| StoreError::LockPoisoned)?;
        let blocks = channels
            .values()
            .flat_map(|channel| channel.blocks.iter())
            .collect::<Vec<_>>();
        Ok(StoreStats {
            channels: channels.len(),
            blocks: blocks.len(),
            messages: channels.values().map(|channel| channel.message_count).sum(),
            compressed_bytes: blocks.iter().map(|block| block.payload.len() as u64).sum(),
            uncompressed_bytes: blocks
                .iter()
                .map(|block| u64::from(block.uncompressed_bytes))
                .sum(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(sequence: u16) -> CanonicalRecord {
        let raw_irc = format!("@id={sequence} PRIVMSG #channel :hello {sequence}").into_bytes();
        CanonicalRecord {
            channel_key: "channel:1".to_owned(),
            event_at_ms: 1_700_000_000_000 + i64::from(sequence),
            received_at_ms: 1_700_000_000_100 + i64::from(sequence),
            event_key: CanonicalRecord::derive_event_key("channel:1", &raw_irc),
            source_id: String::new(),
            fidelity: Default::default(),
            raw_irc,
        }
    }

    #[tokio::test]
    async fn appends_deduplicates_orders_and_queries_blocks() {
        let store = MemoryBlockStore::new(2, 10);
        store
            .append_batch(vec![record(3), record(1), record(2), record(2)])
            .await
            .unwrap();

        let result = store
            .query(QueryRequest {
                channel_key: "channel:1".to_owned(),
                after_ms: Some(1_700_000_000_101),
                before_ms: None,
                limit: 2,
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
        assert_eq!(store.stats().await.unwrap().blocks, 2);
    }

    #[tokio::test]
    async fn permits_one_block_of_overshoot_then_evicts_a_whole_block() {
        let store = MemoryBlockStore::new(2, 3);
        store
            .append_batch((0..4).map(record).collect())
            .await
            .unwrap();
        assert_eq!(store.stats().await.unwrap().messages, 4);

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
        assert_eq!(result.oldest_retained_at_ms, Some(1_700_000_000_102));
        assert_eq!(result.newest_retained_at_ms, Some(1_700_000_000_104));
    }

    #[tokio::test]
    async fn appending_does_not_rewrite_existing_blocks() {
        let store = MemoryBlockStore::new(2, 10);
        store
            .append_batch(vec![record(0), record(1)])
            .await
            .unwrap();
        let first_payload = {
            let channels = store.channels.read().unwrap();
            channels["channel:1"].blocks[0].payload.clone()
        };

        store
            .append_batch(vec![record(2), record(3)])
            .await
            .unwrap();
        let channels = store.channels.read().unwrap();
        assert_eq!(channels["channel:1"].blocks[0].payload, first_payload);
        assert_eq!(channels["channel:1"].blocks.len(), 2);
    }
}
