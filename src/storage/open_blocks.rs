use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use thiserror::Error;

use super::{CanonicalRecord, QueryRequest, record_fidelity_upgrade};

#[derive(Clone, Copy, Debug)]
pub struct OpenBlockLimits {
    pub block_messages: usize,
    pub max_open_channels: usize,
    pub max_open_bytes: usize,
    pub idle_seal_after_ms: u64,
    pub max_open_age_ms: u64,
}

impl OpenBlockLimits {
    fn validate(self) -> Result<Self, OpenBlockError> {
        if self.block_messages == 0
            || self.max_open_channels == 0
            || self.max_open_bytes == 0
            || self.idle_seal_after_ms == 0
            || self.max_open_age_ms == 0
        {
            return Err(OpenBlockError::InvalidLimits);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OpenBlockStats {
    pub open_channels: usize,
    pub open_messages: usize,
    pub estimated_bytes: usize,
}

#[derive(Debug, Error)]
pub enum OpenBlockError {
    #[error("open-block limits must all be greater than zero")]
    InvalidLimits,
    #[error("clock moved backwards for channel {0}")]
    ClockMovedBackwards(String),
    #[error("open-block lock poisoned")]
    LockPoisoned,
}

#[derive(Debug)]
pub struct SharedOpenBlocks {
    inner: RwLock<OpenBlockManager>,
}

impl SharedOpenBlocks {
    pub fn new(limits: OpenBlockLimits) -> Result<Self, OpenBlockError> {
        Ok(Self {
            inner: RwLock::new(OpenBlockManager::new(limits)?),
        })
    }

    pub fn append(
        &self,
        record: CanonicalRecord,
        now_ms: u64,
    ) -> Result<Vec<Vec<CanonicalRecord>>, OpenBlockError> {
        self.inner
            .write()
            .map_err(|_| OpenBlockError::LockPoisoned)?
            .append(record, now_ms)
    }

    pub fn seal_due(&self, now_ms: u64) -> Result<Vec<Vec<CanonicalRecord>>, OpenBlockError> {
        Ok(self
            .inner
            .write()
            .map_err(|_| OpenBlockError::LockPoisoned)?
            .seal_due(now_ms))
    }

    pub fn seal_all(&self) -> Result<Vec<Vec<CanonicalRecord>>, OpenBlockError> {
        Ok(self
            .inner
            .write()
            .map_err(|_| OpenBlockError::LockPoisoned)?
            .seal_all())
    }

    pub fn query(&self, request: &QueryRequest) -> Result<Vec<CanonicalRecord>, OpenBlockError> {
        let manager = self
            .inner
            .read()
            .map_err(|_| OpenBlockError::LockPoisoned)?;
        let mut records = manager
            .blocks
            .get(&request.channel_key)
            .map(|block| block.records.clone())
            .unwrap_or_default()
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
        records.sort_by_key(|record| (record.received_at_ms, record.event_at_ms, record.event_key));
        if records.len() > request.limit {
            records.drain(..records.len() - request.limit);
        }
        Ok(records)
    }

    pub fn stats(&self) -> Result<OpenBlockStats, OpenBlockError> {
        Ok(self
            .inner
            .read()
            .map_err(|_| OpenBlockError::LockPoisoned)?
            .stats())
    }

    pub fn purge_channel(&self, channel_key: &str) -> Result<(), OpenBlockError> {
        self.inner
            .write()
            .map_err(|_| OpenBlockError::LockPoisoned)?
            .take_block(channel_key);
        Ok(())
    }
}

pub type SharedOpenBlocksHandle = Arc<SharedOpenBlocks>;

#[derive(Debug)]
struct OpenBlock {
    records: Vec<CanonicalRecord>,
    event_indices: HashMap<[u8; 32], usize>,
    estimated_bytes: usize,
    opened_at_ms: u64,
    last_append_at_ms: u64,
}

#[derive(Debug)]
pub struct OpenBlockManager {
    limits: OpenBlockLimits,
    blocks: HashMap<String, OpenBlock>,
    estimated_bytes: usize,
    open_messages: usize,
}

impl OpenBlockManager {
    pub fn new(limits: OpenBlockLimits) -> Result<Self, OpenBlockError> {
        Ok(Self {
            limits: limits.validate()?,
            blocks: HashMap::new(),
            estimated_bytes: 0,
            open_messages: 0,
        })
    }

    pub fn append(
        &mut self,
        record: CanonicalRecord,
        now_ms: u64,
    ) -> Result<Vec<Vec<CanonicalRecord>>, OpenBlockError> {
        let channel_key = record.channel_key.clone();
        let record_bytes = estimated_record_bytes(&record);
        let block = self
            .blocks
            .entry(channel_key.clone())
            .or_insert_with(|| OpenBlock {
                records: Vec::new(),
                event_indices: HashMap::new(),
                estimated_bytes: 0,
                opened_at_ms: now_ms,
                last_append_at_ms: now_ms,
            });
        if now_ms < block.last_append_at_ms {
            return Err(OpenBlockError::ClockMovedBackwards(channel_key));
        }
        if let Some(index) = block.event_indices.get(&record.event_key).copied() {
            let retained = &mut block.records[index];
            if record.should_replace(retained) {
                let retained_bytes = estimated_record_bytes(retained);
                *retained = record;
                block.estimated_bytes = block
                    .estimated_bytes
                    .saturating_sub(retained_bytes)
                    .saturating_add(record_bytes);
                self.estimated_bytes = self
                    .estimated_bytes
                    .saturating_sub(retained_bytes)
                    .saturating_add(record_bytes);
                record_fidelity_upgrade("open_tail");
            }
            return Ok(Vec::new());
        }

        block
            .event_indices
            .insert(record.event_key, block.records.len());
        block.records.push(record);
        block.estimated_bytes = block.estimated_bytes.saturating_add(record_bytes);
        block.last_append_at_ms = now_ms;
        self.estimated_bytes = self.estimated_bytes.saturating_add(record_bytes);
        self.open_messages += 1;

        let mut sealed = Vec::new();
        if block.records.len() >= self.limits.block_messages
            && let Some(records) = self.take_block(&channel_key)
        {
            sealed.push(records);
        }
        self.enforce_bounds(&mut sealed);
        Ok(sealed)
    }

    pub fn seal_due(&mut self, now_ms: u64) -> Vec<Vec<CanonicalRecord>> {
        let due = self
            .blocks
            .iter()
            .filter_map(|(channel, block)| {
                let idle_for = now_ms.saturating_sub(block.last_append_at_ms);
                let age = now_ms.saturating_sub(block.opened_at_ms);
                (idle_for >= self.limits.idle_seal_after_ms || age >= self.limits.max_open_age_ms)
                    .then(|| channel.clone())
            })
            .collect::<Vec<_>>();
        due.into_iter()
            .filter_map(|channel| self.take_block(&channel))
            .collect()
    }

    pub fn seal_all(&mut self) -> Vec<Vec<CanonicalRecord>> {
        let channels = self.blocks.keys().cloned().collect::<Vec<_>>();
        channels
            .into_iter()
            .filter_map(|channel| self.take_block(&channel))
            .collect()
    }

    pub fn stats(&self) -> OpenBlockStats {
        OpenBlockStats {
            open_channels: self.blocks.len(),
            open_messages: self.open_messages,
            estimated_bytes: self.estimated_bytes,
        }
    }

    fn enforce_bounds(&mut self, sealed: &mut Vec<Vec<CanonicalRecord>>) {
        while self.blocks.len() > self.limits.max_open_channels
            || self.estimated_bytes > self.limits.max_open_bytes
        {
            let Some(channel) = self
                .blocks
                .iter()
                .min_by_key(|(channel, block)| {
                    (
                        block.last_append_at_ms,
                        block.opened_at_ms,
                        channel.as_str(),
                    )
                })
                .map(|(channel, _)| channel.clone())
            else {
                break;
            };
            if let Some(records) = self.take_block(&channel) {
                sealed.push(records);
            }
        }
    }

    fn take_block(&mut self, channel: &str) -> Option<Vec<CanonicalRecord>> {
        let block = self.blocks.remove(channel)?;
        self.estimated_bytes = self.estimated_bytes.saturating_sub(block.estimated_bytes);
        self.open_messages = self.open_messages.saturating_sub(block.records.len());
        Some(block.records)
    }
}

fn estimated_record_bytes(record: &CanonicalRecord) -> usize {
    record
        .raw_irc
        .len()
        .saturating_add(record.channel_key.len())
        .saturating_add(128)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> OpenBlockLimits {
        OpenBlockLimits {
            block_messages: 100,
            max_open_channels: 128,
            max_open_bytes: 128 * 1024,
            idle_seal_after_ms: 30_000,
            max_open_age_ms: 60_000,
        }
    }

    fn record(channel: usize, message: usize) -> CanonicalRecord {
        let channel_key = format!("channel:{channel}");
        let raw_irc = format!("@id={message} PRIVMSG #{channel} :hello").into_bytes();
        CanonicalRecord {
            event_at_ms: i64::try_from(message).unwrap(),
            received_at_ms: i64::try_from(message).unwrap(),
            event_key: CanonicalRecord::derive_event_key(&channel_key, &raw_irc),
            source_id: String::new(),
            fidelity: Default::default(),
            channel_key,
            raw_irc,
        }
    }

    #[test]
    fn seals_a_full_channel_block() {
        let mut manager = OpenBlockManager::new(limits()).unwrap();
        let mut sealed = Vec::new();
        for message in 0..100 {
            sealed.extend(manager.append(record(1, message), message as u64).unwrap());
        }

        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].len(), 100);
        assert_eq!(manager.stats(), OpenBlockStats::default());
    }

    #[test]
    fn bounds_thousands_of_one_message_quiet_channels() {
        let mut manager = OpenBlockManager::new(limits()).unwrap();
        let mut sealed = Vec::new();
        for channel in 0..10_000 {
            sealed.extend(manager.append(record(channel, 0), channel as u64).unwrap());
            let stats = manager.stats();
            assert!(stats.open_channels <= 128);
            assert!(stats.estimated_bytes <= 128 * 1024);
        }

        assert_eq!(manager.stats().open_channels, 128);
        assert_eq!(sealed.len(), 9_872);
        assert!(sealed.iter().all(|block| block.len() == 1));
    }

    #[test]
    fn one_global_sweep_seals_idle_and_old_blocks() {
        let mut manager = OpenBlockManager::new(limits()).unwrap();
        manager.append(record(1, 0), 10_000).unwrap();
        manager.append(record(2, 0), 20_000).unwrap();
        manager.append(record(1, 1), 40_000).unwrap();

        let sealed = manager.seal_due(60_000);
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0][0].channel_key, "channel:2");

        let sealed = manager.seal_due(71_000);
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0][0].channel_key, "channel:1");
        assert_eq!(manager.stats(), OpenBlockStats::default());
    }

    #[test]
    fn byte_pressure_seals_the_least_recently_active_channel() {
        let mut constrained = limits();
        constrained.max_open_channels = 10;
        constrained.max_open_bytes = 300;
        let mut manager = OpenBlockManager::new(constrained).unwrap();

        manager.append(record(1, 0), 0).unwrap();
        let sealed = manager.append(record(2, 0), 1).unwrap();

        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0][0].channel_key, "channel:1");
        assert_eq!(manager.stats().open_channels, 1);
    }

    #[test]
    fn deduplicates_an_event_while_the_channel_tail_is_open() {
        let mut manager = OpenBlockManager::new(limits()).unwrap();
        let message = record(1, 1);
        manager.append(message.clone(), 1).unwrap();
        assert!(manager.append(message, 2).unwrap().is_empty());
        assert_eq!(manager.stats().open_messages, 1);
    }

    #[test]
    fn upgrades_but_never_downgrades_an_open_event() {
        let mut manager = OpenBlockManager::new(limits()).unwrap();
        let mut reconstructed = record(1, 1);
        reconstructed.source_id = "firehose".to_owned();
        reconstructed.fidelity = crate::storage::SourceFidelity::Reconstructed;
        let mut direct = reconstructed.clone();
        direct.source_id = "owned-irc".to_owned();
        direct.fidelity = crate::storage::SourceFidelity::DirectIrc;
        direct.raw_irc.extend_from_slice(b";first-msg=1");

        manager.append(reconstructed.clone(), 1).unwrap();
        manager.append(direct.clone(), 2).unwrap();
        manager.append(reconstructed, 3).unwrap();

        let sealed = manager.seal_all();
        assert_eq!(sealed[0], vec![direct]);
    }
}
