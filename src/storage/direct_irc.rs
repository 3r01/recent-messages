use thiserror::Error;

use super::{
    CanonicalRecord, RawIrcError, RawSourceBatch, SourceFidelity, canonicalize_raw_irc_from,
};

#[derive(Debug, Error)]
pub enum DirectIrcBatcherError {
    #[error("direct IRC source and stream IDs and batch size must be non-empty")]
    InvalidConfig,
}

pub struct DirectIrcBatcher {
    source_id: String,
    stream_id: String,
    batch_messages: usize,
    next_sequence: u64,
    records: Vec<CanonicalRecord>,
}

impl DirectIrcBatcher {
    pub fn new(
        source_id: impl Into<String>,
        stream_id: impl Into<String>,
        batch_messages: usize,
    ) -> Result<Self, DirectIrcBatcherError> {
        let source_id = source_id.into();
        let stream_id = stream_id.into();
        if source_id.is_empty() || stream_id.is_empty() || batch_messages == 0 {
            return Err(DirectIrcBatcherError::InvalidConfig);
        }
        Ok(Self {
            source_id,
            stream_id,
            batch_messages,
            next_sequence: 1,
            records: Vec::with_capacity(batch_messages),
        })
    }

    pub fn push_raw(
        &mut self,
        raw: &str,
        received_at_ms: i64,
    ) -> Result<Option<RawSourceBatch>, RawIrcError> {
        let record = canonicalize_raw_irc_from(
            raw,
            received_at_ms,
            &self.source_id,
            SourceFidelity::DirectIrc,
        )?;
        self.records.push(record);
        if self.records.len() >= self.batch_messages {
            Ok(self.flush())
        } else {
            Ok(None)
        }
    }

    #[must_use]
    pub fn flush(&mut self) -> Option<RawSourceBatch> {
        if self.records.is_empty() {
            return None;
        }
        let records = std::mem::replace(&mut self.records, Vec::with_capacity(self.batch_messages));
        let first_sequence = self.next_sequence;
        let last_sequence = first_sequence.saturating_add(records.len() as u64 - 1);
        self.next_sequence = last_sequence.saturating_add(1);
        Some(RawSourceBatch {
            source_id: self.source_id.clone(),
            stream_id: self.stream_id.clone(),
            first_sequence,
            last_sequence,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(id: usize) -> String {
        format!(
            "@id={id};tmi-sent-ts={id} :user!user@user.tmi.twitch.tv PRIVMSG #Channel :message {id}"
        )
    }

    #[test]
    fn emits_contiguous_direct_fidelity_batches() {
        let mut batcher = DirectIrcBatcher::new("owned-irc", "startup-1", 2).unwrap();
        assert!(batcher.push_raw(&raw(1), 101).unwrap().is_none());
        let first = batcher.push_raw(&raw(2), 102).unwrap().unwrap();
        assert_eq!((first.first_sequence, first.last_sequence), (1, 2));
        assert_eq!(first.records[0].channel_key, "channel");
        assert!(
            first
                .records
                .iter()
                .all(|record| record.fidelity == SourceFidelity::DirectIrc)
        );

        assert!(batcher.push_raw(&raw(3), 103).unwrap().is_none());
        let second = batcher.flush().unwrap();
        assert_eq!((second.first_sequence, second.last_sequence), (3, 3));
    }

    #[test]
    fn rejected_lines_do_not_create_gaps() {
        let mut batcher = DirectIrcBatcher::new("owned-irc", "startup-1", 10).unwrap();
        assert!(batcher.push_raw("PING :tmi.twitch.tv", 1).is_err());
        batcher.push_raw(&raw(1), 2).unwrap();
        let batch = batcher.flush().unwrap();
        assert_eq!((batch.first_sequence, batch.last_sequence), (1, 1));
    }
}
