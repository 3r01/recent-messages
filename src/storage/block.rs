use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::CanonicalRecord;

pub const BLOCK_FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedBlock {
    pub format_version: u16,
    pub channel_key: String,
    pub first_event_at_ms: i64,
    pub last_event_at_ms: i64,
    pub message_count: u32,
    pub uncompressed_bytes: u32,
    pub checksum: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct BlockPayload {
    records: Vec<CanonicalRecord>,
}

#[derive(Debug, Error)]
pub enum BlockError {
    #[error("cannot encode an empty block")]
    Empty,
    #[error("block contains records for multiple channels")]
    MixedChannels,
    #[error("unsupported block format version {0}")]
    UnsupportedVersion(u16),
    #[error("block checksum mismatch")]
    ChecksumMismatch,
    #[error("block metadata does not match its payload")]
    MetadataMismatch,
    #[error("block is too large to represent")]
    TooLarge,
    #[error("message-pack encoding failed: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("message-pack decoding failed: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("zstandard compression failed: {0}")]
    Compression(std::io::Error),
}

impl EncodedBlock {
    pub fn encode(records: &[CanonicalRecord], compression_level: i32) -> Result<Self, BlockError> {
        let first = records.first().ok_or(BlockError::Empty)?;
        if records
            .iter()
            .any(|record| record.channel_key != first.channel_key)
        {
            return Err(BlockError::MixedChannels);
        }

        let uncompressed = rmp_serde::to_vec_named(&BlockPayload {
            records: records.to_vec(),
        })?;
        let payload = zstd::stream::encode_all(uncompressed.as_slice(), compression_level)
            .map_err(BlockError::Compression)?;
        let message_count = u32::try_from(records.len()).map_err(|_| BlockError::TooLarge)?;
        let uncompressed_bytes =
            u32::try_from(uncompressed.len()).map_err(|_| BlockError::TooLarge)?;

        Ok(Self {
            format_version: BLOCK_FORMAT_VERSION,
            channel_key: first.channel_key.clone(),
            first_event_at_ms: records
                .iter()
                .map(|record| record.event_at_ms)
                .min()
                .unwrap(),
            last_event_at_ms: records
                .iter()
                .map(|record| record.event_at_ms)
                .max()
                .unwrap(),
            message_count,
            uncompressed_bytes,
            checksum: *blake3::hash(&payload).as_bytes(),
            payload,
        })
    }

    pub fn decode(&self) -> Result<Vec<CanonicalRecord>, BlockError> {
        if self.format_version != BLOCK_FORMAT_VERSION {
            return Err(BlockError::UnsupportedVersion(self.format_version));
        }
        if blake3::hash(&self.payload).as_bytes() != &self.checksum {
            return Err(BlockError::ChecksumMismatch);
        }

        let uncompressed =
            zstd::stream::decode_all(self.payload.as_slice()).map_err(BlockError::Compression)?;
        let decoded: BlockPayload = rmp_serde::from_slice(&uncompressed)?;
        let records = decoded.records;
        let metadata_matches = !records.is_empty()
            && records
                .iter()
                .all(|record| record.channel_key == self.channel_key)
            && records.len() == self.message_count as usize
            && uncompressed.len() == self.uncompressed_bytes as usize
            && records.iter().map(|record| record.event_at_ms).min()
                == Some(self.first_event_at_ms)
            && records.iter().map(|record| record.event_at_ms).max() == Some(self.last_event_at_ms);
        if !metadata_matches {
            return Err(BlockError::MetadataMismatch);
        }

        Ok(records)
    }

    pub fn compressed_bytes(&self) -> usize {
        self.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(sequence: u8) -> CanonicalRecord {
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

    #[test]
    fn round_trips_a_versioned_compressed_block() {
        let records = (0..100).map(record).collect::<Vec<_>>();
        let block = EncodedBlock::encode(&records, 3).unwrap();

        assert_eq!(block.format_version, BLOCK_FORMAT_VERSION);
        assert_eq!(block.message_count, 100);
        assert!(block.compressed_bytes() < block.uncompressed_bytes as usize);
        assert_eq!(block.decode().unwrap(), records);
    }

    #[test]
    fn detects_payload_corruption_before_decompression() {
        let mut block = EncodedBlock::encode(&[record(1)], 3).unwrap();
        block.payload[0] ^= 0xff;

        assert!(matches!(block.decode(), Err(BlockError::ChecksumMismatch)));
    }

    #[test]
    fn rejects_mixed_channel_blocks() {
        let first = record(1);
        let mut second = record(2);
        second.channel_key = "channel:2".to_owned();

        assert!(matches!(
            EncodedBlock::encode(&[first, second], 3),
            Err(BlockError::MixedChannels)
        ));
    }
}
