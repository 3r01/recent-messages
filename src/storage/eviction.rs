use std::collections::{HashMap, HashSet};

use thiserror::Error;

#[derive(Clone, Debug)]
pub struct BlockMeta {
    pub id: u64,
    pub channel_key: String,
    pub message_count: usize,
    pub bytes: u64,
    pub last_requested_at_ms: u64,
    pub last_event_at_ms: u64,
    pub always_join: bool,
    pub journal_protected: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct PressurePolicy {
    pub effective_max_bytes: u64,
    pub high_water_ratio: f64,
    pub target_ratio: f64,
    pub emergency_ratio: f64,
    pub pressure_floor_messages_per_channel: usize,
    pub requested_after_ms: u64,
}

impl PressurePolicy {
    fn thresholds(self) -> Result<(u64, u64, u64), EvictionError> {
        if self.effective_max_bytes == 0
            || !(0.0..=1.0).contains(&self.target_ratio)
            || !(0.0..=1.0).contains(&self.high_water_ratio)
            || !(0.0..=1.0).contains(&self.emergency_ratio)
            || self.target_ratio >= self.high_water_ratio
            || self.high_water_ratio >= self.emergency_ratio
        {
            return Err(EvictionError::InvalidPolicy);
        }
        Ok((
            (self.effective_max_bytes as f64 * self.target_ratio) as u64,
            (self.effective_max_bytes as f64 * self.high_water_ratio) as u64,
            (self.effective_max_bytes as f64 * self.emergency_ratio) as u64,
        ))
    }

    pub(crate) fn below_high_water(self, current_store_bytes: u64) -> Result<bool, EvictionError> {
        Ok(current_store_bytes < self.thresholds()?.1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PressureMode {
    Normal,
    HighWater,
    Emergency,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvictionPlan {
    pub mode: PressureMode,
    pub evict_block_ids: Vec<u64>,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub crossed_channel_floor: bool,
}

#[derive(Debug, Error)]
pub enum EvictionError {
    #[error("invalid pressure-eviction policy")]
    InvalidPolicy,
    #[error("protected or unavailable blocks prevent reaching the target")]
    TargetUnreachable,
}

pub fn plan_pressure_eviction(
    blocks: &[BlockMeta],
    current_store_bytes: u64,
    policy: PressurePolicy,
) -> Result<EvictionPlan, EvictionError> {
    let (target_bytes, high_water_bytes, emergency_bytes) = policy.thresholds()?;
    if current_store_bytes < high_water_bytes {
        return Ok(EvictionPlan {
            mode: PressureMode::Normal,
            evict_block_ids: Vec::new(),
            bytes_before: current_store_bytes,
            bytes_after: current_store_bytes,
            crossed_channel_floor: false,
        });
    }

    let mode = if current_store_bytes >= emergency_bytes {
        PressureMode::Emergency
    } else {
        PressureMode::HighWater
    };
    let mut candidates = blocks
        .iter()
        .filter(|block| !block.journal_protected)
        .collect::<Vec<_>>();
    candidates.sort_by_key(|block| {
        (
            request_tier(
                block.always_join,
                block.last_requested_at_ms,
                policy.requested_after_ms,
            ),
            block.last_requested_at_ms,
            block.last_event_at_ms,
            block.id,
        )
    });

    let mut retained_by_channel = HashMap::<&str, usize>::new();
    for block in blocks {
        *retained_by_channel
            .entry(block.channel_key.as_str())
            .or_default() += block.message_count;
    }
    let currently_requested = blocks
        .iter()
        .filter(|block| {
            request_tier(
                block.always_join,
                block.last_requested_at_ms,
                policy.requested_after_ms,
            ) >= 2
        })
        .map(|block| block.channel_key.as_str())
        .collect::<HashSet<_>>();
    let mut bytes_after = current_store_bytes;
    let mut selected = Vec::new();
    let mut deferred_below_floor = Vec::new();

    for block in candidates {
        if bytes_after <= target_bytes {
            break;
        }
        let retained = retained_by_channel[block.channel_key.as_str()];
        let floor = if currently_requested.contains(block.channel_key.as_str()) {
            policy.pressure_floor_messages_per_channel
        } else {
            0
        };
        if retained.saturating_sub(block.message_count) < floor {
            deferred_below_floor.push(block);
            continue;
        }
        retained_by_channel.insert(
            block.channel_key.as_str(),
            retained.saturating_sub(block.message_count),
        );
        bytes_after = bytes_after.saturating_sub(block.bytes);
        selected.push(block.id);
    }

    let mut crossed_channel_floor = false;
    if bytes_after > target_bytes && mode == PressureMode::Emergency {
        crossed_channel_floor = true;
        for block in deferred_below_floor {
            if bytes_after <= target_bytes {
                break;
            }
            bytes_after = bytes_after.saturating_sub(block.bytes);
            selected.push(block.id);
        }
    }
    if bytes_after > target_bytes {
        return Err(EvictionError::TargetUnreachable);
    }

    Ok(EvictionPlan {
        mode,
        evict_block_ids: selected,
        bytes_before: current_store_bytes,
        bytes_after,
        crossed_channel_floor,
    })
}

fn request_tier(always_join: bool, last_requested_at_ms: u64, requested_after_ms: u64) -> u8 {
    if always_join {
        3
    } else if last_requested_at_ms == 0 {
        0
    } else if last_requested_at_ms < requested_after_ms {
        1
    } else {
        2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(id: u64, channel: &str, last_requested_at_ms: u64) -> BlockMeta {
        BlockMeta {
            id,
            channel_key: channel.to_owned(),
            message_count: 100,
            bytes: 100,
            last_requested_at_ms,
            last_event_at_ms: id,
            always_join: false,
            journal_protected: false,
        }
    }

    fn policy() -> PressurePolicy {
        PressurePolicy {
            effective_max_bytes: 1_000,
            high_water_ratio: 0.90,
            target_ratio: 0.80,
            emergency_ratio: 0.98,
            pressure_floor_messages_per_channel: 50,
            requested_after_ms: 100,
        }
    }

    #[test]
    fn does_nothing_below_high_water() {
        let plan = plan_pressure_eviction(&[], 899, policy()).unwrap();
        assert_eq!(plan.mode, PressureMode::Normal);
        assert!(plan.evict_block_ids.is_empty());
    }

    #[test]
    fn evicts_whole_lower_value_blocks_down_to_target() {
        let blocks = vec![
            block(1, "a", 200),
            block(2, "a", 200),
            block(3, "b", 0),
            block(4, "b", 0),
        ];
        let plan = plan_pressure_eviction(&blocks, 950, policy()).unwrap();

        assert_eq!(plan.mode, PressureMode::HighWater);
        assert_eq!(plan.evict_block_ids, vec![3, 4]);
        assert_eq!(plan.bytes_after, 750);
        assert!(!plan.crossed_channel_floor);
    }

    #[test]
    fn never_evicts_protected_journal_data() {
        let mut protected = block(1, "a", 0);
        protected.journal_protected = true;
        assert!(matches!(
            plan_pressure_eviction(&[protected], 950, policy()),
            Err(EvictionError::TargetUnreachable)
        ));
    }

    #[test]
    fn only_crosses_channel_floor_in_emergency() {
        let mut only_block = block(1, "a", 200);
        only_block.bytes = 200;
        let blocks = vec![only_block];
        assert!(matches!(
            plan_pressure_eviction(&blocks, 950, policy()),
            Err(EvictionError::TargetUnreachable)
        ));

        let plan = plan_pressure_eviction(&blocks, 990, policy()).unwrap();
        assert_eq!(plan.mode, PressureMode::Emergency);
        assert_eq!(plan.evict_block_ids, vec![1]);
        assert!(plan.crossed_channel_floor);
    }

    #[test]
    fn never_requested_channels_have_no_pressure_floor() {
        let mut incidental = block(1, "firehose-only", 0);
        incidental.bytes = 200;
        let blocks = vec![incidental, block(2, "requested", 200)];
        let plan = plan_pressure_eviction(&blocks, 950, policy()).unwrap();

        assert_eq!(plan.evict_block_ids, vec![1]);
        assert_eq!(plan.bytes_after, 750);
    }

    #[test]
    fn stale_requests_rank_between_incidental_and_current_channels() {
        let blocks = vec![
            block(1, "current", 200),
            block(2, "stale", 50),
            block(3, "incidental", 0),
        ];
        let mut policy = policy();
        policy.target_ratio = 0.70;
        policy.pressure_floor_messages_per_channel = 0;
        let plan = plan_pressure_eviction(&blocks, 950, policy).unwrap();

        assert_eq!(plan.evict_block_ids, vec![3, 2, 1]);
    }

    #[test]
    fn always_join_channels_rank_after_requested_channels() {
        let mut pinned = block(1, "pinned", 0);
        pinned.always_join = true;
        let blocks = vec![
            pinned,
            block(2, "requested", 200),
            block(3, "incidental", 0),
        ];
        let mut policy = policy();
        policy.target_ratio = 0.70;
        policy.pressure_floor_messages_per_channel = 0;
        let plan = plan_pressure_eviction(&blocks, 950, policy).unwrap();

        assert_eq!(plan.evict_block_ids, vec![3, 2, 1]);
    }
}
