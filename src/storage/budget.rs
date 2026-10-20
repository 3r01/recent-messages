use std::path::Path;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prometheus::{IntCounter, IntGauge, register_int_counter, register_int_gauge};
use thiserror::Error;

use super::eviction::{EvictionError, PressureMode, PressurePolicy, plan_pressure_eviction};
use super::{AsyncSqliteBlockStore, StoreError};

static PHYSICAL_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_storage_physical_bytes",
        "Physical bytes used by SQLite database, WAL, and SHM"
    )
    .unwrap()
});
static EFFECTIVE_MAX_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_storage_effective_max_bytes",
        "Current strictest storage ceiling from node and filesystem limits"
    )
    .unwrap()
});
static EVICTED_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "recentmessages_storage_evicted_blocks_total",
        "Whole SQLite blocks removed by physical budget enforcement"
    )
    .unwrap()
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemSpace {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct StorageBudget {
    pub max_bytes: u64,
    pub max_filesystem_percent: u8,
    pub min_free_bytes: u64,
    pub wal_reserve_bytes: u64,
    pub high_water_ratio: f64,
    pub target_ratio: f64,
    pub emergency_ratio: f64,
    pub pressure_floor_messages_per_channel: usize,
    pub requested_channel_protect_for: Duration,
    pub max_enforcement_passes: usize,
}

#[derive(Debug, Error)]
pub enum StorageBudgetError {
    #[error("invalid storage budget")]
    InvalidBudget,
    #[error("filesystem capacity query failed: {0}")]
    Filesystem(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Eviction(#[from] EvictionError),
    #[error("physical storage remains above its target after bounded enforcement")]
    EnforcementIncomplete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetEnforcement {
    pub mode: PressureMode,
    pub effective_max_bytes: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub evicted_blocks: usize,
    pub passes: usize,
}

#[derive(Clone)]
pub struct StorageBudgetEnforcer {
    store: AsyncSqliteBlockStore,
    budget: StorageBudget,
}

impl StorageBudgetEnforcer {
    pub fn new(
        store: AsyncSqliteBlockStore,
        budget: StorageBudget,
    ) -> Result<Self, StorageBudgetError> {
        validate(budget)?;
        Ok(Self { store, budget })
    }

    pub async fn enforce(&self) -> Result<BudgetEnforcement, StorageBudgetError> {
        self.enforce_with(|_| filesystem_space(self.store.path()))
            .await
    }

    async fn enforce_with<F>(&self, mut measure: F) -> Result<BudgetEnforcement, StorageBudgetError>
    where
        F: FnMut(u64) -> Result<FilesystemSpace, StorageBudgetError>,
    {
        let bytes_before = self.store.physical_bytes().await?;
        let mut current = bytes_before;
        let mut effective = 0;
        let mut evicted_blocks = 0;
        let mut final_mode = PressureMode::Normal;
        for pass in 0..self.budget.max_enforcement_passes {
            let filesystem = measure(current)?;
            effective = effective_max_bytes(self.budget, current, filesystem)?;
            PHYSICAL_BYTES.set(i64::try_from(current).unwrap_or(i64::MAX));
            EFFECTIVE_MAX_BYTES.set(i64::try_from(effective).unwrap_or(i64::MAX));
            let policy = PressurePolicy {
                effective_max_bytes: effective.max(1),
                high_water_ratio: self.budget.high_water_ratio,
                target_ratio: self.budget.target_ratio,
                emergency_ratio: self.budget.emergency_ratio,
                pressure_floor_messages_per_channel: self
                    .budget
                    .pressure_floor_messages_per_channel,
                requested_after_ms: unix_millis()
                    .saturating_sub(self.budget.requested_channel_protect_for.as_millis() as u64),
            };
            if policy.below_high_water(current)? {
                return Ok(BudgetEnforcement {
                    mode: PressureMode::Normal,
                    effective_max_bytes: effective,
                    bytes_before,
                    bytes_after: current,
                    evicted_blocks,
                    passes: pass,
                });
            }
            let mut blocks = self.store.pressure_blocks().await?;
            distribute_physical_overhead(&mut blocks, current);
            let plan = plan_pressure_eviction(&blocks, current, policy)?;
            final_mode = plan.mode;
            if plan.evict_block_ids.is_empty() {
                return Ok(BudgetEnforcement {
                    mode: final_mode,
                    effective_max_bytes: effective,
                    bytes_before,
                    bytes_after: current,
                    evicted_blocks,
                    passes: pass,
                });
            }
            let deleted = self.store.evict_block_ids(plan.evict_block_ids).await?;
            evicted_blocks = evicted_blocks.saturating_add(deleted);
            EVICTED_BLOCKS.inc_by(u64::try_from(deleted).unwrap_or(u64::MAX));
            current = self.store.physical_bytes().await?;
        }
        PHYSICAL_BYTES.set(i64::try_from(current).unwrap_or(i64::MAX));
        let high_water = (effective as f64 * self.budget.high_water_ratio) as u64;
        if current >= high_water {
            return Err(StorageBudgetError::EnforcementIncomplete);
        }
        Ok(BudgetEnforcement {
            mode: final_mode,
            effective_max_bytes: effective,
            bytes_before,
            bytes_after: current,
            evicted_blocks,
            passes: self.budget.max_enforcement_passes,
        })
    }
}

fn distribute_physical_overhead(blocks: &mut [super::eviction::BlockMeta], physical_bytes: u64) {
    if blocks.is_empty() {
        return;
    }
    let attributed = blocks.iter().map(|block| block.bytes).sum::<u64>();
    let overhead = physical_bytes.saturating_sub(attributed);
    let count = u64::try_from(blocks.len()).unwrap_or(u64::MAX).max(1);
    let each = overhead / count;
    let mut remainder = overhead % count;
    for block in blocks {
        block.bytes = block.bytes.saturating_add(each);
        if remainder > 0 {
            block.bytes = block.bytes.saturating_add(1);
            remainder -= 1;
        }
    }
}

pub fn effective_max_bytes(
    budget: StorageBudget,
    current_store_bytes: u64,
    filesystem: FilesystemSpace,
) -> Result<u64, StorageBudgetError> {
    validate(budget)?;
    if filesystem.total_bytes == 0 || filesystem.available_bytes > filesystem.total_bytes {
        return Err(StorageBudgetError::Filesystem(
            "capacity values are inconsistent".to_owned(),
        ));
    }
    let percent_cap = u64::try_from(
        u128::from(filesystem.total_bytes)
            .saturating_mul(u128::from(budget.max_filesystem_percent))
            / 100,
    )
    .unwrap_or(u64::MAX);
    let required_free = budget
        .min_free_bytes
        .saturating_add(budget.wal_reserve_bytes);
    let free_space_cap = current_store_bytes
        .saturating_add(filesystem.available_bytes)
        .saturating_sub(required_free);
    Ok(budget.max_bytes.min(percent_cap).min(free_space_cap))
}

fn validate(budget: StorageBudget) -> Result<(), StorageBudgetError> {
    if budget.max_bytes == 0
        || budget.max_filesystem_percent == 0
        || budget.max_filesystem_percent > 100
        || budget.max_enforcement_passes == 0
        || budget.requested_channel_protect_for.is_zero()
        || !(0.0..=1.0).contains(&budget.target_ratio)
        || !(0.0..=1.0).contains(&budget.high_water_ratio)
        || !(0.0..=1.0).contains(&budget.emergency_ratio)
        || budget.target_ratio >= budget.high_water_ratio
        || budget.high_water_ratio >= budget.emergency_ratio
    {
        return Err(StorageBudgetError::InvalidBudget);
    }
    Ok(())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(unix)]
pub fn filesystem_space(path: &Path) -> Result<FilesystemSpace, StorageBudgetError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let measured_path = if path.exists() {
        path
    } else {
        path.parent().unwrap_or_else(|| Path::new("."))
    };
    let path = CString::new(measured_path.as_os_str().as_bytes())
        .map_err(|error| StorageBudgetError::Filesystem(error.to_string()))?;
    let mut value = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), value.as_mut_ptr()) };
    if result != 0 {
        return Err(StorageBudgetError::Filesystem(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    let value = unsafe { value.assume_init() };
    let fragment_bytes = value.f_frsize;
    Ok(FilesystemSpace {
        total_bytes: value.f_blocks.saturating_mul(fragment_bytes),
        available_bytes: value.f_bavail.saturating_mul(fragment_bytes),
    })
}

#[cfg(not(unix))]
pub fn filesystem_space(_path: &Path) -> Result<FilesystemSpace, StorageBudgetError> {
    Err(StorageBudgetError::Filesystem(
        "filesystem capacity measurement is not implemented on this platform".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{CanonicalRecord, MessageStore};

    fn budget() -> StorageBudget {
        StorageBudget {
            max_bytes: 500,
            max_filesystem_percent: 75,
            min_free_bytes: 100,
            wal_reserve_bytes: 50,
            high_water_ratio: 0.90,
            target_ratio: 0.80,
            emergency_ratio: 0.98,
            pressure_floor_messages_per_channel: 50,
            requested_channel_protect_for: Duration::from_hours(24),
            max_enforcement_passes: 3,
        }
    }

    #[test]
    fn strictest_limit_wins() {
        assert_eq!(
            effective_max_bytes(
                budget(),
                200,
                FilesystemSpace {
                    total_bytes: 1_000,
                    available_bytes: 300,
                }
            )
            .unwrap(),
            350
        );
    }

    #[test]
    fn low_free_space_can_force_a_limit_below_current_usage() {
        assert_eq!(
            effective_max_bytes(
                budget(),
                200,
                FilesystemSpace {
                    total_bytes: 1_000,
                    available_bytes: 50,
                }
            )
            .unwrap(),
            100
        );
    }

    #[cfg(unix)]
    #[test]
    fn reads_real_filesystem_capacity() {
        let space = filesystem_space(std::env::temp_dir().as_path()).unwrap();
        assert!(space.total_bytes > 0);
        assert!(space.available_bytes <= space.total_bytes);
    }

    #[tokio::test]
    async fn enforcement_removes_whole_blocks_and_reclaims_physical_bytes() {
        let path = std::env::temp_dir().join(format!(
            "recent-messages-budget-{}.sqlite",
            std::process::id()
        ));
        cleanup(&path);
        let store = AsyncSqliteBlockStore::open(&path, 10, 800, 16, 1).unwrap();
        store.append_batch(records(240)).await.unwrap();
        store.checkpoint_wal().await.unwrap();
        let auto_vacuum: i64 = rusqlite::Connection::open(&path)
            .unwrap()
            .pragma_query_value(None, "auto_vacuum", |row| row.get(0))
            .unwrap();
        assert_eq!(auto_vacuum, 2);
        let before = store.physical_bytes().await.unwrap();
        let messages_before = store.stats().await.unwrap().messages;
        let candidate_bytes = store
            .pressure_blocks()
            .await
            .unwrap()
            .iter()
            .map(|block| block.bytes)
            .sum::<u64>();
        assert!(
            candidate_bytes > before / 2,
            "candidate bytes {candidate_bytes} unexpectedly small relative to physical bytes {before}"
        );
        let enforcer = StorageBudgetEnforcer::new(
            store.clone(),
            StorageBudget {
                max_bytes: before,
                max_filesystem_percent: 100,
                min_free_bytes: 0,
                wal_reserve_bytes: 0,
                high_water_ratio: 0.90,
                target_ratio: 0.80,
                emergency_ratio: 0.98,
                pressure_floor_messages_per_channel: 0,
                requested_channel_protect_for: Duration::from_hours(24),
                max_enforcement_passes: 5,
            },
        )
        .unwrap();
        let result = enforcer
            .enforce_with(|_| {
                Ok(FilesystemSpace {
                    total_bytes: u64::MAX,
                    available_bytes: u64::MAX,
                })
            })
            .await
            .unwrap();
        let messages_after = store.stats().await.unwrap().messages;

        assert!(result.evicted_blocks > 0);
        assert!(result.bytes_after < result.bytes_before);
        assert!(messages_after < messages_before);
        assert_eq!((messages_before - messages_after) % 10, 0);
        drop(enforcer);
        drop(store);
        cleanup(&path);
    }

    fn records(count: usize) -> Vec<CanonicalRecord> {
        (0..count)
            .map(|sequence| {
                let channel_key = "budget-channel".to_owned();
                let mut raw_irc = vec![0_u8; 8 * 1024];
                let mut hasher = blake3::Hasher::new();
                hasher.update(&sequence.to_le_bytes());
                hasher.finalize_xof().fill(&mut raw_irc);
                CanonicalRecord {
                    channel_key: channel_key.clone(),
                    event_at_ms: sequence as i64,
                    received_at_ms: sequence as i64,
                    event_key: CanonicalRecord::derive_event_key(&channel_key, &raw_irc),
                    source_id: "budget-test".to_owned(),
                    fidelity: Default::default(),
                    raw_irc,
                }
            })
            .collect()
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
