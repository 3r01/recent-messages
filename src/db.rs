use crate::coverage::CoverageInterval;
use crate::web::auth::{TwitchUserAccessToken, UserAuthorization};
use chrono::{TimeZone, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("control-store SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("control-store filesystem error: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error("control-store worker failed: {0}")]
    Worker(String),
    #[error("control-store lock poisoned")]
    LockPoisoned,
    #[error("invalid stored timestamp: {0}")]
    InvalidTimestamp(i64),
    #[error("unsupported control-store schema version: {0}")]
    UnsupportedSchema(i64),
    #[error("invalid coverage interval: {start_ms}..{end_ms}")]
    InvalidCoverageInterval { start_ms: i64, end_ms: i64 },
    #[error("always-join channel limit reached: {0}")]
    AlwaysJoinLimit(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairOutcome {
    Success,
    Partial,
    Failed,
}

impl RepairOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Partial => "partial",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairCheckpoint {
    pub outcome: RepairOutcome,
    pub completed_at_ms: i64,
    pub requested_start_ms: i64,
    pub requested_end_ms: i64,
    pub next_retry_ms: i64,
    pub record_count: usize,
}

#[derive(Clone, Debug)]
pub struct ControlStore {
    connection: Arc<Mutex<Connection>>,
}

impl ControlStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(path)?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let schema_version: i64 =
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if schema_version > 5 {
            return Err(StorageError::UnsupportedSchema(schema_version));
        }
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS channel (
                 channel_login TEXT PRIMARY KEY NOT NULL,
                 ignored_at_ms INTEGER,
                 last_access_ms INTEGER NOT NULL,
                 always_join INTEGER NOT NULL DEFAULT 0 CHECK(always_join IN (0, 1))
             );
             CREATE INDEX IF NOT EXISTS channel_join_candidates
                 ON channel(ignored_at_ms, last_access_ms DESC);
             CREATE TABLE IF NOT EXISTS user_authorization (
                 access_token TEXT PRIMARY KEY NOT NULL,
                 twitch_access_token TEXT NOT NULL,
                 twitch_refresh_token TEXT NOT NULL,
                 twitch_authorization_last_validated_ms INTEGER NOT NULL,
                 valid_until_ms INTEGER NOT NULL,
                 user_id TEXT NOT NULL,
                 user_login TEXT NOT NULL,
                 user_name TEXT NOT NULL,
                 user_profile_image_url TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS user_authorization_expiry
                 ON user_authorization(valid_until_ms);
             CREATE TABLE IF NOT EXISTS coverage_interval (
                 id INTEGER PRIMARY KEY,
                 channel_login TEXT NOT NULL,
                 source TEXT NOT NULL,
                 start_ms INTEGER NOT NULL,
                 end_ms INTEGER NOT NULL,
                 CHECK(end_ms >= start_ms)
             );
             CREATE INDEX IF NOT EXISTS coverage_interval_channel_time
                 ON coverage_interval(channel_login, end_ms, start_ms);
             CREATE UNIQUE INDEX IF NOT EXISTS coverage_interval_identity
                 ON coverage_interval(channel_login, source, start_ms, end_ms);
             CREATE TABLE IF NOT EXISTS live_coverage (
                 channel_login TEXT PRIMARY KEY NOT NULL,
                 source TEXT NOT NULL,
                 started_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS repair_attempt (
                 channel_login TEXT PRIMARY KEY NOT NULL,
                 source TEXT NOT NULL,
                 attempted_at_ms INTEGER NOT NULL,
                 requested_start_ms INTEGER NOT NULL,
                 requested_end_ms INTEGER NOT NULL,
                 record_count INTEGER NOT NULL,
                 CHECK(requested_end_ms >= requested_start_ms),
                 CHECK(record_count >= 0)
             );
             CREATE TABLE IF NOT EXISTS repair_checkpoint (
                 id INTEGER PRIMARY KEY,
                 channel_login TEXT NOT NULL,
                 source TEXT NOT NULL,
                 outcome TEXT NOT NULL CHECK(outcome IN ('success', 'partial', 'failed')),
                 completed_at_ms INTEGER NOT NULL,
                 requested_start_ms INTEGER NOT NULL,
                 requested_end_ms INTEGER NOT NULL,
                 next_retry_ms INTEGER NOT NULL,
                 record_count INTEGER NOT NULL,
                 CHECK(requested_end_ms >= requested_start_ms),
                 CHECK(record_count >= 0)
             );
             CREATE INDEX IF NOT EXISTS repair_checkpoint_channel_time
                 ON repair_checkpoint(channel_login, completed_at_ms DESC);
             CREATE TABLE IF NOT EXISTS join_state (
                 channel_login TEXT PRIMARY KEY NOT NULL,
                 state TEXT NOT NULL CHECK(state IN ('pending', 'joined', 'unavailable')),
                 consecutive_failures INTEGER NOT NULL DEFAULT 0,
                 last_attempt_ms INTEGER,
                 last_confirmed_ms INTEGER,
                 next_retry_ms INTEGER NOT NULL DEFAULT 0,
                 failure_kind TEXT
             );
             CREATE TABLE IF NOT EXISTS control_metadata (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );
             INSERT INTO repair_checkpoint(
                 channel_login, source, outcome, completed_at_ms,
                 requested_start_ms, requested_end_ms, next_retry_ms, record_count)
             SELECT channel_login, source, 'success', attempted_at_ms,
                    requested_start_ms, requested_end_ms,
                    attempted_at_ms + 86400000, record_count
             FROM repair_attempt
             WHERE NOT EXISTS (
                 SELECT 1 FROM repair_checkpoint checkpoint
                 WHERE checkpoint.channel_login=repair_attempt.channel_login
             );
             DROP TABLE repair_attempt;
             DELETE FROM live_coverage;
             PRAGMA user_version=5;",
        )?;
        ensure_channel_always_join_column(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    async fn execute<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StorageError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let mut connection = connection.lock().map_err(|_| StorageError::LockPoisoned)?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| StorageError::Worker(error.to_string()))?
    }

    pub async fn get_channel_logins_to_join(
        &self,
        channel_expiry: Duration,
    ) -> Result<HashSet<String>, StorageError> {
        let cutoff = now_ms().saturating_sub(duration_ms(channel_expiry));
        self.execute(move |connection| {
            let mut statement = connection.prepare(
                "SELECT channel.channel_login FROM channel
                 LEFT JOIN join_state USING(channel_login)
                 WHERE ignored_at_ms IS NULL
                   AND (always_join = 1 OR last_access_ms > ?1)
                   AND (join_state.state IS NULL
                        OR join_state.state != 'unavailable'
                        OR join_state.next_retry_ms <= ?2)
                 ORDER BY last_access_ms DESC",
            )?;
            let rows = statement.query_map(params![cutoff, now_ms()], |row| row.get(0))?;
            Ok(rows.collect::<rusqlite::Result<HashSet<_>>>()?)
        })
        .await
    }

    pub async fn get_always_join_channels(&self) -> Result<Vec<String>, StorageError> {
        self.execute(move |connection| {
            let mut statement = connection.prepare(
                "SELECT channel_login FROM channel WHERE always_join = 1 ORDER BY channel_login",
            )?;
            let rows = statement.query_map([], |row| row.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
    }

    pub async fn check_readable(&self) -> Result<(), StorageError> {
        self.execute(move |connection| {
            connection.query_row("SELECT COUNT(*) FROM sqlite_schema", [], |_| Ok(()))?;
            Ok(())
        })
        .await
    }

    pub async fn bootstrap_always_join_channels(
        &self,
        channels: &[String],
    ) -> Result<bool, StorageError> {
        if channels.is_empty() {
            return Ok(false);
        }
        let channels = channels
            .iter()
            .map(|channel| channel.to_ascii_lowercase())
            .collect::<Vec<_>>();
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let applied = tx
                .query_row(
                    "SELECT 1 FROM control_metadata WHERE key='always_join_bootstrap_applied'",
                    [],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if applied {
                return Ok(false);
            }
            for channel in channels {
                tx.execute(
                    "INSERT INTO channel(channel_login, last_access_ms, always_join)
                     VALUES (?1, 0, 1)
                     ON CONFLICT(channel_login) DO UPDATE SET always_join=1",
                    [channel],
                )?;
            }
            mark_always_join_bootstrap_applied(&tx)?;
            tx.commit()?;
            Ok(true)
        })
        .await
    }

    pub async fn set_always_join_channel(
        &self,
        channel_login: &str,
        enabled: bool,
        maximum: usize,
    ) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if enabled {
                let already_enabled: bool = tx.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM channel WHERE channel_login=?1 AND always_join=1
                     )",
                    [&channel_login],
                    |row| row.get(0),
                )?;
                let count: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM channel WHERE always_join=1",
                    [],
                    |row| row.get(0),
                )?;
                if !already_enabled && usize::try_from(count).unwrap_or(usize::MAX) >= maximum {
                    return Err(StorageError::AlwaysJoinLimit(maximum));
                }
                tx.execute(
                    "INSERT INTO channel(channel_login, last_access_ms, always_join)
                     VALUES (?1, 0, 1)
                     ON CONFLICT(channel_login) DO UPDATE SET always_join=1",
                    [channel_login],
                )?;
            } else {
                tx.execute(
                    "UPDATE channel SET always_join=0 WHERE channel_login=?1",
                    [channel_login],
                )?;
            }
            mark_always_join_bootstrap_applied(&tx)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn replace_always_join_channels(
        &self,
        channels: &[String],
    ) -> Result<(), StorageError> {
        let channels = channels
            .iter()
            .map(|channel| channel.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute("UPDATE channel SET always_join=0 WHERE always_join=1", [])?;
            for channel in channels {
                tx.execute(
                    "INSERT INTO channel(channel_login, last_access_ms, always_join)
                     VALUES (?1, 0, 1)
                     ON CONFLICT(channel_login) DO UPDATE SET always_join=1",
                    [channel],
                )?;
            }
            mark_always_join_bootstrap_applied(&tx)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn touch_or_add_channel(&self, channel_login: &str) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        let now = now_ms();
        let update_before = now.saturating_sub(30 * 60 * 1_000);
        self.execute(move |connection| {
            connection.execute(
                "INSERT INTO channel(channel_login, last_access_ms) VALUES (?1, ?2)
                 ON CONFLICT(channel_login) DO UPDATE SET last_access_ms=excluded.last_access_ms
                 WHERE channel.last_access_ms < ?3",
                params![channel_login, now, update_before],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn is_channel_ignored(&self, channel_login: &str) -> Result<bool, StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        self.execute(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT ignored_at_ms IS NOT NULL FROM channel WHERE channel_login=?1",
                    [channel_login],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(false))
        })
        .await
    }

    pub async fn get_ignored_channels(&self) -> Result<HashSet<String>, StorageError> {
        self.execute(move |connection| {
            let mut statement = connection
                .prepare("SELECT channel_login FROM channel WHERE ignored_at_ms IS NOT NULL")?;
            let rows = statement.query_map([], |row| row.get(0))?;
            Ok(rows.collect::<rusqlite::Result<HashSet<_>>>()?)
        })
        .await
    }

    pub async fn set_channel_ignored(
        &self,
        channel_login: &str,
        ignored: bool,
    ) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        let now = now_ms();
        let ignored_at = ignored.then_some(now);
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO channel(channel_login, ignored_at_ms, last_access_ms)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(channel_login) DO UPDATE SET ignored_at_ms=excluded.ignored_at_ms",
                params![channel_login, ignored_at, now],
            )?;
            if ignored {
                tx.execute(
                    "DELETE FROM coverage_interval WHERE channel_login=?1",
                    [&channel_login],
                )?;
                tx.execute(
                    "DELETE FROM live_coverage WHERE channel_login=?1",
                    [&channel_login],
                )?;
                tx.execute(
                    "DELETE FROM repair_checkpoint WHERE channel_login=?1",
                    [&channel_login],
                )?;
                tx.execute(
                    "DELETE FROM join_state WHERE channel_login=?1",
                    [&channel_login],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn invalidate_channel_coverage(
        &self,
        channel_login: &str,
    ) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "DELETE FROM coverage_interval WHERE channel_login=?1",
                [&channel_login],
            )?;
            tx.execute(
                "DELETE FROM live_coverage WHERE channel_login=?1",
                [&channel_login],
            )?;
            tx.execute(
                "DELETE FROM repair_checkpoint WHERE channel_login=?1",
                [&channel_login],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn discard_live_coverage(&self, channel_login: &str) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        self.execute(move |connection| {
            connection.execute(
                "DELETE FROM live_coverage WHERE channel_login=?1",
                [channel_login],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn discard_all_live_coverage(&self) -> Result<(), StorageError> {
        self.execute(move |connection| {
            connection.execute("DELETE FROM live_coverage", [])?;
            Ok(())
        })
        .await
    }

    pub async fn prune_coverage_ended_before(&self, cutoff_ms: i64) -> Result<usize, StorageError> {
        self.execute(move |connection| {
            Ok(connection.execute(
                "DELETE FROM coverage_interval WHERE end_ms < ?1",
                [cutoff_ms],
            )?)
        })
        .await
    }

    pub async fn prune_inactive_channels(&self, cutoff_ms: i64) -> Result<usize, StorageError> {
        self.execute(move |connection| {
            const CANDIDATES: &str = "SELECT channel_login FROM channel
                 WHERE ignored_at_ms IS NULL AND always_join=0 AND last_access_ms <= ?1";
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for table in [
                "coverage_interval",
                "live_coverage",
                "repair_checkpoint",
                "join_state",
            ] {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE channel_login IN ({CANDIDATES})"),
                    [cutoff_ms],
                )?;
            }
            let deleted = tx.execute(
                "DELETE FROM channel
                 WHERE ignored_at_ms IS NULL AND always_join=0 AND last_access_ms <= ?1",
                [cutoff_ms],
            )?;
            tx.commit()?;
            Ok(deleted)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record_repair_checkpoint(
        &self,
        channel_login: &str,
        source: &str,
        outcome: RepairOutcome,
        completed_at_ms: i64,
        requested_start_ms: i64,
        requested_end_ms: i64,
        next_retry_ms: i64,
        record_count: usize,
    ) -> Result<(), StorageError> {
        if requested_end_ms < requested_start_ms {
            return Err(StorageError::InvalidCoverageInterval {
                start_ms: requested_start_ms,
                end_ms: requested_end_ms,
            });
        }
        let channel_login = channel_login.to_ascii_lowercase();
        let source = source.to_owned();
        let outcome = outcome.as_str();
        let record_count = i64::try_from(record_count).unwrap_or(i64::MAX);
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO repair_checkpoint(
                    channel_login, source, outcome, completed_at_ms,
                    requested_start_ms, requested_end_ms, next_retry_ms, record_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    channel_login,
                    source,
                    outcome,
                    completed_at_ms,
                    requested_start_ms,
                    requested_end_ms,
                    next_retry_ms,
                    record_count
                ],
            )?;
            tx.execute(
                "DELETE FROM repair_checkpoint
                 WHERE channel_login=?1 AND id NOT IN (
                     SELECT id FROM repair_checkpoint WHERE channel_login=?1
                     ORDER BY completed_at_ms DESC, id DESC LIMIT 8
                 )",
                [&channel_login],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn get_repair_checkpoints(
        &self,
        channel_login: &str,
    ) -> Result<Vec<RepairCheckpoint>, StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        self.execute(move |connection| {
            let mut statement = connection.prepare(
                "SELECT outcome, completed_at_ms, requested_start_ms,
                        requested_end_ms, next_retry_ms, record_count
                 FROM repair_checkpoint WHERE channel_login=?1
                 ORDER BY completed_at_ms DESC, id DESC LIMIT 8",
            )?;
            let rows = statement.query_map([channel_login], |row| {
                let outcome = match row.get::<_, String>(0)?.as_str() {
                    "success" => RepairOutcome::Success,
                    "partial" => RepairOutcome::Partial,
                    _ => RepairOutcome::Failed,
                };
                Ok(RepairCheckpoint {
                    outcome,
                    completed_at_ms: row.get(1)?,
                    requested_start_ms: row.get(2)?,
                    requested_end_ms: row.get(3)?,
                    next_retry_ms: row.get(4)?,
                    record_count: usize::try_from(row.get::<_, i64>(5)?).unwrap_or(usize::MAX),
                })
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
    }

    pub async fn begin_join_attempt_if_due(
        &self,
        channel_login: &str,
        confirmation_timeout: Duration,
        retry_min: Duration,
        retry_max: Duration,
    ) -> Result<bool, StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        let now = now_ms();
        let confirmation_deadline = now.saturating_add(duration_ms(confirmation_timeout));
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let state = tx
                .query_row(
                    "SELECT state, next_retry_ms, consecutive_failures
                     FROM join_state WHERE channel_login=?1",
                    [&channel_login],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, u32>(2)?,
                        ))
                    },
                )
                .optional()?;
            if state
                .as_ref()
                .is_some_and(|(state, next_retry, _)| state != "joined" && *next_retry > now)
            {
                return Ok(false);
            }
            if let Some((state, _, failures)) = &state
                && state == "pending"
            {
                let retry_min_ms = duration_ms(retry_min).max(1);
                let retry_max_ms = duration_ms(retry_max).max(retry_min_ms);
                let delay = retry_min_ms
                    .saturating_mul(1_i64.checked_shl((*failures).min(30)).unwrap_or(i64::MAX))
                    .min(retry_max_ms);
                tx.execute(
                    "UPDATE join_state SET state='unavailable',
                        consecutive_failures=consecutive_failures + 1,
                        next_retry_ms=?2, failure_kind='confirmation_timeout'
                     WHERE channel_login=?1",
                    params![channel_login, now.saturating_add(delay)],
                )?;
                tx.commit()?;
                return Ok(false);
            }
            tx.execute(
                "INSERT INTO join_state(
                    channel_login, state, last_attempt_ms, next_retry_ms)
                 VALUES (?1, 'pending', ?2, ?3)
                 ON CONFLICT(channel_login) DO UPDATE SET
                    state='pending', last_attempt_ms=excluded.last_attempt_ms,
                    next_retry_ms=excluded.next_retry_ms, failure_kind=NULL",
                params![channel_login, now, confirmation_deadline],
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
    }

    pub async fn mark_join_confirmed(&self, channel_login: &str) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        let now = now_ms();
        self.execute(move |connection| {
            connection.execute(
                "INSERT INTO join_state(
                    channel_login, state, consecutive_failures,
                    last_confirmed_ms, next_retry_ms)
                 VALUES (?1, 'joined', 0, ?2, 0)
                 ON CONFLICT(channel_login) DO UPDATE SET
                    state='joined', consecutive_failures=0,
                    last_confirmed_ms=excluded.last_confirmed_ms,
                    next_retry_ms=0, failure_kind=NULL
                 WHERE join_state.state != 'joined'",
                params![channel_login, now],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn mark_join_unavailable(
        &self,
        channel_login: &str,
        failure_kind: &str,
        retry_after: Duration,
    ) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        let failure_kind = failure_kind.to_owned();
        let next_retry = now_ms().saturating_add(duration_ms(retry_after));
        self.execute(move |connection| {
            connection.execute(
                "INSERT INTO join_state(
                    channel_login, state, consecutive_failures,
                    next_retry_ms, failure_kind)
                 VALUES (?1, 'unavailable', 1, ?2, ?3)
                 ON CONFLICT(channel_login) DO UPDATE SET
                    state='unavailable',
                    consecutive_failures=join_state.consecutive_failures + 1,
                    next_retry_ms=MAX(join_state.next_retry_ms, excluded.next_retry_ms),
                    failure_kind=CASE
                        WHEN excluded.next_retry_ms >= join_state.next_retry_ms
                        THEN excluded.failure_kind ELSE join_state.failure_kind END",
                params![channel_login, next_retry, failure_kind],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn fail_stale_join_attempts(
        &self,
        retry_min: Duration,
        retry_max: Duration,
    ) -> Result<usize, StorageError> {
        let now = now_ms();
        let retry_min_ms = duration_ms(retry_min).max(1);
        let retry_max_ms = duration_ms(retry_max).max(retry_min_ms);
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let stale = {
                let mut statement = tx.prepare(
                    "SELECT channel_login, consecutive_failures FROM join_state
                     WHERE state='pending' AND next_retry_ms <= ?1",
                )?;
                let rows = statement.query_map([now], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            for (channel_login, failures) in &stale {
                let shift = (*failures).min(30);
                let delay = retry_min_ms
                    .saturating_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
                    .min(retry_max_ms);
                tx.execute(
                    "UPDATE join_state SET state='unavailable',
                        consecutive_failures=consecutive_failures + 1,
                        next_retry_ms=?2, failure_kind='confirmation_timeout'
                     WHERE channel_login=?1",
                    params![channel_login, now.saturating_add(delay)],
                )?;
            }
            tx.commit()?;
            Ok(stale.len())
        })
        .await
    }

    pub async fn begin_live_coverage(
        &self,
        channel_login: &str,
        source: &str,
        started_at_ms: i64,
    ) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        let source = source.to_owned();
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "DELETE FROM live_coverage WHERE channel_login=?1",
                [&channel_login],
            )?;
            tx.execute(
                "INSERT INTO live_coverage(channel_login, source, started_at_ms)
                 VALUES (?1, ?2, ?3)",
                params![channel_login, source, started_at_ms],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn end_live_coverage(
        &self,
        channel_login: &str,
        ended_at_ms: i64,
    ) -> Result<(), StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let active = tx
                .query_row(
                    "SELECT source, started_at_ms FROM live_coverage WHERE channel_login=?1",
                    [&channel_login],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?;
            tx.execute(
                "DELETE FROM live_coverage WHERE channel_login=?1",
                [&channel_login],
            )?;
            if let Some((source, started_at_ms)) = active
                && ended_at_ms >= started_at_ms
            {
                tx.execute(
                    "INSERT INTO coverage_interval(channel_login, source, start_ms, end_ms)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![channel_login, source, started_at_ms, ended_at_ms],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn close_all_live_coverage(&self, ended_at_ms: i64) -> Result<(), StorageError> {
        self.execute(move |connection| {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO coverage_interval(channel_login, source, start_ms, end_ms)
                 SELECT channel_login, source, started_at_ms, ?1 FROM live_coverage
                 WHERE started_at_ms <= ?1",
                [ended_at_ms],
            )?;
            tx.execute("DELETE FROM live_coverage", [])?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn record_coverage_interval(
        &self,
        channel_login: &str,
        source: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(), StorageError> {
        if end_ms < start_ms {
            return Err(StorageError::InvalidCoverageInterval { start_ms, end_ms });
        }
        let channel_login = channel_login.to_ascii_lowercase();
        let source = source.to_owned();
        self.execute(move |connection| {
            connection.execute(
                "INSERT OR IGNORE INTO coverage_interval(channel_login, source, start_ms, end_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![channel_login, source, start_ms, end_ms],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_coverage_intervals(
        &self,
        channel_login: &str,
        through_ms: i64,
    ) -> Result<Vec<CoverageInterval>, StorageError> {
        let channel_login = channel_login.to_ascii_lowercase();
        self.execute(move |connection| {
            let mut statement = connection.prepare(
                "SELECT source, start_ms, end_ms FROM coverage_interval
                 WHERE channel_login=?1
                 UNION ALL
                 SELECT source, started_at_ms, ?2 FROM live_coverage
                 WHERE channel_login=?1 AND started_at_ms <= ?2
                 ORDER BY start_ms, end_ms",
            )?;
            let rows = statement.query_map(params![channel_login, through_ms], |row| {
                Ok(CoverageInterval {
                    source: row.get(0)?,
                    start_ms: row.get(1)?,
                    end_ms: row.get(2)?,
                })
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
    }

    pub async fn append_user_authorization(
        &self,
        authorization: &UserAuthorization,
    ) -> Result<(), StorageError> {
        let authorization = authorization.clone();
        self.execute(move |connection| {
            let validated = authorization
                .twitch_authorization_last_validated
                .timestamp_millis();
            let valid_until = authorization.valid_until.timestamp_millis();
            connection.execute(
                "INSERT INTO user_authorization(
                    access_token, twitch_access_token, twitch_refresh_token,
                    twitch_authorization_last_validated_ms, valid_until_ms, user_id,
                    user_login, user_name, user_profile_image_url)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    authorization.access_token,
                    authorization.twitch_token.access_token,
                    authorization.twitch_token.refresh_token,
                    validated,
                    valid_until,
                    authorization.user_id,
                    authorization.user_login,
                    authorization.user_name,
                    authorization.user_profile_image_url,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_user_authorization(
        &self,
        access_token: &str,
    ) -> Result<Option<UserAuthorization>, StorageError> {
        let access_token = access_token.to_owned();
        let now = now_ms();
        self.execute(move |connection| {
            let values = connection
                .query_row(
                    "SELECT access_token, twitch_access_token, twitch_refresh_token,
                            twitch_authorization_last_validated_ms, valid_until_ms, user_id,
                            user_login, user_name, user_profile_image_url
                     FROM user_authorization WHERE access_token=?1 AND valid_until_ms>=?2",
                    params![access_token, now],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, String>(8)?,
                        ))
                    },
                )
                .optional()?;
            values.map(authorization_from_values).transpose()
        })
        .await
    }

    pub async fn update_user_authorization(
        &self,
        authorization: &UserAuthorization,
    ) -> Result<(), StorageError> {
        let authorization = authorization.clone();
        self.execute(move |connection| {
            let validated = authorization
                .twitch_authorization_last_validated
                .timestamp_millis();
            let valid_until = authorization.valid_until.timestamp_millis();
            connection.execute(
                "INSERT INTO user_authorization(
                    access_token, twitch_access_token, twitch_refresh_token,
                    twitch_authorization_last_validated_ms, valid_until_ms, user_id,
                    user_login, user_name, user_profile_image_url)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(access_token) DO UPDATE SET
                    twitch_access_token=excluded.twitch_access_token,
                    twitch_refresh_token=excluded.twitch_refresh_token,
                    twitch_authorization_last_validated_ms=excluded.twitch_authorization_last_validated_ms,
                    valid_until_ms=excluded.valid_until_ms, user_id=excluded.user_id,
                    user_login=excluded.user_login, user_name=excluded.user_name,
                    user_profile_image_url=excluded.user_profile_image_url",
                params![
                    authorization.access_token,
                    authorization.twitch_token.access_token,
                    authorization.twitch_token.refresh_token,
                    validated,
                    valid_until,
                    authorization.user_id,
                    authorization.user_login,
                    authorization.user_name,
                    authorization.user_profile_image_url,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn delete_user_authorization(&self, access_token: &str) -> Result<(), StorageError> {
        let access_token = access_token.to_owned();
        self.execute(move |connection| {
            connection.execute(
                "DELETE FROM user_authorization WHERE access_token=?1",
                [access_token],
            )?;
            Ok(())
        })
        .await
    }
}

#[allow(clippy::type_complexity)]
fn authorization_from_values(
    values: (
        String,
        String,
        String,
        i64,
        i64,
        String,
        String,
        String,
        String,
    ),
) -> Result<UserAuthorization, StorageError> {
    let (
        access_token,
        twitch_access_token,
        twitch_refresh_token,
        validated,
        valid_until,
        user_id,
        user_login,
        user_name,
        user_profile_image_url,
    ) = values;
    Ok(UserAuthorization {
        access_token,
        twitch_token: TwitchUserAccessToken {
            access_token: twitch_access_token,
            refresh_token: twitch_refresh_token,
        },
        twitch_authorization_last_validated: timestamp(validated)?,
        valid_until: timestamp(valid_until)?,
        user_id,
        user_login,
        user_name,
        user_profile_image_url,
    })
}

fn timestamp(milliseconds: i64) -> Result<chrono::DateTime<Utc>, StorageError> {
    Utc.timestamp_millis_opt(milliseconds)
        .single()
        .ok_or(StorageError::InvalidTimestamp(milliseconds))
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn mark_always_join_bootstrap_applied(
    tx: &rusqlite::Transaction<'_>,
) -> Result<(), rusqlite::Error> {
    tx.execute(
        "INSERT OR REPLACE INTO control_metadata(key, value)
         VALUES ('always_join_bootstrap_applied', '1')",
        [],
    )?;
    Ok(())
}

fn ensure_channel_always_join_column(connection: &Connection) -> Result<(), StorageError> {
    let mut statement = connection.prepare("PRAGMA table_info(channel)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|column| column == "always_join") {
        connection.execute(
            "ALTER TABLE channel ADD COLUMN always_join INTEGER NOT NULL DEFAULT 0
             CHECK(always_join IN (0, 1))",
            [],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_STORE: AtomicU64 = AtomicU64::new(0);

    fn store() -> ControlStore {
        ControlStore::open(std::env::temp_dir().join(format!(
            "recent-messages-control-{}-{}.sqlite",
            std::process::id(),
            NEXT_STORE.fetch_add(1, Ordering::Relaxed)
        )))
        .unwrap()
    }

    #[test]
    fn configures_busy_timeout() {
        let store = store();
        let timeout_ms: i64 = store
            .connection
            .lock()
            .unwrap()
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert_eq!(timeout_ms, 5_000);
    }

    #[tokio::test]
    async fn waits_for_a_brief_external_writer() {
        let path = std::env::temp_dir().join(format!(
            "recent-messages-control-busy-{}-{}.sqlite",
            std::process::id(),
            NEXT_STORE.fetch_add(1, Ordering::Relaxed)
        ));
        let store = ControlStore::open(&path).unwrap();
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let writer = tokio::spawn(async move { store.touch_or_add_channel("example").await });
        std::thread::sleep(Duration::from_millis(100));
        blocker.execute_batch("COMMIT").unwrap();
        writer.await.unwrap().unwrap();

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn controls_ignored_and_join_candidate_state() {
        let store = store();
        store.touch_or_add_channel("Example").await.unwrap();
        assert!(
            store
                .get_channel_logins_to_join(Duration::from_hours(1))
                .await
                .unwrap()
                .contains("example")
        );
        store.set_channel_ignored("example", true).await.unwrap();
        assert!(store.is_channel_ignored("EXAMPLE").await.unwrap());
        assert!(
            !store
                .get_channel_logins_to_join(Duration::from_hours(1))
                .await
                .unwrap()
                .contains("example")
        );
    }

    #[tokio::test]
    async fn prunes_only_unpinned_unignored_inactive_channel_state() {
        let store = store();
        store
            .execute(|connection| {
                connection.execute(
                    "INSERT INTO channel(channel_login,last_access_ms) VALUES ('stale',0)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO join_state(channel_login,state,next_retry_ms)
                     VALUES ('stale','unavailable',9999999999999)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        store
            .set_always_join_channel("pinned", true, 10)
            .await
            .unwrap();
        store.set_channel_ignored("ignored", true).await.unwrap();
        store.touch_or_add_channel("recent").await.unwrap();

        assert_eq!(store.prune_inactive_channels(0).await.unwrap(), 1);
        let remaining = store
            .execute(|connection| {
                let mut statement = connection
                    .prepare("SELECT channel_login FROM channel ORDER BY channel_login")?;
                let rows = statement.query_map([], |row| row.get(0))?;
                Ok(rows.collect::<rusqlite::Result<Vec<String>>>()?)
            })
            .await
            .unwrap();
        assert_eq!(remaining, vec!["ignored", "pinned", "recent"]);
    }

    #[tokio::test]
    async fn ignoring_a_channel_invalidates_its_coverage() {
        let store = store();
        store
            .record_coverage_interval("example", "backfill", 0, 50)
            .await
            .unwrap();
        store
            .begin_live_coverage("example", "direct-irc", 50)
            .await
            .unwrap();
        store
            .record_repair_checkpoint(
                "example",
                "mirror",
                RepairOutcome::Success,
                75,
                0,
                75,
                100,
                10,
            )
            .await
            .unwrap();
        store.set_channel_ignored("example", true).await.unwrap();
        assert!(
            store
                .get_coverage_intervals("example", 100)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .get_repair_checkpoints("example")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn authorization_round_trips_and_revokes() {
        let store = store();
        let now = Utc.timestamp_millis_opt(now_ms()).unwrap();
        let authorization = UserAuthorization {
            access_token: "local-token".to_owned(),
            twitch_token: TwitchUserAccessToken {
                access_token: "twitch-token".to_owned(),
                refresh_token: "refresh-token".to_owned(),
            },
            twitch_authorization_last_validated: now,
            valid_until: now + chrono::Duration::hours(1),
            user_id: "1".to_owned(),
            user_login: "example".to_owned(),
            user_name: "Example".to_owned(),
            user_profile_image_url: "https://example.invalid/image".to_owned(),
        };
        store
            .append_user_authorization(&authorization)
            .await
            .unwrap();
        assert_eq!(
            store.get_user_authorization("local-token").await.unwrap(),
            Some(authorization)
        );
        store
            .delete_user_authorization("local-token")
            .await
            .unwrap();
        assert!(
            store
                .get_user_authorization("local-token")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tracks_closed_live_and_backfill_coverage() {
        let store = store();
        store
            .begin_live_coverage("Example", "direct-irc", 100)
            .await
            .unwrap();
        assert_eq!(
            store.get_coverage_intervals("example", 150).await.unwrap(),
            vec![CoverageInterval {
                start_ms: 100,
                end_ms: 150,
                source: "direct-irc".to_owned(),
            }]
        );
        store.end_live_coverage("example", 160).await.unwrap();
        store
            .record_coverage_interval("example", "peer-backfill", 0, 100)
            .await
            .unwrap();
        store
            .record_coverage_interval("example", "peer-backfill", 0, 100)
            .await
            .unwrap();
        let intervals = store.get_coverage_intervals("example", 200).await.unwrap();
        assert_eq!(intervals.len(), 2);
        assert_eq!(intervals[0].start_ms, 0);
        assert_eq!(intervals[1].end_ms, 160);
        assert_eq!(store.prune_coverage_ended_before(120).await.unwrap(), 1);
        assert_eq!(
            store.get_coverage_intervals("example", 200).await.unwrap(),
            vec![CoverageInterval {
                start_ms: 100,
                end_ms: 160,
                source: "direct-irc".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn tracks_bounded_best_effort_repair_checkpoints_separately() {
        let store = store();
        store
            .record_repair_checkpoint(
                "Example",
                "mirror",
                RepairOutcome::Success,
                100,
                0,
                100,
                200,
                5,
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_repair_checkpoints("example").await.unwrap(),
            vec![RepairCheckpoint {
                outcome: RepairOutcome::Success,
                completed_at_ms: 100,
                requested_start_ms: 0,
                requested_end_ms: 100,
                next_retry_ms: 200,
                record_count: 5,
            }]
        );
        assert!(
            store
                .get_coverage_intervals("example", 200)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn join_attempts_timeout_into_backoff_and_become_due_again() {
        let store = store();
        store.touch_or_add_channel("Example").await.unwrap();
        assert!(
            store
                .begin_join_attempt_if_due(
                    "example",
                    Duration::from_millis(100),
                    Duration::from_millis(20),
                    Duration::from_millis(40),
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .begin_join_attempt_if_due(
                    "example",
                    Duration::from_millis(100),
                    Duration::from_millis(20),
                    Duration::from_millis(40),
                )
                .await
                .unwrap()
        );
        tokio::time::sleep(Duration::from_millis(110)).await;
        assert_eq!(
            store
                .fail_stale_join_attempts(Duration::from_millis(20), Duration::from_millis(40))
                .await
                .unwrap(),
            1
        );
        assert!(
            !store
                .get_channel_logins_to_join(Duration::from_hours(1))
                .await
                .unwrap()
                .contains("example")
        );
        tokio::time::sleep(Duration::from_millis(45)).await;
        assert!(
            store
                .get_channel_logins_to_join(Duration::from_hours(1))
                .await
                .unwrap()
                .contains("example")
        );
        assert!(
            store
                .begin_join_attempt_if_due(
                    "example",
                    Duration::from_secs(1),
                    Duration::from_millis(20),
                    Duration::from_millis(40),
                )
                .await
                .unwrap()
        );
        store.mark_join_confirmed("example").await.unwrap();
        assert!(
            store
                .get_channel_logins_to_join(Duration::from_hours(1))
                .await
                .unwrap()
                .contains("example")
        );
    }

    #[tokio::test]
    async fn reopening_invalidates_an_unclosed_join_epoch() {
        let path = std::env::temp_dir().join(format!(
            "recent-messages-coverage-reopen-{}-{}.sqlite",
            std::process::id(),
            now_ms()
        ));
        let store = ControlStore::open(&path).unwrap();
        store
            .begin_live_coverage("example", "direct-irc", 100)
            .await
            .unwrap();
        drop(store);
        let reopened = ControlStore::open(path).unwrap();
        assert!(
            reopened
                .get_coverage_intervals("example", 200)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn migrates_v3_repair_marker_to_period_checkpoint() {
        let path = std::env::temp_dir().join(format!(
            "recent-messages-v3-migration-{}-{}.sqlite",
            std::process::id(),
            NEXT_STORE.fetch_add(1, Ordering::Relaxed)
        ));
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE repair_attempt (
                     channel_login TEXT PRIMARY KEY NOT NULL,
                     source TEXT NOT NULL,
                     attempted_at_ms INTEGER NOT NULL,
                     requested_start_ms INTEGER NOT NULL,
                     requested_end_ms INTEGER NOT NULL,
                     record_count INTEGER NOT NULL
                 );
                 INSERT INTO repair_attempt VALUES ('example', 'mirror', 100, 0, 100, 5);
                 PRAGMA user_version=3;",
            )
            .unwrap();
        drop(connection);

        let store = ControlStore::open(&path).unwrap();
        assert_eq!(
            store.get_repair_checkpoints("example").await.unwrap(),
            vec![RepairCheckpoint {
                outcome: RepairOutcome::Success,
                completed_at_ms: 100,
                requested_start_ms: 0,
                requested_end_ms: 100,
                next_retry_ms: 86_400_100,
                record_count: 5,
            }]
        );
        drop(store);
        assert!(ControlStore::open(path).is_ok());
    }

    #[tokio::test]
    async fn always_join_bootstrap_is_one_time_and_api_state_is_authoritative() {
        let store = store();
        assert!(
            store
                .bootstrap_always_join_channels(&["Alpha".to_owned(), "beta".to_owned()])
                .await
                .unwrap()
        );
        assert_eq!(
            store.get_always_join_channels().await.unwrap(),
            vec!["alpha".to_owned(), "beta".to_owned()]
        );
        assert!(
            store
                .get_channel_logins_to_join(Duration::ZERO)
                .await
                .unwrap()
                .contains("alpha")
        );

        store
            .set_always_join_channel("alpha", false, 10)
            .await
            .unwrap();
        assert!(
            !store
                .get_channel_logins_to_join(Duration::ZERO)
                .await
                .unwrap()
                .contains("alpha")
        );
        assert!(
            !store
                .bootstrap_always_join_channels(&["gamma".to_owned()])
                .await
                .unwrap()
        );
        assert_eq!(
            store.get_always_join_channels().await.unwrap(),
            vec!["beta".to_owned()]
        );

        store
            .replace_always_join_channels(&["delta".to_owned(), "DELTA".to_owned()])
            .await
            .unwrap();
        assert_eq!(
            store.get_always_join_channels().await.unwrap(),
            vec!["delta".to_owned()]
        );
        store.set_channel_ignored("delta", true).await.unwrap();
        assert!(
            !store
                .get_channel_logins_to_join(Duration::ZERO)
                .await
                .unwrap()
                .contains("delta")
        );

        assert!(matches!(
            store.set_always_join_channel("epsilon", true, 1).await,
            Err(StorageError::AlwaysJoinLimit(1))
        ));
    }

    #[tokio::test]
    async fn migrates_v4_channel_rows_to_non_pinned_state() {
        let path = std::env::temp_dir().join(format!(
            "recent-messages-v4-migration-{}-{}.sqlite",
            std::process::id(),
            NEXT_STORE.fetch_add(1, Ordering::Relaxed)
        ));
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE channel (
                    channel_login TEXT PRIMARY KEY NOT NULL,
                    ignored_at_ms INTEGER,
                    last_access_ms INTEGER NOT NULL
                 );
                 INSERT INTO channel VALUES ('example', NULL, 1);
                 PRAGMA user_version=4;",
            )
            .unwrap();
        drop(connection);

        let store = ControlStore::open(path).unwrap();
        assert!(store.get_always_join_channels().await.unwrap().is_empty());
        store
            .set_always_join_channel("example", true, 10)
            .await
            .unwrap();
        assert_eq!(
            store.get_always_join_channels().await.unwrap(),
            vec!["example".to_owned()]
        );
    }
}
