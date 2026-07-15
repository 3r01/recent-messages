use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;
use thiserror::Error;

const DEFAULT_CONFIG_PATH: &str = "config.toml";

/// Command line arguments
#[derive(Clone, Debug, StructOpt)]
#[structopt(rename_all = "kebab")]
pub struct Args {
    /// File path to read config from
    #[structopt(
        short = "C",
        long = "config",
        env = "RM2_CONFIG",
        default_value = DEFAULT_CONFIG_PATH
    )]
    pub config_path: PathBuf,
}

/// Config file options
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub app: AppConfig,

    #[serde(default)]
    pub irc: IrcConfig,

    pub web: WebConfig,

    #[serde(default)]
    pub control_store: ControlStoreConfig,

    #[serde(default)]
    pub block_store: BlockStoreConfig,

    #[serde(default)]
    pub ingest: IngestConfig,

    #[serde(default)]
    pub storage_budget: StorageBudgetConfig,

    #[serde(default)]
    pub peer: PeerConfig,

    #[serde(default)]
    pub repair: RepairConfig,

    #[serde(default)]
    pub admin: AdminConfig,

    #[serde(default)]
    pub health: HealthConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    #[serde(with = "humantime_serde")]
    pub firehose_max_event_age: Duration,
    #[serde(with = "humantime_serde")]
    pub ingest_max_accept_age: Duration,
    pub max_queue_batches: usize,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            firehose_max_event_age: Duration::from_mins(5),
            ingest_max_accept_age: Duration::from_mins(2),
            max_queue_batches: 200,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    pub token_file: Option<PathBuf>,
    pub max_always_join_channels: usize,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            token_file: None,
            max_always_join_channels: 100_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RepairProviderKind {
    #[default]
    RecentMessages,
    Rustlog,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RepairProviderConfig {
    pub name: String,
    pub base_url: String,
    #[serde(default)]
    pub kind: RepairProviderKind,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RepairConfig {
    pub providers: Vec<RepairProviderConfig>,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub fast_return_wait: Duration,
    #[serde(with = "humantime_serde")]
    pub foreground_wait: Duration,
    pub max_inflight: usize,
    pub max_response_bytes: usize,
    pub failure_threshold: u32,
    #[serde(with = "humantime_serde")]
    pub open_duration: Duration,
    #[serde(with = "humantime_serde")]
    pub refresh_after: Duration,
    #[serde(with = "humantime_serde")]
    pub partial_retry_after: Duration,
    #[serde(with = "humantime_serde")]
    pub failure_retry_after: Duration,
    #[serde(with = "humantime_serde")]
    pub handoff_grace: Duration,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            providers: Vec::new(),
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(10),
            fast_return_wait: Duration::from_millis(350),
            foreground_wait: Duration::from_millis(1500),
            max_inflight: 16,
            max_response_bytes: 4 * 1024 * 1024,
            failure_threshold: 5,
            open_duration: Duration::from_secs(30),
            refresh_after: Duration::from_hours(6),
            partial_retry_after: Duration::from_mins(5),
            failure_retry_after: Duration::from_mins(1),
            handoff_grace: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerNodeConfig {
    pub name: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PeerConfig {
    pub shared_token: Option<String>,
    pub nodes: Vec<PeerNodeConfig>,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub delegate_timeout: Duration,
    pub max_inflight: usize,
    pub max_delegate_response_bytes: usize,
    pub failure_threshold: u32,
    #[serde(with = "humantime_serde")]
    pub open_duration: Duration,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            shared_token: None,
            nodes: Vec::new(),
            connect_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_millis(250),
            delegate_timeout: Duration::from_millis(1_500),
            max_inflight: 32,
            max_delegate_response_bytes: 8 * 1024 * 1024,
            failure_threshold: 5,
            open_duration: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ControlStoreConfig {
    pub path: PathBuf,
}

impl Default for ControlStoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data/control.sqlite"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IngestConfig {
    pub enabled: bool,
    pub raw_firehoses: Vec<String>,
    pub origin: String,
    pub batch_messages: usize,
    #[serde(with = "humantime_serde")]
    pub batch_max_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_min_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_max_delay: Duration,
    pub queue_batches: usize,
    pub max_open_channels: usize,
    pub max_open_bytes: usize,
    #[serde(with = "humantime_serde")]
    pub idle_seal_after: Duration,
    #[serde(with = "humantime_serde")]
    pub max_open_age: Duration,
    #[serde(with = "humantime_serde")]
    pub seal_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub checkpoint_poll_interval: Duration,
    pub checkpoint_journal_bytes: u64,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            raw_firehoses: vec![
                "wss://logs.spanix.team/firehose".to_owned(),
                "wss://logs.supa.codes/firehose".to_owned(),
                "wss://logs.susgee.dev/firehose".to_owned(),
                "wss://logxx.dev/firehose".to_owned(),
            ],
            origin: "https://tv.supa.sh".to_owned(),
            batch_messages: 100,
            batch_max_delay: Duration::from_millis(100),
            reconnect_min_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(30),
            queue_batches: 256,
            max_open_channels: 100_000,
            max_open_bytes: 512 * 1024 * 1024,
            idle_seal_after: Duration::from_mins(5),
            max_open_age: Duration::from_mins(10),
            seal_interval: Duration::from_secs(1),
            checkpoint_interval: Duration::from_mins(10),
            checkpoint_poll_interval: Duration::from_secs(30),
            checkpoint_journal_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageBudgetConfig {
    pub max_bytes: u64,
    pub max_filesystem_percent: u8,
    pub min_free_bytes: u64,
    pub wal_reserve_bytes: u64,
    pub high_water_ratio: f64,
    pub target_ratio: f64,
    pub emergency_ratio: f64,
    pub pressure_floor_messages_per_channel: usize,
    pub max_enforcement_passes: usize,
}

impl Default for StorageBudgetConfig {
    fn default() -> Self {
        Self {
            max_bytes: 2 * 1024 * 1024 * 1024,
            max_filesystem_percent: 75,
            min_free_bytes: 1024 * 1024 * 1024,
            wal_reserve_bytes: 256 * 1024 * 1024,
            high_water_ratio: 0.90,
            target_ratio: 0.80,
            emergency_ratio: 0.98,
            pressure_floor_messages_per_channel: 50,
            max_enforcement_passes: 8,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BlockStoreConfig {
    pub path: PathBuf,
    pub block_messages: usize,
    pub max_messages_per_channel: usize,
    pub writer_queue_batches: usize,
    pub read_connections: usize,
}

impl Default for BlockStoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data/recent-messages.sqlite"),
            block_messages: 100,
            max_messages_per_channel: 800,
            writer_queue_batches: 256,
            read_connections: 4,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    #[serde(with = "humantime_serde")]
    pub vacuum_channels_every: Duration,
    #[serde(with = "humantime_serde")]
    pub channels_expire_after: Duration,
    pub max_buffer_size: usize,
    #[serde(with = "humantime_serde")]
    pub message_ttl: Duration,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            vacuum_channels_every: Duration::from_mins(30),
            channels_expire_after: Duration::from_hours(24),
            max_buffer_size: 800,
            message_ttl: Duration::from_hours(24),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IrcConfig {
    pub always_join_channels: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub new_connection_every: Duration,
    #[serde(with = "humantime_serde")]
    pub join_confirmation_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub join_retry_poll: Duration,
    #[serde(with = "humantime_serde")]
    pub join_retry_min: Duration,
    #[serde(with = "humantime_serde")]
    pub join_retry_max: Duration,
    #[serde(with = "humantime_serde")]
    pub unavailable_retry_after: Duration,
}

impl Default for IrcConfig {
    fn default() -> Self {
        IrcConfig {
            always_join_channels: Vec::new(),
            new_connection_every: Duration::from_millis(550), // value determined empirically
            join_confirmation_timeout: Duration::from_secs(30),
            join_retry_poll: Duration::from_secs(30),
            join_retry_min: Duration::from_mins(1),
            join_retry_max: Duration::from_hours(6),
            unavailable_retry_after: Duration::from_hours(6),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TwitchApiClientCredentials {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_address: ListenAddr,
    #[serde(flatten)]
    pub twitch_api_credentials: TwitchApiClientCredentials,
    #[serde(with = "humantime_serde", default = "seven_days")]
    pub sessions_expire_after: Duration,
    #[serde(with = "humantime_serde", default = "one_hour")]
    pub recheck_twitch_auth_after: Duration,
    #[serde(with = "humantime_serde", default = "ten_seconds")]
    pub request_timeout: Duration,
    #[serde(default = "default_max_inflight_recent_requests")]
    pub max_inflight_recent_requests: usize,
    #[serde(default = "default_true")]
    pub adaptive_response_cache_enabled: bool,
    #[serde(with = "humantime_serde", default = "one_second")]
    pub adaptive_response_cache_max_age: Duration,
    #[serde(with = "humantime_serde", default = "two_seconds")]
    pub adaptive_response_cache_pressure_hold: Duration,
    #[serde(default = "default_adaptive_response_cache_enter_inflight")]
    pub adaptive_response_cache_enter_inflight: usize,
    #[serde(default = "default_adaptive_response_cache_max_entries")]
    pub adaptive_response_cache_max_entries: usize,
    #[serde(default = "default_adaptive_response_cache_max_bytes")]
    pub adaptive_response_cache_max_bytes: usize,
    #[serde(default = "default_adaptive_response_cache_max_entry_bytes")]
    pub adaptive_response_cache_max_entry_bytes: usize,
}

fn default_listen_addr() -> ListenAddr {
    ListenAddr::Tcp {
        address: "127.0.0.1:2790".parse().unwrap(),
    }
}

fn seven_days() -> Duration {
    Duration::from_hours(7 * 24)
}

fn one_hour() -> Duration {
    Duration::from_hours(1)
}

fn ten_seconds() -> Duration {
    Duration::from_secs(10)
}

fn one_second() -> Duration {
    Duration::from_secs(1)
}

fn two_seconds() -> Duration {
    Duration::from_secs(2)
}

fn default_true() -> bool {
    true
}

fn default_max_inflight_recent_requests() -> usize {
    16
}

fn default_adaptive_response_cache_enter_inflight() -> usize {
    16
}

fn default_adaptive_response_cache_max_entries() -> usize {
    256
}

fn default_adaptive_response_cache_max_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_adaptive_response_cache_max_entry_bytes() -> usize {
    1024 * 1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ListenAddr {
    #[serde(rename = "tcp")]
    Tcp { address: SocketAddr },
    #[cfg(unix)]
    #[serde(rename = "unix")]
    Unix { path: PathBuf },
}

#[derive(Error, Debug)]
pub enum LoadConfigError {
    #[error("Failed to read file: {0}")]
    ReadFile(std::io::Error),
    #[error("Failed to parse contents: {0}")]
    ParseContents(toml::de::Error),
}

pub async fn load_config(args: &Args) -> Result<Config, LoadConfigError> {
    let file_contents = tokio::fs::read(&args.config_path)
        .await
        .map_err(LoadConfigError::ReadFile)?;
    let config = toml::from_slice(&file_contents).map_err(LoadConfigError::ParseContents)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_configuration_example_parses() {
        let config: Config = toml::from_slice(include_bytes!("../config.toml")).unwrap();
        assert_eq!(
            config.control_store.path,
            PathBuf::from("data/control.sqlite")
        );
        assert_eq!(config.block_store.max_messages_per_channel, 800);

        let documented: Config =
            toml::from_slice(include_bytes!("../config/example.toml")).unwrap();
        assert_eq!(documented.block_store.max_messages_per_channel, 800);

        assert_eq!(documented.ingest.raw_firehoses.len(), 4);
        assert_eq!(documented.storage_budget.max_bytes, 2 * 1024 * 1024 * 1024);
    }
}
