use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};
use recent_messages2::storage::{
    CanonicalRecord, RawIrcError, SourceFidelity, canonicalize_raw_irc_from,
};
use reqwest::Url;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Semaphore, mpsc, watch};

use crate::config::{RepairConfig, RepairProviderConfig, RepairProviderKind};
use crate::coverage::{CoverageAssessment, CoverageInterval};
use crate::db::{ControlStore, RepairCheckpoint, RepairOutcome};
use crate::web::get_recent_messages::GetRecentMessagesQueryOptions;
use recent_messages2::storage::{AsyncSqliteBlockStore, MessageStore};

static REPAIR_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_repair_requests_total",
        "Conditional channel-repair attempts by provider and outcome",
        &["provider", "outcome"]
    )
    .unwrap()
});
static REPAIR_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "recentmessages_repair_request_seconds",
        "Conditional channel-repair request duration by provider",
        &["provider"]
    )
    .unwrap()
});

#[derive(Debug, Error)]
pub enum RepairConfigError {
    #[error(
        "repair bounds must be nonzero and fast_return_wait <= foreground_wait <= request_timeout"
    )]
    InvalidBounds,
    #[error("repair provider names must be non-empty and unique")]
    InvalidNames,
    #[error("repair provider `{name}` has an invalid base URL: {reason}")]
    InvalidBaseUrl { name: String, reason: String },
    #[error("failed to build repair HTTP client: {0}")]
    HttpClient(reqwest::Error),
}

#[derive(Debug, Error)]
pub enum RepairFetchError {
    #[error("repair request deadline expired")]
    Deadline,
    #[error("all configured repair providers failed")]
    AllFailed,
}

#[derive(Clone)]
struct Provider {
    name: String,
    base_url: Url,
    kind: RepairProviderKind,
    circuit: Arc<Mutex<CircuitState>>,
    failure_threshold: u32,
    open_duration: Duration,
}

#[derive(Default)]
struct CircuitState {
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    half_open_probe_inflight: bool,
}

#[derive(Clone)]
pub struct RepairClient {
    client: reqwest::Client,
    providers: Arc<[Provider]>,
    request_timeout: Duration,
    max_response_bytes: usize,
    permits: Arc<Semaphore>,
}

pub struct RepairResult {
    pub any_provider_succeeded: bool,
}

struct ProviderRecords {
    records: Vec<CanonicalRecord>,
    identity_complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairState {
    pub best_effort: bool,
    pub retry_suppressed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairCompletion {
    pub outcome: RepairOutcome,
}

#[derive(Clone)]
pub struct RepairCoordinator {
    client: RepairClient,
    control_store: &'static ControlStore,
    block_store: &'static AsyncSqliteBlockStore,
    in_flight: Arc<AsyncMutex<HashMap<String, watch::Receiver<Option<RepairCompletion>>>>>,
    foreground_wait: Duration,
    fast_return_wait: Duration,
    success_retry: Duration,
    partial_retry: Duration,
    failure_retry: Duration,
}

impl RepairCoordinator {
    pub fn from_config(
        config: &RepairConfig,
        control_store: &'static ControlStore,
        block_store: &'static AsyncSqliteBlockStore,
    ) -> Result<Option<Self>, RepairConfigError> {
        Ok(RepairClient::from_config(config)?.map(|client| Self {
            client,
            control_store,
            block_store,
            in_flight: Arc::new(AsyncMutex::new(HashMap::new())),
            foreground_wait: config.foreground_wait,
            fast_return_wait: config.fast_return_wait,
            success_retry: config.refresh_after,
            partial_retry: config.partial_retry_after,
            failure_retry: config.failure_retry_after,
        }))
    }

    pub async fn start_or_attach(
        &self,
        channel_login: &str,
        options: GetRecentMessagesQueryOptions,
        limit: usize,
        requested_start_ms: i64,
        requested_end_ms: i64,
    ) -> watch::Receiver<Option<RepairCompletion>> {
        let channel_login = channel_login.to_ascii_lowercase();
        let mut in_flight = self.in_flight.lock().await;
        if let Some(receiver) = in_flight.get(&channel_login) {
            return receiver.clone();
        }
        let (sender, receiver) = watch::channel(None);
        in_flight.insert(channel_login.clone(), receiver.clone());
        let coordinator = self.clone();
        tokio::spawn(async move {
            let completion = coordinator
                .run_job(
                    &channel_login,
                    options,
                    limit,
                    requested_start_ms,
                    requested_end_ms,
                    &sender,
                )
                .await;
            sender.send_replace(Some(completion));
            coordinator.in_flight.lock().await.remove(&channel_login);
        });
        receiver
    }

    pub async fn wait_foreground(
        &self,
        receiver: &mut watch::Receiver<Option<RepairCompletion>>,
    ) -> Option<RepairCompletion> {
        if let Some(completion) = *receiver.borrow() {
            return Some(completion);
        }
        if tokio::time::timeout(self.fast_return_wait, receiver.changed())
            .await
            .is_ok()
        {
            return *receiver.borrow();
        }
        let remaining = self.foreground_wait.saturating_sub(self.fast_return_wait);
        tokio::time::timeout(remaining, receiver.changed())
            .await
            .ok()
            .and_then(|_| *receiver.borrow())
    }

    async fn run_job(
        &self,
        channel_login: &str,
        options: GetRecentMessagesQueryOptions,
        limit: usize,
        requested_start_ms: i64,
        requested_end_ms: i64,
        completion_sender: &watch::Sender<Option<RepairCompletion>>,
    ) -> RepairCompletion {
        let (update_sender, mut updates) = mpsc::unbounded_channel();
        let fetch = self.client.fetch(
            channel_login,
            options,
            limit,
            requested_start_ms,
            requested_end_ms,
            update_sender,
        );
        tokio::pin!(fetch);
        let mut persisted_count = 0usize;
        let result = loop {
            tokio::select! {
                result = &mut fetch => break result,
                Some(records) = updates.recv() => {
                    let count = records.len();
                    if self.block_store.append_batch(records).await.is_ok() {
                        persisted_count = persisted_count.saturating_add(count);
                        completion_sender.send_replace(Some(RepairCompletion {
                            outcome: RepairOutcome::Partial,
                        }));
                    } else {
                        tracing::warn!(%channel_login, "Failed to persist repair records");
                    }
                }
            }
        };
        while let Ok(records) = updates.try_recv() {
            let count = records.len();
            if self.block_store.append_batch(records).await.is_ok() {
                persisted_count = persisted_count.saturating_add(count);
                completion_sender.send_replace(Some(RepairCompletion {
                    outcome: RepairOutcome::Partial,
                }));
            }
        }
        let (outcome, record_count) = match result {
            Ok(result) => {
                if persisted_count > 0 {
                    (RepairOutcome::Success, persisted_count)
                } else if result.any_provider_succeeded {
                    (RepairOutcome::Partial, persisted_count)
                } else {
                    (RepairOutcome::Failed, 0)
                }
            }
            Err(error) => {
                tracing::warn!(%error, %channel_login, "External channel repair failed");
                (RepairOutcome::Failed, 0)
            }
        };
        let completed_at_ms = chrono::Utc::now().timestamp_millis();
        let retry_after = match outcome {
            RepairOutcome::Success => self.success_retry,
            RepairOutcome::Partial => self.partial_retry,
            RepairOutcome::Failed => self.failure_retry,
        };
        let next_retry_ms = completed_at_ms
            .saturating_add(i64::try_from(retry_after.as_millis()).unwrap_or(i64::MAX));
        if let Err(error) = self
            .control_store
            .record_repair_checkpoint(
                channel_login,
                "configured-providers",
                outcome,
                completed_at_ms,
                requested_start_ms,
                requested_end_ms,
                next_retry_ms,
                record_count,
            )
            .await
        {
            tracing::warn!(%error, %channel_login, "Failed to persist repair checkpoint");
        }
        RepairCompletion { outcome }
    }
}

pub fn assess_repair_state(
    checkpoints: &[RepairCheckpoint],
    assessment: &CoverageAssessment,
    coverage: &[CoverageInterval],
    now_ms: i64,
    handoff_grace: Duration,
) -> RepairState {
    let grace_ms = i64::try_from(handoff_grace.as_millis()).unwrap_or(i64::MAX);
    let mut state = RepairState {
        best_effort: false,
        retry_suppressed: false,
    };
    for checkpoint in checkpoints {
        let covers_start = checkpoint.requested_start_ms <= assessment.required_start_ms;
        let overlaps = checkpoint.requested_end_ms >= assessment.required_start_ms;
        let exact_or_bridged = checkpoint.requested_end_ms >= assessment.required_end_ms
            || coverage_bridges(
                coverage,
                checkpoint.requested_end_ms,
                assessment.required_end_ms,
                grace_ms,
            );
        if checkpoint.outcome == RepairOutcome::Success
            && covers_start
            && (exact_or_bridged || checkpoint.next_retry_ms > now_ms)
        {
            state.best_effort = true;
        }
        if covers_start && overlaps && checkpoint.next_retry_ms > now_ms {
            state.retry_suppressed = true;
        }
    }
    state
}

fn coverage_bridges(
    coverage: &[CoverageInterval],
    checkpoint_end_ms: i64,
    required_end_ms: i64,
    handoff_grace_ms: i64,
) -> bool {
    if checkpoint_end_ms >= required_end_ms {
        return true;
    }
    let mut intervals = coverage.to_vec();
    intervals.sort_by_key(|interval| (interval.start_ms, interval.end_ms));
    let mut cursor = checkpoint_end_ms;
    let mut first = true;
    for interval in intervals {
        if interval.end_ms < cursor {
            continue;
        }
        let allowed_start = if first {
            cursor.saturating_add(handoff_grace_ms)
        } else {
            cursor.saturating_add(1)
        };
        if interval.start_ms > allowed_start {
            continue;
        }
        first = false;
        cursor = cursor.max(interval.end_ms);
        if cursor >= required_end_ms {
            return true;
        }
    }
    false
}

#[derive(Deserialize)]
struct CompatibleResponse {
    #[serde(default)]
    messages: Vec<String>,
}

impl RepairClient {
    pub fn from_config(config: &RepairConfig) -> Result<Option<Self>, RepairConfigError> {
        if config.providers.is_empty() {
            return Ok(None);
        }
        if config.max_inflight == 0
            || config.max_response_bytes == 0
            || config.request_timeout.is_zero()
            || config.fast_return_wait.is_zero()
            || config.foreground_wait.is_zero()
            || config.fast_return_wait > config.foreground_wait
            || config.foreground_wait > config.request_timeout
        {
            return Err(RepairConfigError::InvalidBounds);
        }
        let mut names = HashSet::new();
        if config
            .providers
            .iter()
            .any(|provider| provider.name.is_empty() || !names.insert(provider.name.clone()))
        {
            return Err(RepairConfigError::InvalidNames);
        }
        let providers = config
            .providers
            .iter()
            .map(|provider| {
                parse_provider(provider, config.failure_threshold, config.open_duration)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(RepairConfigError::HttpClient)?;
        Ok(Some(Self {
            client,
            providers: providers.into(),
            request_timeout: config.request_timeout,
            max_response_bytes: config.max_response_bytes,
            permits: Arc::new(Semaphore::new(config.max_inflight)),
        }))
    }

    pub async fn fetch(
        &self,
        channel_login: &str,
        mut options: GetRecentMessagesQueryOptions,
        limit: usize,
        requested_start_ms: i64,
        requested_end_ms: i64,
        updates: mpsc::UnboundedSender<Vec<CanonicalRecord>>,
    ) -> Result<RepairResult, RepairFetchError> {
        options.limit = Some(limit);
        options.hide_moderation_messages = false;
        options.hide_moderated_messages = false;
        options.clearchat_to_notice = false;
        let deadline = Instant::now() + self.request_timeout;
        let mut requests = FuturesUnordered::new();
        for provider in self.providers.iter() {
            if provider.begin_attempt(Instant::now()) {
                requests.push(self.fetch_provider(
                    provider,
                    channel_login,
                    options,
                    requested_start_ms,
                    requested_end_ms,
                    deadline,
                ));
            } else {
                observe(provider, "circuit_open", Instant::now());
            }
        }
        let mut any_succeeded = false;
        while let Some(result) = requests.next().await {
            match result {
                Ok(provider_result) => {
                    any_succeeded = true;
                    if !provider_result.records.is_empty() {
                        let _ = updates.send(provider_result.records);
                    }
                }
                Err(()) => {}
            }
        }
        if any_succeeded {
            Ok(RepairResult {
                any_provider_succeeded: true,
            })
        } else if Instant::now() >= deadline {
            Err(RepairFetchError::Deadline)
        } else {
            Err(RepairFetchError::AllFailed)
        }
    }

    async fn fetch_provider(
        &self,
        provider: &Provider,
        channel_login: &str,
        options: GetRecentMessagesQueryOptions,
        requested_start_ms: i64,
        requested_end_ms: i64,
        deadline: Instant,
    ) -> Result<ProviderRecords, ()> {
        let started = Instant::now();
        let remaining = deadline.saturating_duration_since(started);
        let _permit = tokio::time::timeout(remaining, self.permits.acquire())
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = tokio::time::timeout(
            remaining,
            self.fetch_provider_inner(
                provider,
                channel_login,
                options,
                requested_start_ms,
                requested_end_ms,
            ),
        )
        .await;
        match result {
            Ok(Ok(result)) => {
                provider.finish_attempt(result.identity_complete, Instant::now());
                observe(
                    provider,
                    if result.identity_complete {
                        "success"
                    } else {
                        "missing_identity"
                    },
                    started,
                );
                Ok(result)
            }
            Ok(Err(outcome)) => {
                provider.finish_attempt(false, Instant::now());
                observe(provider, outcome, started);
                Err(())
            }
            Err(_) => {
                provider.finish_attempt(false, Instant::now());
                observe(provider, "timeout", started);
                Err(())
            }
        }
    }

    async fn fetch_provider_inner(
        &self,
        provider: &Provider,
        channel_login: &str,
        options: GetRecentMessagesQueryOptions,
        requested_start_ms: i64,
        requested_end_ms: i64,
    ) -> Result<ProviderRecords, &'static str> {
        let channel_key = channel_login.to_ascii_lowercase();
        let request = match provider.kind {
            RepairProviderKind::RecentMessages => {
                let url = provider.base_url.join(&channel_key).map_err(|_| "url")?;
                self.client.get(url).query(&options)
            }
            RepairProviderKind::Rustlog => {
                let mut url = provider
                    .base_url
                    .join(&format!("channel/{channel_key}"))
                    .map_err(|_| "url")?;
                let from = chrono::DateTime::from_timestamp_millis(requested_start_ms)
                    .ok_or("range")?
                    .to_rfc3339();
                let to = chrono::DateTime::from_timestamp_millis(requested_end_ms)
                    .ok_or("range")?
                    .to_rfc3339();
                url.query_pairs_mut()
                    .append_pair("from", &from)
                    .append_pair("to", &to)
                    .append_pair("raw", "")
                    .append_pair("reverse", "")
                    .append_pair("limit", &options.limit.unwrap_or(800).to_string());
                self.client.get(url)
            }
        };
        let mut response = request
            .send()
            .await
            .map_err(|_| "transport")?
            .error_for_status()
            .map_err(|_| "status")?;
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err("oversize");
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "transport")? {
            if body.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err("oversize");
            }
            body.extend_from_slice(&chunk);
        }
        let messages = match provider.kind {
            RepairProviderKind::RecentMessages => {
                serde_json::from_slice::<CompatibleResponse>(&body)
                    .map_err(|_| "decode")?
                    .messages
            }
            RepairProviderKind::Rustlog => String::from_utf8(body)
                .map_err(|_| "decode")?
                .lines()
                .map(str::to_owned)
                .collect(),
        };
        let source_id = format!("repair:{}", provider.name);
        let fallback_received_at_ms = chrono::Utc::now().timestamp_millis();
        let mut records = Vec::with_capacity(messages.len());
        let mut identity_complete = true;
        for raw in messages {
            match canonicalize_raw_irc_from(
                &raw,
                fallback_received_at_ms,
                &source_id,
                SourceFidelity::Reconstructed,
            ) {
                Ok(mut record) if record.channel_key == channel_key => {
                    if provider.kind == RepairProviderKind::Rustlog {
                        record.received_at_ms = record.event_at_ms;
                    }
                    records.push(record)
                }
                Ok(_) | Err(RawIrcError::UnsupportedCommand(_)) => {}
                Err(RawIrcError::MissingIdentity { .. }) => identity_complete = false,
                Err(_) => return Err("invalid_irc"),
            }
        }
        Ok(ProviderRecords {
            records,
            identity_complete,
        })
    }
}

fn parse_provider(
    config: &RepairProviderConfig,
    failure_threshold: u32,
    open_duration: Duration,
) -> Result<Provider, RepairConfigError> {
    let mut base_url =
        Url::parse(&config.base_url).map_err(|error| RepairConfigError::InvalidBaseUrl {
            name: config.name.clone(),
            reason: error.to_string(),
        })?;
    if !matches!(base_url.scheme(), "http" | "https")
        || base_url.cannot_be_a_base()
        || base_url.query().is_some()
        || base_url.fragment().is_some()
    {
        return Err(RepairConfigError::InvalidBaseUrl {
            name: config.name.clone(),
            reason: "must be an HTTP(S) base URL without query or fragment".to_owned(),
        });
    }
    if !base_url.path().ends_with('/') {
        base_url.set_path(&format!("{}/", base_url.path()));
    }
    Ok(Provider {
        name: config.name.clone(),
        base_url,
        kind: config.kind,
        circuit: Arc::new(Mutex::new(CircuitState::default())),
        failure_threshold: failure_threshold.max(1),
        open_duration,
    })
}

impl Provider {
    fn begin_attempt(&self, now: Instant) -> bool {
        let mut state = self
            .circuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(opened_at) = state.opened_at else {
            return true;
        };
        if now.saturating_duration_since(opened_at) < self.open_duration
            || state.half_open_probe_inflight
        {
            return false;
        }
        state.half_open_probe_inflight = true;
        true
    }

    fn finish_attempt(&self, succeeded: bool, now: Instant) {
        let mut state = self
            .circuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if succeeded {
            *state = CircuitState::default();
            return;
        }
        state.half_open_probe_inflight = false;
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.opened_at.is_some() || state.consecutive_failures >= self.failure_threshold {
            state.opened_at = Some(now);
        }
    }
}

fn observe(provider: &Provider, outcome: &str, started: Instant) {
    REPAIR_REQUESTS
        .with_label_values(&[&provider.name, outcome])
        .inc();
    REPAIR_DURATION
        .with_label_values(&[&provider.name])
        .observe(started.elapsed().as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::{Path, RawQuery, State},
        routing::get,
    };
    use recent_messages2::storage::{QueryRequest, SourceFidelity};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn assessment() -> CoverageAssessment {
        CoverageAssessment {
            sufficient: false,
            required_start_ms: 100,
            required_end_ms: 200,
            gaps: vec![(100, 200)],
        }
    }

    fn checkpoint(outcome: RepairOutcome, next_retry_ms: i64) -> RepairCheckpoint {
        RepairCheckpoint {
            outcome,
            completed_at_ms: 150,
            requested_start_ms: 50,
            requested_end_ms: 150,
            next_retry_ms,
            record_count: 10,
        }
    }

    fn config() -> RepairConfig {
        RepairConfig {
            providers: vec![RepairProviderConfig {
                name: "mirror".to_owned(),
                base_url: "https://recent-messages.example/api/".to_owned(),
                kind: RepairProviderKind::RecentMessages,
            }],
            ..RepairConfig::default()
        }
    }

    #[test]
    fn validates_provider_configuration() {
        assert!(
            RepairClient::from_config(&RepairConfig::default())
                .unwrap()
                .is_none()
        );
        assert!(RepairClient::from_config(&config()).unwrap().is_some());
        let mut invalid = config();
        invalid.providers[0].base_url = "file:///tmp/messages".to_owned();
        assert!(matches!(
            RepairClient::from_config(&invalid),
            Err(RepairConfigError::InvalidBaseUrl { .. })
        ));
        let mut duplicate = config();
        duplicate.providers.push(duplicate.providers[0].clone());
        assert!(matches!(
            RepairClient::from_config(&duplicate),
            Err(RepairConfigError::InvalidNames)
        ));
        let mut invalid_deadline = config();
        invalid_deadline.foreground_wait = Duration::from_secs(11);
        assert!(matches!(
            RepairClient::from_config(&invalid_deadline),
            Err(RepairConfigError::InvalidBounds)
        ));
    }

    #[test]
    fn successful_checkpoint_suppresses_refresh_during_cooldown() {
        let state = assess_repair_state(
            &[checkpoint(RepairOutcome::Success, 300)],
            &assessment(),
            &[],
            200,
            Duration::from_secs(5),
        );
        assert_eq!(
            state,
            RepairState {
                best_effort: true,
                retry_suppressed: true,
            }
        );
    }

    #[test]
    fn authoritative_tail_keeps_a_completed_repair_relevant() {
        let state = assess_repair_state(
            &[checkpoint(RepairOutcome::Success, 160)],
            &assessment(),
            &[CoverageInterval {
                source: "direct-irc".to_owned(),
                start_ms: 152,
                end_ms: 200,
            }],
            200,
            Duration::from_millis(5),
        );
        assert!(state.best_effort);
        assert!(!state.retry_suppressed);
    }

    #[test]
    fn failed_checkpoint_only_applies_retry_cooldown() {
        let state = assess_repair_state(
            &[checkpoint(RepairOutcome::Failed, 300)],
            &assessment(),
            &[],
            200,
            Duration::from_secs(5),
        );
        assert!(!state.best_effort);
        assert!(state.retry_suppressed);
    }

    #[tokio::test]
    async fn concurrent_callers_share_background_repair_after_foreground_timeout() {
        async fn delayed_messages(
            State(requests): State<Arc<AtomicUsize>>,
        ) -> Json<serde_json::Value> {
            requests.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(80)).await;
            Json(json!({
                "messages": [
                    "@id=11111111-1111-1111-1111-111111111111;tmi-sent-ts=100;rm-received-ts=100 :user!user@user.tmi.twitch.tv PRIVMSG #example :hello",
                    "@tmi-sent-ts=101;rm-received-ts=101 :tmi.twitch.tv USERNOTICE #example"
                ]
            }))
        }

        let requests = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/{channel}", get(delayed_messages))
            .with_state(Arc::clone(&requests));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let unique = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let control_store = Box::leak(Box::new(
            ControlStore::open(std::env::temp_dir().join(format!(
                "rm-repair-coordinator-control-{}-{unique}.sqlite",
                std::process::id()
            )))
            .unwrap(),
        ));
        let block_store = Box::leak(Box::new(
            AsyncSqliteBlockStore::open(
                std::env::temp_dir().join(format!(
                    "rm-repair-coordinator-blocks-{}-{unique}.sqlite",
                    std::process::id()
                )),
                2,
                800,
                8,
                1,
            )
            .unwrap(),
        ));
        let mut config = config();
        config.providers[0].base_url = format!("http://{address}/");
        config.fast_return_wait = Duration::from_millis(5);
        config.foreground_wait = Duration::from_millis(10);
        config.request_timeout = Duration::from_millis(250);
        let coordinator = RepairCoordinator::from_config(&config, control_store, block_store)
            .unwrap()
            .unwrap();

        let mut first = coordinator
            .start_or_attach(
                "example",
                GetRecentMessagesQueryOptions::default(),
                800,
                0,
                200,
            )
            .await;
        let mut second = coordinator
            .start_or_attach(
                "example",
                GetRecentMessagesQueryOptions::default(),
                800,
                0,
                200,
            )
            .await;
        assert!(coordinator.wait_foreground(&mut first).await.is_none());
        assert!(coordinator.wait_foreground(&mut second).await.is_none());
        tokio::time::sleep(Duration::from_millis(120)).await;

        assert_eq!(requests.load(Ordering::Relaxed), 1);
        assert_eq!(
            control_store
                .get_repair_checkpoints("example")
                .await
                .unwrap()[0]
                .outcome,
            RepairOutcome::Success
        );
        let stored = block_store
            .query(QueryRequest {
                channel_key: "example".to_owned(),
                after_ms: None,
                before_ms: None,
                limit: 800,
            })
            .await
            .unwrap();
        assert_eq!(stored.records.len(), 1);
        assert_eq!(stored.records[0].fidelity, SourceFidelity::Reconstructed);
        server.abort();
    }

    #[tokio::test]
    async fn rustlog_adapter_requests_a_bounded_raw_reverse_window() {
        async fn rustlog(RawQuery(query): RawQuery) -> String {
            let query = query.unwrap_or_default();
            assert!(query.contains("from="));
            assert!(query.contains("to="));
            assert!(query.contains("raw="));
            assert!(query.contains("reverse="));
            assert!(query.contains("limit=800"));
            "@id=11111111-1111-1111-1111-111111111111;tmi-sent-ts=100 :user!user@user.tmi.twitch.tv PRIVMSG #example :hello\r\n".to_owned()
        }

        let app = Router::new().route("/channel/{channel}", get(rustlog));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut config = config();
        config.providers[0].base_url = format!("http://{address}/");
        config.providers[0].kind = RepairProviderKind::Rustlog;
        let client = RepairClient::from_config(&config).unwrap().unwrap();
        let (updates, mut records) = mpsc::unbounded_channel();
        let result = client
            .fetch(
                "example",
                GetRecentMessagesQueryOptions::default(),
                800,
                0,
                200,
                updates,
            )
            .await
            .unwrap();
        let record = records.recv().await.unwrap().pop().unwrap();
        assert!(result.any_provider_succeeded);
        assert_eq!(record.received_at_ms, 100);
        assert_eq!(record.source_id, "repair:mirror");
        server.abort();
    }

    #[tokio::test]
    async fn first_viable_provider_is_delivered_while_slower_work_continues() {
        async fn provider(
            Path((speed, channel)): Path<(String, String)>,
        ) -> Json<serde_json::Value> {
            let (delay, id, timestamp) = if speed == "fast" {
                (20, "11111111-1111-1111-1111-111111111111", 100)
            } else {
                (150, "22222222-2222-2222-2222-222222222222", 101)
            };
            tokio::time::sleep(Duration::from_millis(delay)).await;
            Json(json!({
                "messages": [format!(
                    "@id={id};tmi-sent-ts={timestamp};rm-received-ts={timestamp} :user!user@user.tmi.twitch.tv PRIVMSG #{channel} :hello"
                )]
            }))
        }

        let app = Router::new().route("/{speed}/{channel}", get(provider));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut config = config();
        config.providers = vec![
            RepairProviderConfig {
                name: "fast".to_owned(),
                base_url: format!("http://{address}/fast/"),
                kind: RepairProviderKind::RecentMessages,
            },
            RepairProviderConfig {
                name: "slow".to_owned(),
                base_url: format!("http://{address}/slow/"),
                kind: RepairProviderKind::RecentMessages,
            },
        ];
        let client = RepairClient::from_config(&config).unwrap().unwrap();
        let (updates, mut records) = mpsc::unbounded_channel();
        let fetch = tokio::spawn(async move {
            client
                .fetch(
                    "example",
                    GetRecentMessagesQueryOptions::default(),
                    800,
                    0,
                    200,
                    updates,
                )
                .await
        });

        let first = tokio::time::timeout(Duration::from_millis(100), records.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first[0].source_id, "repair:fast");
        assert!(!fetch.is_finished());
        assert!(fetch.await.unwrap().unwrap().any_provider_succeeded);
        assert_eq!(records.recv().await.unwrap()[0].source_id, "repair:slow");
        server.abort();
    }
}
