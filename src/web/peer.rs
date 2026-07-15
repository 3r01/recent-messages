use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::Utc;
use http::{HeaderMap, Request, StatusCode, Uri, header};
use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};
use recent_messages2::storage::{CanonicalRecord, MessageStore, QueryRequest};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::config::{PeerConfig, PeerNodeConfig};
use crate::coverage::CoverageInterval;
use crate::web::WebAppData;
use crate::web::error::ApiError;
use crate::web::get_recent_messages::{GetRecentMessagesPath, GetRecentMessagesQueryOptions};

const HOP_HEADER: &str = "x-rm-hop";
const NO_PEER_HEADER: &str = "x-rm-no-peer";
const OVERLOADED_HEADER: &str = "x-rm-overloaded";
const COVERAGE_HEADER: &str = "x-rm-coverage";
const PUBLIC_RECENT_PREFIX: &str = "/api/v2/recent-messages/";

static PEER_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_peer_requests_total",
        "Owned-peer local-read and whole-request attempts by peer and outcome",
        &["peer", "outcome"]
    )
    .unwrap()
});
static PEER_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "recentmessages_peer_request_seconds",
        "Owned-peer local-read and whole-request duration by peer",
        &["peer"]
    )
    .unwrap()
});

#[derive(Debug, Error)]
pub enum PeerConfigError {
    #[error("peer nodes require a non-empty shared_token")]
    MissingToken,
    #[error(
        "peer max_inflight, delegate_timeout, and max_delegate_response_bytes must be positive"
    )]
    InvalidLimits,
    #[error("peer `{name}` has an invalid base URL: {reason}")]
    InvalidBaseUrl { name: String, reason: String },
    #[error("failed to build peer HTTP client: {0}")]
    HttpClient(reqwest::Error),
}

#[derive(Debug, Error)]
pub enum PeerFetchError {
    #[error("peer request deadline expired")]
    Deadline,
    #[error("all configured peers failed")]
    AllFailed,
}

#[derive(Clone)]
struct PeerNode {
    name: String,
    base_url: Url,
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
pub struct PeerClient {
    client: reqwest::Client,
    nodes: Arc<[PeerNode]>,
    shared_token: Arc<str>,
    request_timeout: std::time::Duration,
    delegate_timeout: std::time::Duration,
    max_delegate_response_bytes: usize,
    permits: Arc<Semaphore>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PeerLocalResponse {
    pub records: Vec<CanonicalRecord>,
    #[serde(default)]
    pub coverage: Vec<CoverageInterval>,
    pub oldest_retained_at_ms: Option<i64>,
    pub newest_retained_at_ms: Option<i64>,
}

impl PeerClient {
    pub fn from_config(config: &PeerConfig) -> Result<Option<Self>, PeerConfigError> {
        if config.nodes.is_empty() {
            return Ok(None);
        }
        if config.max_inflight == 0
            || config.delegate_timeout.is_zero()
            || config.max_delegate_response_bytes == 0
        {
            return Err(PeerConfigError::InvalidLimits);
        }
        let shared_token = config
            .shared_token
            .as_deref()
            .filter(|token| !token.is_empty())
            .ok_or(PeerConfigError::MissingToken)?;
        let nodes = config
            .nodes
            .iter()
            .map(|node| parse_node(node, config.failure_threshold, config.open_duration))
            .collect::<Result<Vec<_>, _>>()?;
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(PeerConfigError::HttpClient)?;
        Ok(Some(Self {
            client,
            nodes: nodes.into(),
            shared_token: Arc::from(shared_token),
            request_timeout: config.request_timeout,
            delegate_timeout: config.delegate_timeout,
            max_delegate_response_bytes: config.max_delegate_response_bytes,
            permits: Arc::new(Semaphore::new(config.max_inflight)),
        }))
    }

    pub async fn fetch_local(
        &self,
        channel_login: &str,
        mut options: GetRecentMessagesQueryOptions,
        limit: usize,
    ) -> Result<Option<PeerLocalResponse>, PeerFetchError> {
        options.limit = Some(limit);
        let deadline = Instant::now() + self.request_timeout;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _permit = tokio::time::timeout(remaining, self.permits.acquire())
            .await
            .map_err(|_| PeerFetchError::Deadline)?
            .map_err(|_| PeerFetchError::AllFailed)?;

        for node in self.nodes.iter() {
            let attempt_started = Instant::now();
            if !node.begin_attempt(attempt_started) {
                observe_attempt(node, "circuit_open", attempt_started);
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                node.finish_attempt(false, Instant::now());
                return Err(PeerFetchError::Deadline);
            }
            let Ok(url) = node
                .base_url
                .join(&format!("internal/v1/local-recent/{channel_login}"))
            else {
                node.finish_attempt(false, Instant::now());
                continue;
            };
            let response = self
                .client
                .get(url)
                .bearer_auth(self.shared_token.as_ref())
                .header(HOP_HEADER, "1")
                .header(NO_PEER_HEADER, "1")
                .query(&options)
                .timeout(remaining)
                .send()
                .await;
            let Ok(response) = response else {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "transport", attempt_started);
                continue;
            };
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "not_found", attempt_started);
                continue;
            }
            let Ok(response) = response.error_for_status() else {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "status", attempt_started);
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                node.finish_attempt(false, Instant::now());
                return Err(PeerFetchError::Deadline);
            }
            if let Ok(result) = tokio::time::timeout(remaining, response.json()).await
                && let Ok(result) = result
            {
                node.finish_attempt(true, Instant::now());
                observe_attempt(node, "success", attempt_started);
                return Ok(Some(result));
            }
            node.finish_attempt(false, Instant::now());
            observe_attempt(node, "decode", attempt_started);
        }
        Err(PeerFetchError::AllFailed)
    }

    async fn delegate_recent(
        &self,
        channel_login: &str,
        query: Option<&str>,
    ) -> Result<Response, PeerFetchError> {
        let deadline = Instant::now() + self.delegate_timeout;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _permit = tokio::time::timeout(remaining, self.permits.acquire())
            .await
            .map_err(|_| PeerFetchError::Deadline)?
            .map_err(|_| PeerFetchError::AllFailed)?;

        for node in self.nodes.iter() {
            let attempt_started = Instant::now();
            if !node.begin_attempt(attempt_started) {
                observe_attempt(node, "delegate_circuit_open", attempt_started);
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                node.finish_attempt(false, Instant::now());
                return Err(PeerFetchError::Deadline);
            }
            let Ok(mut url) = node
                .base_url
                .join(&format!("internal/v1/serve/{channel_login}"))
            else {
                node.finish_attempt(false, Instant::now());
                continue;
            };
            url.set_query(query);
            let response = self
                .client
                .get(url)
                .bearer_auth(self.shared_token.as_ref())
                .header(HOP_HEADER, "1")
                .header(NO_PEER_HEADER, "1")
                .timeout(remaining)
                .send()
                .await;
            let Ok(mut response) = response else {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "delegate_transport", attempt_started);
                continue;
            };
            let status = response.status();
            if !status.is_success() {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "delegate_status", attempt_started);
                continue;
            }
            if response.content_length().is_some_and(|length| {
                length > u64::try_from(self.max_delegate_response_bytes).unwrap_or(u64::MAX)
            }) {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "delegate_oversize", attempt_started);
                continue;
            }

            let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
            let coverage = response.headers().get(COVERAGE_HEADER).cloned();
            let mut body = Vec::new();
            let mut failed = false;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    failed = true;
                    break;
                }
                match tokio::time::timeout(remaining, response.chunk()).await {
                    Ok(Ok(Some(chunk)))
                        if body.len().saturating_add(chunk.len())
                            <= self.max_delegate_response_bytes =>
                    {
                        body.extend_from_slice(&chunk);
                    }
                    Ok(Ok(None)) => break,
                    _ => {
                        failed = true;
                        break;
                    }
                }
            }
            if failed {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "delegate_body", attempt_started);
                continue;
            }

            let mut delegated = Response::builder().status(status);
            if let Some(content_type) = content_type {
                delegated = delegated.header(header::CONTENT_TYPE, content_type);
            }
            if let Some(coverage) = coverage {
                delegated = delegated.header(COVERAGE_HEADER, coverage);
            }
            let Ok(delegated) = delegated.body(Body::from(body)) else {
                node.finish_attempt(false, Instant::now());
                observe_attempt(node, "delegate_response", attempt_started);
                continue;
            };
            node.finish_attempt(true, Instant::now());
            observe_attempt(node, "delegate_success", attempt_started);
            return Ok(delegated);
        }
        Err(PeerFetchError::AllFailed)
    }
}

fn parse_node(
    config: &PeerNodeConfig,
    failure_threshold: u32,
    open_duration: Duration,
) -> Result<PeerNode, PeerConfigError> {
    let mut base_url =
        Url::parse(&config.base_url).map_err(|error| PeerConfigError::InvalidBaseUrl {
            name: config.name.clone(),
            reason: error.to_string(),
        })?;
    if !matches!(base_url.scheme(), "http" | "https")
        || base_url.cannot_be_a_base()
        || base_url.query().is_some()
        || base_url.fragment().is_some()
    {
        return Err(PeerConfigError::InvalidBaseUrl {
            name: config.name.clone(),
            reason: "must be an HTTP(S) base URL without query or fragment".to_owned(),
        });
    }
    if !base_url.path().ends_with('/') {
        base_url.set_path(&format!("{}/", base_url.path()));
    }
    Ok(PeerNode {
        name: config.name.clone(),
        base_url,
        circuit: Arc::new(Mutex::new(CircuitState::default())),
        failure_threshold: failure_threshold.max(1),
        open_duration,
    })
}

impl PeerNode {
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

fn observe_attempt(node: &PeerNode, outcome: &str, started: Instant) {
    PEER_REQUESTS
        .with_label_values(&[&node.name, outcome])
        .inc();
    PEER_DURATION
        .with_label_values(&[&node.name])
        .observe(started.elapsed().as_secs_f64());
}

pub fn allows_peer_fetch(headers: &HeaderMap) -> Result<bool, ApiError> {
    if headers
        .get(NO_PEER_HEADER)
        .is_some_and(|value| value.as_bytes() == b"1")
    {
        return Ok(false);
    }
    let hop = match headers.get(HOP_HEADER) {
        None => 0,
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u8>().ok())
            .ok_or(ApiError::InvalidPeerHop)?,
    };
    if hop > 1 {
        return Err(ApiError::InvalidPeerHop);
    }
    Ok(hop == 0)
}

pub async fn delegate_overload(request: Request<Body>, next: Next) -> Response {
    let app_data = request.extensions().get::<WebAppData>().cloned();
    let target = delegate_target(request.uri());
    let peer_allowed = allows_peer_fetch(request.headers()).unwrap_or(false);
    let response = next.run(request).await;
    if response.status() != StatusCode::SERVICE_UNAVAILABLE
        || response
            .headers()
            .get(OVERLOADED_HEADER)
            .is_none_or(|value| value.as_bytes() != b"1")
        || !peer_allowed
    {
        return response;
    }
    let Some(app_data) = app_data else {
        return response;
    };
    let Some(peer_client) = app_data.peer_client.as_ref() else {
        return response;
    };
    let Some((channel_login, query)) = target else {
        return response;
    };
    peer_client
        .delegate_recent(&channel_login, query.as_deref())
        .await
        .unwrap_or(response)
}

fn delegate_target(uri: &Uri) -> Option<(String, Option<String>)> {
    let channel = uri.path().strip_prefix(PUBLIC_RECENT_PREFIX)?;
    if channel.is_empty()
        || channel.contains('/')
        || twitch_irc::validate::validate_login(channel).is_err()
    {
        return None;
    }
    Some((
        channel.to_owned(),
        uri.query().map(std::borrow::ToOwned::to_owned),
    ))
}

pub fn merge_records(
    local: Vec<CanonicalRecord>,
    peer: Vec<CanonicalRecord>,
    limit: usize,
) -> Vec<CanonicalRecord> {
    let mut retained = HashMap::<[u8; 32], CanonicalRecord>::new();
    for record in local.into_iter().chain(peer) {
        match retained.get_mut(&record.event_key) {
            Some(current) if record.should_replace(current) => *current = record,
            Some(_) => {}
            None => {
                retained.insert(record.event_key, record);
            }
        }
    }
    let mut records = retained.into_values().collect::<Vec<_>>();
    records.sort_by_key(|record| (record.received_at_ms, record.event_at_ms, record.event_key));
    if records.len() > limit {
        records.drain(..records.len() - limit);
    }
    records
}

pub async fn get_local_recent(
    path_options: Result<Path<GetRecentMessagesPath>, PathRejection>,
    query_options: Result<Query<GetRecentMessagesQueryOptions>, QueryRejection>,
    headers: HeaderMap,
    Extension(app_data): Extension<WebAppData>,
) -> Result<Json<PeerLocalResponse>, ApiError> {
    authorize_peer(&headers, app_data.config.peer.shared_token.as_deref())?;
    let Path(GetRecentMessagesPath { channel_login }) =
        path_options.map_err(|_| ApiError::InvalidPath)?;
    let Query(options) = query_options.map_err(|_| ApiError::InvalidQuery)?;
    twitch_irc::validate::validate_login(&channel_login).map_err(ApiError::InvalidChannelLogin)?;
    if app_data
        .control_store
        .is_channel_ignored(&channel_login)
        .await
        .map_err(ApiError::GetChannelIgnored)?
    {
        return Err(ApiError::ChannelIgnored(channel_login));
    }
    let limit = options
        .limit
        .unwrap_or(app_data.config.app.max_buffer_size)
        .min(app_data.config.app.max_buffer_size);
    if limit == 0 {
        return Ok(Json(PeerLocalResponse {
            records: Vec::new(),
            coverage: Vec::new(),
            oldest_retained_at_ms: None,
            newest_retained_at_ms: None,
        }));
    }
    let request = QueryRequest {
        channel_key: channel_login.to_ascii_lowercase(),
        after_ms: options.after.map(|value| value.timestamp_millis()),
        before_ms: options.before.map(|value| value.timestamp_millis()),
        limit,
    };
    let result = app_data
        .block_store
        .query(request.clone())
        .await
        .map_err(ApiError::GetLocalMessages)?;
    let open = app_data
        .open_blocks
        .as_deref()
        .map(|open| open.query(&request))
        .transpose()
        .map_err(|_| {
            ApiError::GetLocalMessages(recent_messages2::storage::StoreError::LockPoisoned)
        })?
        .unwrap_or_default();
    let records = merge_records(result.records, open, limit);
    let oldest_retained_at_ms = records.first().map(|record| record.received_at_ms);
    let newest_retained_at_ms = records.last().map(|record| record.received_at_ms);
    let coverage = app_data
        .control_store
        .get_coverage_intervals(&channel_login, Utc::now().timestamp_millis())
        .await
        .map_err(ApiError::GetCoverage)?;
    Ok(Json(PeerLocalResponse {
        records,
        coverage,
        oldest_retained_at_ms,
        newest_retained_at_ms,
    }))
}

pub async fn serve_recent(
    path_options: Result<Path<GetRecentMessagesPath>, PathRejection>,
    query_options: Result<Query<GetRecentMessagesQueryOptions>, QueryRejection>,
    mut headers: HeaderMap,
    Extension(app_data): Extension<WebAppData>,
) -> Response {
    if let Err(error) = authorize_peer(&headers, app_data.config.peer.shared_token.as_deref()) {
        return error.into_response();
    }
    headers.insert(HOP_HEADER, http::HeaderValue::from_static("1"));
    headers.insert(NO_PEER_HEADER, http::HeaderValue::from_static("1"));
    crate::web::get_recent_messages::get_recent_messages(
        path_options,
        query_options,
        headers,
        Extension(app_data),
    )
    .await
    .into_response()
}

fn authorize_peer(headers: &HeaderMap, expected: Option<&str>) -> Result<(), ApiError> {
    let expected = expected
        .filter(|token| !token.is_empty())
        .ok_or(ApiError::PeerAccessDisabled)?;
    let supplied = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ApiError::UnauthorizedPeer)?;
    if !constant_time_equal(supplied.as_bytes(), expected.as_bytes()) {
        return Err(ApiError::UnauthorizedPeer);
    }
    Ok(())
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::Query;
    use axum::routing::get;
    use recent_messages2::storage::SourceFidelity;
    use tokio::net::TcpListener;

    fn record(sequence: u8, fidelity: SourceFidelity) -> CanonicalRecord {
        CanonicalRecord {
            channel_key: "channel".to_owned(),
            event_at_ms: i64::from(sequence),
            received_at_ms: i64::from(sequence),
            event_key: [sequence; 32],
            source_id: "test".to_owned(),
            fidelity,
            raw_irc: vec![sequence],
        }
    }

    #[test]
    fn merge_deduplicates_upgrades_orders_and_limits() {
        let local = vec![
            record(1, SourceFidelity::Reconstructed),
            record(2, SourceFidelity::Reconstructed),
        ];
        let peer = vec![
            record(1, SourceFidelity::DirectIrc),
            record(3, SourceFidelity::Reconstructed),
        ];
        let merged = merge_records(local.clone(), peer.clone(), 3);
        assert_eq!(merged[0].fidelity, SourceFidelity::DirectIrc);
        let merged = merge_records(local, peer, 2);
        assert_eq!(
            merged
                .iter()
                .map(|record| record.event_at_ms)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn no_peer_and_hop_headers_prevent_recursion() {
        let mut headers = HeaderMap::new();
        assert!(allows_peer_fetch(&headers).unwrap());
        headers.insert(NO_PEER_HEADER, "1".parse().unwrap());
        assert!(!allows_peer_fetch(&headers).unwrap());
        headers.remove(NO_PEER_HEADER);
        headers.insert(HOP_HEADER, "1".parse().unwrap());
        assert!(!allows_peer_fetch(&headers).unwrap());
        headers.insert(HOP_HEADER, "2".parse().unwrap());
        assert!(allows_peer_fetch(&headers).is_err());
    }

    #[test]
    fn extracts_only_valid_public_delegate_targets() {
        let target = delegate_target(
            &"/api/v2/recent-messages/example_1?limit=50&before=123"
                .parse()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(target.0, "example_1");
        assert_eq!(target.1.as_deref(), Some("limit=50&before=123"));
        assert!(delegate_target(&"/api/v2/recent-messages/".parse().unwrap()).is_none());
        assert!(delegate_target(&"/api/v2/recent-messages/bad/path".parse().unwrap()).is_none());
    }

    #[test]
    fn only_explicit_overload_errors_are_delegation_candidates() {
        let response = ApiError::ServiceOverloaded.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[OVERLOADED_HEADER], "1");
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        let response = ApiError::GetCoverage(crate::db::StorageError::Worker("test".to_owned()))
            .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!response.headers().contains_key(OVERLOADED_HEADER));
    }

    #[test]
    fn validates_peer_configuration_and_tokens() {
        let mut config = PeerConfig::default();
        config.nodes.push(PeerNodeConfig {
            name: "node-b".to_owned(),
            base_url: "http://10.0.0.2:2790/".to_owned(),
        });
        assert!(matches!(
            PeerClient::from_config(&config),
            Err(PeerConfigError::MissingToken)
        ));
        config.shared_token = Some("secret".to_owned());
        assert!(PeerClient::from_config(&config).unwrap().is_some());
        assert!(constant_time_equal(b"secret", b"secret"));
        assert!(!constant_time_equal(b"secret", b"wrong!"));
    }

    #[test]
    fn circuit_opens_and_allows_one_recovery_probe() {
        let node = parse_node(
            &PeerNodeConfig {
                name: "node-b".to_owned(),
                base_url: "http://127.0.0.1:2790/".to_owned(),
            },
            2,
            Duration::from_millis(10),
        )
        .unwrap();
        let now = Instant::now();
        assert!(node.begin_attempt(now));
        node.finish_attempt(false, now);
        assert!(node.begin_attempt(now));
        node.finish_attempt(false, now);
        assert!(!node.begin_attempt(now));

        let after_open = now + Duration::from_millis(11);
        assert!(node.begin_attempt(after_open));
        assert!(!node.begin_attempt(after_open));
        node.finish_attempt(true, after_open);
        assert!(node.begin_attempt(after_open));
    }

    #[tokio::test]
    #[ignore = "binds a loopback peer server"]
    async fn whole_request_delegation_preserves_query_headers_and_body() {
        async fn serve(
            headers: HeaderMap,
            Query(query): Query<HashMap<String, String>>,
        ) -> Response {
            let authorized = headers
                .get(header::AUTHORIZATION)
                .is_some_and(|value| value.as_bytes() == b"Bearer secret");
            let guarded = headers
                .get(HOP_HEADER)
                .is_some_and(|value| value.as_bytes() == b"1")
                && headers
                    .get(NO_PEER_HEADER)
                    .is_some_and(|value| value.as_bytes() == b"1");
            if !authorized || !guarded || query.get("limit").map(String::as_str) != Some("2") {
                return StatusCode::BAD_REQUEST.into_response();
            }
            (
                [(COVERAGE_HEADER, "best-effort")],
                Json(serde_json::json!({
                    "messages": [],
                    "error": null,
                    "error_code": null
                })),
            )
                .into_response()
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/internal/v1/serve/channel", get(serve)),
            )
            .await
            .unwrap();
        });
        let mut config = PeerConfig {
            shared_token: Some("secret".to_owned()),
            ..PeerConfig::default()
        };
        config.nodes.push(PeerNodeConfig {
            name: "node-b".to_owned(),
            base_url: format!("http://{address}/"),
        });
        let client = PeerClient::from_config(&config).unwrap().unwrap();
        let response = client
            .delegate_recent("channel", Some("limit=2"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[COVERAGE_HEADER], "best-effort");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["messages"], serde_json::json!([]));
        server.abort();
    }
}
