use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes, HttpBody, to_bytes};
use axum::middleware::Next;
use axum::response::Response;
use http::{Request, StatusCode, header};
use prometheus::{IntCounterVec, IntGauge, register_int_counter_vec, register_int_gauge};
use thiserror::Error;

use crate::config::WebConfig;
use crate::web::WebAppData;

const COVERAGE_HEADER: &str = "x-rm-coverage";

static CACHE_EVENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_adaptive_response_cache_total",
        "Adaptive recent-response cache events by outcome",
        &["outcome"]
    )
    .unwrap()
});
static CACHE_ENTRIES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_adaptive_response_cache_entries",
        "Exact recent responses retained for pressure reuse"
    )
    .unwrap()
});
static CACHE_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_adaptive_response_cache_bytes",
        "Body bytes retained for adaptive recent-response reuse"
    )
    .unwrap()
});
#[derive(Debug, Error)]
pub enum ResponseCacheConfigError {
    #[error("adaptive response-cache limits and durations must be positive")]
    InvalidLimits,
    #[error("adaptive response-cache entry limit cannot exceed its total byte limit")]
    EntryExceedsTotal,
}

#[derive(Clone)]
pub struct AdaptiveResponseCache {
    inner: Arc<CacheInner>,
}

struct CacheInner {
    config: CacheConfig,
    entries: Mutex<CacheState>,
    pressure: Mutex<PressureState>,
}

#[derive(Clone, Copy)]
struct CacheConfig {
    max_age: Duration,
    pressure_hold: Duration,
    enter_inflight: usize,
    max_entries: usize,
    max_bytes: usize,
    max_entry_bytes: usize,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<String, CachedResponse>,
    insertion_order: VecDeque<String>,
    bytes: usize,
}

#[derive(Clone)]
struct CachedResponse {
    inserted_at: Instant,
    content_type: Option<http::HeaderValue>,
    coverage: Option<http::HeaderValue>,
    body: Bytes,
}

#[derive(Default)]
struct PressureState {
    until: Option<Instant>,
}

impl AdaptiveResponseCache {
    pub fn from_config(config: &WebConfig) -> Result<Option<Self>, ResponseCacheConfigError> {
        if !config.adaptive_response_cache_enabled {
            return Ok(None);
        }
        let config = CacheConfig {
            max_age: config.adaptive_response_cache_max_age,
            pressure_hold: config.adaptive_response_cache_pressure_hold,
            enter_inflight: config.adaptive_response_cache_enter_inflight,
            max_entries: config.adaptive_response_cache_max_entries,
            max_bytes: config.adaptive_response_cache_max_bytes,
            max_entry_bytes: config.adaptive_response_cache_max_entry_bytes,
        };
        if config.max_age.is_zero()
            || config.pressure_hold.is_zero()
            || config.enter_inflight == 0
            || config.max_entries == 0
            || config.max_bytes == 0
            || config.max_entry_bytes == 0
        {
            return Err(ResponseCacheConfigError::InvalidLimits);
        }
        if config.max_entry_bytes > config.max_bytes {
            return Err(ResponseCacheConfigError::EntryExceedsTotal);
        }
        Ok(Some(Self {
            inner: Arc::new(CacheInner {
                config,
                entries: Mutex::new(CacheState::default()),
                pressure: Mutex::new(PressureState::default()),
            }),
        }))
    }

    fn pressure_active(&self, active_requests: usize, now: Instant) -> bool {
        let Ok(mut state) = self.inner.pressure.lock() else {
            return false;
        };
        if active_requests >= self.inner.config.enter_inflight {
            state.until = now.checked_add(self.inner.config.pressure_hold);
        }
        let active = state.until.is_some_and(|until| until > now);
        if !active {
            state.until = None;
        }
        active
    }

    fn get(&self, key: &str, now: Instant) -> Option<CachedResponse> {
        let Ok(mut state) = self.inner.entries.lock() else {
            return None;
        };
        let entry = state.entries.get(key)?.clone();
        if now.saturating_duration_since(entry.inserted_at) > self.inner.config.max_age {
            remove_entry(&mut state, key);
            record_size_metrics(&state);
            return None;
        }
        Some(entry)
    }

    fn insert(&self, key: String, response: CachedResponse) {
        let mut state = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        remove_entry(&mut state, &key);
        state.bytes = state.bytes.saturating_add(response.body.len());
        state.insertion_order.push_back(key.clone());
        state.entries.insert(key, response);
        while state.entries.len() > self.inner.config.max_entries
            || state.bytes > self.inner.config.max_bytes
        {
            let Some(oldest) = state.insertion_order.pop_front() else {
                break;
            };
            if let Some(removed) = state.entries.remove(&oldest) {
                state.bytes = state.bytes.saturating_sub(removed.body.len());
                CACHE_EVENTS.with_label_values(&["evicted"]).inc();
            }
        }
        record_size_metrics(&state);
    }
}

pub async fn adaptive_response_cache(request: Request<Body>, next: Next) -> Response {
    let cache = request
        .extensions()
        .get::<WebAppData>()
        .and_then(|app_data| app_data.response_cache.clone());
    let Some(cache) = cache else {
        return next.run(request).await;
    };
    let key = cache_key(&request);
    let now = Instant::now();
    if cache.pressure_active(super::get_recent_messages::active_request_count(), now) {
        if let Some(response) = cache.get(&key, now) {
            CACHE_EVENTS.with_label_values(&["pressure_hit"]).inc();
            return response.into_response();
        }
        CACHE_EVENTS.with_label_values(&["pressure_miss"]).inc();
    } else {
        CACHE_EVENTS.with_label_values(&["fresh"]).inc();
    }

    let response = next.run(request).await;
    capture_response(cache, key, response, now).await
}

async fn capture_response(
    cache: AdaptiveResponseCache,
    key: String,
    response: Response,
    now: Instant,
) -> Response {
    if response.status() != StatusCode::OK {
        CACHE_EVENTS.with_label_values(&["not_success"]).inc();
        return response;
    }
    let hint = response.body().size_hint();
    let Some(upper) = hint.upper() else {
        CACHE_EVENTS.with_label_values(&["unknown_size"]).inc();
        return response;
    };
    let Ok(upper) = usize::try_from(upper) else {
        CACHE_EVENTS.with_label_values(&["oversize"]).inc();
        return response;
    };
    if upper != usize::try_from(hint.lower()).unwrap_or(usize::MAX)
        || upper > cache.inner.config.max_entry_bytes
    {
        CACHE_EVENTS.with_label_values(&["oversize"]).inc();
        return response;
    }

    let (parts, body) = response.into_parts();
    let content_type = parts.headers.get(header::CONTENT_TYPE).cloned();
    let coverage = parts.headers.get(COVERAGE_HEADER).cloned();
    let Ok(body) = to_bytes(body, cache.inner.config.max_entry_bytes).await else {
        CACHE_EVENTS.with_label_values(&["body_error"]).inc();
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .expect("static response is valid");
    };
    cache.insert(
        key,
        CachedResponse {
            inserted_at: now,
            content_type,
            coverage,
            body: body.clone(),
        },
    );
    CACHE_EVENTS.with_label_values(&["stored"]).inc();
    Response::from_parts(parts, Body::from(body))
}

impl CachedResponse {
    fn into_response(self) -> Response {
        let mut response = Response::builder().status(StatusCode::OK);
        if let Some(content_type) = self.content_type {
            response = response.header(header::CONTENT_TYPE, content_type);
        }
        if let Some(coverage) = self.coverage {
            response = response.header(COVERAGE_HEADER, coverage);
        }
        response
            .body(Body::from(self.body))
            .expect("cached response headers were accepted previously")
    }
}

fn cache_key(request: &Request<Body>) -> String {
    let peer_mode = super::peer::allows_peer_fetch(request.headers()).unwrap_or(false);
    format!("{}|peer={peer_mode}", request.uri())
}

fn remove_entry(state: &mut CacheState, key: &str) {
    if let Some(removed) = state.entries.remove(key) {
        state.bytes = state.bytes.saturating_sub(removed.body.len());
    }
    state.insertion_order.retain(|existing| existing != key);
}

fn record_size_metrics(state: &CacheState) {
    CACHE_ENTRIES.set(i64::try_from(state.entries.len()).unwrap_or(i64::MAX));
    CACHE_BYTES.set(i64::try_from(state.bytes).unwrap_or(i64::MAX));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ListenAddr, TwitchApiClientCredentials};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn web_config() -> WebConfig {
        WebConfig {
            listen_address: ListenAddr::Tcp {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            },
            twitch_api_credentials: TwitchApiClientCredentials {
                client_id: String::new(),
                client_secret: String::new(),
                redirect_uri: String::new(),
            },
            sessions_expire_after: Duration::from_secs(1),
            recheck_twitch_auth_after: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            max_inflight_recent_requests: 512,
            adaptive_response_cache_enabled: true,
            adaptive_response_cache_max_age: Duration::from_secs(1),
            adaptive_response_cache_pressure_hold: Duration::from_secs(2),
            adaptive_response_cache_enter_inflight: 24,
            adaptive_response_cache_max_entries: 2,
            adaptive_response_cache_max_bytes: 8,
            adaptive_response_cache_max_entry_bytes: 4,
        }
    }

    fn cached(now: Instant, body: &'static [u8]) -> CachedResponse {
        CachedResponse {
            inserted_at: now,
            content_type: None,
            coverage: None,
            body: Bytes::from_static(body),
        }
    }

    #[test]
    fn pressure_has_a_bounded_hysteresis_window() {
        let cache = AdaptiveResponseCache::from_config(&web_config())
            .unwrap()
            .unwrap();
        let now = Instant::now();
        assert!(!cache.pressure_active(23, now));
        assert!(cache.pressure_active(24, now));
        assert!(cache.pressure_active(0, now + Duration::from_millis(1_999)));
        assert!(!cache.pressure_active(0, now + Duration::from_secs(2)));
    }

    #[test]
    fn expires_and_evicts_exact_responses_within_both_bounds() {
        let cache = AdaptiveResponseCache::from_config(&web_config())
            .unwrap()
            .unwrap();
        let now = Instant::now();
        cache.insert("one".to_owned(), cached(now, b"1111"));
        cache.insert("two".to_owned(), cached(now, b"2222"));
        assert!(cache.get("one", now).is_some());
        cache.insert("three".to_owned(), cached(now, b"3333"));
        assert!(cache.get("one", now).is_none());
        assert!(cache.get("two", now).is_some());
        assert!(
            cache
                .get("two", now + Duration::from_millis(1_001))
                .is_none()
        );
    }

    #[tokio::test]
    async fn captures_and_replays_compatible_headers_and_body() {
        let cache = AdaptiveResponseCache::from_config(&web_config())
            .unwrap()
            .unwrap();
        let now = Instant::now();
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(COVERAGE_HEADER, "partial")
            .body(Body::from("test"))
            .unwrap();
        let response = capture_response(cache.clone(), "key".to_owned(), response, now).await;
        assert_eq!(to_bytes(response.into_body(), 4).await.unwrap(), "test");

        let cached = cache.get("key", now).unwrap().into_response();
        assert_eq!(cached.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(cached.headers()[COVERAGE_HEADER], "partial");
        assert_eq!(to_bytes(cached.into_body(), 4).await.unwrap(), "test");
    }
}
