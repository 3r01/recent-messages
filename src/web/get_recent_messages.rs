use crate::coverage::{
    CoverageAssessment, CoverageInterval, CoverageRequest, assess_coverage, merge_intervals,
};
use crate::web::WebAppData;
use crate::web::error::ApiError;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query};
use axum::response::IntoResponse;
use axum::{Extension, Json};
use chrono::serde::ts_milliseconds_option;
use chrono::{DateTime, TimeZone, Utc};
use http::{HeaderMap, HeaderValue};
use prometheus::{
    HistogramVec, IntCounter, IntCounterVec, IntGauge, linear_buckets, register_histogram_vec,
    register_int_counter, register_int_counter_vec, register_int_gauge,
};
use recent_messages2::storage::{
    AsyncSqliteBlockStore, CanonicalRecord, MessageStore, QueryRequest, SharedOpenBlocks,
    StoreError,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, LazyLock};
use tokio::sync::Semaphore;

const REQUEST_PRIORITY_COALESCE_MS: i64 = 30 * 60 * 1_000;

static COMPONENTS_PERFORMANCE_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "recentmessages_get_recent_messages_endpoint_components_seconds",
        "Time taken to complete the different stages/elements of the /api/v2/recent-messages/:channel_login endpoint",
        &["stage"]
    )
    .unwrap()
});
static MESSAGE_COUNT_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "recentmessages_get_recent_messages_endpoint_message_count",
        "Number of messages returned from the database/actually sent to the user from the /api/v2/recent-messages/:channel_login endpoint",
        &["point"],
        // Default buckets are roughly exponential between 0.001 and 10, intended for use with durations/response times.
        // This creates 100 buckets, starting at 10.0, and each following buckets is 10.0 larger
        // (= 10, 20, 30, ... 1000, +Inf)
        linear_buckets(10.0, 10.0, 99).unwrap()
    )
    .unwrap()
});
static COVERAGE_RESPONSES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "recentmessages_query_coverage_total",
        "Public recent-message responses by completeness assessment",
        &["status"]
    )
    .unwrap()
});
static OVERLOAD_REJECTIONS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "recentmessages_public_overload_rejections_total",
        "Recent-message requests rejected by the bounded admission gate"
    )
    .unwrap()
});
static ACTIVE_REQUESTS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "recentmessages_public_active_requests",
        "Recent-message requests currently inside the bounded admission gate"
    )
    .unwrap()
});

pub(super) fn active_request_count() -> usize {
    usize::try_from(ACTIVE_REQUESTS.get()).unwrap_or(usize::MAX)
}

struct ActiveRequestGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        ACTIVE_REQUESTS.dec();
    }
}

fn admit_public_request(permits: Arc<Semaphore>) -> Result<ActiveRequestGuard, ApiError> {
    let permit = permits.try_acquire_owned().map_err(|_| {
        OVERLOAD_REJECTIONS.inc();
        ApiError::ServiceOverloaded
    })?;
    ACTIVE_REQUESTS.inc();
    Ok(ActiveRequestGuard { _permit: permit })
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetRecentMessagesPath {
    pub(super) channel_login: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(default)]
pub struct GetRecentMessagesQueryOptions {
    // aliases are used to keep compatibility with the API from version 1.
    #[serde(alias = "hideModerationMessages")]
    pub hide_moderation_messages: bool,
    #[serde(alias = "hideModeratedMessages")]
    pub hide_moderated_messages: bool,
    #[serde(alias = "clearchatToNotice")]
    pub clearchat_to_notice: bool,
    pub limit: Option<usize>,
    #[serde(with = "ts_milliseconds_option")]
    pub before: Option<DateTime<Utc>>,
    #[serde(with = "ts_milliseconds_option")]
    pub after: Option<DateTime<Utc>>,
}

#[allow(clippy::derivable_impls)]
impl Default for GetRecentMessagesQueryOptions {
    fn default() -> Self {
        GetRecentMessagesQueryOptions {
            hide_moderation_messages: false,
            hide_moderated_messages: false,
            clearchat_to_notice: false,
            limit: None,
            before: None,
            after: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct GetRecentMessagesResponse {
    messages: Vec<String>,
    error: Option<&'static str>,
    error_code: Option<&'static str>,
}

async fn query_local_messages(
    store: &AsyncSqliteBlockStore,
    open_blocks: Option<&SharedOpenBlocks>,
    channel_login: &str,
    options: GetRecentMessagesQueryOptions,
    max_buffer_size: usize,
) -> Result<Vec<CanonicalRecord>, StoreError> {
    let limit = options
        .limit
        .unwrap_or(max_buffer_size)
        .min(max_buffer_size);
    if limit == 0 {
        return Ok(Vec::new());
    }

    let request = QueryRequest {
        channel_key: channel_login.to_ascii_lowercase(),
        after_ms: options.after.map(|value| value.timestamp_millis()),
        before_ms: options.before.map(|value| value.timestamp_millis()),
        limit,
    };
    let sealed = store
        .query(request.clone())
        .await
        .map(|result| result.records)?;
    let open = open_blocks
        .map(|open| open.query(&request))
        .transpose()
        .map_err(|_| StoreError::LockPoisoned)?
        .unwrap_or_default();
    Ok(crate::web::peer::merge_records(sealed, open, limit))
}

fn requested_limit(options: GetRecentMessagesQueryOptions, max_buffer_size: usize) -> usize {
    options
        .limit
        .unwrap_or(max_buffer_size)
        .min(max_buffer_size)
}

fn should_attempt_repair(
    assessment: &CoverageAssessment,
    coverage: &[CoverageInterval],
    record_count: usize,
    requested_limit: usize,
) -> bool {
    if assessment.sufficient {
        return false;
    }
    let has_relevant_coverage_evidence = coverage.iter().any(|interval| {
        interval.end_ms >= assessment.required_start_ms
            && interval.start_ms <= assessment.required_end_ms
    });
    record_count < requested_limit || has_relevant_coverage_evidence
}

#[allow(clippy::too_many_lines)]
pub async fn get_recent_messages(
    path_options: Result<Path<GetRecentMessagesPath>, PathRejection>,
    query_options: Result<Query<GetRecentMessagesQueryOptions>, QueryRejection>,
    headers: HeaderMap,
    Extension(app_data): Extension<WebAppData>,
) -> impl IntoResponse {
    let request_snapshot_ms = Utc::now().timestamp_millis();
    let Path(GetRecentMessagesPath { channel_login }) =
        path_options.map_err(|_| ApiError::InvalidPath)?;
    let Query(query_options) = query_options.map_err(|_| ApiError::InvalidQuery)?;
    let snapshot_exclusive = Utc
        .timestamp_millis_opt(request_snapshot_ms.saturating_add(1))
        .single()
        .expect("a current UTC timestamp plus one millisecond is representable");
    let mut storage_options = query_options;
    if storage_options
        .before
        .is_none_or(|before| before > snapshot_exclusive)
    {
        storage_options.before = Some(snapshot_exclusive);
    }

    if let Err(e) = twitch_irc::validate::validate_login(&channel_login) {
        return Err(ApiError::InvalidChannelLogin(e));
    }

    let _active_request = admit_public_request(app_data.public_requests.clone())?;

    let timer = COMPONENTS_PERFORMANCE_HISTOGRAM
        .with_label_values(&["is_channel_ignored"])
        .start_timer();
    let result = app_data
        .control_store
        .is_channel_ignored(&channel_login)
        .await;
    timer.observe_duration();
    if result.map_err(ApiError::GetChannelIgnored)? {
        return Err(ApiError::ChannelIgnored(channel_login));
    }

    let join_app_data = app_data.clone();
    let join_channel = channel_login.clone();
    tokio::spawn(async move {
        if let Err(error) = join_app_data
            .irc_listener
            .join_if_needed(join_channel.clone())
            .await
        {
            tracing::error!(%error, channel_login = %join_channel, "Failed to persist requested IRC channel");
        }
    });

    let priority_store = app_data.block_store.clone();
    let priority_channel = channel_login.to_ascii_lowercase();
    tokio::spawn(async move {
        let requested_at_ms = u64::try_from(request_snapshot_ms).unwrap_or(0);
        let coalesce_before_ms =
            u64::try_from(request_snapshot_ms.saturating_sub(REQUEST_PRIORITY_COALESCE_MS))
                .unwrap_or(0);
        if let Err(error) = priority_store
            .touch_channel_requested(
                priority_channel.clone(),
                requested_at_ms,
                coalesce_before_ms,
            )
            .await
        {
            tracing::error!(%error, channel_login = %priority_channel, "Failed to persist requested channel priority");
        }
    });

    let timer = COMPONENTS_PERFORMANCE_HISTOGRAM
        .with_label_values(&["get_messages"])
        .start_timer();
    let mut records = query_local_messages(
        app_data.block_store,
        app_data.open_blocks.as_deref(),
        &channel_login,
        storage_options,
        app_data.config.app.max_buffer_size,
    )
    .await
    .map_err(ApiError::GetLocalMessages)?;
    timer.observe_duration();
    MESSAGE_COUNT_HISTOGRAM
        .with_label_values(&["from_block_store"])
        .observe(records.len() as f64);

    let mut coverage = app_data
        .control_store
        .get_coverage_intervals(&channel_login, request_snapshot_ms)
        .await
        .map_err(ApiError::GetCoverage)?;
    let coverage_request = CoverageRequest {
        after_ms: query_options.after.map(|value| value.timestamp_millis()),
        before_ms: query_options.before.map(|value| value.timestamp_millis()),
        limit: requested_limit(query_options, app_data.config.app.max_buffer_size),
        now_ms: request_snapshot_ms,
        retention_ms: i64::try_from(app_data.config.app.message_ttl.as_millis())
            .unwrap_or(i64::MAX),
    };
    let mut coverage_assessment = assess_coverage(&records, &coverage, coverage_request);
    let mut repair_checkpoints = app_data
        .control_store
        .get_repair_checkpoints(&channel_login)
        .await
        .map_err(ApiError::GetCoverage)?;
    let mut repair_state = crate::web::repair::assess_repair_state(
        &repair_checkpoints,
        &coverage_assessment,
        &coverage,
        request_snapshot_ms,
        app_data.config.repair.handoff_grace,
    );

    let allow_peer = crate::web::peer::allows_peer_fetch(&headers)?;
    let requested_limit = requested_limit(query_options, app_data.config.app.max_buffer_size);
    let repair_limit = app_data.config.app.max_buffer_size;
    let should_attempt_peer_repair = should_attempt_repair(
        &coverage_assessment,
        &coverage,
        records.len(),
        requested_limit,
    );
    if allow_peer
        && should_attempt_peer_repair
        && !repair_state.retry_suppressed
        && let Some(peer_client) = &app_data.peer_client
    {
        match peer_client
            .fetch_local(&channel_login, storage_options, repair_limit)
            .await
        {
            Ok(Some(peer_result)) => {
                let peer_records = peer_result.records;
                let peer_coverage = peer_result.coverage;
                let peer_records_persisted = if peer_records.is_empty() {
                    true
                } else if let Err(error) = app_data
                    .block_store
                    .append_batch(peer_records.clone())
                    .await
                {
                    tracing::warn!(%error, %channel_login, "Failed to persist owned-peer records");
                    false
                } else {
                    true
                };
                if peer_records_persisted {
                    for interval in &peer_coverage {
                        let source = format!("owned-peer:{}", interval.source);
                        if let Err(error) = app_data
                            .control_store
                            .record_coverage_interval(
                                &channel_login,
                                &source,
                                interval.start_ms,
                                interval.end_ms,
                            )
                            .await
                        {
                            tracing::warn!(%error, %channel_login, "Failed to persist owned-peer coverage");
                        }
                    }
                }
                records = crate::web::peer::merge_records(records, peer_records, requested_limit);
                coverage.extend(peer_coverage);
                coverage = merge_intervals(coverage);
                coverage_assessment = assess_coverage(&records, &coverage, coverage_request);
                repair_state = crate::web::repair::assess_repair_state(
                    &repair_checkpoints,
                    &coverage_assessment,
                    &coverage,
                    request_snapshot_ms,
                    app_data.config.repair.handoff_grace,
                );
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, %channel_login, "Owned-peer local read failed");
            }
        }
    }

    let should_attempt_external_repair = should_attempt_repair(
        &coverage_assessment,
        &coverage,
        records.len(),
        requested_limit,
    );
    if allow_peer
        && should_attempt_external_repair
        && !repair_state.retry_suppressed
        && let Some(repair_coordinator) = &app_data.repair_coordinator
    {
        let mut receiver = repair_coordinator
            .start_or_attach(
                &channel_login,
                storage_options,
                repair_limit,
                coverage_assessment.required_start_ms,
                coverage_assessment.required_end_ms,
            )
            .await;
        if repair_coordinator
            .wait_foreground(&mut receiver)
            .await
            .is_some()
        {
            records = query_local_messages(
                app_data.block_store,
                app_data.open_blocks.as_deref(),
                &channel_login,
                storage_options,
                app_data.config.app.max_buffer_size,
            )
            .await
            .map_err(ApiError::GetLocalMessages)?;
            repair_checkpoints = app_data
                .control_store
                .get_repair_checkpoints(&channel_login)
                .await
                .map_err(ApiError::GetCoverage)?;
            coverage_assessment = assess_coverage(&records, &coverage, coverage_request);
            repair_state = crate::web::repair::assess_repair_state(
                &repair_checkpoints,
                &coverage_assessment,
                &coverage,
                Utc::now().timestamp_millis(),
                app_data.config.repair.handoff_grace,
            );
        }
    }

    let timer = COMPONENTS_PERFORMANCE_HISTOGRAM
        .with_label_values(&["export_messages"])
        .start_timer();
    let exported_messages = crate::message_export::export_canonical_records(records, query_options);
    timer.observe_duration();
    MESSAGE_COUNT_HISTOGRAM
        .with_label_values(&["after_export"])
        .observe(exported_messages.len() as f64);

    let timer = COMPONENTS_PERFORMANCE_HISTOGRAM
        .with_label_values(&["is_join_confirmed"])
        .start_timer();
    let is_confirmed_joined = app_data
        .irc_listener
        .is_join_confirmed(channel_login.clone())
        .await;
    timer.observe_duration();

    let (error, error_code) = if is_confirmed_joined || !exported_messages.is_empty() {
        (None, None)
    } else {
        (
            Some(
                "The bot is currently not joined to this channel (in progress or failed previously)",
            ),
            Some("channel_not_joined"),
        )
    };

    let mut response_headers = HeaderMap::new();
    let coverage_status = if coverage_assessment.sufficient {
        "complete"
    } else if repair_state.best_effort {
        "best-effort"
    } else {
        "partial"
    };
    COVERAGE_RESPONSES
        .with_label_values(&[coverage_status])
        .inc();
    response_headers.insert("x-rm-coverage", HeaderValue::from_static(coverage_status));
    Ok((
        response_headers,
        Json(GetRecentMessagesResponse {
            messages: exported_messages,
            error,
            error_code,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use recent_messages2::storage::{
        DurableIngest, IngestRuntimeConfig, OpenBlockLimits, RawIngestRuntime, RawSourceBatch,
        SourceFidelity,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;

    static NEXT_STORE: AtomicU64 = AtomicU64::new(0);

    fn incomplete_assessment() -> CoverageAssessment {
        CoverageAssessment {
            sufficient: false,
            required_start_ms: 0,
            required_end_ms: 100,
            gaps: vec![(0, 100)],
        }
    }

    #[test]
    fn public_admission_fails_fast_and_recovers_after_release() {
        let permits = Arc::new(Semaphore::new(1));
        let first = admit_public_request(permits.clone()).unwrap();
        assert!(matches!(
            admit_public_request(permits.clone()),
            Err(ApiError::ServiceOverloaded)
        ));
        drop(first);
        assert!(admit_public_request(permits).is_ok());
    }

    #[test]
    fn full_firehose_only_result_stays_partial_without_forcing_repair() {
        assert!(!should_attempt_repair(
            &incomplete_assessment(),
            &[],
            800,
            800
        ));
    }

    #[test]
    fn short_or_known_gap_results_attempt_repair() {
        assert!(should_attempt_repair(
            &incomplete_assessment(),
            &[],
            799,
            800
        ));
        assert!(should_attempt_repair(
            &incomplete_assessment(),
            &[CoverageInterval {
                start_ms: 50,
                end_ms: 100,
                source: "direct-irc".to_owned(),
            }],
            800,
            800
        ));
    }

    fn test_store() -> AsyncSqliteBlockStore {
        let id = NEXT_STORE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "recent-messages-http-{}-{id}.sqlite",
            std::process::id()
        ));
        AsyncSqliteBlockStore::open(path, 2, 800, 8, 2).unwrap()
    }

    fn test_store_path(label: &str) -> std::path::PathBuf {
        let id = NEXT_STORE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "recent-messages-http-{label}-{}-{id}.sqlite",
            std::process::id()
        ))
    }

    fn record(sequence: i64, received_at_ms: i64, event_at_ms: i64) -> CanonicalRecord {
        let raw_irc = format!(
            "@badge-info=;badges=;color=;display-name=User;emotes=;first-msg=0;flags=;id=00000000-0000-0000-0000-{sequence:012};mod=0;room-id=123;subscriber=0;tmi-sent-ts={event_at_ms};turbo=0;user-id=456;user-type= :user!user@user.tmi.twitch.tv PRIVMSG #testchannel :message {sequence}"
        )
        .into_bytes();
        CanonicalRecord {
            channel_key: "testchannel".to_owned(),
            event_at_ms,
            received_at_ms,
            event_key: CanonicalRecord::derive_event_key("testchannel", &raw_irc),
            source_id: "test".to_owned(),
            fidelity: SourceFidelity::DirectIrc,
            raw_irc,
        }
    }

    #[test]
    fn accepts_legacy_camel_case_moderation_aliases() {
        let options: GetRecentMessagesQueryOptions = serde_urlencoded::from_str(
            "hideModerationMessages=true&hideModeratedMessages=true&clearchatToNotice=true",
        )
        .unwrap();

        assert!(options.hide_moderation_messages);
        assert!(options.hide_moderated_messages);
        assert!(options.clearchat_to_notice);
    }

    #[test]
    fn accepts_snake_case_moderation_options() {
        let options: GetRecentMessagesQueryOptions = serde_urlencoded::from_str(
            "hide_moderation_messages=true&hide_moderated_messages=true&clearchat_to_notice=true",
        )
        .unwrap();

        assert!(options.hide_moderation_messages);
        assert!(options.hide_moderated_messages);
        assert!(options.clearchat_to_notice);
    }

    #[test]
    fn parses_limit_and_millisecond_interval_bounds() {
        let options: GetRecentMessagesQueryOptions =
            serde_urlencoded::from_str("limit=800&after=1699999999000&before=1700000000123")
                .unwrap();

        assert_eq!(options.limit, Some(800));
        assert_eq!(
            options.after.map(|value| value.timestamp_millis()),
            Some(1_699_999_999_000)
        );
        assert_eq!(
            options.before.map(|value| value.timestamp_millis()),
            Some(1_700_000_000_123)
        );
    }

    #[test]
    fn rejects_invalid_boolean_and_timestamp_values() {
        assert!(
            serde_urlencoded::from_str::<GetRecentMessagesQueryOptions>(
                "hideModerationMessages=maybe"
            )
            .is_err()
        );
        assert!(
            serde_urlencoded::from_str::<GetRecentMessagesQueryOptions>("before=not-a-timestamp")
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_query_returns_newest_limit_in_receipt_order() {
        let store = test_store();
        store
            .append_batch(vec![
                record(3, 3_000, 10),
                record(1, 1_000, 30),
                record(2, 2_000, 20),
            ])
            .await
            .unwrap();

        let records = query_local_messages(
            &store,
            None,
            "TestChannel",
            GetRecentMessagesQueryOptions {
                limit: Some(2),
                ..Default::default()
            },
            800,
        )
        .await
        .unwrap();

        assert_eq!(
            records
                .iter()
                .map(|record| record.received_at_ms)
                .collect::<Vec<_>>(),
            vec![2_000, 3_000]
        );
        let exported = crate::message_export::export_canonical_records(
            records,
            GetRecentMessagesQueryOptions::default(),
        );
        assert!(exported[0].contains("rm-received-ts=2000"));
        assert!(exported[1].contains("rm-received-ts=3000"));
    }

    #[tokio::test]
    async fn local_query_includes_unsealed_open_tail() {
        let store = test_store();
        let mut ingest = DurableIngest::new(
            store.clone(),
            OpenBlockLimits {
                block_messages: 100,
                max_open_channels: 128,
                max_open_bytes: 1024 * 1024,
                idle_seal_after_ms: 60_000,
                max_open_age_ms: 60_000,
            },
        )
        .unwrap();
        let open = ingest.open_blocks_handle();
        ingest
            .accept_batch(
                "source",
                "stream",
                1,
                1,
                vec![record(1, 4_000, 4_000)],
                4_000,
            )
            .await
            .unwrap();

        let records = query_local_messages(
            &store,
            Some(&open),
            "testchannel",
            GetRecentMessagesQueryOptions {
                limit: Some(1),
                ..Default::default()
            },
            800,
        )
        .await
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].received_at_ms, 4_000);
    }

    #[tokio::test]
    async fn local_query_bounds_are_strict_and_use_receipt_time() {
        let store = test_store();
        store
            .append_batch(vec![
                record(1, 1_000, 30_000),
                record(2, 2_000, 20_000),
                record(3, 3_000, 10_000),
            ])
            .await
            .unwrap();

        let records = query_local_messages(
            &store,
            None,
            "testchannel",
            GetRecentMessagesQueryOptions {
                after: Utc.timestamp_millis_opt(1_000).single(),
                before: Utc.timestamp_millis_opt(3_000).single(),
                ..Default::default()
            },
            800,
        )
        .await
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].received_at_ms, 2_000);
        assert_eq!(records[0].event_at_ms, 20_000);
    }

    #[tokio::test]
    async fn local_query_handles_zero_limit_and_empty_channels() {
        let store = test_store();
        store
            .append_batch(vec![record(1, 1_000, 1_000)])
            .await
            .unwrap();

        let empty_limit = query_local_messages(
            &store,
            None,
            "testchannel",
            GetRecentMessagesQueryOptions {
                limit: Some(0),
                ..Default::default()
            },
            800,
        )
        .await
        .unwrap();
        assert!(empty_limit.is_empty());

        let missing = query_local_messages(
            &store,
            None,
            "anotherchannel",
            GetRecentMessagesQueryOptions::default(),
            800,
        )
        .await
        .unwrap();
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn acknowledged_quiet_tail_recovers_through_http_export_after_restart() {
        let path = test_store_path("restart");
        let store = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
        let mut ingest = DurableIngest::new(
            store.clone(),
            OpenBlockLimits {
                block_messages: 100,
                max_open_channels: 128,
                max_open_bytes: 1024 * 1024,
                idle_seal_after_ms: 30_000,
                max_open_age_ms: 60_000,
            },
        )
        .unwrap();
        let record = record(7, 7_000, 6_000);
        ingest
            .accept_batch("owned-irc", "before-crash", 1, 1, vec![record], 7_000)
            .await
            .unwrap();
        assert_eq!(store.pending_journal_batches().await.unwrap().len(), 1);
        drop(ingest);
        drop(store);

        let reopened = AsyncSqliteBlockStore::open(&path, 100, 800, 8, 2).unwrap();
        let mut recovered = DurableIngest::new(
            reopened.clone(),
            OpenBlockLimits {
                block_messages: 100,
                max_open_channels: 128,
                max_open_bytes: 1024 * 1024,
                idle_seal_after_ms: 30_000,
                max_open_age_ms: 60_000,
            },
        )
        .unwrap();
        assert_eq!(recovered.recover_pending(8_000).await.unwrap(), 1);
        assert_eq!(recovered.checkpoint().await.unwrap(), 1);

        let records = query_local_messages(
            &reopened,
            None,
            "testchannel",
            GetRecentMessagesQueryOptions::default(),
            800,
        )
        .await
        .unwrap();
        let exported = crate::message_export::export_canonical_records(
            records,
            GetRecentMessagesQueryOptions::default(),
        );
        assert_eq!(exported.len(), 1);
        assert!(exported[0].contains("historical=1"));
        assert!(exported[0].contains("rm-received-ts=7000"));
        assert!(exported[0].ends_with(" PRIVMSG #testchannel :message 7"));
        assert!(reopened.pending_journal_batches().await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "100k-event release-mode sustained ingest-to-export soak"]
    async fn sustains_twenty_thousand_owned_events_per_second_through_export() {
        const EVENTS: u64 = 100_000;
        const BATCH: u64 = 500;
        const CHANNELS: u64 = 1_000;

        let path = test_store_path("soak");
        let store = AsyncSqliteBlockStore::open(&path, 100, 800, 256, 4).unwrap();
        let ingest = DurableIngest::new(
            store.clone(),
            OpenBlockLimits {
                block_messages: 100,
                max_open_channels: 10_000,
                max_open_bytes: 256 * 1024 * 1024,
                idle_seal_after_ms: 30_000,
                max_open_age_ms: 60_000,
            },
        )
        .unwrap();
        let (output, input) = tokio::sync::mpsc::channel(256);
        let cancellation = CancellationToken::new();
        let producer_cancellation = cancellation.clone();
        let producer = tokio::spawn(async move {
            for first in (1..=EVENTS).step_by(BATCH as usize) {
                let last = (first + BATCH - 1).min(EVENTS);
                let records = (first..=last)
                    .map(|sequence| soak_record(sequence, sequence % CHANNELS))
                    .collect();
                output
                    .send(RawSourceBatch {
                        source_id: "owned-irc-soak".to_owned(),
                        stream_id: "soak-1".to_owned(),
                        first_sequence: first,
                        last_sequence: last,
                        records,
                    })
                    .await
                    .unwrap();
            }
            drop(output);
            producer_cancellation.cancel();
        });
        let runtime = RawIngestRuntime::new_with_external_input(
            ingest,
            Vec::new(),
            IngestRuntimeConfig {
                queue_batches: 256,
                seal_interval: Duration::from_secs(1),
                checkpoint_interval: Duration::from_mins(1),
                checkpoint_poll_interval: Duration::from_secs(1),
                checkpoint_journal_bytes: 256 * 1024 * 1024,
                message_ttl: Duration::MAX,
            },
            input,
        )
        .unwrap();

        let started = Instant::now();
        runtime.run(cancellation).await.unwrap();
        producer.await.unwrap();
        let elapsed = started.elapsed();
        let rate = EVENTS as f64 / elapsed.as_secs_f64();
        eprintln!("accepted {EVENTS} events in {elapsed:?}: {rate:.0} events/s");
        assert!(
            rate >= 20_000.0,
            "sustained rate was only {rate:.0} events/s"
        );

        assert!(store.pending_journal_batches().await.unwrap().is_empty());
        let records = query_local_messages(
            &store,
            None,
            "soak0",
            GetRecentMessagesQueryOptions::default(),
            800,
        )
        .await
        .unwrap();
        let exported = crate::message_export::export_canonical_records(
            records,
            GetRecentMessagesQueryOptions::default(),
        );
        assert_eq!(exported.len(), 100);
        assert!(
            exported
                .iter()
                .all(|message| message.contains("historical=1"))
        );
    }

    fn soak_record(sequence: u64, channel: u64) -> CanonicalRecord {
        let channel_key = format!("soak{channel}");
        let raw_irc = format!(
            "@badge-info=;badges=;color=;display-name=User;emotes=;first-msg=0;flags=;id=00000000-0000-0000-{channel:04}-{sequence:012};mod=0;room-id={channel};subscriber=0;tmi-sent-ts={sequence};turbo=0;user-id=456;user-type= :user!user@user.tmi.twitch.tv PRIVMSG #{channel_key} :message {sequence}"
        )
        .into_bytes();
        CanonicalRecord {
            event_at_ms: sequence as i64,
            received_at_ms: sequence as i64,
            event_key: CanonicalRecord::derive_event_key(&channel_key, &raw_irc),
            channel_key,
            source_id: "owned-irc-soak".to_owned(),
            fidelity: SourceFidelity::DirectIrc,
            raw_irc,
        }
    }
}
