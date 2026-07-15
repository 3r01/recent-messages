use axum::Extension;
use http::StatusCode;
use recent_messages2::storage::{
    StorageBudget, effective_max_bytes, filesystem_space, ingest_is_fresh, source_is_fresh,
};
use std::time::{SystemTime, UNIX_EPOCH};

use super::WebAppData;

pub async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub async fn ready(Extension(app): Extension<WebAppData>) -> StatusCode {
    if app.block_store.check_readable().await.is_err()
        || app.control_store.check_readable().await.is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let Ok(physical_bytes) = app.block_store.physical_bytes().await else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    let budget = StorageBudget {
        max_bytes: app.config.storage_budget.max_bytes,
        max_filesystem_percent: app.config.storage_budget.max_filesystem_percent,
        min_free_bytes: app.config.storage_budget.min_free_bytes,
        wal_reserve_bytes: app.config.storage_budget.wal_reserve_bytes,
        high_water_ratio: app.config.storage_budget.high_water_ratio,
        target_ratio: app.config.storage_budget.target_ratio,
        emergency_ratio: app.config.storage_budget.emergency_ratio,
        pressure_floor_messages_per_channel: app
            .config
            .storage_budget
            .pressure_floor_messages_per_channel,
        requested_channel_protect_for: app.config.app.channels_expire_after,
        max_enforcement_passes: app.config.storage_budget.max_enforcement_passes,
    };
    let Ok(filesystem) = filesystem_space(&app.config.block_store.path) else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    let Ok(effective_max) = effective_max_bytes(budget, physical_bytes, filesystem) else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    if physical_bytes as f64 >= effective_max as f64 * budget.emergency_ratio {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if !app.config.ingest.enabled {
        return StatusCode::NO_CONTENT;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        });
    if !ingest_is_fresh(
        now,
        app.config.health.ingest_max_accept_age,
        app.config.health.max_queue_batches,
    ) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if app
        .config
        .ingest
        .raw_firehoses
        .iter()
        .enumerate()
        .any(|(index, _)| {
            !source_is_fresh(
                &format!("raw-firehose-{index}"),
                now,
                app.config.health.firehose_max_event_age,
            )
        })
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    StatusCode::NO_CONTENT
}
