use crate::web::WebAppData;
use crate::web::auth::UserAuthorization;
use crate::web::error::ApiError;
use axum::Extension;
use chrono::Utc;
use http::StatusCode;
use recent_messages2::storage::StoreError;

pub async fn purge_messages(
    Extension(authorization): Extension<UserAuthorization>,
    app_data: Extension<WebAppData>,
) -> Result<StatusCode, ApiError> {
    let cutoff_ms = Utc::now().timestamp_millis();
    app_data
        .control_store
        .invalidate_channel_coverage(&authorization.user_login)
        .await
        .map_err(ApiError::GetCoverage)?;
    app_data
        .block_store
        .purge_channel(authorization.user_login.to_ascii_lowercase(), cutoff_ms)
        .await
        .map_err(ApiError::PurgeMessages)?;
    if let Some(open_blocks) = &app_data.open_blocks {
        open_blocks
            .purge_channel(&authorization.user_login.to_ascii_lowercase())
            .map_err(|_| ApiError::PurgeMessages(StoreError::LockPoisoned))?;
    }
    if app_data
        .irc_listener
        .is_join_confirmed(authorization.user_login.clone())
        .await
    {
        app_data
            .control_store
            .begin_live_coverage(&authorization.user_login, "direct-irc", cutoff_ms)
            .await
            .map_err(ApiError::GetCoverage)?;
    }
    Ok(StatusCode::NO_CONTENT)
}
