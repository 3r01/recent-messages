use crate::web::auth::UserAuthorization;
use crate::web::{ApiError, WebAppData};
use axum::extract::rejection::JsonRejection;
use axum::{Extension, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct GetIgnoredResponse {
    ignored: bool,
}

pub async fn get_ignored(
    Extension(authorization): Extension<UserAuthorization>,
    Extension(app_data): Extension<WebAppData>,
) -> Result<Json<GetIgnoredResponse>, ApiError> {
    let is_ignored = app_data
        .control_store
        .is_channel_ignored(&authorization.user_login)
        .await
        .map_err(ApiError::GetChannelIgnored)?;

    Ok(Json(GetIgnoredResponse {
        ignored: is_ignored,
    }))
}

#[derive(Deserialize)]
pub struct SetIgnoredBodyOptions {
    ignored: bool,
}

pub async fn set_ignored(
    Extension(authorization): Extension<UserAuthorization>,
    Extension(app_data): Extension<WebAppData>,
    options: Result<Json<SetIgnoredBodyOptions>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let Json(SetIgnoredBodyOptions {
        ignored: should_be_ignored,
    }) = options.map_err(|_| ApiError::InvalidPayload)?;

    app_data
        .control_store
        .set_channel_ignored(&authorization.user_login, should_be_ignored)
        .await
        .map_err(ApiError::SetChannelIgnored)?;
    app_data
        .block_store
        .set_channel_blocked(
            authorization.user_login.to_ascii_lowercase(),
            should_be_ignored,
        )
        .await
        .map_err(ApiError::PurgeMessages)?;

    if should_be_ignored {
        app_data
            .irc_listener
            .irc_client
            .part(authorization.user_login.clone());
    } else {
        app_data
            .irc_listener
            .irc_client
            .join(authorization.user_login)
            .unwrap();
    }

    // 204 No Content, empty body
    Ok(StatusCode::NO_CONTENT)
}
