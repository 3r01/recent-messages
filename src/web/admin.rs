use std::collections::BTreeSet;

use axum::extract::Path as AxumPath;
use axum::extract::rejection::JsonRejection;
use axum::{Extension, Json};
use http::header::AUTHORIZATION;
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::config::AdminConfig;
use crate::web::{ApiError, WebAppData};

#[derive(Clone)]
pub(super) struct AdminAuth {
    token_hash: [u8; 32],
}

#[derive(Debug, Error)]
pub enum AdminConfigError {
    #[error("failed to read admin token file {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("admin token must contain at least 32 non-whitespace characters")]
    TokenTooShort,
    #[error("admin max_always_join_channels must be greater than zero")]
    InvalidLimit,
}

impl AdminAuth {
    pub(super) fn from_config(config: &AdminConfig) -> Result<Option<Self>, AdminConfigError> {
        if config.max_always_join_channels == 0 {
            return Err(AdminConfigError::InvalidLimit);
        }
        let Some(path) = &config.token_file else {
            return Ok(None);
        };
        let token = std::fs::read_to_string(path).map_err(|source| AdminConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_token(token.trim()).map(Some)
    }

    fn from_token(token: &str) -> Result<Self, AdminConfigError> {
        if token.len() < 32 {
            return Err(AdminConfigError::TokenTooShort);
        }
        Ok(Self {
            token_hash: *blake3::hash(token.as_bytes()).as_bytes(),
        })
    }

    fn authorize(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        let supplied = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default();
        let supplied_hash = blake3::hash(supplied.as_bytes());
        if self.token_hash.ct_eq(supplied_hash.as_bytes()).into() {
            Ok(())
        } else {
            Err(ApiError::UnauthorizedAdmin)
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ReplaceAlwaysJoinRequest {
    channels: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct AlwaysJoinResponse {
    channels: Vec<String>,
}

pub async fn get_always_join(
    headers: HeaderMap,
    Extension(app_data): Extension<WebAppData>,
) -> Result<Json<AlwaysJoinResponse>, ApiError> {
    authorize(&app_data, &headers)?;
    let channels = app_data
        .control_store
        .get_always_join_channels()
        .await
        .map_err(ApiError::ManageAlwaysJoin)?;
    Ok(Json(AlwaysJoinResponse { channels }))
}

pub async fn replace_always_join(
    headers: HeaderMap,
    Extension(app_data): Extension<WebAppData>,
    body: Result<Json<ReplaceAlwaysJoinRequest>, JsonRejection>,
) -> Result<Json<AlwaysJoinResponse>, ApiError> {
    authorize(&app_data, &headers)?;
    let Json(body) = body.map_err(|_| ApiError::InvalidPayload)?;
    let channels = normalize_channels(
        body.channels,
        app_data.config.admin.max_always_join_channels,
    )?;
    reject_ignored(&app_data, &channels).await?;
    app_data
        .control_store
        .replace_always_join_channels(&channels)
        .await
        .map_err(ApiError::ManageAlwaysJoin)?;
    synchronize(&app_data).await?;
    Ok(Json(AlwaysJoinResponse { channels }))
}

pub async fn add_always_join(
    headers: HeaderMap,
    AxumPath(channel_login): AxumPath<String>,
    Extension(app_data): Extension<WebAppData>,
) -> Result<StatusCode, ApiError> {
    authorize(&app_data, &headers)?;
    let channels = normalize_channels(
        vec![channel_login],
        app_data.config.admin.max_always_join_channels,
    )?;
    reject_ignored(&app_data, &channels).await?;
    match app_data
        .control_store
        .set_always_join_channel(
            &channels[0],
            true,
            app_data.config.admin.max_always_join_channels,
        )
        .await
    {
        Ok(()) => {}
        Err(crate::db::StorageError::AlwaysJoinLimit(maximum)) => {
            return Err(ApiError::AlwaysJoinLimit(maximum));
        }
        Err(error) => return Err(ApiError::ManageAlwaysJoin(error)),
    }
    synchronize(&app_data).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_always_join(
    headers: HeaderMap,
    AxumPath(channel_login): AxumPath<String>,
    Extension(app_data): Extension<WebAppData>,
) -> Result<StatusCode, ApiError> {
    authorize(&app_data, &headers)?;
    let channels = normalize_channels(
        vec![channel_login],
        app_data.config.admin.max_always_join_channels,
    )?;
    app_data
        .control_store
        .set_always_join_channel(
            &channels[0],
            false,
            app_data.config.admin.max_always_join_channels,
        )
        .await
        .map_err(ApiError::ManageAlwaysJoin)?;
    synchronize(&app_data).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn authorize(app_data: &WebAppData, headers: &HeaderMap) -> Result<(), ApiError> {
    app_data
        .admin_auth
        .as_ref()
        .ok_or(ApiError::AdminAccessDisabled)?
        .authorize(headers)
}

fn normalize_channels(channels: Vec<String>, maximum: usize) -> Result<Vec<String>, ApiError> {
    let mut normalized = BTreeSet::new();
    for channel in channels {
        let channel = channel.to_ascii_lowercase();
        twitch_irc::validate::validate_login(&channel).map_err(ApiError::InvalidChannelLogin)?;
        normalized.insert(channel);
        if normalized.len() > maximum {
            return Err(ApiError::AlwaysJoinLimit(maximum));
        }
    }
    Ok(normalized.into_iter().collect())
}

async fn reject_ignored(app_data: &WebAppData, channels: &[String]) -> Result<(), ApiError> {
    let ignored = app_data
        .control_store
        .get_ignored_channels()
        .await
        .map_err(ApiError::ManageAlwaysJoin)?;
    if let Some(channel) = channels.iter().find(|channel| ignored.contains(*channel)) {
        return Err(ApiError::AlwaysJoinIgnored(channel.clone()));
    }
    Ok(())
}

async fn synchronize(app_data: &WebAppData) -> Result<(), ApiError> {
    let channels = app_data
        .control_store
        .get_always_join_channels()
        .await
        .map_err(ApiError::ManageAlwaysJoin)?;
    app_data
        .block_store
        .sync_always_join_channels(channels)
        .await
        .map_err(ApiError::SyncAlwaysJoin)?;
    app_data.irc_listener.request_reconcile();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_auth_is_constant_time_hash_based_and_strict() {
        let auth = AdminAuth::from_token("0123456789abcdef0123456789abcdef").unwrap();
        let mut valid = HeaderMap::new();
        valid.insert(
            AUTHORIZATION,
            "Bearer 0123456789abcdef0123456789abcdef".parse().unwrap(),
        );
        assert!(auth.authorize(&valid).is_ok());

        let mut invalid = HeaderMap::new();
        invalid.insert(AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert!(matches!(
            auth.authorize(&invalid),
            Err(ApiError::UnauthorizedAdmin)
        ));
    }

    #[test]
    fn normalization_deduplicates_and_sorts() {
        assert_eq!(
            normalize_channels(
                vec!["Beta".to_owned(), "alpha".to_owned(), "ALPHA".to_owned()],
                2
            )
            .unwrap(),
            vec!["alpha".to_owned(), "beta".to_owned()]
        );
    }
}
