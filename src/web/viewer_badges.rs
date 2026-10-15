use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use axum::{Extension, Json, extract::Path, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::{HTTP_CLIENT, WebAppData};

const BADGE_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const MAX_CHANNEL_CACHE_ENTRIES: usize = 128;

#[derive(Clone, Serialize)]
pub struct Badge {
    url: String,
    title: String,
}

#[derive(Clone, Serialize)]
pub struct BadgeResponse {
    badges: HashMap<String, Badge>,
}

#[derive(Deserialize)]
struct AppTokenResponse {
    access_token: String,
    expires_in: u64,
}

struct CachedToken {
    value: String,
    valid_until: Instant,
}

#[derive(Clone, Deserialize)]
struct HelixBadgeResponse {
    data: Vec<HelixBadgeSet>,
}

#[derive(Clone, Deserialize)]
struct HelixBadgeSet {
    set_id: String,
    versions: Vec<HelixBadgeVersion>,
}

#[derive(Clone, Deserialize)]
struct HelixBadgeVersion {
    id: String,
    image_url_2x: String,
    title: String,
}

struct CachedBadges {
    value: Arc<HelixBadgeResponse>,
    fetched_at: Instant,
}

#[derive(Default)]
struct BadgeCache {
    global: Option<CachedBadges>,
    channels: HashMap<String, CachedBadges>,
}

static APP_TOKEN: LazyLock<Mutex<Option<CachedToken>>> = LazyLock::new(Mutex::default);
static BADGE_CACHE: LazyLock<Mutex<BadgeCache>> = LazyLock::new(Mutex::default);

pub async fn get_badges(
    Path(channel_id): Path<String>,
    Extension(app_data): Extension<WebAppData>,
) -> impl IntoResponse {
    if channel_id.is_empty()
        || channel_id.len() > 20
        || !channel_id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return StatusCode::BAD_REQUEST.into_response();
    }

    match load_badges(&channel_id, app_data.config).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => {
            tracing::warn!(%error, "Failed to load viewer badge metadata");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

async fn load_badges(
    channel_id: &str,
    config: &crate::Config,
) -> Result<BadgeResponse, &'static str> {
    let now = Instant::now();
    let (cached_global, cached_channel) = {
        let cache = BADGE_CACHE.lock().await;
        (
            cache
                .global
                .as_ref()
                .filter(|entry| now.duration_since(entry.fetched_at) < BADGE_CACHE_TTL)
                .map(|entry| Arc::clone(&entry.value)),
            cache
                .channels
                .get(channel_id)
                .filter(|entry| now.duration_since(entry.fetched_at) < BADGE_CACHE_TTL)
                .map(|entry| Arc::clone(&entry.value)),
        )
    };

    let token = app_token(config).await?;
    let global = match cached_global {
        Some(value) => value,
        None => Arc::new(
            fetch_badges(
                "https://api.twitch.tv/helix/chat/badges/global",
                config,
                &token,
            )
            .await?,
        ),
    };
    let channel = match cached_channel {
        Some(value) => value,
        None => Arc::new(
            fetch_badges(
                &format!("https://api.twitch.tv/helix/chat/badges?broadcaster_id={channel_id}"),
                config,
                &token,
            )
            .await?,
        ),
    };

    {
        let mut cache = BADGE_CACHE.lock().await;
        cache.global = Some(CachedBadges {
            value: Arc::clone(&global),
            fetched_at: now,
        });
        if cache.channels.len() >= MAX_CHANNEL_CACHE_ENTRIES
            && !cache.channels.contains_key(channel_id)
            && let Some(oldest) = cache
                .channels
                .iter()
                .min_by_key(|(_, entry)| entry.fetched_at)
                .map(|(channel_id, _)| channel_id.clone())
        {
            cache.channels.remove(&oldest);
        }
        cache.channels.insert(
            channel_id.to_owned(),
            CachedBadges {
                value: Arc::clone(&channel),
                fetched_at: now,
            },
        );
    }

    let mut badges = HashMap::new();
    for set in global.data.iter().chain(channel.data.iter()) {
        for version in &set.versions {
            badges.insert(
                format!("{}/{}", set.set_id, version.id),
                Badge {
                    url: version.image_url_2x.clone(),
                    title: version.title.clone(),
                },
            );
        }
    }
    Ok(BadgeResponse { badges })
}

async fn app_token(config: &crate::Config) -> Result<String, &'static str> {
    let mut cached = APP_TOKEN.lock().await;
    let now = Instant::now();
    if let Some(token) = cached.as_ref()
        && token.valid_until > now + Duration::from_secs(60)
    {
        return Ok(token.value.clone());
    }
    let response = HTTP_CLIENT
        .post("https://id.twitch.tv/oauth2/token")
        .query(&[
            (
                "client_id",
                config.web.twitch_api_credentials.client_id.as_str(),
            ),
            (
                "client_secret",
                config.web.twitch_api_credentials.client_secret.as_str(),
            ),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await
        .map_err(|_| "token transport")?
        .error_for_status()
        .map_err(|_| "token status")?
        .json::<AppTokenResponse>()
        .await
        .map_err(|_| "token decode")?;
    let valid_for = Duration::from_secs(response.expires_in.saturating_sub(30));
    *cached = Some(CachedToken {
        value: response.access_token.clone(),
        valid_until: now + valid_for,
    });
    Ok(response.access_token)
}

async fn fetch_badges(
    url: &str,
    config: &crate::Config,
    token: &str,
) -> Result<HelixBadgeResponse, &'static str> {
    HTTP_CLIENT
        .get(url)
        .header("Client-Id", &config.web.twitch_api_credentials.client_id)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|_| "badge transport")?
        .error_for_status()
        .map_err(|_| "badge status")?
        .json()
        .await
        .map_err(|_| "badge decode")
}

#[cfg(test)]
mod tests {
    #[test]
    fn channel_ids_are_numeric() {
        assert!("22484632".bytes().all(|byte| byte.is_ascii_digit()));
        assert!(!"forsen".bytes().all(|byte| byte.is_ascii_digit()));
    }
}
