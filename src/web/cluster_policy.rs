use axum::{
    Extension,
    body::Body,
    extract::Path,
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::Request;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use super::{ApiError, WebAppData, get_recent_messages::GetRecentMessagesPath};
use crate::cluster::ChannelPolicy;

#[derive(Clone, Default)]
pub struct AppliedPolicies(Arc<Mutex<HashMap<String, ChannelPolicy>>>);

impl AppliedPolicies {
    pub(super) async fn apply(
        &self,
        app: &WebAppData,
        channel: &str,
        policy: ChannelPolicy,
    ) -> Result<(), ApiError> {
        let mut applied = self.0.lock().await;
        if !applied.contains_key(channel)
            && let Some(previous) = app
                .control_store
                .applied_cluster_policy(channel)
                .await
                .map_err(ApiError::GetCoverage)?
        {
            applied.insert(channel.to_owned(), previous);
        }
        if applied.get(channel) == Some(&policy) {
            return Ok(());
        }
        if policy == ChannelPolicy::default() && !applied.contains_key(channel) {
            return Ok(());
        }
        let previous_cutoff = applied.get(channel).map_or(0, |p| p.purge_ms);
        if policy.purge_ms < previous_cutoff {
            return Err(ApiError::ServiceOverloaded);
        }
        app.block_store
            .set_channel_blocked(channel.to_owned(), policy.ignored)
            .await
            .map_err(ApiError::PurgeMessages)?;
        if policy.ignored || policy.purge_ms > previous_cutoff {
            app.block_store
                .purge_channel(channel.to_owned(), policy.purge_ms)
                .await
                .map_err(ApiError::PurgeMessages)?;
            if let Some(open) = &app.open_blocks {
                open.purge_channel(channel).map_err(|_| {
                    ApiError::PurgeMessages(recent_messages2::storage::StoreError::LockPoisoned)
                })?;
            }
            app.control_store
                .invalidate_channel_coverage(channel)
                .await
                .map_err(ApiError::GetCoverage)?;
        }
        if policy.ignored {
            app.irc_listener.irc_client.part(channel.to_owned());
        }
        app.control_store
            .apply_cluster_policy(channel, policy)
            .await
            .map_err(ApiError::GetCoverage)?;
        applied.insert(channel.to_owned(), policy);
        Ok(())
    }
}

pub async fn gate(
    Path(path): Path<GetRecentMessagesPath>,
    Extension(app): Extension<WebAppData>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(cluster) = &app.control_store.cluster else {
        return next.run(request).await;
    };
    let channel = path.channel_login.to_ascii_lowercase();
    if let Err(error) = twitch_irc::validate::validate_login(&channel) {
        return ApiError::InvalidChannelLogin(error).into_response();
    }
    if (request.uri().path().contains("/local-recent/") || request.uri().path().contains("/serve/"))
        && let Err(error) =
            super::peer::authorize_peer(request.headers(), app.config.peer.shared_token.as_deref())
    {
        return error.into_response();
    }
    if cluster.check().await.is_err() {
        return ApiError::ServiceOverloaded.into_response();
    }
    let policy = match cluster.policy(&channel).await {
        Ok(policy) => policy,
        Err(_) => return ApiError::ServiceOverloaded.into_response(),
    };
    if let Err(error) = app.applied_policies.apply(&app, &channel, policy).await {
        return error.into_response();
    }
    if policy.ignored {
        return ApiError::ChannelIgnored(channel).into_response();
    }
    request.extensions_mut().insert(policy);
    next.run(request).await
}

pub async fn synchronize(app: &WebAppData) -> Result<(), ApiError> {
    let Some(cluster) = &app.control_store.cluster else {
        return Ok(());
    };
    let policies = cluster.policies().await.map_err(ApiError::GetCoverage)?;
    for (channel, policy) in &policies {
        app.applied_policies.apply(app, channel, *policy).await?;
    }
    app.block_store
        .sync_always_join_channels(
            policies
                .into_iter()
                .filter(|(_, p)| p.always_join)
                .map(|(c, _)| c)
                .collect(),
        )
        .await
        .map_err(ApiError::SyncAlwaysJoin)?;
    app.irc_listener.request_reconcile();
    Ok(())
}

pub async fn accept_snapshot(
    app: &WebAppData,
    snapshot: crate::cluster::Snapshot,
) -> Result<(), ApiError> {
    let cluster = app
        .control_store
        .cluster
        .as_ref()
        .ok_or(ApiError::ServiceOverloaded)?;
    let _guard = cluster.receive_lock.lock().await;
    cluster
        .receive(snapshot)
        .await
        .map_err(ApiError::GetCoverage)?;
    synchronize(app).await?;
    cluster.mark_ready().await.map_err(ApiError::GetCoverage)?;
    Ok(())
}
pub async fn state_get(
    headers: http::HeaderMap,
    Extension(app): Extension<WebAppData>,
) -> Result<axum::Json<crate::cluster::Snapshot>, ApiError> {
    super::peer::authorize_peer(&headers, app.config.peer.shared_token.as_deref())?;
    let cluster = app
        .control_store
        .cluster
        .as_ref()
        .filter(|c| c.is_owner())
        .ok_or(ApiError::ServiceOverloaded)?;
    cluster.check().await.map_err(ApiError::GetCoverage)?;
    Ok(axum::Json(
        cluster.snapshot().await.map_err(ApiError::GetCoverage)?,
    ))
}
pub async fn state_post(
    headers: http::HeaderMap,
    Extension(app): Extension<WebAppData>,
    axum::Json(snapshot): axum::Json<crate::cluster::Snapshot>,
) -> Result<axum::Json<bool>, ApiError> {
    super::peer::authorize_peer(&headers, app.config.peer.shared_token.as_deref())?;
    accept_snapshot(&app, snapshot).await?;
    Ok(axum::Json(true))
}
pub async fn mutation_post(
    headers: http::HeaderMap,
    Extension(app): Extension<WebAppData>,
    axum::Json(operation): axum::Json<crate::cluster::Mutation>,
) -> Result<axum::Json<bool>, ApiError> {
    super::peer::authorize_peer(&headers, app.config.peer.shared_token.as_deref())?;
    let cluster = app
        .control_store
        .cluster
        .as_ref()
        .filter(|c| c.is_owner())
        .ok_or(ApiError::ServiceOverloaded)?;
    let result = cluster
        .mutate(operation)
        .await
        .map_err(ApiError::GetCoverage)?;
    synchronize(&app).await?;
    Ok(axum::Json(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, middleware, routing::get};
    use http::StatusCode;
    use recent_messages2::storage::{AsyncSqliteBlockStore, MessageStore, canonicalize_raw_irc};
    use tower::ServiceExt;
    #[tokio::test]
    async fn local_read_gate_applies_purges_and_optouts_without_contacting_a_peer() {
        let root = std::env::temp_dir().join(format!(
            "rm-policy-gate-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let owner_path = root.join("owner.sqlite");
        let owner_db =
            rusqlite::Connection::open(owner_path.with_extension("owner.sqlite")).unwrap();
        owner_db.execute_batch("CREATE TABLE rm_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);INSERT INTO rm_metadata VALUES('initialized','1'),('epoch','gate-fixture'),('revision','0'),('pins_bootstrapped','1');").unwrap();
        drop(owner_db);
        let owner = crate::cluster::ClusterControl::from_config(
            &crate::config::ClusterConfig {
                role: "owner".into(),
                peer_url: "http://127.0.0.1:1/".into(),
                ..Default::default()
            },
            Some("synthetic-peer-token-at-least-32-characters"),
            &owner_path,
        )
        .await
        .unwrap()
        .unwrap();
        let mirror = crate::cluster::ClusterControl::from_config(
            &crate::config::ClusterConfig {
                role: "replica".into(),
                peer_url: "http://127.0.0.1:1/".into(),
                ..Default::default()
            },
            Some("synthetic-peer-token-at-least-32-characters"),
            &root.join("mirror.sqlite"),
        )
        .await
        .unwrap()
        .unwrap();
        let mut config: crate::config::Config =
            toml::from_slice(include_bytes!("../../config/compact-ha.example.toml")).unwrap();
        config.ingest.enabled = false;
        let config = Box::leak(Box::new(config));
        let mut control = crate::db::ControlStore::open(root.join("control.sqlite")).unwrap();
        control.cluster = Some(mirror);
        let control = Box::leak(Box::new(control));
        let store = Box::leak(Box::new(
            AsyncSqliteBlockStore::open(root.join("cache.sqlite"), 100, 5000, 16, 2).unwrap(),
        ));
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let (irc, _, _) = crate::irc_listener::IrcListener::start_with_direct_ingest(
            control, config, None, cancel,
        );
        let app = WebAppData {
            applied_policies: AppliedPolicies::default(),
            control_store: control,
            config,
            block_store: store,
            irc_listener: Box::leak(Box::new(irc)),
            open_blocks: None,
            peer_client: None,
            repair_coordinator: None,
            admin_auth: None,
            public_requests: Arc::new(tokio::sync::Semaphore::new(8)),
            response_cache: None,
        };
        let route = |app: WebAppData| {
            Router::new()
                .route(
                    "/recent/{channel_login}",
                    get(|| async { StatusCode::OK }).route_layer(middleware::from_fn(gate)),
                )
                .layer(Extension(app))
        };
        let request = || {
            Request::builder()
                .uri("/recent/fixture")
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            route(app.clone())
                .oneshot(request())
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let old = canonicalize_raw_irc(
            "@id=old;tmi-sent-ts=1234 :u!u@u PRIVMSG #fixture :synthetic",
            1234,
        )
        .unwrap();
        store.append_batch(vec![old]).await.unwrap();
        assert!(owner.purge("fixture", 2000).await.is_err()); // No peer available: locally durable, pending.
        accept_snapshot(&app, owner.snapshot().await.unwrap())
            .await
            .unwrap();
        assert_eq!(store.stats().await.unwrap().messages, 0);
        assert_eq!(
            route(app.clone())
                .oneshot(request())
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let new = canonicalize_raw_irc(
            "@id=new;tmi-sent-ts=3000 :u!u@u PRIVMSG #fixture :synthetic",
            3000,
        )
        .unwrap();
        store.append_batch(vec![new]).await.unwrap();
        let mut restarted = app.clone();
        restarted.applied_policies = AppliedPolicies::default();
        assert_eq!(
            route(restarted.clone())
                .oneshot(request())
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            store.stats().await.unwrap().messages,
            1,
            "completed purges must not delete newer history after restart"
        );
        assert!(owner.set_ignored("fixture", true).await.is_err());
        accept_snapshot(&restarted, owner.snapshot().await.unwrap())
            .await
            .unwrap();
        assert!(
            !route(restarted.clone())
                .oneshot(request())
                .await
                .unwrap()
                .status()
                .is_success()
        );
        assert_eq!(store.stats().await.unwrap().messages, 0);
        assert!(owner.set_ignored("fixture", false).await.is_err());
        accept_snapshot(&restarted, owner.snapshot().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            route(restarted).oneshot(request()).await.unwrap().status(),
            StatusCode::OK
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
