use crate::Config;
use crate::config::ListenAddr;
use crate::db::ControlStore;
use crate::irc_listener::IrcListener;
use crate::web::error::ApiError;
use axum::routing::{get, post, put};
use axum::{Extension, Router, middleware};
use futures::future::BoxFuture;
use http::{Method, header};
use recent_messages2::storage::{AsyncSqliteBlockStore, SharedOpenBlocksHandle};
use std::{
    net::SocketAddr,
    sync::{Arc, LazyLock},
};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::{
    cors::{self, CorsLayer},
    trace::TraceLayer,
};
#[cfg(unix)]
use {
    std::fs::Permissions, std::io::ErrorKind, std::os::unix::fs::PermissionsExt, std::path::Path,
    tokio::net::UnixListener,
};

mod admin;
pub mod auth;
mod auth_endpoints;
mod auth_middleware;
pub mod error;
mod get_metrics;
pub mod get_recent_messages;
mod health;
mod ignored;
mod peer;
mod purge;
mod record_metrics;
mod repair;
mod response_cache;
mod timeout;
mod viewer_badges;

#[derive(Clone)]
pub struct WebAppData {
    control_store: &'static ControlStore,
    irc_listener: &'static IrcListener,
    config: &'static Config,
    block_store: &'static AsyncSqliteBlockStore,
    open_blocks: Option<SharedOpenBlocksHandle>,
    peer_client: Option<peer::PeerClient>,
    repair_coordinator: Option<repair::RepairCoordinator>,
    admin_auth: Option<admin::AdminAuth>,
    public_requests: Arc<Semaphore>,
    response_cache: Option<response_cache::AdaptiveResponseCache>,
}

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

#[derive(Error, Debug)]
pub enum BindError {
    #[error("Failed to bind to address `{0}`: {1}")]
    BindTcp(&'static SocketAddr, std::io::Error),
    #[cfg(unix)]
    #[error(
        "Failed to delete old unix socket at `{path}`: {err}",
        path = .0.display(),
        err = .1
    )]
    DeleteOldSocketFile(&'static Path, std::io::Error),
    #[cfg(unix)]
    #[error(
        "Failed to create parent directory for unix socket `{path}`: {err}",
        path = .0.display(),
        err = .1
    )]
    CreateParentDir(&'static Path, std::io::Error),
    #[cfg(unix)]
    #[error(
        "Failed to bind to unix socket `{path}`: {err}",
        path = .0.display(),
        err = .1
    )]
    BindUnix(&'static Path, std::io::Error),
    #[cfg(unix)]
    #[error(
        "Failed to alter permissions on unix socket `{path}` to `{permissions:?}`: {err}",
        path = .0.display(),
        permissions = .1,
        err = .2
    )]
    SetPermissions(&'static Path, Permissions, std::io::Error),
    #[error("Invalid peer configuration: {0}")]
    InvalidPeerConfig(#[from] peer::PeerConfigError),
    #[error("Invalid repair configuration: {0}")]
    InvalidRepairConfig(#[from] repair::RepairConfigError),
    #[error("Invalid admin configuration: {0}")]
    InvalidAdminConfig(#[from] admin::AdminConfigError),
    #[error("web max_inflight_recent_requests must be greater than zero")]
    InvalidPublicRequestLimit,
    #[error("Invalid adaptive response-cache configuration: {0}")]
    InvalidResponseCacheConfig(#[from] response_cache::ResponseCacheConfigError),
}

#[cfg_attr(not(unix), allow(clippy::unused_async))]
pub async fn run(
    control_store: &'static ControlStore,
    irc_listener: &'static IrcListener,
    config: &'static Config,
    block_store: &'static AsyncSqliteBlockStore,
    open_blocks: Option<SharedOpenBlocksHandle>,
    shutdown_signal: CancellationToken,
) -> Result<BoxFuture<'static, std::io::Result<()>>, BindError> {
    if config.web.max_inflight_recent_requests == 0 {
        return Err(BindError::InvalidPublicRequestLimit);
    }
    let peer_client = peer::PeerClient::from_config(&config.peer)?;
    let repair_coordinator =
        repair::RepairCoordinator::from_config(&config.repair, control_store, block_store)?;
    let admin_auth = admin::AdminAuth::from_config(&config.admin)?;
    let response_cache = response_cache::AdaptiveResponseCache::from_config(&config.web)?;
    let shared_state = WebAppData {
        control_store,
        irc_listener,
        config,
        block_store,
        open_blocks,
        peer_client,
        repair_coordinator,
        admin_auth,
        public_requests: Arc::new(Semaphore::new(config.web.max_inflight_recent_requests)),
        response_cache,
    };

    let cors = CorsLayer::new()
        .allow_methods(vec![Method::GET, Method::POST])
        .allow_headers(vec![
            header::AUTHORIZATION,
            header::ACCEPT,
            header::CONTENT_TYPE,
        ])
        .expose_headers([
            header::HeaderName::from_static("x-rm-coverage"),
            header::RETRY_AFTER,
        ])
        .allow_origin(cors::Any);

    let auth_middleware = || {
        let app_data = shared_state.clone();
        middleware::from_fn(move |req, next| {
            auth_middleware::with_authorization(req, next, app_data.clone())
        })
    };
    let api = Router::new()
        .route(
            "/recent-messages/{channel_login}",
            get(get_recent_messages::get_recent_messages)
                .route_layer(middleware::from_fn(peer::delegate_overload))
                .route_layer(middleware::from_fn(response_cache::adaptive_response_cache)),
        )
        .route(
            "//recent-messages/{channel_login}",
            get(get_recent_messages::get_recent_messages)
                .route_layer(middleware::from_fn(peer::delegate_overload))
                .route_layer(middleware::from_fn(response_cache::adaptive_response_cache)),
        )
        .route(
            "/ignored",
            get(ignored::get_ignored)
                .post(ignored::set_ignored)
                .route_layer(auth_middleware()),
        )
        .route(
            "/purge",
            post(purge::purge_messages).route_layer(auth_middleware()),
        )
        .route("/auth/create", post(auth_endpoints::create_token))
        .route(
            "/auth/extend",
            post(auth_endpoints::extend_token).route_layer(auth_middleware()),
        )
        .route(
            "/auth/revoke",
            post(auth_endpoints::revoke_token).route_layer(auth_middleware()),
        )
        .route(
            "/admin/always-join",
            get(admin::get_always_join).put(admin::replace_always_join),
        )
        .route(
            "/admin/always-join/{channel_login}",
            put(admin::add_always_join).delete(admin::remove_always_join),
        )
        .route("/metrics", get(get_metrics::get_metrics))
        .method_not_allowed_fallback(|| async { ApiError::MethodNotAllowed })
        .fallback(|| async { ApiError::NotFound })
        .layer(cors.clone());

    let internal = Router::new()
        .route("/local-recent/{channel_login}", get(peer::get_local_recent))
        .route("/serve/{channel_login}", get(peer::serve_recent))
        .method_not_allowed_fallback(|| async { ApiError::MethodNotAllowed })
        .fallback(|| async { ApiError::NotFound });

    let servedir = ServeDir::new("web/dist")
        .append_index_html_on_directories(true)
        .fallback(ServeFile::new("web/dist/index.html"));

    let app = Router::new()
        .route("/health/live", get(health::live))
        .route("/health/ready", get(health::ready))
        .route(
            "/api/viewer/badges/{channel_id}",
            get(viewer_badges::get_badges).layer(cors.clone()),
        )
        .route(
            "/api/{channel_login}",
            get(get_recent_messages::get_recent_messages)
                .route_layer(middleware::from_fn(peer::delegate_overload))
                .route_layer(middleware::from_fn(response_cache::adaptive_response_cache))
                .layer(cors.clone()),
        )
        .nest("/api/v2", api)
        .nest("/internal/v1", internal)
        .fallback_service(servedir)
        .layer(
            ServiceBuilder::new()
                .layer(Extension(shared_state))
                .layer(middleware::from_fn(record_metrics::record_metrics))
                .layer(middleware::from_fn(timeout::timeout))
                .layer(TraceLayer::new_for_http()),
        );

    Ok(match &config.web.listen_address {
        ListenAddr::Tcp { address } => {
            let listener = TcpListener::bind(address)
                .await
                .map_err(|e| BindError::BindTcp(address, e))?;

            Box::pin(
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        shutdown_signal.cancelled().await;
                    })
                    .into_future(),
            )
        }
        #[cfg(unix)]
        ListenAddr::Unix { path } => {
            if let Err(e) = tokio::fs::remove_file(&path).await
                && e.kind() != ErrorKind::NotFound
            {
                return Err(BindError::DeleteOldSocketFile(path, e));
            }
            tokio::fs::create_dir_all(path.parent().unwrap())
                .await
                .map_err(|e| BindError::CreateParentDir(path, e))?;

            let listener =
                UnixListener::bind(path.clone()).map_err(|e| BindError::BindUnix(path, e))?;

            let permissions = Permissions::from_mode(0o777);
            tokio::fs::set_permissions(path, permissions.clone())
                .await
                .map_err(|e| BindError::SetPermissions(path, permissions, e))?;

            Box::pin(
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        shutdown_signal.cancelled().await;
                    })
                    .into_future(),
            )
        }
    })
}
