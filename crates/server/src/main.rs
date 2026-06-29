//! Engine entry point. Loads config, brings up transports (WS/SSE), Server API (HTTP/gRPC),
//! admin, health. Skeleton: currently starts health + a WS stub.

mod admin;
mod events;
mod grpc_api;
mod health;
mod http_api;
mod sse;
mod ws;
// mod http_api;  // TODO(impl): Server API (HTTP/JSON)
// mod grpc_api;  // TODO(impl): Server API (gRPC/tonic)
// mod admin;     // TODO(impl): admin UI + sessions

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use manifold_broker::{Broker, MemoryBroker, RedisBroker};
use manifold_core::{api::ApiService, delivery::HubDelivery, hub::Hub, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::var("MANIFOLD_CONFIG").unwrap_or_else(|_| "config.toml".into());

    let cfg = Config::load(&config_path)
        .unwrap_or_else(|e| panic!("failed to load {config_path}: {e}"));

    let json_logs = cfg.telemetry.log_format.as_deref() == Some("json");
    init_tracing(&cfg.server.log_level, json_logs);
    tracing::info!(node = %cfg.server.node_name, "manifold-server starting");

    let cfg = Arc::new(cfg);
    let hub = Hub::new();
    let delivery = HubDelivery::new(hub.clone());

    let broker: Arc<dyn Broker> = if cfg.redis.enabled {
        tracing::info!(url = %cfg.redis.url, "broker: Redis (multi-node)");
        RedisBroker::connect(&cfg.redis.url, cfg.redis.prefix.clone(), delivery).await?
    } else {
        tracing::info!("broker: in-memory (single node)");
        MemoryBroker::new(delivery)
    };

    let mut api = ApiService::new(cfg.clone(), hub, broker);
    if cfg.events.enabled {
        tracing::info!(endpoint = %cfg.events.endpoint, "lifecycle events → webhook");
        api.set_event_sink(Arc::new(events::HttpEventSink::new(
            cfg.events.endpoint.clone(),
            cfg.events.types.clone(),
        )));
    }
    let api = Arc::new(api);

    // health/readiness/metrics on a separate port
    let health_addr = cfg.server.health.listen.clone();
    let health_app = health::router(api.clone());
    let health = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&health_addr).await.unwrap();
        tracing::info!(%health_addr, "health/metrics listening");
        axum::serve(listener, health_app).await.unwrap();
    });

    // WS + SSE on a single listener
    let ws_addr = cfg.server.ws.listen.clone();
    let ws_path = cfg.server.ws.path.clone();
    let mut ws_app = Router::new().route(&ws_path, get(ws::handler));
    if cfg.server.sse.enabled {
        tracing::info!("SSE fallback enabled");
        ws_app = ws_app
            .route(&cfg.server.sse.path, get(sse::stream))
            .route(&cfg.server.sse.emit_path, post(sse::emit));
    }
    let ws_app = ws_app.with_state(api.clone());
    let ws = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&ws_addr).await.unwrap();
        tracing::info!(%ws_addr, "WS listening");
        axum::serve(listener, ws_app).await.unwrap();
    });

    // Server API — HTTP/JSON
    let http_addr = cfg.server.http_api.listen.clone();
    let http_app = http_api::router(api.clone());
    let http = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();
        tracing::info!(%http_addr, "HTTP Server API listening");
        axum::serve(listener, http_app).await.unwrap();
    });

    // Server API — gRPC
    let grpc_addr: std::net::SocketAddr =
        cfg.server.grpc_api.listen.parse().expect("invalid grpc_api.listen");
    let grpc_impl = grpc_api::GrpcApi { api: api.clone() };
    let grpc = tokio::spawn(async move {
        tracing::info!(%grpc_addr, "gRPC Server API listening");
        tonic::transport::Server::builder()
            .add_service(manifold_protocol::server_api_server::ServerApiServer::new(grpc_impl))
            .serve(grpc_addr)
            .await
            .unwrap();
    });

    let mut tasks = vec![health, ws, http, grpc];

    // Admin UI (third access tier: password → session)
    if cfg.server.admin.enabled {
        let listen = cfg.server.admin.listen.clone();
        let local = listen.starts_with("127.0.0.1") || listen.starts_with("localhost");
        if cfg.server.admin.password.is_empty() && !local {
            panic!("admin: empty password on a public interface ({listen}) — refusing to start");
        }
        let admin_app = admin::router(api.clone(), cfg.server.admin.password.clone(), "web/dist");
        tasks.push(tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(&listen).await.unwrap();
            tracing::info!(%listen, "admin UI listening");
            axum::serve(listener, admin_app).await.unwrap();
        }));
    }

    // TODO(impl): graceful shutdown (SIGTERM → drain).
    futures::future::join_all(tasks).await;
    Ok(())
}

fn init_tracing(level: &str, json: bool) {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let builder = fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}
