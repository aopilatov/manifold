//! Точка входа движка. Загружает конфиг, поднимает транспорты (WS/SSE), Server API (HTTP/gRPC),
//! admin, health. Скелет: сейчас стартует health + WS-заглушку.

mod health;
mod ws;
// mod sse;       // TODO(impl): SSE-транспорт
// mod http_api;  // TODO(impl): Server API (HTTP/JSON)
// mod grpc_api;  // TODO(impl): Server API (gRPC/tonic)
// mod admin;     // TODO(impl): admin UI + сессии

use std::sync::Arc;

use axum::{routing::get, Router};
use socket_core::{api::ApiService, hub::Hub, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::var("SOCKET_CONFIG").unwrap_or_else(|_| "config.toml".into());

    let cfg = Config::load(&config_path)
        .unwrap_or_else(|e| panic!("не удалось загрузить {config_path}: {e}"));

    init_tracing(&cfg.server.log_level);
    tracing::info!(node = %cfg.server.node_name, "socket-server запускается");

    let cfg = Arc::new(cfg);
    let hub = Hub::new();
    let api = Arc::new(ApiService::new(cfg.clone(), hub));

    // health/readiness на отдельном порту (раздел 12)
    let health_addr = cfg.server.health.listen.clone();
    let health = tokio::spawn(serve_health(health_addr));

    // WS-транспорт (этап 1: коннект + подписки + локальный fan-out)
    let ws_addr = cfg.server.ws.listen.clone();
    let ws_path = cfg.server.ws.path.clone();
    let ws_app = Router::new().route(&ws_path, get(ws::handler)).with_state(api.clone());
    let ws = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&ws_addr).await.unwrap();
        tracing::info!(%ws_addr, "WS слушает");
        axum::serve(listener, ws_app).await.unwrap();
    });

    // TODO(impl): http_api, grpc_api, admin, graceful shutdown (SIGTERM → drain).
    let _ = tokio::try_join!(health, ws);
    Ok(())
}

async fn serve_health(addr: String) {
    let app = Router::new()
        .route("/health", get(health::liveness))
        .route("/ready", get(health::readiness));
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!(%addr, "health слушает");
    axum::serve(listener, app).await.unwrap();
}

fn init_tracing(level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));
    fmt().with_env_filter(filter).init();
}
