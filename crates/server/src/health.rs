//! Health/readiness + Prometheus `/metrics` (этап 8). На отдельном порту для k8s/LB.

use std::sync::Arc;

use axum::{
    extract::State, http::header, http::StatusCode, response::IntoResponse, routing::get, Router,
};
use socket_core::api::ApiService;
use socket_core::metrics::Metrics;

pub fn router(api: Arc<ApiService>) -> Router {
    Router::new()
        .route("/health", get(liveness))
        .route("/ready", get(readiness))
        .route("/metrics", get(metrics))
        .with_state(api)
}

async fn liveness() -> StatusCode {
    StatusCode::OK
}

async fn readiness() -> StatusCode {
    // TODO(impl): 503 при дренаже (graceful shutdown) или недоступном Redis.
    StatusCode::OK
}

async fn metrics(State(api): State<Arc<ApiService>>) -> impl IntoResponse {
    let m = &api.metrics;
    let s = api.api_info();
    let body = format!(
        "# HELP socket_connections Current connections on node\n\
# TYPE socket_connections gauge\n\
socket_connections{{node=\"{node}\"}} {conns}\n\
# HELP socket_channels Active channels on node\n\
# TYPE socket_channels gauge\n\
socket_channels{{node=\"{node}\"}} {chans}\n\
# HELP socket_messages_published_total Messages published\n\
# TYPE socket_messages_published_total counter\n\
socket_messages_published_total{{node=\"{node}\"}} {published}\n\
# HELP socket_subscriptions_total Subscriptions\n\
# TYPE socket_subscriptions_total counter\n\
socket_subscriptions_total{{node=\"{node}\"}} {subs}\n\
# HELP socket_connections_opened_total Connections opened\n\
# TYPE socket_connections_opened_total counter\n\
socket_connections_opened_total{{node=\"{node}\"}} {opened}\n\
# HELP socket_connections_closed_total Connections closed\n\
# TYPE socket_connections_closed_total counter\n\
socket_connections_closed_total{{node=\"{node}\"}} {closed}\n",
        node = s.node,
        conns = s.num_connections,
        chans = s.num_channels,
        published = Metrics::val(&m.messages_published),
        subs = Metrics::val(&m.subscriptions),
        opened = Metrics::val(&m.connections_opened),
        closed = Metrics::val(&m.connections_closed),
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}
