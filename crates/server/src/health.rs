//! Health/readiness + Prometheus `/metrics` (stage 8). On a separate port for k8s/LB.

use std::sync::Arc;

use axum::{
    extract::State, http::header, http::StatusCode, response::IntoResponse, routing::get, Router,
};
use manifold_core::api::ApiService;
use manifold_core::metrics::Metrics;

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
    // TODO(impl): 503 while draining (graceful shutdown) or when Redis is unavailable.
    StatusCode::OK
}

async fn metrics(State(api): State<Arc<ApiService>>) -> impl IntoResponse {
    let m = &api.metrics;
    let s = api.api_info();
    let body = format!(
        "# HELP manifold_connections Current connections on node\n\
# TYPE manifold_connections gauge\n\
manifold_connections{{node=\"{node}\"}} {conns}\n\
# HELP manifold_channels Active channels on node\n\
# TYPE manifold_channels gauge\n\
manifold_channels{{node=\"{node}\"}} {chans}\n\
# HELP manifold_messages_published_total Messages published\n\
# TYPE manifold_messages_published_total counter\n\
manifold_messages_published_total{{node=\"{node}\"}} {published}\n\
# HELP manifold_subscriptions_total Subscriptions\n\
# TYPE manifold_subscriptions_total counter\n\
manifold_subscriptions_total{{node=\"{node}\"}} {subs}\n\
# HELP manifold_connections_opened_total Connections opened\n\
# TYPE manifold_connections_opened_total counter\n\
manifold_connections_opened_total{{node=\"{node}\"}} {opened}\n\
# HELP manifold_connections_closed_total Connections closed\n\
# TYPE manifold_connections_closed_total counter\n\
manifold_connections_closed_total{{node=\"{node}\"}} {closed}\n",
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
