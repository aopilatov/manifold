//! Admin UI backend (stage 9): third access tier. Password login → session (admin JWT in an
//! httpOnly cookie). Endpoints are thin wrappers over `ApiService::api_*`. Serves static `web/dist`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use socket_core::api::ApiService;
use socket_core::metrics::Metrics;
use tower_http::services::{ServeDir, ServeFile};

const COOKIE: &str = "admin_session";
const SESSION_TTL_SECS: u64 = 86_400;

#[derive(Clone)]
pub struct AdminState {
    pub api: Arc<ApiService>,
    pub password: Arc<String>,
}

#[derive(Serialize, Deserialize)]
struct AdminClaims {
    exp: usize,
}

pub fn router(api: Arc<ApiService>, password: String, static_dir: &str) -> Router {
    let state = AdminState { api, password: Arc::new(password) };

    let spa = ServeDir::new(static_dir)
        .not_found_service(ServeFile::new(format!("{static_dir}/index.html")));

    Router::new()
        .route("/admin/login", post(login))
        .route("/admin/me", get(me))
        .route("/admin/info", get(info))
        .route("/admin/channels", get(channels))
        .route("/admin/presence", post(presence))
        .route("/admin/publish", post(publish))
        .route("/admin/disconnect", post(disconnect))
        .with_state(state)
        .fallback_service(spa)
}

// ─── session ───

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn mint(password: &str) -> String {
    let claims = AdminClaims { exp: (now() + SESSION_TTL_SECS) as usize };
    encode(&Header::default(), &claims, &EncodingKey::from_secret(password.as_bytes())).unwrap_or_default()
}

fn valid_session(headers: &HeaderMap, password: &str) -> bool {
    let cookie = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let token = cookie
        .split(';')
        .filter_map(|p| p.trim().strip_prefix(&format!("{COOKIE}=")))
        .next()
        .unwrap_or("");
    if token.is_empty() {
        return false;
    }
    decode::<AdminClaims>(token, &DecodingKey::from_secret(password.as_bytes()), &Validation::default()).is_ok()
}

fn guard(headers: &HeaderMap, st: &AdminState) -> Result<(), Response> {
    if valid_session(headers, &st.password) {
        Ok(())
    } else {
        Err((StatusCode::UNAUTHORIZED, "auth required").into_response())
    }
}

// ─── handlers ───

#[derive(Deserialize)]
struct LoginReq {
    password: String,
}

async fn login(State(st): State<AdminState>, Json(req): Json<LoginReq>) -> Response {
    if st.password.is_empty() || req.password != *st.password {
        return (StatusCode::UNAUTHORIZED, "bad password").into_response();
    }
    let token = mint(&st.password);
    let cookie = format!("{COOKIE}={token}; HttpOnly; Path=/; Max-Age={SESSION_TTL_SECS}; SameSite=Lax");
    ([(header::SET_COOKIE, cookie)], Json(serde_json::json!({ "ok": true }))).into_response()
}

async fn me(State(st): State<AdminState>, headers: HeaderMap) -> Response {
    match guard(&headers, &st) {
        Ok(()) => Json(serde_json::json!({ "authenticated": true })).into_response(),
        Err(r) => r,
    }
}

async fn info(State(st): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = guard(&headers, &st) {
        return r;
    }
    let s = st.api.api_info();
    let m = &st.api.metrics;
    Json(serde_json::json!({
        "node": s.node,
        "num_connections": s.num_connections,
        "num_channels": s.num_channels,
        "messages_published": Metrics::val(&m.messages_published),
        "subscriptions": Metrics::val(&m.subscriptions),
        "connections_opened": Metrics::val(&m.connections_opened),
        "connections_closed": Metrics::val(&m.connections_closed),
    }))
    .into_response()
}

async fn channels(State(st): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = guard(&headers, &st) {
        return r;
    }
    Json(serde_json::json!({ "channels": st.api.api_channels(None) })).into_response()
}

#[derive(Deserialize)]
struct ChannelReq {
    channel: String,
}

async fn presence(State(st): State<AdminState>, headers: HeaderMap, Json(req): Json<ChannelReq>) -> Response {
    if let Err(r) = guard(&headers, &st) {
        return r;
    }
    match st.api.api_presence(&req.channel).await {
        Ok(map) => {
            let users: Vec<String> = map.into_values().map(|i| i.user).collect();
            Json(serde_json::json!({ "users": users })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct PublishReq {
    channel: String,
    data: String,
}

async fn publish(State(st): State<AdminState>, headers: HeaderMap, Json(req): Json<PublishReq>) -> Response {
    if let Err(r) = guard(&headers, &st) {
        return r;
    }
    match st.api.api_publish(&req.channel, req.data.into_bytes(), None).await {
        Ok(pos) => Json(serde_json::json!({ "offset": pos.offset, "epoch": pos.epoch })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct DisconnectReq {
    #[serde(default)]
    user: String,
    #[serde(default)]
    client: String,
}

async fn disconnect(State(st): State<AdminState>, headers: HeaderMap, Json(req): Json<DisconnectReq>) -> Response {
    if let Err(r) = guard(&headers, &st) {
        return r;
    }
    st.api.api_disconnect(&req.user, &req.client, 0, "admin").await;
    StatusCode::ACCEPTED.into_response()
}
