//! Server API — HTTP/JSON adapter (stage 5). A thin wrapper over `ApiService::api_*`.
//! Auth — API key: `Authorization: apikey <key>` + method `allow` check.
//! `data` in JSON is base64.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use manifold_core::api::ApiService;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

type Api = Arc<ApiService>;

pub fn router(api: Api) -> Router {
    Router::new()
        .route("/api/publish", post(publish))
        .route("/api/broadcast", post(broadcast))
        .route("/api/presence", post(presence))
        .route("/api/history", post(history))
        .route("/api/channels", post(channels))
        .route("/api/info", post(info))
        .route("/api/disconnect", post(disconnect))
        .route("/api/unsubscribe", post(unsubscribe))
        .route("/api/user_online", post(user_online))
        .with_state(api)
}

/// Validate the API key and the permission for the method.
fn authorized(headers: &HeaderMap, method: &str, api: &ApiService) -> bool {
    let key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("apikey "))
        .unwrap_or("");
    if key.is_empty() {
        return false;
    }
    api.cfg
        .api_keys
        .iter()
        .any(|k| k.key == key && (k.allow.is_empty() || k.allow.iter().any(|a| a == method)))
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "invalid api key").into_response()
}

// ─── publish ───
#[derive(Deserialize)]
struct PublishReq {
    channel: String,
    #[serde(default)]
    data: String,
    #[serde(default)]
    idempotency_key: Option<String>,
}
#[derive(Serialize)]
struct PublishResp {
    offset: u64,
    epoch: String,
}

async fn publish(State(api): State<Api>, headers: HeaderMap, Json(req): Json<PublishReq>) -> Response {
    if !authorized(&headers, "publish", &api) {
        return unauthorized();
    }
    let data = B64.decode(req.data.as_bytes()).unwrap_or_default();
    match api.api_publish(&req.channel, data, req.idempotency_key.as_deref()).await {
        Ok(pos) => Json(PublishResp { offset: pos.offset, epoch: pos.epoch }).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ─── broadcast ───
#[derive(Deserialize)]
struct BroadcastReq {
    channels: Vec<String>,
    #[serde(default)]
    data: String,
}
async fn broadcast(State(api): State<Api>, headers: HeaderMap, Json(req): Json<BroadcastReq>) -> Response {
    if !authorized(&headers, "broadcast", &api) {
        return unauthorized();
    }
    let data = B64.decode(req.data.as_bytes()).unwrap_or_default();
    let res = api.api_broadcast(&req.channels, data).await;
    Json(serde_json::json!({ "offsets": res })).into_response()
}

// ─── presence ───
#[derive(Deserialize)]
struct ChannelReq {
    channel: String,
}
#[derive(Serialize)]
struct PresenceEntry {
    user: String,
    client: String,
}
async fn presence(State(api): State<Api>, headers: HeaderMap, Json(req): Json<ChannelReq>) -> Response {
    if !authorized(&headers, "presence", &api) {
        return unauthorized();
    }
    match api.api_presence(&req.channel).await {
        Ok(map) => {
            let out: BTreeMap<String, PresenceEntry> = map
                .into_iter()
                .map(|(k, v)| (k, PresenceEntry { user: v.user, client: v.client }))
                .collect();
            Json(serde_json::json!({ "presence": out })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ─── history ───
#[derive(Deserialize)]
struct HistoryReq {
    channel: String,
    #[serde(default)]
    limit: usize,
}
async fn history(State(api): State<Api>, headers: HeaderMap, Json(req): Json<HistoryReq>) -> Response {
    if !authorized(&headers, "history", &api) {
        return unauthorized();
    }
    let limit = if req.limit > 0 { req.limit } else { 100 };
    match api.api_history(&req.channel, limit).await {
        Ok((pubs, pos)) => {
            let items: Vec<_> = pubs
                .into_iter()
                .map(|p| serde_json::json!({ "offset": p.offset, "data": B64.encode(p.data) }))
                .collect();
            Json(serde_json::json!({
                "publications": items,
                "position": { "offset": pos.offset, "epoch": pos.epoch }
            }))
            .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ─── channels ───
#[derive(Deserialize)]
struct ChannelsReq {
    #[serde(default)]
    pattern: Option<String>,
}
async fn channels(State(api): State<Api>, headers: HeaderMap, Json(req): Json<ChannelsReq>) -> Response {
    if !authorized(&headers, "channels", &api) {
        return unauthorized();
    }
    let list = api.api_channels(req.pattern.as_deref());
    Json(serde_json::json!({ "channels": list })).into_response()
}

// ─── info ───
async fn info(State(api): State<Api>, headers: HeaderMap) -> Response {
    if !authorized(&headers, "info", &api) {
        return unauthorized();
    }
    let s = api.api_info();
    Json(serde_json::json!({
        "node": s.node, "num_connections": s.num_connections, "num_channels": s.num_channels
    }))
    .into_response()
}

// ─── disconnect ───
#[derive(Deserialize)]
struct DisconnectReq {
    #[serde(default)]
    user: String,
    #[serde(default)]
    client: String,
    #[serde(default)]
    code: u32,
    #[serde(default)]
    reason: String,
}
async fn disconnect(State(api): State<Api>, headers: HeaderMap, Json(req): Json<DisconnectReq>) -> Response {
    if !authorized(&headers, "disconnect", &api) {
        return unauthorized();
    }
    api.api_disconnect(&req.user, &req.client, req.code, &req.reason).await;
    StatusCode::ACCEPTED.into_response()
}

// ─── unsubscribe ───
#[derive(Deserialize)]
struct UnsubReq {
    #[serde(default)]
    user: String,
    channel: String,
}
async fn unsubscribe(State(api): State<Api>, headers: HeaderMap, Json(req): Json<UnsubReq>) -> Response {
    if !authorized(&headers, "unsubscribe", &api) {
        return unauthorized();
    }
    api.api_unsubscribe(&req.user, &req.channel).await;
    StatusCode::ACCEPTED.into_response()
}

// ─── user_online ───
#[derive(Deserialize)]
struct UserReq {
    user: String,
}
async fn user_online(State(api): State<Api>, headers: HeaderMap, Json(req): Json<UserReq>) -> Response {
    if !authorized(&headers, "user_online", &api) {
        return unauthorized();
    }
    let (online, n) = api.api_user_online(&req.user);
    Json(serde_json::json!({ "online": online, "num_connections": n })).into_response()
}
