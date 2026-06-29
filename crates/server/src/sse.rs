//! SSE transport (stage 4) — a fallback for networks that block WS. Split session:
//!
//! - `GET /connection/sse?token=JWT` — downstream (EventSource): the server authenticates, sets up
//!   a session in the same hub and streams `Reply` as **base64(protobuf)** in the `data:` field. The
//!   first event is `ConnectResult` (carries `client` = session_id).
//! - `POST /connection/sse/emit` (`X-Session-Id` header, body — protobuf `Command`) — upstream.
//!   The response goes back down the SSE of the same session.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::IntoResponse,
};
use base64::Engine;
use prost::Message as _;
use socket_core::api::ApiService;
use socket_protocol::{reply, Command, ConnectRequest, Reply};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Tears down the session when the downstream breaks (EventSource closed).
struct CleanupGuard {
    api: Arc<ApiService>,
    sid: String,
}
impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let api = self.api.clone();
        let sid = self.sid.clone();
        tokio::spawn(async move { api.cleanup(&sid).await });
    }
}

pub async fn stream(
    State(api): State<Arc<ApiService>>,
    Query(q): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let token = q.get("token").cloned().unwrap_or_default();
    let claims = match api.authenticate(&token) {
        Ok(c) => Some(c),
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let (tx, rx) = mpsc::channel::<Reply>(256);
    let sid = api.register(claims, tx);
    let connect = api.connect(&sid, &ConnectRequest::default()).await;
    let initial = Reply { id: 0, error: None, payload: Some(reply::Payload::Connect(connect)) };
    let guard = CleanupGuard { api: api.clone(), sid: sid.clone() };

    let body = async_stream::stream! {
        let _g = guard; // lives as long as the stream; Drop → session cleanup
        yield Ok::<Event, std::convert::Infallible>(sse_event(&initial));
        let mut rs = ReceiverStream::new(rx);
        while let Some(reply) = rs.next().await {
            yield Ok(sse_event(&reply));
        }
    };

    Sse::new(body).keep_alive(KeepAlive::default()).into_response()
}

pub async fn emit(State(api): State<Arc<ApiService>>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let sid = match headers.get("x-session-id").and_then(|v| v.to_str().ok()) {
        Some(s) => s.to_string(),
        None => return StatusCode::BAD_REQUEST,
    };
    let cmd = match Command::decode(body.as_ref()) {
        Ok(c) => c,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    if let Some(reply) = api.handle_command(&sid, cmd).await {
        // clone tx and drop the DashMap guard before the await
        let tx = api.hub.connections.get(&sid).map(|c| c.tx.clone());
        if let Some(tx) = tx {
            let _ = tx.send(reply).await;
        }
    }
    StatusCode::ACCEPTED
}

fn sse_event(reply: &Reply) -> Event {
    Event::default().data(B64.encode(reply.encode_to_vec()))
}
