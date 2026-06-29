//! SSE-транспорт (этап 4) — фолбэк для сетей, режущих WS. Расщеплённая сессия:
//!
//! - `GET /connection/sse?token=JWT` — downstream (EventSource): сервер аутентифицирует, заводит
//!   сессию в том же hub и стримит `Reply` как **base64(protobuf)** в поле `data:`. Первое событие —
//!   `ConnectResult` (несёт `client` = session_id).
//! - `POST /connection/sse/emit` (заголовок `X-Session-Id`, тело — protobuf `Command`) — upstream.
//!   Ответ уходит вниз по SSE той же сессии.

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

/// Снимает сессию при разрыве downstream (EventSource закрылся).
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
        let _g = guard; // живёт пока жив стрим; Drop → cleanup сессии
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
        // tx клонируем и роняем guard DashMap до await
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
