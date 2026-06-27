//! WebSocket-транспорт (этап 1). Handshake → JWT → writer-задача → цикл Command→Reply.
//!
//! TODO: проверка allowed_origins (CSWSH), require_subprotocol, conn-лимиты, handshake_timeout.

use std::sync::Arc;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::Response,
};
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use socket_core::api::ApiService;
use socket_protocol::{command, reply, Command, Reply};
use tokio::sync::mpsc;

pub async fn handler(State(api): State<Arc<ApiService>>, ws: WebSocketUpgrade) -> Response {
    // Согласование subprotocol: если клиент предложил socket.v1 — выбираем его (эхо в ответе).
    ws.protocols(["socket.v1"])
        .on_upgrade(move |socket| connection(api, socket))
}

async fn connection(api: Arc<ApiService>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Reply>(256);

    // writer-задача: Reply → protobuf → бинарный WS-кадр
    let writer = tokio::spawn(async move {
        while let Some(reply) = rx.recv().await {
            let bytes = reply.encode_to_vec();
            if sink.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
        }
    });

    // 1) первый кадр обязан быть ConnectRequest
    let client_id = match handshake(&api, &mut stream, &tx).await {
        Some(id) => id,
        None => {
            writer.abort();
            return;
        }
    };

    // 2) цикл команд
    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Binary(b) => {
                if let Ok(cmd) = Command::decode(b.as_slice()) {
                    if let Some(reply) = api.handle_command(&client_id, cmd) {
                        if tx.send(reply).await.is_err() {
                            break;
                        }
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    api.cleanup(&client_id);
    writer.abort();
}

/// Принять и проверить ConnectRequest, зарегистрировать соединение, отдать ConnectResult.
async fn handshake(
    api: &Arc<ApiService>,
    stream: &mut futures::stream::SplitStream<WebSocket>,
    tx: &mpsc::Sender<Reply>,
) -> Option<String> {
    let Some(Ok(Message::Binary(b))) = stream.next().await else {
        return None;
    };
    let cmd = Command::decode(b.as_slice()).ok()?;
    let id = cmd.id;
    let Some(command::Method::Connect(creq)) = cmd.method else {
        return None; // первый кадр — не connect
    };

    match api.authenticate(&creq.token) {
        Ok(claims) => {
            let client_id = api.register(Some(claims), tx.clone());
            let res = api.connect(&client_id, &creq);
            let reply = Reply {
                id,
                error: None,
                payload: Some(reply::Payload::Connect(res)),
            };
            let _ = tx.send(reply).await;
            Some(client_id)
        }
        Err(error) => {
            let _ = tx.send(Reply { id, error: Some(error), payload: None }).await;
            None
        }
    }
}
