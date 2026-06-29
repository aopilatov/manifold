//! WebSocket transport (stage 1). Handshake → JWT → writer task → Command→Reply loop.
//!
//! TODO: allowed_origins check (CSWSH), require_subprotocol, conn limits, handshake_timeout.

use std::sync::Arc;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::Response,
};
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use manifold_core::api::ApiService;
use manifold_protocol::{command, reply, Command, Reply};
use tokio::sync::mpsc;

pub async fn handler(State(api): State<Arc<ApiService>>, ws: WebSocketUpgrade) -> Response {
    // Subprotocol negotiation: if the client offered manifold.v1, select it (echoed in the response).
    ws.protocols(["manifold.v1"])
        .on_upgrade(move |socket| connection(api, socket))
}

async fn connection(api: Arc<ApiService>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    // Bounded outbound buffer (write_buffer_limit). Overflow => disconnect a slow/non-reading client.
    let (tx, mut rx) = mpsc::channel::<Reply>(1024);

    // writer task: Reply → protobuf → binary WS frame
    let writer = tokio::spawn(async move {
        while let Some(reply) = rx.recv().await {
            let bytes = reply.encode_to_vec();
            if sink.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
        }
    });

    // 1) the first frame must be a ConnectRequest
    let client_id = match handshake(&api, &mut stream, &tx).await {
        Some(id) => id,
        None => {
            writer.abort();
            return;
        }
    };

    // 2) command loop
    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Binary(b) => {
                if let Ok(cmd) = Command::decode(b.as_slice()) {
                    if let Some(reply) = api.handle_command(&client_id, cmd).await {
                        // try_send (not .await): a client that doesn't drain its replies must not
                        // block the read loop — disconnect it instead (slow-consumer policy).
                        if tx.try_send(reply).is_err() {
                            break;
                        }
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    api.cleanup(&client_id).await;
    writer.abort();
}

/// Accept and validate a ConnectRequest, register the connection, return ConnectResult.
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
        return None; // first frame is not a connect
    };

    match api.authenticate(&creq.token) {
        Ok(claims) => {
            let client_id = api.register(Some(claims), tx.clone());
            let res = api.connect(&client_id, &creq).await;
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
