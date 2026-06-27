//! WebSocket-транспорт (скелет). На апгрейде: проверка Origin, subprotocol `socket.v1`,
//! затем цикл Command→Reply через ядро.

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
};

pub async fn handler(ws: WebSocketUpgrade) -> Response {
    // TODO(impl): проверка allowed_origins (CSWSH), require_subprotocol, conn-лимиты.
    ws.on_upgrade(connection)
}

async fn connection(mut socket: WebSocket) {
    // TODO(impl):
    //  1. ждать ConnectRequest (handshake_timeout), валидировать JWT, восстановить subs (1 RTT).
    //  2. отдельная writer-задача из mpsc<Reply> (см. hub::ConnHandle.tx).
    //  3. цикл: decode Command (protobuf) → ApiService → encode Reply.
    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Close(_) = msg {
            break;
        }
        // эхо-заглушка
        let _ = socket.send(Message::Binary(b"todo".to_vec())).await;
    }
}
