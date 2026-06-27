//! Hub: in-memory маршрутизация на ноде (см. design-doc, раздел 7).
//!
//! Хранит только локальные соединения и индекс «канал → локальные подписчики».
//! Межнодовый fan-out, история, presence — через [`socket_broker::Broker`].

use dashmap::DashMap;
use socket_protocol::Reply;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

pub type ClientId = String;
pub type Channel = String;

/// Дескриптор живого соединения на этой ноде.
pub struct ConnHandle {
    pub user_id: String,
    /// Писатель в WS/SSE-сокет (отдельная writer-задача читает из rx).
    pub tx: mpsc::Sender<Reply>,
    pub subs: HashSet<Channel>,
}

#[derive(Default)]
pub struct Hub {
    /// Все соединения этой ноды.
    pub connections: DashMap<ClientId, ConnHandle>,
    /// Индекс «канал → локальные подписчики».
    pub channels: DashMap<Channel, HashSet<ClientId>>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Локальный fan-out: разослать Reply всем локальным подписчикам канала.
    pub async fn fan_out(&self, channel: &str, reply: Reply) {
        if let Some(subs) = self.channels.get(channel) {
            for client in subs.iter() {
                if let Some(conn) = self.connections.get(client) {
                    let _ = conn.tx.try_send(reply.clone());
                    // TODO(impl): при переполнении буфера — дисконнект (write_buffer_limit).
                }
            }
        }
    }

    pub fn add_local_sub(&self, channel: &str, client: &str) -> bool {
        let mut set = self.channels.entry(channel.to_string()).or_default();
        let first = set.is_empty();
        set.insert(client.to_string());
        first // true ⇒ первый подписчик ⇒ нода должна SUBSCRIBE в брокере
    }

    pub fn remove_local_sub(&self, channel: &str, client: &str) -> bool {
        if let Some(mut set) = self.channels.get_mut(channel) {
            set.remove(client);
            if set.is_empty() {
                drop(set);
                self.channels.remove(channel);
                return true; // последний ушёл ⇒ UNSUBSCRIBE в брокере
            }
        }
        false
    }
}
