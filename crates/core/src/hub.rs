//! Hub: in-memory маршрутизация на ноде. Только соединения и индекс «канал → локальные
//! подписчики». Состояние каналов (offset/epoch/история/presence) — в [`socket_broker::Broker`].

use crate::auth::Claims;
use dashmap::DashMap;
use socket_protocol::{push, reply, ClientInfo, Join, Leave, Push, Reply};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

pub type ClientId = String;
pub type Channel = String;

/// Дескриптор живого соединения на этой ноде.
pub struct ConnHandle {
    pub user_id: String,
    pub claims: Option<Claims>,
    pub tx: mpsc::Sender<Reply>,
    pub subs: HashSet<Channel>,
}

#[derive(Default)]
pub struct Hub {
    pub connections: DashMap<ClientId, ConnHandle>,
    channels: DashMap<Channel, HashSet<ClientId>>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// true ⇒ первый локальный подписчик (в мультиноде — повод для ленивого SUBSCRIBE, TODO).
    pub fn add_sub(&self, channel: &str, client: &str) -> bool {
        let mut set = self.channels.entry(channel.to_string()).or_default();
        let first = set.is_empty();
        set.insert(client.to_string());
        first
    }

    /// true ⇒ ушёл последний локальный подписчик.
    pub fn remove_sub(&self, channel: &str, client: &str) -> bool {
        if let Some(mut set) = self.channels.get_mut(channel) {
            set.remove(client);
            if set.is_empty() {
                drop(set);
                self.channels.remove(channel);
                return true;
            }
        }
        false
    }

    /// Локальный fan-out: разослать Reply всем локальным подписчикам канала.
    pub fn fan_out(&self, channel: &str, reply: Reply) {
        let targets: Vec<ClientId> = match self.channels.get(channel) {
            Some(set) => set.iter().cloned().collect(),
            None => return,
        };
        for cid in targets {
            if let Some(conn) = self.connections.get(&cid) {
                let _ = conn.tx.try_send(reply.clone()); // переполнение → дисконнект (TODO)
            }
        }
    }

    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }
}

fn push_reply(channel: &str, event: push::Event) -> Reply {
    Reply {
        id: 0,
        error: None,
        payload: Some(reply::Payload::Push(Push {
            channel: channel.to_string(),
            event: Some(event),
        })),
    }
}

pub fn join_push(channel: &str, info: ClientInfo) -> Reply {
    push_reply(channel, push::Event::Join(Join { info: Some(info) }))
}

pub fn leave_push(channel: &str, info: ClientInfo) -> Reply {
    push_reply(channel, push::Event::Leave(Leave { info: Some(info) }))
}
