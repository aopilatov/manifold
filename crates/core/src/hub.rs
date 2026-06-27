//! Hub: in-memory маршрутизация и состояние каналов на ноде (этап 1, без Redis).
//!
//! Хранит локальные соединения, подписчиков, а также — для одной ноды — offset/epoch,
//! историю (recovery-окно) и presence в памяти. В мультиноде (этап 3) история/presence
//! уезжают в [`socket_broker::Broker`], а здесь остаётся только маршрутизация.

use crate::auth::Claims;
use dashmap::DashMap;
use socket_protocol::{ClientInfo, Publication, StreamPosition};
use socket_protocol::{Push, Reply};
use socket_protocol::{push, reply};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

pub type ClientId = String;
pub type Channel = String;

/// Дескриптор живого соединения на этой ноде.
pub struct ConnHandle {
    pub user_id: String,
    pub claims: Option<Claims>,
    /// Писатель в WS/SSE-сокет (writer-задача читает из соответствующего rx).
    pub tx: mpsc::Sender<Reply>,
    pub subs: HashSet<Channel>,
}

struct ChannelState {
    offset: u64,
    epoch: String,
    history: VecDeque<Publication>,
    presence: HashMap<ClientId, ClientInfo>,
    subscribers: HashSet<ClientId>,
}

impl ChannelState {
    fn new() -> Self {
        Self {
            offset: 0,
            epoch: new_epoch(),
            history: VecDeque::new(),
            presence: HashMap::new(),
            subscribers: HashSet::new(),
        }
    }
}

#[derive(Default)]
pub struct Hub {
    pub connections: DashMap<ClientId, ConnHandle>,
    states: DashMap<Channel, ChannelState>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Создать состояние канала (если ещё нет) и вернуть текущую позицию.
    pub fn ensure_state(&self, channel: &str) -> StreamPosition {
        let st = self.states.entry(channel.to_string()).or_insert_with(ChannelState::new);
        StreamPosition { offset: st.offset, epoch: st.epoch.clone() }
    }

    /// Добавить локального подписчика. true ⇒ первый (в мультиноде → SUBSCRIBE в брокере).
    pub fn add_sub(&self, channel: &str, client: &str) -> bool {
        let mut st = self.states.entry(channel.to_string()).or_insert_with(ChannelState::new);
        let first = st.subscribers.is_empty();
        st.subscribers.insert(client.to_string());
        first
    }

    /// Убрать подписчика. true ⇒ последний (в мультиноде → UNSUBSCRIBE в брокере).
    /// Состояние канала НЕ удаляем — история живёт для recovery.
    pub fn remove_sub(&self, channel: &str, client: &str) -> bool {
        if let Some(mut st) = self.states.get_mut(channel) {
            st.subscribers.remove(client);
            return st.subscribers.is_empty();
        }
        false
    }

    /// Локальный fan-out: разослать Reply всем локальным подписчикам канала.
    pub fn fan_out(&self, channel: &str, reply: Reply) {
        let targets: Vec<ClientId> = match self.states.get(channel) {
            Some(st) => st.subscribers.iter().cloned().collect(),
            None => return,
        };
        for cid in targets {
            if let Some(conn) = self.connections.get(&cid) {
                // try_send: переполнение буфера ⇒ дисконнект медленного потребителя (TODO).
                let _ = conn.tx.try_send(reply.clone());
            }
        }
    }

    /// Публикация: присвоить offset (если recoverable и !transient), записать историю, fan-out.
    pub fn publish(
        &self,
        channel: &str,
        data: Vec<u8>,
        info: Option<ClientInfo>,
        transient: bool,
        history_size: usize,
    ) -> StreamPosition {
        let recoverable = history_size > 0 && !transient;

        let (publication, epoch, targets) = {
            let mut st = self.states.entry(channel.to_string()).or_insert_with(ChannelState::new);
            let offset = if recoverable {
                st.offset += 1;
                st.offset
            } else {
                0
            };
            let publication = Publication { data, offset, info, tags: HashMap::new() };
            if recoverable {
                st.history.push_back(publication.clone());
                while st.history.len() > history_size {
                    st.history.pop_front();
                }
            }
            let targets: Vec<ClientId> = st.subscribers.iter().cloned().collect();
            (publication, st.epoch.clone(), targets)
        }; // guard на states здесь снят — fan-out ниже не словит дедлок

        let position = StreamPosition { offset: publication.offset, epoch };
        let reply = push_reply(channel, push::Event::Pub(publication));
        for cid in targets {
            if let Some(conn) = self.connections.get(&cid) {
                let _ = conn.tx.try_send(reply.clone());
            }
        }
        position
    }

    /// Recovery: вернуть пропущенное с позиции `since` (тот же epoch, offset > since.offset).
    pub fn recover(
        &self,
        channel: &str,
        since: &StreamPosition,
        limit: usize,
    ) -> (bool, Vec<Publication>, StreamPosition) {
        if let Some(st) = self.states.get(channel) {
            let pos = StreamPosition { offset: st.offset, epoch: st.epoch.clone() };
            if !since.epoch.is_empty() && since.epoch != st.epoch {
                return (false, vec![], pos); // epoch сменился ⇒ восстановить нельзя
            }
            let missed: Vec<Publication> = st
                .history
                .iter()
                .filter(|p| p.offset > since.offset)
                .take(limit)
                .cloned()
                .collect();
            // recovered, если нет дыры: самый старый в истории ≤ since.offset+1.
            let recovered = match st.history.front() {
                Some(first) => first.offset <= since.offset + 1,
                None => st.offset == since.offset,
            };
            (recovered, missed, pos)
        } else {
            (true, vec![], StreamPosition::default())
        }
    }

    pub fn presence_add(&self, channel: &str, client: &str, info: ClientInfo) {
        let mut st = self.states.entry(channel.to_string()).or_insert_with(ChannelState::new);
        st.presence.insert(client.to_string(), info);
    }

    pub fn presence_remove(&self, channel: &str, client: &str) -> Option<ClientInfo> {
        self.states.get_mut(channel).and_then(|mut st| st.presence.remove(client))
    }

    pub fn presence_list(&self, channel: &str) -> HashMap<String, ClientInfo> {
        self.states.get(channel).map(|st| st.presence.clone()).unwrap_or_default()
    }

    pub fn num_channels(&self) -> usize {
        self.states.len()
    }
}

/// Построить асинхронный Push-Reply (`id = 0`).
pub fn push_reply(channel: &str, event: push::Event) -> Reply {
    Reply {
        id: 0,
        error: None,
        payload: Some(reply::Payload::Push(Push {
            channel: channel.to_string(),
            event: Some(event),
        })),
    }
}

fn new_epoch() -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{n:x}")
}
