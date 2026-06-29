//! Абстракция брокера: источник истины для offset/epoch, истории (recovery), presence и
//! межнодового fan-out. Две реализации:
//!
//! - [`MemoryBroker`] — одна нода, всё в памяти (этап 1/2).
//! - [`RedisBroker`] — мультинода: Lua-публикация, pub/sub fan-out, presence в Redis (этап 3).
//!
//! Доставка локальным подписчикам инвертирована через [`Delivery`] — брокер не знает про hub,
//! а зовёт колбэк (core реализует его поверх hub).

use async_trait::async_trait;
use socket_protocol::{push, reply, ClientInfo, Publication, Push, Reply, StreamPosition};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub mod memory;
pub mod redis_broker;
pub use memory::MemoryBroker;
pub use redis_broker::RedisBroker;

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("decode: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, BrokerError>;

/// Результат recovery.
pub struct Recovered {
    pub recovered: bool,
    pub publications: Vec<Publication>,
    pub position: StreamPosition,
}

/// Куда брокер отдаёт пришедшие (в т.ч. с других нод) сообщения — локальным подписчикам.
/// Реализуется в core поверх hub.
pub trait Delivery: Send + Sync {
    fn deliver(&self, channel: &str, reply: Reply);
}

#[async_trait]
pub trait Broker: Send + Sync {
    /// Создать поток канала (epoch) при необходимости и вернуть текущую позицию.
    async fn ensure_epoch(&self, channel: &str) -> Result<StreamPosition>;

    /// Публикация данных: присвоить offset (если recoverable && !transient), записать историю,
    /// разослать на все ноды. Возвращает позицию.
    async fn publish(
        &self,
        channel: &str,
        data: Vec<u8>,
        info: Option<ClientInfo>,
        transient: bool,
        history_size: usize,
    ) -> Result<StreamPosition>;

    /// Разослать готовый Push (join/leave/unsubscribe) на все ноды — без offset/истории.
    async fn broadcast(&self, channel: &str, reply: Reply) -> Result<()>;

    async fn recover(&self, channel: &str, since: &StreamPosition, limit: usize) -> Result<Recovered>;

    async fn presence_add(&self, channel: &str, client: &str, info: ClientInfo, ttl_secs: u64) -> Result<()>;
    async fn presence_remove(&self, channel: &str, client: &str) -> Result<()>;
    async fn presence_list(&self, channel: &str) -> Result<HashMap<String, ClientInfo>>;
}

/// Построить Push-Reply с публикацией (`id = 0`).
pub fn pub_push(channel: &str, publication: Publication) -> Reply {
    Reply {
        id: 0,
        error: None,
        payload: Some(reply::Payload::Push(Push {
            channel: channel.to_string(),
            event: Some(push::Event::Pub(publication)),
        })),
    }
}

pub(crate) fn new_epoch() -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{n:x}")
}

pub(crate) fn unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
