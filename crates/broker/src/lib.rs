//! Абстракция брокера: источник истины для offset/epoch, истории (recovery), presence и
//! межнодового fan-out. Две реализации:
//!
//! - [`MemoryBroker`] — одна нода, всё в памяти (этап 1/2).
//! - [`RedisBroker`] — мультинода: Lua-публикация, pub/sub fan-out, presence в Redis (этап 3).
//!
//! Доставка локальным подписчикам инвертирована через [`Delivery`] — брокер не знает про hub,
//! а зовёт колбэк (core реализует его поверх hub).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
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

/// Адресные команды между нодами (Server API → нода-владелец соединения).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    Disconnect { user: String, client: String, code: u32, reason: String },
    Unsubscribe { user: String, channel: String },
}

/// Куда брокер отдаёт пришедшие (в т.ч. с других нод) сообщения — локальным подписчикам.
/// Реализуется в core поверх hub.
pub trait Delivery: Send + Sync {
    fn deliver(&self, channel: &str, reply: Reply);
    /// Control-команда с любой ноды: выполнить, если соединение локальное.
    fn control(&self, cmd: ControlCommand);
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

    /// Идемпотентность Server API: вернуть закэшированную позицию по ключу (если была).
    async fn idempotency_get(&self, key: &str) -> Result<Option<StreamPosition>>;
    async fn idempotency_put(&self, key: &str, pos: &StreamPosition, ttl_secs: u64) -> Result<()>;

    /// Разослать control-команду всем нодам (нода-владелец соединения её выполнит).
    async fn control_publish(&self, cmd: &ControlCommand) -> Result<()>;
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
