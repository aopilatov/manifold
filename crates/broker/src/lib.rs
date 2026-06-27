//! Абстракция брокера: межнодовый fan-out, история (recovery-окно), presence, control-канал.
//!
//! В MVP — единственная реализация на Redis (`RedisBroker`). Trait скрывает Redis, чтобы позже
//! подключить NATS / sharded Redis pub/sub без правок hub.

use async_trait::async_trait;
use socket_protocol::{ClientInfo, Publication, StreamPosition};
use std::collections::HashMap;

pub mod redis_broker;
pub use redis_broker::RedisBroker;

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("redis error: {0}")]
    Redis(String),
    #[error("recovery unavailable: epoch mismatch or out of window")]
    Unrecoverable,
}

pub type Result<T> = std::result::Result<T, BrokerError>;

/// Опции публикации (см. namespace-конфиг: recoverable ли канал).
pub struct PublishOpts {
    pub recoverable: bool,
    pub transient: bool,
    pub history_size: usize,
    pub history_ttl_secs: u64,
}

/// Результат recovery при подписке.
pub struct Recovered {
    pub position: StreamPosition,
    pub recovered: bool,
    pub publications: Vec<Publication>,
}

#[async_trait]
pub trait Broker: Send + Sync {
    /// Опубликовать в канал. Recoverable + !transient → INCR offset + XADD history.
    /// Возвращает позицию (offset/epoch) публикации.
    async fn publish(&self, channel: &str, pub_: Publication, opts: &PublishOpts)
        -> Result<StreamPosition>;

    /// Подписать ноду на live-поток канала (ленивая подписка: первый локальный подписчик).
    async fn subscribe(&self, channel: &str) -> Result<()>;
    async fn unsubscribe(&self, channel: &str) -> Result<()>;

    /// Recovery: вернуть пропущенное с позиции (offset > since.offset, тот же epoch).
    async fn recover(&self, channel: &str, since: &StreamPosition, limit: usize)
        -> Result<Recovered>;

    /// Presence: запись с TTL (heartbeat).
    async fn presence_add(&self, channel: &str, client: &str, info: ClientInfo, ttl_secs: u64)
        -> Result<()>;
    async fn presence_remove(&self, channel: &str, client: &str) -> Result<()>;
    async fn presence(&self, channel: &str) -> Result<HashMap<String, ClientInfo>>;

    /// Control-канал: адресные команды соединению на любой ноде (unsubscribe/disconnect).
    async fn control_publish(&self, payload: Vec<u8>) -> Result<()>;

    /// Идемпотентность Server API: вернуть кэш по ключу (если был), либо None.
    async fn idempotency_get(&self, key: &str) -> Result<Option<StreamPosition>>;
    async fn idempotency_put(&self, key: &str, pos: &StreamPosition, ttl_secs: u64) -> Result<()>;
}
