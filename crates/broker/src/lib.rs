//! Broker abstraction: source of truth for offset/epoch, history (recovery), presence, and
//! cross-node fan-out. Two implementations:
//!
//! - [`MemoryBroker`] — single node, everything in memory (stage 1/2).
//! - [`RedisBroker`] — multi-node: Lua publish, pub/sub fan-out, presence in Redis (stage 3).
//!
//! Delivery to local subscribers is inverted via [`Delivery`] — the broker doesn't know about the hub,
//! it calls a callback (core implements it on top of the hub).

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

/// Recovery result.
pub struct Recovered {
    pub recovered: bool,
    pub publications: Vec<Publication>,
    pub position: StreamPosition,
}

/// Addressed commands between nodes (Server API → the node owning the connection).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    Disconnect { user: String, client: String, code: u32, reason: String },
    Unsubscribe { user: String, channel: String },
}

/// Where the broker delivers incoming messages (including from other nodes) — to local subscribers.
/// Implemented in core on top of the hub.
pub trait Delivery: Send + Sync {
    fn deliver(&self, channel: &str, reply: Reply);
    /// Control command from any node: execute if the connection is local.
    fn control(&self, cmd: ControlCommand);
}

#[async_trait]
pub trait Broker: Send + Sync {
    /// Create the channel stream (epoch) if needed and return the current position.
    async fn ensure_epoch(&self, channel: &str) -> Result<StreamPosition>;

    /// Publish data: assign an offset (if recoverable && !transient), write history,
    /// broadcast to all nodes. Returns the position.
    async fn publish(
        &self,
        channel: &str,
        data: Vec<u8>,
        info: Option<ClientInfo>,
        transient: bool,
        history_size: usize,
    ) -> Result<StreamPosition>;

    /// Broadcast a ready Push (join/leave/unsubscribe) to all nodes — without offset/history.
    async fn broadcast(&self, channel: &str, reply: Reply) -> Result<()>;

    async fn recover(&self, channel: &str, since: &StreamPosition, limit: usize) -> Result<Recovered>;

    async fn presence_add(&self, channel: &str, client: &str, info: ClientInfo, ttl_secs: u64) -> Result<()>;
    async fn presence_remove(&self, channel: &str, client: &str) -> Result<()>;
    async fn presence_list(&self, channel: &str) -> Result<HashMap<String, ClientInfo>>;

    /// Server API idempotency: return the cached position for a key (if any).
    async fn idempotency_get(&self, key: &str) -> Result<Option<StreamPosition>>;
    async fn idempotency_put(&self, key: &str, pos: &StreamPosition, ttl_secs: u64) -> Result<()>;

    /// Broadcast a control command to all nodes (the node owning the connection executes it).
    async fn control_publish(&self, cmd: &ControlCommand) -> Result<()>;
}

/// Build a Push Reply carrying a publication (`id = 0`).
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
