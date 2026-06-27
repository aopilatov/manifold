//! Redis-реализация брокера (скелет).
//!
//! TODO(impl): Lua-скрипт публикации (INCR seq + XADD hist + PUBLISH), ленивая SUBSCRIBE/UNSUBSCRIBE,
//! XRANGE для recovery, presence-хэш с TTL, control pub/sub, idempotency-кэш.

use crate::{Broker, BrokerError, PublishOpts, Recovered, Result};
use async_trait::async_trait;
use socket_protocol::{ClientInfo, Publication, StreamPosition};
use std::collections::HashMap;

#[derive(Clone)]
pub struct RedisBroker {
    prefix: String,
    // conn: redis::aio::ConnectionManager,  // TODO(impl)
}

impl RedisBroker {
    pub async fn connect(_url: &str, prefix: impl Into<String>) -> Result<Self> {
        // TODO(impl): подключение, загрузка Lua-скриптов (SCRIPT LOAD).
        Ok(Self { prefix: prefix.into() })
    }

    fn key(&self, kind: &str, channel: &str) -> String {
        format!("{}:{}:{}", self.prefix, kind, channel)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn publish(&self, _channel: &str, _pub_: Publication, _opts: &PublishOpts)
        -> Result<StreamPosition> {
        let _ = self.key("seq", _channel);
        Err(BrokerError::Redis("publish not implemented".into())) // TODO(impl)
    }

    async fn subscribe(&self, _channel: &str) -> Result<()> { Ok(()) }      // TODO(impl)
    async fn unsubscribe(&self, _channel: &str) -> Result<()> { Ok(()) }    // TODO(impl)

    async fn recover(&self, _channel: &str, _since: &StreamPosition, _limit: usize)
        -> Result<Recovered> {
        Err(BrokerError::Unrecoverable) // TODO(impl)
    }

    async fn presence_add(&self, _channel: &str, _client: &str, _info: ClientInfo, _ttl: u64)
        -> Result<()> { Ok(()) }                                            // TODO(impl)
    async fn presence_remove(&self, _channel: &str, _client: &str) -> Result<()> { Ok(()) }
    async fn presence(&self, _channel: &str) -> Result<HashMap<String, ClientInfo>> {
        Ok(HashMap::new())                                                  // TODO(impl)
    }

    async fn control_publish(&self, _payload: Vec<u8>) -> Result<()> { Ok(()) } // TODO(impl)

    async fn idempotency_get(&self, _key: &str) -> Result<Option<StreamPosition>> { Ok(None) }
    async fn idempotency_put(&self, _key: &str, _pos: &StreamPosition, _ttl: u64) -> Result<()> {
        Ok(())
    }
}
