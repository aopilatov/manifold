//! Единый `ApiService`: за ним стоят и клиентские WS-команды, и HTTP/gRPC Server API.
//! Идемпотентность, путь публикации в брокер, control-канал — здесь, а не в транспортах.

use socket_broker::Broker;
use socket_protocol::StreamPosition;
use std::sync::Arc;

use crate::config::Config;
use crate::hub::Hub;

#[derive(Clone)]
pub struct ApiService {
    pub cfg: Arc<Config>,
    pub hub: Arc<Hub>,
    pub broker: Arc<dyn Broker>,
}

impl ApiService {
    pub fn new(cfg: Arc<Config>, hub: Arc<Hub>, broker: Arc<dyn Broker>) -> Self {
        Self { cfg, hub, broker }
    }

    /// Публикация в канал (единый путь и для клиента, и для Server API).
    /// TODO(impl): идемпотентность, namespace-политика, transient, presence-info.
    pub async fn publish(
        &self,
        _channel: &str,
        _data: Vec<u8>,
        _idempotency_key: Option<&str>,
    ) -> anyhow::Result<StreamPosition> {
        anyhow::bail!("publish not implemented")
    }

    // TODO(impl): broadcast, presence, history, subscribe/unsubscribe, disconnect,
    //             user_online, channels, info, batch.
}
