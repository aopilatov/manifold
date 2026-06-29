//! Single orchestrator for client commands. Channel state lives in the broker (single node: Memory,
//! multi-node: Redis). The hub only does local routing.

use manifold_broker::{Broker, BrokerError, ControlCommand};
use manifold_protocol::{
    command, reply, ClientInfo, Command, ConnectRequest, ConnectResult, Error, HistoryRequest,
    HistoryResult, PongResult, PresenceResult, Publication, PublishRequest, PublishResult, Reply,
    StreamPosition, SubscribeRequest, SubscribeResult, UnsubscribeResult,
};
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::auth::{self, Claims};
use crate::config::Config;
use crate::events::{EventSink, LifecycleEvent, NoopSink};
use crate::hub::{join_push, leave_push, ConnHandle, Hub};
use crate::metrics::Metrics;
use crate::namespace::{self, Action, Decision};

const RECOVER_LIMIT: usize = 100;
const PRESENCE_TTL_SECS: u64 = 60;
const IDEMPOTENCY_TTL_SECS: u64 = 300;

/// Node summary for the Server API `info`.
pub struct NodeStats {
    pub node: String,
    pub num_connections: usize,
    pub num_channels: usize,
}

#[derive(Clone)]
pub struct ApiService {
    pub cfg: Arc<Config>,
    pub hub: Arc<Hub>,
    pub broker: Arc<dyn Broker>,
    pub metrics: Arc<Metrics>,
    events: Arc<dyn EventSink>,
    node: String,
    counter: Arc<AtomicU64>,
}

impl ApiService {
    pub fn new(cfg: Arc<Config>, hub: Arc<Hub>, broker: Arc<dyn Broker>) -> Self {
        let node = cfg.server.node_name.clone();
        Self {
            cfg,
            hub,
            broker,
            metrics: Arc::new(Metrics::default()),
            events: Arc::new(NoopSink),
            node,
            counter: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Attach a lifecycle event sink (defaults to Noop).
    pub fn set_event_sink(&mut self, sink: Arc<dyn EventSink>) {
        self.events = sink;
    }

    fn emit_event(&self, kind: &str, user: &str, client: &str, channel: Option<String>) {
        self.events.emit(LifecycleEvent {
            kind: kind.to_string(),
            node: self.node.clone(),
            user: user.to_string(),
            client: client.to_string(),
            channel,
        });
    }

    /// Convenience constructor for a single node (MemoryBroker).
    pub fn in_memory(cfg: Arc<Config>) -> Self {
        let hub = Hub::new();
        let delivery = crate::delivery::HubDelivery::new(hub.clone());
        let broker = manifold_broker::MemoryBroker::new(delivery);
        Self::new(cfg, hub, broker)
    }

    pub fn authenticate(&self, token: &str) -> Result<Claims, Error> {
        auth::validate_jwt(token, &self.cfg.auth.jwt).map_err(|e| err(101, &e.to_string(), false))
    }

    pub fn register(&self, claims: Option<Claims>, tx: mpsc::Sender<Reply>) -> String {
        let id = format!("{}-{}", self.node, self.counter.fetch_add(1, Ordering::Relaxed));
        let user = claims.as_ref().map(|c| c.sub.clone()).unwrap_or_default();
        self.hub.connections.insert(
            id.clone(),
            ConnHandle { user_id: user, claims, tx, subs: HashSet::new() },
        );
        Metrics::inc(&self.metrics.connections_opened);
        id
    }

    pub async fn connect(&self, client_id: &str, req: &ConnectRequest) -> ConnectResult {
        let mut subs = HashMap::new();
        for (channel, sreq) in &req.subs {
            let mut sreq = sreq.clone();
            sreq.channel = channel.clone();
            if let Ok(res) = self.subscribe(client_id, &sreq).await {
                subs.insert(channel.clone(), res);
            }
        }
        let user = self.hub.connections.get(client_id).map(|c| c.user_id.clone()).unwrap_or_default();
        self.emit_event("connected", &user, client_id, None);
        ConnectResult {
            client: client_id.to_string(),
            ping_interval_ms: self.cfg.server.ws.ping_interval.as_millis() as u32,
            expires_in_s: 0,
            data: vec![],
            subs,
            session: String::new(),
        }
    }

    pub async fn subscribe(&self, client_id: &str, req: &SubscribeRequest) -> Result<SubscribeResult, Error> {
        let channel = &req.channel;
        let claims = self.claims_of(client_id);
        self.authorize(channel, Action::Subscribe, claims.as_ref())?;

        let ns = self.cfg.namespace(channel);
        let recoverable = ns.history_size > 0;
        self.broker.ensure_epoch(channel).await.map_err(broker_err)?;
        self.hub.add_sub(channel, client_id);
        if let Some(mut conn) = self.hub.connections.get_mut(client_id) {
            conn.subs.insert(channel.clone());
        }

        let info = self.client_info(client_id, claims.as_ref());
        if ns.presence {
            self.broker
                .presence_add(channel, client_id, info.clone(), PRESENCE_TTL_SECS)
                .await
                .map_err(broker_err)?;
        }
        if ns.join_leave {
            self.broker.broadcast(channel, join_push(channel, info)).await.map_err(broker_err)?;
        }

        let (recovered, publications, position) = if req.recover && recoverable {
            let since = req.position.clone().unwrap_or_default();
            let r = self.broker.recover(channel, &since, RECOVER_LIMIT).await.map_err(broker_err)?;
            (r.recovered, r.publications, r.position)
        } else {
            let pos = self.broker.ensure_epoch(channel).await.map_err(broker_err)?;
            (false, vec![], pos)
        };

        Metrics::inc(&self.metrics.subscriptions);
        let user = claims.as_ref().map(|c| c.sub.clone()).unwrap_or_default();
        self.emit_event("subscribed", &user, client_id, Some(channel.clone()));

        Ok(SubscribeResult {
            recoverable,
            position: Some(position),
            recovered,
            publications,
            positioned: recoverable,
        })
    }

    pub async fn unsubscribe(&self, client_id: &str, channel: &str) -> UnsubscribeResult {
        self.hub.remove_sub(channel, client_id);
        if let Some(mut conn) = self.hub.connections.get_mut(client_id) {
            conn.subs.remove(channel);
        }
        let ns = self.cfg.namespace(channel);
        if ns.presence {
            let _ = self.broker.presence_remove(channel, client_id).await;
            if ns.join_leave {
                let info = self.client_info(client_id, self.claims_of(client_id).as_ref());
                let _ = self.broker.broadcast(channel, leave_push(channel, info)).await;
            }
        }
        Metrics::inc(&self.metrics.unsubscriptions);
        let user = self.claims_of(client_id).map(|c| c.sub).unwrap_or_default();
        self.emit_event("unsubscribed", &user, client_id, Some(channel.to_string()));
        UnsubscribeResult {}
    }

    pub async fn publish(&self, client_id: &str, req: &PublishRequest) -> Result<PublishResult, Error> {
        let channel = &req.channel;
        let claims = self.claims_of(client_id);
        self.authorize(channel, Action::Publish, claims.as_ref())?;

        let ns = self.cfg.namespace(channel);
        let info = Some(self.client_info(client_id, claims.as_ref()));
        let pos = self
            .broker
            .publish(channel, req.data.clone(), info, req.transient, ns.history_size)
            .await
            .map_err(broker_err)?;
        Metrics::inc(&self.metrics.messages_published);
        Ok(PublishResult { position: Some(pos) })
    }

    pub async fn presence(&self, client_id: &str, channel: &str) -> Result<PresenceResult, Error> {
        let claims = self.claims_of(client_id);
        self.authorize(channel, Action::Presence, claims.as_ref())?;
        let presence = self.broker.presence_list(channel).await.map_err(broker_err)?;
        Ok(PresenceResult { presence })
    }

    pub async fn history(&self, client_id: &str, req: &HistoryRequest) -> Result<HistoryResult, Error> {
        let claims = self.claims_of(client_id);
        self.authorize(&req.channel, Action::History, claims.as_ref())?;
        let limit = if req.limit > 0 { req.limit as usize } else { RECOVER_LIMIT };
        let r = self
            .broker
            .recover(&req.channel, &StreamPosition::default(), limit)
            .await
            .map_err(broker_err)?;
        Ok(HistoryResult { publications: r.publications, position: Some(r.position) })
    }

    pub async fn handle_command(&self, client_id: &str, cmd: Command) -> Option<Reply> {
        use command::Method as M;
        use reply::Payload as P;
        let id = cmd.id;
        let method = cmd.method?;

        let result: Result<P, Error> = match method {
            M::Subscribe(req) => self.subscribe(client_id, &req).await.map(P::Subscribe),
            M::Unsubscribe(req) => Ok(P::Unsubscribe(self.unsubscribe(client_id, &req.channel).await)),
            M::Publish(req) => self.publish(client_id, &req).await.map(P::Publish),
            M::Presence(req) => self.presence(client_id, &req.channel).await.map(P::Presence),
            M::History(req) => self.history(client_id, &req).await.map(P::History),
            M::Ping(_) => Ok(P::Pong(PongResult {})),
            M::Connect(_) => Err(err(108, "already_connected", false)),
            M::Refresh(_) | M::SubRefresh(_) => Err(err(109, "refresh_not_implemented", false)),
        };

        Some(match result {
            Ok(payload) => Reply { id, error: None, payload: Some(payload) },
            Err(error) => Reply { id, error: Some(error), payload: None },
        })
    }

    pub async fn cleanup(&self, client_id: &str) {
        if let Some((_, conn)) = self.hub.connections.remove(client_id) {
            Metrics::inc(&self.metrics.connections_closed);
            self.emit_event("disconnected", &conn.user_id, client_id, None);
            let info = ClientInfo {
                user: conn.user_id.clone(),
                client: client_id.to_string(),
                conn_info: vec![],
                chan_info: vec![],
            };
            for channel in conn.subs {
                self.hub.remove_sub(&channel, client_id);
                let ns = self.cfg.namespace(&channel);
                if ns.presence {
                    let _ = self.broker.presence_remove(&channel, client_id).await;
                    if ns.join_leave {
                        let _ = self.broker.broadcast(&channel, leave_push(&channel, info.clone())).await;
                    }
                }
            }
        }
    }

    // ─────────── Server API (trusted; authorization handled at the HTTP/gRPC layer) ───────────

    /// Publish from the backend. Idempotent when a key is provided.
    pub async fn api_publish(
        &self,
        channel: &str,
        data: Vec<u8>,
        idempotency_key: Option<&str>,
    ) -> Result<StreamPosition, BrokerError> {
        if let Some(k) = idempotency_key {
            if let Some(pos) = self.broker.idempotency_get(k).await? {
                return Ok(pos);
            }
        }
        let ns = self.cfg.namespace(channel);
        let pos = self.broker.publish(channel, data, None, false, ns.history_size).await?;
        Metrics::inc(&self.metrics.messages_published);
        if let Some(k) = idempotency_key {
            self.broker.idempotency_put(k, &pos, IDEMPOTENCY_TTL_SECS).await?;
        }
        Ok(pos)
    }

    /// A single message to several channels.
    pub async fn api_broadcast(&self, channels: &[String], data: Vec<u8>) -> BTreeMap<String, u64> {
        let mut out = BTreeMap::new();
        for ch in channels {
            if let Ok(pos) = self.api_publish(ch, data.clone(), None).await {
                out.insert(ch.clone(), pos.offset);
            }
        }
        out
    }

    pub async fn api_presence(&self, channel: &str) -> Result<std::collections::HashMap<String, ClientInfo>, BrokerError> {
        self.broker.presence_list(channel).await
    }

    pub async fn api_history(&self, channel: &str, limit: usize) -> Result<(Vec<Publication>, StreamPosition), BrokerError> {
        let r = self.broker.recover(channel, &StreamPosition::default(), limit).await?;
        Ok((r.publications, r.position))
    }

    /// Active channels on this node (optional glob filter).
    pub fn api_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let all = self.hub.channels_list();
        match pattern {
            Some(p) if !p.is_empty() => all.into_iter().filter(|c| auth::glob_match(p, c)).collect(),
            _ => all,
        }
    }

    pub fn api_info(&self) -> NodeStats {
        NodeStats {
            node: self.node.clone(),
            num_connections: self.hub.num_connections(),
            num_channels: self.hub.num_channels(),
        }
    }

    /// Forced disconnect (cluster-wide via the control channel).
    pub async fn api_disconnect(&self, user: &str, client: &str, code: u32, reason: &str) {
        let _ = self
            .broker
            .control_publish(&ControlCommand::Disconnect {
                user: user.to_string(),
                client: client.to_string(),
                code,
                reason: reason.to_string(),
            })
            .await;
    }

    pub async fn api_unsubscribe(&self, user: &str, channel: &str) {
        let _ = self
            .broker
            .control_publish(&ControlCommand::Unsubscribe {
                user: user.to_string(),
                channel: channel.to_string(),
            })
            .await;
    }

    /// Whether the user is online (locally on this node). Cluster-wide aggregation is TODO (node registry).
    pub fn api_user_online(&self, user: &str) -> (bool, usize) {
        let n = self.hub.user_connection_count(user);
        (n > 0, n)
    }

    // --- helpers ---

    fn claims_of(&self, client_id: &str) -> Option<Claims> {
        self.hub.connections.get(client_id).and_then(|c| c.claims.clone())
    }

    fn authorize(&self, channel: &str, action: Action, claims: Option<&Claims>) -> Result<(), Error> {
        match namespace::check(&self.cfg, claims, channel, action) {
            Decision::Allow => Ok(()),
            Decision::Deny(reason) => Err(err(103, reason, false)),
        }
    }

    fn client_info(&self, client_id: &str, claims: Option<&Claims>) -> ClientInfo {
        ClientInfo {
            user: claims.map(|c| c.sub.clone()).unwrap_or_default(),
            client: client_id.to_string(),
            conn_info: vec![],
            chan_info: vec![],
        }
    }
}

fn err(code: u32, message: &str, temporary: bool) -> Error {
    Error { code, message: message.to_string(), temporary }
}

fn broker_err(e: manifold_broker::BrokerError) -> Error {
    err(110, &format!("broker: {e}"), true)
}
