//! Единый оркестратор клиентских команд (этап 1, одна нода). За ним позже встанут и
//! HTTP/gRPC Server API. Идемпотентность, control-канал, мультинода — этапы 3+.

use socket_protocol::{
    command, reply, Command, ConnectRequest, ConnectResult, Error, HistoryRequest, HistoryResult,
    Join, Leave, Reply, SubscribeRequest, SubscribeResult, PongResult, PresenceResult,
    PublishRequest, PublishResult, StreamPosition, UnsubscribeResult, ClientInfo,
};
use socket_protocol::push;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::auth::{self, Claims};
use crate::config::Config;
use crate::hub::{push_reply, ConnHandle, Hub};
use crate::namespace::{self, Action, Decision};

const RECOVER_LIMIT: usize = 100;

#[derive(Clone)]
pub struct ApiService {
    pub cfg: Arc<Config>,
    pub hub: Arc<Hub>,
    node: String,
    counter: Arc<AtomicU64>,
}

impl ApiService {
    pub fn new(cfg: Arc<Config>, hub: Arc<Hub>) -> Self {
        let node = cfg.server.node_name.clone();
        Self { cfg, hub, node, counter: Arc::new(AtomicU64::new(1)) }
    }

    /// Проверка connection JWT. Ошибка → протокольный `Error`.
    pub fn authenticate(&self, token: &str) -> Result<Claims, Error> {
        auth::validate_jwt(token, &self.cfg.auth.jwt)
            .map_err(|e| err(101, &e.to_string(), false))
    }

    /// Зарегистрировать соединение в hub, вернуть выданный client id.
    pub fn register(&self, claims: Option<Claims>, tx: mpsc::Sender<Reply>) -> String {
        let id = format!("{}-{}", self.node, self.counter.fetch_add(1, Ordering::Relaxed));
        let user = claims.as_ref().map(|c| c.sub.clone()).unwrap_or_default();
        self.hub.connections.insert(
            id.clone(),
            ConnHandle { user_id: user, claims, tx, subs: HashSet::new() },
        );
        id
    }

    /// Обработать ConnectRequest: восстановить подписки из `subs` за 1 RTT (реконнект).
    pub fn connect(&self, client_id: &str, req: &ConnectRequest) -> ConnectResult {
        let mut subs = HashMap::new();
        for (channel, sreq) in &req.subs {
            let mut sreq = sreq.clone();
            sreq.channel = channel.clone();
            if let Ok(res) = self.subscribe(client_id, &sreq) {
                subs.insert(channel.clone(), res);
            }
        }
        ConnectResult {
            client: client_id.to_string(),
            ping_interval_ms: self.cfg.server.ws.ping_interval.as_millis() as u32,
            expires_in_s: 0,
            data: vec![],
            subs,
            session: String::new(),
        }
    }

    pub fn subscribe(&self, client_id: &str, req: &SubscribeRequest) -> Result<SubscribeResult, Error> {
        let channel = &req.channel;
        let claims = self.claims_of(client_id);
        self.authorize(channel, Action::Subscribe, claims.as_ref())?;

        let ns = self.cfg.namespace(channel);
        let recoverable = ns.history_size > 0;
        self.hub.ensure_state(channel);
        self.hub.add_sub(channel, client_id);
        if let Some(mut conn) = self.hub.connections.get_mut(client_id) {
            conn.subs.insert(channel.clone());
        }

        let info = self.client_info(client_id, claims.as_ref());
        if ns.presence {
            self.hub.presence_add(channel, client_id, info.clone());
        }
        if ns.join_leave {
            self.hub
                .fan_out(channel, push_reply(channel, push::Event::Join(Join { info: Some(info) })));
        }

        let (recovered, publications, position) = if req.recover && recoverable {
            let since = req.position.clone().unwrap_or_default();
            self.hub.recover(channel, &since, RECOVER_LIMIT)
        } else {
            (false, vec![], self.hub.ensure_state(channel))
        };

        Ok(SubscribeResult {
            recoverable,
            position: Some(position),
            recovered,
            publications,
            positioned: recoverable,
        })
    }

    pub fn unsubscribe(&self, client_id: &str, channel: &str) -> UnsubscribeResult {
        self.hub.remove_sub(channel, client_id);
        if let Some(mut conn) = self.hub.connections.get_mut(client_id) {
            conn.subs.remove(channel);
        }
        let ns = self.cfg.namespace(channel);
        if ns.presence {
            let removed = self.hub.presence_remove(channel, client_id);
            if ns.join_leave {
                if let Some(info) = removed {
                    self.hub.fan_out(
                        channel,
                        push_reply(channel, push::Event::Leave(Leave { info: Some(info) })),
                    );
                }
            }
        }
        UnsubscribeResult {}
    }

    pub fn publish(&self, client_id: &str, req: &PublishRequest) -> Result<PublishResult, Error> {
        let channel = &req.channel;
        let claims = self.claims_of(client_id);
        self.authorize(channel, Action::Publish, claims.as_ref())?;

        let ns = self.cfg.namespace(channel);
        let info = Some(self.client_info(client_id, claims.as_ref()));
        let pos = self.hub.publish(channel, req.data.clone(), info, req.transient, ns.history_size);
        Ok(PublishResult { position: Some(pos) })
    }

    pub fn presence(&self, client_id: &str, channel: &str) -> Result<PresenceResult, Error> {
        let claims = self.claims_of(client_id);
        self.authorize(channel, Action::Presence, claims.as_ref())?;
        Ok(PresenceResult { presence: self.hub.presence_list(channel) })
    }

    pub fn history(&self, client_id: &str, req: &HistoryRequest) -> Result<HistoryResult, Error> {
        let claims = self.claims_of(client_id);
        self.authorize(&req.channel, Action::History, claims.as_ref())?;
        let limit = if req.limit > 0 { req.limit as usize } else { RECOVER_LIMIT };
        let (_rec, pubs, pos) = self.hub.recover(&req.channel, &StreamPosition::default(), limit);
        Ok(HistoryResult { publications: pubs, position: Some(pos) })
    }

    /// Диспетчер: Command → Reply (с тем же id). None ⇒ команда без ответа.
    pub fn handle_command(&self, client_id: &str, cmd: Command) -> Option<Reply> {
        use command::Method as M;
        use reply::Payload as P;
        let id = cmd.id;
        let method = cmd.method?;

        let result: Result<P, Error> = match method {
            M::Subscribe(req) => self.subscribe(client_id, &req).map(P::Subscribe),
            M::Unsubscribe(req) => Ok(P::Unsubscribe(self.unsubscribe(client_id, &req.channel))),
            M::Publish(req) => self.publish(client_id, &req).map(P::Publish),
            M::Presence(req) => self.presence(client_id, &req.channel).map(P::Presence),
            M::History(req) => self.history(client_id, &req).map(P::History),
            M::Ping(_) => Ok(P::Pong(PongResult {})),
            M::Connect(_) => Err(err(108, "already_connected", false)),
            // TODO(stage): RefreshRequest / SubRefreshRequest (вариант B).
            M::Refresh(_) | M::SubRefresh(_) => Err(err(109, "refresh_not_implemented", false)),
        };

        Some(match result {
            Ok(payload) => Reply { id, error: None, payload: Some(payload) },
            Err(error) => Reply { id, error: Some(error), payload: None },
        })
    }

    /// Очистка при разрыве: снять подписки, presence-leave, убрать соединение.
    pub fn cleanup(&self, client_id: &str) {
        if let Some((_, conn)) = self.hub.connections.remove(client_id) {
            for channel in conn.subs {
                self.hub.remove_sub(&channel, client_id);
                let ns = self.cfg.namespace(&channel);
                if ns.presence {
                    let removed = self.hub.presence_remove(&channel, client_id);
                    if ns.join_leave {
                        if let Some(info) = removed {
                            self.hub.fan_out(
                                &channel,
                                push_reply(&channel, push::Event::Leave(Leave { info: Some(info) })),
                            );
                        }
                    }
                }
            }
        }
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
