//! Hub: in-memory routing on a node. Only connections and a "channel → local subscribers" index.
//! Channel state (offset/epoch/history/presence) lives in [`manifold_broker::Broker`].

use crate::auth::Claims;
use dashmap::DashMap;
use manifold_protocol::{push, reply, ClientInfo, Disconnect, Join, Leave, Push, Reply, Unsubscribe};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

pub type ClientId = String;
pub type Channel = String;

/// Descriptor of a live connection on this node.
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

    /// true ⇒ first local subscriber (in multi-node, a reason for a lazy SUBSCRIBE, TODO).
    pub fn add_sub(&self, channel: &str, client: &str) -> bool {
        let mut set = self.channels.entry(channel.to_string()).or_default();
        let first = set.is_empty();
        set.insert(client.to_string());
        first
    }

    /// true ⇒ the last local subscriber left.
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

    /// Local fan-out: send Reply to all local subscribers of the channel.
    pub fn fan_out(&self, channel: &str, reply: Reply) {
        let targets: Vec<ClientId> = match self.channels.get(channel) {
            Some(set) => set.iter().cloned().collect(),
            None => return,
        };
        for cid in targets {
            if let Some(conn) = self.connections.get(&cid) {
                let _ = conn.tx.try_send(reply.clone()); // overflow → disconnect (TODO)
            }
        }
    }

    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }

    pub fn num_connections(&self) -> usize {
        self.connections.len()
    }

    /// How many local connections the user has.
    pub fn user_connection_count(&self, user: &str) -> usize {
        self.connections.iter().filter(|e| e.user_id == user).count()
    }

    /// List of active channels (locally), optional filter by exact prefix match.
    pub fn channels_list(&self) -> Vec<String> {
        self.channels.iter().map(|e| e.key().clone()).collect()
    }

    /// Forcibly disconnect connections matching user and/or client (control command).
    pub fn disconnect_matching(&self, user: &str, client: &str, code: u32, reason: &str) {
        let ids: Vec<ClientId> = self
            .connections
            .iter()
            .filter(|e| (client.is_empty() || e.key() == client) && (user.is_empty() || e.user_id == user))
            .map(|e| e.key().clone())
            .collect();
        for id in ids {
            if let Some((_, conn)) = self.connections.remove(&id) {
                let _ = conn.tx.try_send(disconnect_push(code, reason));
                for channel in &conn.subs {
                    self.remove_sub(channel, &id);
                }
            }
        }
    }

    /// Forcibly unsubscribe a user's connections from a channel (control command).
    pub fn unsubscribe_matching(&self, user: &str, channel: &str) {
        let ids: Vec<ClientId> = self
            .connections
            .iter()
            .filter(|e| (user.is_empty() || e.user_id == user) && e.subs.contains(channel))
            .map(|e| e.key().clone())
            .collect();
        for id in ids {
            self.remove_sub(channel, &id);
            if let Some(mut conn) = self.connections.get_mut(&id) {
                conn.subs.remove(channel);
                let _ = conn.tx.try_send(unsubscribe_push(channel));
            }
        }
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

pub fn disconnect_push(code: u32, reason: &str) -> Reply {
    push_reply(
        "",
        push::Event::Disconnect(Disconnect { code, reason: reason.to_string(), reconnect: false }),
    )
}

pub fn unsubscribe_push(channel: &str) -> Reply {
    push_reply(channel, push::Event::Unsubscribe(Unsubscribe { code: 0, reason: String::new() }))
}
