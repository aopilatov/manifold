//! In-memory broker (single node). Keeps offset/epoch/history/presence in memory, fan-out —
//! immediately via [`Delivery`].

use crate::{new_epoch, pub_push, Broker, ControlCommand, Delivery, Recovered, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use manifold_protocol::{ClientInfo, Publication, Reply, StreamPosition};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

struct State {
    offset: u64,
    epoch: String,
    history: VecDeque<Publication>,
    presence: HashMap<String, ClientInfo>,
}

impl State {
    fn new() -> Self {
        Self { offset: 0, epoch: new_epoch(), history: VecDeque::new(), presence: HashMap::new() }
    }
}

pub struct MemoryBroker {
    delivery: Arc<dyn Delivery>,
    states: DashMap<String, State>,
    idempotency: DashMap<String, StreamPosition>,
}

impl MemoryBroker {
    pub fn new(delivery: Arc<dyn Delivery>) -> Arc<Self> {
        Arc::new(Self { delivery, states: DashMap::new(), idempotency: DashMap::new() })
    }
}

#[async_trait]
impl Broker for MemoryBroker {
    async fn ensure_epoch(&self, channel: &str) -> Result<StreamPosition> {
        let st = self.states.entry(channel.to_string()).or_insert_with(State::new);
        Ok(StreamPosition { offset: st.offset, epoch: st.epoch.clone() })
    }

    async fn publish(
        &self,
        channel: &str,
        data: Vec<u8>,
        info: Option<ClientInfo>,
        transient: bool,
        history_size: usize,
    ) -> Result<StreamPosition> {
        let recoverable = history_size > 0 && !transient;
        let (publication, epoch) = {
            let mut st = self.states.entry(channel.to_string()).or_insert_with(State::new);
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
            (publication, st.epoch.clone())
        };
        let pos = StreamPosition { offset: publication.offset, epoch };
        self.delivery.deliver(channel, pub_push(channel, publication));
        Ok(pos)
    }

    async fn broadcast(&self, channel: &str, reply: Reply) -> Result<()> {
        self.delivery.deliver(channel, reply);
        Ok(())
    }

    async fn recover(&self, channel: &str, since: &StreamPosition, limit: usize) -> Result<Recovered> {
        if let Some(st) = self.states.get(channel) {
            let position = StreamPosition { offset: st.offset, epoch: st.epoch.clone() };
            if !since.epoch.is_empty() && since.epoch != st.epoch {
                return Ok(Recovered { recovered: false, publications: vec![], position });
            }
            let publications: Vec<Publication> = st
                .history
                .iter()
                .filter(|p| p.offset > since.offset)
                .take(limit)
                .cloned()
                .collect();
            let recovered = match st.history.front() {
                Some(f) => f.offset <= since.offset + 1,
                None => st.offset == since.offset,
            };
            Ok(Recovered { recovered, publications, position })
        } else {
            Ok(Recovered { recovered: true, publications: vec![], position: StreamPosition::default() })
        }
    }

    async fn presence_add(&self, channel: &str, client: &str, info: ClientInfo, _ttl: u64) -> Result<()> {
        let mut st = self.states.entry(channel.to_string()).or_insert_with(State::new);
        st.presence.insert(client.to_string(), info);
        Ok(())
    }

    async fn presence_remove(&self, channel: &str, client: &str) -> Result<()> {
        if let Some(mut st) = self.states.get_mut(channel) {
            st.presence.remove(client);
        }
        Ok(())
    }

    async fn presence_list(&self, channel: &str) -> Result<HashMap<String, ClientInfo>> {
        Ok(self.states.get(channel).map(|st| st.presence.clone()).unwrap_or_default())
    }

    async fn idempotency_get(&self, key: &str) -> Result<Option<StreamPosition>> {
        Ok(self.idempotency.get(key).map(|v| v.clone()))
    }

    async fn idempotency_put(&self, key: &str, pos: &StreamPosition, _ttl: u64) -> Result<()> {
        self.idempotency.insert(key.to_string(), pos.clone());
        Ok(())
    }

    async fn control_publish(&self, cmd: &ControlCommand) -> Result<()> {
        // single node — execute immediately
        self.delivery.control(cmd.clone());
        Ok(())
    }
}
