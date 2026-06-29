//! Redis-брокер (мультинода).
//!
//! - **publish**: Lua-скрипт атомарно `INCR seq` + `XADD hist` (id = `offset-0`), затем `PUBLISH`
//!   сериализованного Push на `ch:{channel}`.
//! - **fan-out**: фоновая задача держит pub/sub-соединение с `PSUBSCRIBE {prefix}:ch:*` и отдаёт
//!   пришедшее (в т.ч. с других нод) локальным подписчикам через [`Delivery`].
//!   _Замечание:_ PSUBSCRIBE-all проще ленивой per-channel подписки; ленивость — оптимизация (TODO).
//! - **presence**: ZSET `pz:{ch}` (score = expire_at) + HASH `ph:{ch}` (client → ClientInfo).

use crate::{new_epoch, pub_push, unix_secs, Broker, BrokerError, ControlCommand, Delivery, Recovered, Result};
use async_trait::async_trait;
use futures::StreamExt;
use prost::Message as _;
use redis::aio::ConnectionManager;
use redis::streams::StreamRangeReply;
use redis::{AsyncCommands, Script};
use socket_protocol::{ClientInfo, Publication, Reply, StreamPosition};
use std::collections::HashMap;
use std::sync::Arc;

const PUBLISH_LUA: &str = r#"
local offset = redis.call('INCR', KEYS[1])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[1], offset .. '-0', 'd', ARGV[2])
local epoch = redis.call('GET', KEYS[3])
if not epoch then epoch = ARGV[3]; redis.call('SET', KEYS[3], epoch) end
return {offset, epoch}
"#;

pub struct RedisBroker {
    conn: ConnectionManager,
    prefix: String,
    publish_script: Script,
}

impl RedisBroker {
    /// Подключиться и запустить фоновую pub/sub-задачу (fan-out на эту ноду).
    pub async fn connect(url: &str, prefix: impl Into<String>, delivery: Arc<dyn Delivery>) -> Result<Arc<Self>> {
        let prefix = prefix.into();
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client.clone()).await?;

        let pattern = format!("{prefix}:ch:*");
        let strip = format!("{prefix}:ch:");
        let control = format!("{prefix}:control");
        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.psubscribe(&pattern).await?;
        pubsub.subscribe(&control).await?;
        tokio::spawn(async move {
            let mut stream = pubsub.on_message();
            while let Some(msg) = stream.next().await {
                let full = msg.get_channel_name().to_string();
                let Ok(payload) = msg.get_payload::<Vec<u8>>() else { continue };
                if full == control {
                    if let Ok(cmd) = serde_json::from_slice::<ControlCommand>(&payload) {
                        delivery.control(cmd);
                    }
                } else {
                    let logical = full.strip_prefix(&strip).unwrap_or(&full).to_string();
                    if let Ok(reply) = Reply::decode(payload.as_slice()) {
                        delivery.deliver(&logical, reply);
                    }
                }
            }
            tracing::warn!("redis pub/sub поток завершился");
        });

        Ok(Arc::new(Self { conn, prefix, publish_script: Script::new(PUBLISH_LUA) }))
    }

    fn key(&self, kind: &str, channel: &str) -> String {
        format!("{}:{}:{}", self.prefix, kind, channel)
    }

    async fn get_epoch(&self, channel: &str) -> Result<String> {
        let mut c = self.conn.clone();
        let epochk = self.key("epoch", channel);
        let existing: Option<String> = c.get(&epochk).await?;
        match existing {
            Some(e) => Ok(e),
            None => {
                let e = new_epoch();
                // SET NX: первый победитель фиксирует epoch
                let ok: bool = redis::cmd("SET").arg(&epochk).arg(&e).arg("NX").query_async(&mut c).await?;
                if ok {
                    Ok(e)
                } else {
                    let e2: String = c.get(&epochk).await?;
                    Ok(e2)
                }
            }
        }
    }

    async fn current_offset(&self, channel: &str) -> Result<u64> {
        let mut c = self.conn.clone();
        let v: Option<i64> = c.get(self.key("seq", channel)).await?;
        Ok(v.unwrap_or(0) as u64)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn ensure_epoch(&self, channel: &str) -> Result<StreamPosition> {
        let epoch = self.get_epoch(channel).await?;
        let offset = self.current_offset(channel).await?;
        Ok(StreamPosition { offset, epoch })
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

        let (offset, epoch) = if recoverable {
            let mut c = self.conn.clone();
            let cand = new_epoch();
            let res: (i64, String) = self
                .publish_script
                .key(self.key("seq", channel))
                .key(self.key("hist", channel))
                .key(self.key("epoch", channel))
                .arg(history_size)
                .arg(&data)
                .arg(&cand)
                .invoke_async(&mut c)
                .await?;
            (res.0 as u64, res.1)
        } else {
            (0, self.get_epoch(channel).await?)
        };

        let publication = Publication { data, offset, info, tags: HashMap::new() };
        let reply = pub_push(channel, publication);
        self.broadcast(channel, reply).await?;
        Ok(StreamPosition { offset, epoch })
    }

    async fn broadcast(&self, channel: &str, reply: Reply) -> Result<()> {
        let mut c = self.conn.clone();
        let payload = reply.encode_to_vec();
        let _: () = c.publish(self.key("ch", channel), payload).await?;
        Ok(())
    }

    async fn recover(&self, channel: &str, since: &StreamPosition, limit: usize) -> Result<Recovered> {
        let current = self.current_offset(channel).await?;
        let epoch = self.get_epoch(channel).await?;
        let position = StreamPosition { offset: current, epoch: epoch.clone() };

        if !since.epoch.is_empty() && since.epoch != epoch {
            return Ok(Recovered { recovered: false, publications: vec![], position });
        }

        let mut c = self.conn.clone();
        let start = format!("({}-0", since.offset); // строго больше since.offset
        let reply: StreamRangeReply = redis::cmd("XRANGE")
            .arg(self.key("hist", channel))
            .arg(&start)
            .arg("+")
            .arg("COUNT")
            .arg(limit)
            .query_async(&mut c)
            .await?;

        let mut publications = Vec::with_capacity(reply.ids.len());
        for entry in &reply.ids {
            let offset: u64 = entry.id.split('-').next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let data: Vec<u8> = entry
                .map
                .get("d")
                .and_then(|v| redis::from_redis_value(v).ok())
                .unwrap_or_default();
            publications.push(Publication { data, offset, info: None, tags: HashMap::new() });
        }

        let recovered = match publications.first() {
            Some(first) => first.offset <= since.offset + 1,
            None => current == since.offset,
        };
        Ok(Recovered { recovered, publications, position })
    }

    async fn presence_add(&self, channel: &str, client: &str, info: ClientInfo, ttl_secs: u64) -> Result<()> {
        let mut c = self.conn.clone();
        let expire = (unix_secs() + ttl_secs) as f64;
        let (pz, ph) = (self.key("pz", channel), self.key("ph", channel));
        let _: () = c.zadd(&pz, client, expire).await?;
        let _: () = c.hset(&ph, client, info.encode_to_vec()).await?;
        let keep = (ttl_secs + 60) as i64;
        let _: () = c.expire(&pz, keep).await?;
        let _: () = c.expire(&ph, keep).await?;
        Ok(())
    }

    async fn presence_remove(&self, channel: &str, client: &str) -> Result<()> {
        let mut c = self.conn.clone();
        let _: () = c.zrem(self.key("pz", channel), client).await?;
        let _: () = c.hdel(self.key("ph", channel), client).await?;
        Ok(())
    }

    async fn presence_list(&self, channel: &str) -> Result<HashMap<String, ClientInfo>> {
        let mut c = self.conn.clone();
        let (pz, ph) = (self.key("pz", channel), self.key("ph", channel));
        let now = unix_secs() as f64;
        // очистка протухших
        let _: () = c.zrembyscore(&pz, "-inf", now).await?;
        let live: Vec<String> = c.zrangebyscore(&pz, now, "+inf").await?;
        if live.is_empty() {
            return Ok(HashMap::new());
        }
        let raw: Vec<Option<Vec<u8>>> = c.hget(&ph, &live).await?;
        let mut out = HashMap::new();
        for (client, bytes) in live.into_iter().zip(raw.into_iter()) {
            if let Some(b) = bytes {
                if let Ok(info) = ClientInfo::decode(b.as_slice()) {
                    out.insert(client, info);
                }
            }
        }
        Ok(out)
    }

    async fn idempotency_get(&self, key: &str) -> Result<Option<StreamPosition>> {
        let mut c = self.conn.clone();
        let v: Option<String> = c.get(self.key("idem", key)).await?;
        Ok(v.and_then(|s| {
            let (o, e) = s.split_once(':')?;
            Some(StreamPosition { offset: o.parse().ok()?, epoch: e.to_string() })
        }))
    }

    async fn idempotency_put(&self, key: &str, pos: &StreamPosition, ttl_secs: u64) -> Result<()> {
        let mut c = self.conn.clone();
        let val = format!("{}:{}", pos.offset, pos.epoch);
        let _: () = c.set_ex(self.key("idem", key), val, ttl_secs.max(1)).await?;
        Ok(())
    }

    async fn control_publish(&self, cmd: &ControlCommand) -> Result<()> {
        let mut c = self.conn.clone();
        let payload = serde_json::to_vec(cmd).map_err(|e| BrokerError::Decode(e.to_string()))?;
        let _: () = c.publish(format!("{}:control", self.prefix), payload).await?;
        Ok(())
    }
}

impl From<prost::DecodeError> for BrokerError {
    fn from(e: prost::DecodeError) -> Self {
        BrokerError::Decode(e.to_string())
    }
}
