//! Мультинода против живого Redis: fan-out между нодами, recovery, presence.
//! Если Redis недоступен — тесты пропускаются (не падают).

use std::sync::Arc;
use std::time::Duration;

use socket_broker::{Broker, Delivery, RedisBroker};
use socket_protocol::{push, reply, ClientInfo, Reply};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

const URL: &str = "redis://127.0.0.1:6379";

struct ChanDelivery {
    tx: UnboundedSender<(String, Reply)>,
}
impl Delivery for ChanDelivery {
    fn deliver(&self, channel: &str, reply: Reply) {
        let _ = self.tx.send((channel.to_string(), reply));
    }
}

fn pub_data(reply: &Reply) -> Option<Vec<u8>> {
    if let Some(reply::Payload::Push(p)) = &reply.payload {
        if let Some(push::Event::Pub(pb)) = &p.event {
            return Some(pb.data.clone());
        }
    }
    None
}

async fn node(prefix: &str) -> Option<(Arc<RedisBroker>, UnboundedReceiver<(String, Reply)>)> {
    let (tx, rx) = unbounded_channel();
    match RedisBroker::connect(URL, prefix, Arc::new(ChanDelivery { tx })).await {
        Ok(b) => Some((b, rx)),
        Err(_) => None,
    }
}

fn uniq(suffix: &str) -> String {
    format!("ittest{}:{}", std::process::id(), suffix)
}

#[tokio::test]
async fn cross_node_fanout() {
    let prefix = uniq("fanout");
    let Some((a, _ra)) = node(&prefix).await else {
        eprintln!("redis недоступен — пропуск");
        return;
    };
    let (_b, mut rb) = node(&prefix).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await; // дать pub/sub подняться

    let ch = "chat:room:xnode";
    a.publish(ch, b"hi-from-a".to_vec(), None, false, 100).await.unwrap();

    // Нода B должна получить публикацию ноды A (через Redis pub/sub)
    let (gch, reply) = tokio::time::timeout(Duration::from_secs(3), rb.recv())
        .await
        .expect("таймаут: кросс-нода не доставила")
        .expect("канал закрыт");
    assert_eq!(gch, ch);
    assert_eq!(pub_data(&reply).as_deref(), Some(&b"hi-from-a"[..]));
}

#[tokio::test]
async fn recovery_via_redis() {
    let prefix = uniq("recover");
    let Some((a, _ra)) = node(&prefix).await else {
        return;
    };

    let ch = "chat:room:rec";
    let p1 = a.publish(ch, vec![1], None, false, 100).await.unwrap();
    a.publish(ch, vec![2], None, false, 100).await.unwrap();
    a.publish(ch, vec![3], None, false, 100).await.unwrap();

    // восстановиться с offset=1 (эпоха та же) → должны прийти offset 2 и 3
    let since = socket_protocol::StreamPosition { offset: 1, epoch: p1.epoch.clone() };
    let r = a.recover(ch, &since, 10).await.unwrap();
    assert!(r.recovered, "разрыв в пределах истории");
    assert_eq!(r.publications.len(), 2);
    assert_eq!(r.publications[0].offset, 2);
    assert_eq!(r.publications[0].data, vec![2]);

    // чужая эпоха → не восстановить
    let bad = socket_protocol::StreamPosition { offset: 1, epoch: "deadbeef".into() };
    let r2 = a.recover(ch, &bad, 10).await.unwrap();
    assert!(!r2.recovered);
}

#[tokio::test]
async fn presence_shared_across_nodes() {
    let prefix = uniq("presence");
    let Some((a, _ra)) = node(&prefix).await else {
        return;
    };
    let (b, _rb) = node(&prefix).await.unwrap();

    let ch = "chat:room:pres";
    let info = ClientInfo { user: "u1".into(), client: "c1".into(), conn_info: vec![], chan_info: vec![] };
    a.presence_add(ch, "c1", info, 60).await.unwrap();

    // другая нода видит presence (общий Redis)
    let list = b.presence_list(ch).await.unwrap();
    assert!(list.contains_key("c1"));
    assert_eq!(list["c1"].user, "u1");

    a.presence_remove(ch, "c1").await.unwrap();
    let list2 = b.presence_list(ch).await.unwrap();
    assert!(!list2.contains_key("c1"));
}
