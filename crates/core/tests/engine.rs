//! End-to-end тесты ядра: auth-гейтинг, fan-out, recovery — через ApiService (MemoryBroker).

use std::sync::Arc;
use std::time::Duration;

use socket_core::api::ApiService;
use socket_core::auth::{ChannelGrant, Claims};
use socket_core::Config;
use socket_protocol::{push, reply, PublishRequest, Reply, StreamPosition, SubscribeRequest};
use tokio::sync::mpsc;

fn cfg() -> Arc<Config> {
    Arc::new(
        Config::load(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config.toml"))
            .expect("config.toml"),
    )
}

fn api() -> ApiService {
    ApiService::in_memory(cfg())
}

fn claims(user: &str, pattern: &str, allow: &[&str]) -> Claims {
    Claims {
        sub: user.into(),
        exp: None,
        channels: vec![ChannelGrant {
            pattern: pattern.into(),
            allow: allow.iter().map(|s| s.to_string()).collect(),
        }],
    }
}

fn sub_req(channel: &str) -> SubscribeRequest {
    SubscribeRequest { channel: channel.into(), ..Default::default() }
}

async fn next_pub(rx: &mut mpsc::Receiver<Reply>) -> socket_protocol::Publication {
    loop {
        let reply = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout: публикация не пришла")
            .expect("канал закрыт");
        if let Some(reply::Payload::Push(p)) = reply.payload {
            if let Some(push::Event::Pub(pb)) = p.event {
                return pb;
            }
        }
    }
}

#[tokio::test]
async fn publish_fans_out_to_subscribers() {
    let api = api();
    let (tx_a, _rx_a) = mpsc::channel(32);
    let (tx_b, mut rx_b) = mpsc::channel(32);
    let a = api.register(Some(claims("u-a", "chat:room:*", &["sub", "pub"])), tx_a);
    let b = api.register(Some(claims("u-b", "chat:room:*", &["sub"])), tx_b);

    api.subscribe(&a, &sub_req("chat:room:1")).await.unwrap();
    api.subscribe(&b, &sub_req("chat:room:1")).await.unwrap();

    api.publish(&a, &PublishRequest { channel: "chat:room:1".into(), data: b"hello".to_vec(), transient: false })
        .await
        .unwrap();

    let got = next_pub(&mut rx_b).await;
    assert_eq!(got.data, b"hello");
    assert_eq!(got.offset, 1, "chat recoverable ⇒ offset присвоен");
}

#[tokio::test]
async fn subscribe_denied_without_grant() {
    let api = api();
    let (tx, _rx) = mpsc::channel(8);
    let c = api.register(Some(claims("u", "chat:room:*", &["sub"])), tx);
    let err = api.subscribe(&c, &sub_req("user:u:notifications")).await.unwrap_err();
    assert_eq!(err.code, 103);
}

#[tokio::test]
async fn public_namespace_subscribes_without_token() {
    let api = api();
    let (tx, _rx) = mpsc::channel(8);
    let c = api.register(None, tx);
    let res = api.subscribe(&c, &sub_req("news:sports")).await.unwrap();
    assert!(res.recoverable);
}

#[tokio::test]
async fn client_publish_to_public_feed_is_denied() {
    let api = api();
    let (tx, _rx) = mpsc::channel(8);
    let c = api.register(None, tx);
    let err = api
        .publish(&c, &PublishRequest { channel: "news:sports".into(), data: b"x".to_vec(), transient: false })
        .await
        .unwrap_err();
    assert_eq!(err.code, 103);
}

#[tokio::test]
async fn transient_publish_skips_history() {
    let api = api();
    let (tx, _rx) = mpsc::channel(8);
    let c = api.register(Some(claims("u", "chat:room:*", &["sub", "pub", "history"])), tx);
    api.subscribe(&c, &sub_req("chat:room:9")).await.unwrap();

    api.publish(&c, &PublishRequest { channel: "chat:room:9".into(), data: b"typing".to_vec(), transient: true })
        .await
        .unwrap();

    let res = api
        .history(&c, &socket_protocol::HistoryRequest {
            channel: "chat:room:9".into(),
            limit: 10,
            since: None,
            reverse: false,
        })
        .await
        .unwrap();
    assert!(res.publications.is_empty(), "transient не пишется в историю");
}

#[tokio::test]
async fn recovery_returns_missed_publications() {
    let api = api();
    let (tx_pub, _r) = mpsc::channel(32);
    let pubr = api.register(Some(claims("p", "chat:room:*", &["sub", "pub"])), tx_pub);
    let sub_res = api.subscribe(&pubr, &sub_req("chat:room:5")).await.unwrap();
    let epoch = sub_res.position.unwrap().epoch;

    for i in 0..3u8 {
        api.publish(&pubr, &PublishRequest { channel: "chat:room:5".into(), data: vec![i], transient: false })
            .await
            .unwrap();
    }

    let (tx_late, _r2) = mpsc::channel(32);
    let late = api.register(Some(claims("l", "chat:room:*", &["sub"])), tx_late);
    let res = api
        .subscribe(&late, &SubscribeRequest {
            channel: "chat:room:5".into(),
            token: String::new(),
            recover: true,
            position: Some(StreamPosition { offset: 1, epoch }),
        })
        .await
        .unwrap();

    assert!(res.recovered, "разрыв в пределах истории");
    assert_eq!(res.publications.len(), 2, "пропущены offset 2 и 3");
    assert_eq!(res.publications[0].offset, 2);
}
