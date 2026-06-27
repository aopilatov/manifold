//! End-to-end тесты ядра (этап 1): auth-гейтинг, fan-out, recovery — через ApiService.

use std::sync::Arc;
use std::time::Duration;

use socket_core::api::ApiService;
use socket_core::auth::{ChannelGrant, Claims};
use socket_core::hub::Hub;
use socket_core::Config;
use socket_protocol::{push, reply, PublishRequest, Reply, StreamPosition, SubscribeRequest};
use tokio::sync::mpsc;

fn cfg() -> Arc<Config> {
    Arc::new(
        Config::load(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config.toml"))
            .expect("config.toml"),
    )
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

/// Первая публикация из канала, пропуская Join/Leave.
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
    let api = ApiService::new(cfg(), Hub::new());

    let (tx_a, mut _rx_a) = mpsc::channel(32);
    let (tx_b, mut rx_b) = mpsc::channel(32);
    let a = api.register(Some(claims("u-a", "chat:room:*", &["sub", "pub"])), tx_a);
    let b = api.register(Some(claims("u-b", "chat:room:*", &["sub"])), tx_b);

    api.subscribe(&a, &sub_req("chat:room:1")).unwrap();
    api.subscribe(&b, &sub_req("chat:room:1")).unwrap();

    api.publish(
        &a,
        &PublishRequest { channel: "chat:room:1".into(), data: b"hello".to_vec(), transient: false },
    )
    .unwrap();

    let got = next_pub(&mut rx_b).await;
    assert_eq!(got.data, b"hello");
    assert_eq!(got.offset, 1, "chat recoverable ⇒ offset присвоен");
}

#[tokio::test]
async fn subscribe_denied_without_grant() {
    let api = ApiService::new(cfg(), Hub::new());
    let (tx, _rx) = mpsc::channel(8);
    // грант только на chat:room:*, а подписываемся на user:* — отказ
    let c = api.register(Some(claims("u", "chat:room:*", &["sub"])), tx);

    let err = api.subscribe(&c, &sub_req("user:u:notifications")).unwrap_err();
    assert_eq!(err.code, 103, "not_permitted / token_required");
}

#[tokio::test]
async fn public_namespace_subscribes_without_token() {
    let api = ApiService::new(cfg(), Hub::new());
    let (tx, _rx) = mpsc::channel(8);
    // news: subscribe=public — без claims
    let c = api.register(None, tx);

    let res = api.subscribe(&c, &sub_req("news:sports")).unwrap();
    assert!(res.recoverable, "у news есть история");
}

#[tokio::test]
async fn client_publish_to_public_feed_is_denied() {
    let api = ApiService::new(cfg(), Hub::new());
    let (tx, _rx) = mpsc::channel(8);
    let c = api.register(None, tx);
    // news.publish = off → клиент публиковать не может (только Server API)
    let err = api
        .publish(&c, &PublishRequest { channel: "news:sports".into(), data: b"x".to_vec(), transient: false })
        .unwrap_err();
    assert_eq!(err.code, 103);
}

#[tokio::test]
async fn transient_publish_skips_history() {
    let api = ApiService::new(cfg(), Hub::new());
    let (tx, _rx) = mpsc::channel(8);
    let c = api.register(Some(claims("u", "chat:room:*", &["sub", "pub", "history"])), tx);
    api.subscribe(&c, &sub_req("chat:room:9")).unwrap();

    api.publish(
        &c,
        &PublishRequest { channel: "chat:room:9".into(), data: b"typing".to_vec(), transient: true },
    )
    .unwrap();

    // история пуста → recover ничего не вернёт
    let res = api.history(&c, &socket_protocol::HistoryRequest {
        channel: "chat:room:9".into(),
        limit: 10,
        since: None,
        reverse: false,
    }).unwrap();
    assert!(res.publications.is_empty(), "transient не пишется в историю");
}

#[tokio::test]
async fn recovery_returns_missed_publications() {
    let api = ApiService::new(cfg(), Hub::new());
    let (tx_pub, _r) = mpsc::channel(32);
    let pubr = api.register(Some(claims("p", "chat:room:*", &["sub", "pub"])), tx_pub);
    api.subscribe(&pubr, &sub_req("chat:room:5")).unwrap();

    // три публикации
    for i in 0..3u8 {
        api.publish(&pubr, &PublishRequest {
            channel: "chat:room:5".into(),
            data: vec![i],
            transient: false,
        })
        .unwrap();
    }

    // новый клиент восстанавливается с offset=1 (видел только первую)
    let (tx_late, _r2) = mpsc::channel(32);
    let late = api.register(Some(claims("l", "chat:room:*", &["sub"])), tx_late);
    let pos = StreamPosition { offset: 1, epoch: api.hub.ensure_state("chat:room:5").epoch };
    let res = api
        .subscribe(&late, &SubscribeRequest {
            channel: "chat:room:5".into(),
            token: String::new(),
            recover: true,
            position: Some(pos),
        })
        .unwrap();

    assert!(res.recovered, "разрыв в пределах истории");
    assert_eq!(res.publications.len(), 2, "пропущены offset 2 и 3");
    assert_eq!(res.publications[0].offset, 2);
}
