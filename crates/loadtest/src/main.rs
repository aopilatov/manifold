//! Load/stress test for the Manifold engine.
//!
//! Spawns many WebSocket subscribers (each subscribing to a random subset of channels across
//! 1..N namespaces) plus a pool of publisher connections that blast millions of messages.
//! Measures publish/receive throughput, end-to-end latency (p50/p99) and an approximate
//! delivery ratio.
//!
//! Example:
//!   manifold-loadtest --subscribers 2000 --publishers 16 --namespaces 100 \
//!                   --max-channels 50 --messages 5000000
//!
//! Tip: raise the fd limit before running many connections, e.g. `ulimit -n 100000`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use prost::Message as _;
use manifold_protocol::{command, push, reply, Command, ConnectRequest, PublishRequest, Reply, SubscribeRequest};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser, Debug, Clone)]
#[command(about = "Manifold engine load/stress test")]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:8000/connection/websocket")]
    url: String,
    /// HMAC secret matching the server's [auth.jwt].hmac_secret.
    #[arg(long, default_value = "dev-secret")]
    secret: String,
    #[arg(long, default_value = "manifold")]
    audience: String,

    /// Number of subscriber connections.
    #[arg(long, default_value_t = 1000)]
    subscribers: usize,
    /// Number of publisher connections.
    #[arg(long, default_value_t = 8)]
    publishers: usize,

    /// Namespace count (channel = nsK:cN). Each sub picks channels across these.
    #[arg(long, default_value_t = 100)]
    namespaces: usize,
    /// Channels per namespace.
    #[arg(long, default_value_t = 20)]
    channels_per_ns: usize,
    /// Min/max channels each subscriber joins.
    #[arg(long, default_value_t = 1)]
    min_channels: usize,
    #[arg(long, default_value_t = 100)]
    max_channels: usize,

    /// Total messages to publish.
    #[arg(long, default_value_t = 5_000_000)]
    messages: u64,
    /// Target publish rate (msgs/sec across all publishers). 0 = unbounded burst (max ingest).
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Payload size in bytes (first 8 bytes carry a timestamp).
    #[arg(long, default_value_t = 16)]
    payload: usize,
    /// Publish as transient (no history/offset) — maximizes throughput.
    #[arg(long, default_value_t = true)]
    transient: bool,
    /// Do NOT drain publisher reply frames — simulates a misbehaving non-reading client.
    #[arg(long, default_value_t = false)]
    no_pub_drain: bool,

    /// Overall publish timeout (seconds).
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn mint(secret: &str, aud: &str) -> String {
    use jsonwebtoken::{encode, EncodingKey, Header};
    #[derive(serde::Serialize)]
    struct C<'a> {
        sub: &'a str,
        aud: &'a str,
        exp: usize,
    }
    let exp = (now_nanos() / 1_000_000_000) as usize + 86_400;
    encode(&Header::default(), &C { sub: "load", aud, exp }, &EncodingKey::from_secret(secret.as_bytes()))
        .expect("mint jwt")
}

/// Cheap xorshift PRNG (no rand dependency).
fn xs(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

struct Pool {
    namespaces: usize,
    per_ns: usize,
}
impl Pool {
    fn total(&self) -> usize {
        (self.namespaces * self.per_ns).max(1)
    }
    fn name(&self, idx: usize) -> String {
        let idx = idx % self.total();
        format!("ns{}:c{}", idx / self.per_ns, idx % self.per_ns)
    }
}

async fn open(url: &str, token: &str) -> Result<Ws> {
    let mut req = url.into_client_request()?;
    req.headers_mut().insert("Sec-WebSocket-Protocol", "manifold.v1".parse()?);
    let (mut ws, _) = connect_async(req).await?;
    let cmd = Command {
        id: 1,
        method: Some(command::Method::Connect(ConnectRequest { token: token.to_string(), ..Default::default() })),
    };
    ws.send(Message::Binary(cmd.encode_to_vec().into())).await?;
    // first reply is the ConnectResult — drain it
    let _ = ws.next().await;
    Ok(ws)
}

async fn subscriber(
    url: String,
    token: String,
    channels: Vec<String>,
    received: Arc<AtomicU64>,
    total_subs: Arc<AtomicU64>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Histogram<u64> {
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let ws = match open(&url, &token).await {
        Ok(w) => w,
        Err(_) => return hist,
    };
    let (mut tx, mut rx) = ws.split();
    let mut id = 2u32;
    for ch in &channels {
        let cmd = Command {
            id,
            method: Some(command::Method::Subscribe(SubscribeRequest { channel: ch.clone(), ..Default::default() })),
        };
        id += 1;
        if tx.send(Message::Binary(cmd.encode_to_vec().into())).await.is_err() {
            return hist;
        }
        total_subs.fetch_add(1, Ordering::Relaxed);
    }
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            msg = rx.next() => match msg {
                Some(Ok(Message::Binary(b))) => {
                    if let Ok(rep) = Reply::decode(&b[..]) {
                        if let Some(reply::Payload::Push(p)) = rep.payload {
                            if let Some(push::Event::Pub(pb)) = p.event {
                                received.fetch_add(1, Ordering::Relaxed);
                                if pb.data.len() >= 8 {
                                    let ts = u64::from_le_bytes(pb.data[..8].try_into().unwrap());
                                    let lat_us = now_nanos().saturating_sub(ts) / 1000;
                                    let _ = hist.record(lat_us.min(60_000_000));
                                }
                            }
                        }
                    }
                }
                Some(Ok(_)) => {}
                _ => break,
            }
        }
    }
    hist
}

async fn publisher(
    url: String,
    token: String,
    pool: Arc<Pool>,
    payload: usize,
    transient: bool,
    published: Arc<AtomicU64>,
    target: u64,
    errors: Arc<AtomicU64>,
    seed: u64,
    rate_per_pub: u64,
    drain_replies: bool,
) {
    let ws = match open(&url, &token).await {
        Ok(w) => w,
        Err(_) => {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    // The server replies to every publish (PublishResult); drain those replies in the background
    // so the connection's reply channel never backs up. With --pub-drain false we intentionally
    // do NOT drain, to verify the server disconnects such a non-reading client instead of stalling.
    let (mut ws, rx) = ws.split();
    let drain = if drain_replies {
        Some(tokio::spawn(async move {
            let mut rx = rx;
            while let Some(Ok(_)) = rx.next().await {}
        }))
    } else {
        drop(rx);
        None
    };

    let mut s = seed | 1;
    let total = pool.total();
    let size = payload.max(8);

    // Pacing: with a rate, send `per_tick` every 10ms; otherwise burst (per_tick = unbounded).
    let mut ticker = (rate_per_pub > 0).then(|| tokio::time::interval(Duration::from_millis(10)));
    let per_tick = if rate_per_pub > 0 { (rate_per_pub / 100).max(1) } else { u64::MAX };

    'outer: loop {
        if let Some(t) = ticker.as_mut() {
            t.tick().await;
        }
        let mut sent = 0u64;
        while sent < per_tick {
            let n = published.fetch_add(1, Ordering::Relaxed);
            if n >= target {
                published.fetch_sub(1, Ordering::Relaxed);
                break 'outer;
            }
            let ch = pool.name(xs(&mut s) as usize % total);
            let mut data = vec![0u8; size];
            data[..8].copy_from_slice(&now_nanos().to_le_bytes());
            let cmd = Command {
                id: 2,
                method: Some(command::Method::Publish(PublishRequest { channel: ch, data, transient })),
            };
            if ws.send(Message::Binary(cmd.encode_to_vec().into())).await.is_err() {
                errors.fetch_add(1, Ordering::Relaxed);
                break 'outer;
            }
            sent += 1;
        }
    }
    let _ = ws.flush().await;
    if let Some(d) = drain {
        d.abort();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let token = mint(&args.secret, &args.audience);
    let pool = Arc::new(Pool { namespaces: args.namespaces, per_ns: args.channels_per_ns });
    let received = Arc::new(AtomicU64::new(0));
    let published = Arc::new(AtomicU64::new(0));
    let total_subs = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let span = (args.max_channels - args.min_channels + 1).max(1);
    println!(
        "connecting {} subscribers (pool {} channels) ...",
        args.subscribers,
        pool.total()
    );

    let mut sub_handles = Vec::with_capacity(args.subscribers);
    for i in 0..args.subscribers {
        let mut s = (i as u64).wrapping_mul(2_654_435_761).wrapping_add(1) | 1;
        let k = args.min_channels + (xs(&mut s) as usize % span);
        let mut chans = Vec::with_capacity(k);
        for _ in 0..k {
            chans.push(pool.name(xs(&mut s) as usize));
        }
        sub_handles.push(tokio::spawn(subscriber(
            args.url.clone(),
            token.clone(),
            chans,
            received.clone(),
            total_subs.clone(),
            stop_rx.clone(),
        )));
        if i % 200 == 199 {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("subscribed: {} total subscriptions", total_subs.load(Ordering::Relaxed));

    // live reporter
    let rep_pub = published.clone();
    let rep_rec = received.clone();
    let mut rep_stop = stop_rx.clone();
    let reporter = tokio::spawn(async move {
        let (mut lp, mut lr) = (0u64, 0u64);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            if *rep_stop.borrow_and_update() {
                break;
            }
            let (p, r) = (rep_pub.load(Ordering::Relaxed), rep_rec.load(Ordering::Relaxed));
            println!("pub {:>11}  recv {:>11}  | pub/s {:>9}  recv/s {:>9}", p, r, p - lp, r - lr);
            (lp, lr) = (p, r);
        }
    });

    println!("publishing {} messages with {} publishers ...", args.messages, args.publishers);
    let start = Instant::now();
    let rate_per_pub = if args.rate > 0 { (args.rate / args.publishers as u64).max(1) } else { 0 };
    let mut pub_handles = Vec::with_capacity(args.publishers);
    for j in 0..args.publishers {
        let seed = (j as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(12_345);
        pub_handles.push(tokio::spawn(publisher(
            args.url.clone(),
            token.clone(),
            pool.clone(),
            args.payload,
            args.transient,
            published.clone(),
            args.messages,
            errors.clone(),
            seed,
            rate_per_pub,
            !args.no_pub_drain,
        )));
    }
    let _ = tokio::time::timeout(Duration::from_secs(args.timeout_secs), futures::future::join_all(pub_handles)).await;
    let pub_elapsed = start.elapsed();

    // drain in-flight fan-out, then stop subscribers
    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = stop_tx.send(true);

    let mut merged = Histogram::<u64>::new(3).unwrap();
    for h in futures::future::join_all(sub_handles).await {
        if let Ok(hist) = h {
            let _ = merged.add(&hist);
        }
    }
    let _ = reporter.await;

    let p = published.load(Ordering::Relaxed);
    let r = received.load(Ordering::Relaxed);
    let subs = total_subs.load(Ordering::Relaxed);
    let expected = (p as f64 * subs as f64 / pool.total() as f64) as u64;
    let secs = pub_elapsed.as_secs_f64().max(0.001);

    println!("\n================ SUMMARY ================");
    println!("subscribers        : {}", args.subscribers);
    println!("publishers         : {}", args.publishers);
    println!("channel pool       : {} ({} ns x {})", pool.total(), args.namespaces, args.channels_per_ns);
    println!("total subscriptions: {}", subs);
    println!("published          : {}", p);
    println!("publish wall time  : {:.2}s   ({:.0} msg/s)", secs, p as f64 / secs);
    println!("received (fan-out) : {}", r);
    println!("expected (~)       : {}", expected);
    println!(
        "delivery ratio     : {:.1}%",
        if expected > 0 { 100.0 * r as f64 / expected as f64 } else { 0.0 }
    );
    println!("publish errors     : {}", errors.load(Ordering::Relaxed));
    if merged.len() > 0 {
        println!(
            "e2e latency        : p50 {} us | p99 {} us | max {} us",
            merged.value_at_quantile(0.5),
            merged.value_at_quantile(0.99),
            merged.max()
        );
    }
    Ok(())
}
