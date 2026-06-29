---
title: Architecture
description: Design document for a realtime engine (WebSocket pub/sub)
---

# Manifold — design document

A standalone realtime engine server (pub/sub over WebSocket): "like Centrifugo,
but more configurable". Deployed and run independently, not embedded as a library.
Monorepo: Rust backend + React frontend (admin) + Markdown documentation.

---

## 1. Goals and non-goals

**Goals:**

- Public channels — subscription **without a token**, with dynamic sub/unsub to subchannels.
- Private channels — the same, but with subscription authorization.
- Flexible permission model: a list of allowed **channel patterns** right in the JWT.
- Multi-node from day one (horizontal scaling).
- Presence, message history, and recovery.
- Server API for publishing from the application backend (HTTP **and** gRPC).
- Admin/monitoring UI.

**Non-goals:**

- The engine does **not** store users and does **not** issue tokens — that is done by the external (application) backend.
- The engine does **not** parse the application message payload (`data` is always `bytes`).

---

## 2. Technology stack

| Part | Stack |
|---|---|
| **Backend** | Rust: `axum` + `tokio`; custom protocol over **Protobuf** (`prost`); gRPC via `tonic` |
| **Broker** | **Redis** (pub/sub + history + presence), hidden behind a `Broker` trait (NATS later) |
| **Client SDK** | TS package (`client-ts`): transport, reconnect, subscription registry, recovery, protobuf |
| **Frontend (admin)** | Vite + React + TS + **Mantine**; live data via `protobuf-es`; depends on `client-ts` |
| **Docs** | **docmd** (zero-config SSG, framework-free, Markdown-in → static HTML) |
| **Monorepo** | Cargo workspace + Vite + docmd; orchestration via `just` |

A single "full-stack Rust+React" framework does not exist — the standard here is a monorepo of
independent parts tied together by a shared `.proto` contract.

---

## 3. Repository structure

```
manifold/
├── Cargo.toml               # cargo workspace
├── proto/                   # .proto — the SINGLE contract (source of truth)
├── crates/
│   ├── server/              # axum: WS server, HTTP API, gRPC API, serving admin statics
│   │   ├── ws/              # client WebSocket protocol
│   │   ├── http_api/        # Server API: HTTP/JSON adapter
│   │   └── grpc_api/        # Server API: gRPC (tonic) adapter
│   ├── core/                # ApiService, hub, channel registry, glob matching, auth
│   ├── protocol/            # prost-generated types from proto/
│   └── broker/              # trait Broker + Redis implementation
├── packages/
│   ├── client-ts/           # CLIENT SDK (TS): reconnect, subscription registry, recovery, protobuf
│   └── proto-gen/           # generated protobuf-es types (shared by client-ts and web)
├── web/                     # React + Mantine admin UI (depends on client-ts)
├── docs/                    # docmd: Markdown documentation (+ autogen from proto/config)
├── config.toml              # server config
├── justfile                 # dev/build/codegen
└── docker-compose.yml       # server + redis
```

**Type codegen:** `proto/*.proto` → Rust (`prost`) and TS (`protobuf-es`). The frontend and backend
physically cannot drift apart on the schema. Some `.md` docs are also generated from `.proto` and
config structures.

---

## 4. Protocol (client ⇆ server)

Binary, over Protobuf. `Command` (from the client) and `Reply` (from the server) travel the wire.
A `Reply` is either a response to a command (same `id`) or an asynchronous `Push` (`id = 0`).

### Transports (behind a `Transport` trait)

The core (session/hub/recovery/auth) works with `Stream<Command>` + `Sink<Reply>` and is unaware of
the transport. In the MVP — two:

```rust
trait Transport {
    fn commands(&mut self) -> impl Stream<Item = Command>;  // upstream (client→server)
    fn replies(&mut self)  -> impl Sink<Reply>;             // downstream (server→client)
}
```

- **WebSocket** — a single bidirectional socket, raw binary frames.
- **SSE** (fallback for networks that cut WS) — a split session:
  `GET /connection/sse` (EventSource, downstream) + `POST /connection/sse/emit`
  (Command upstream, correlated by `X-Session-Id`). SSE is text → frames as
  **base64(protobuf)**. Setup: EventSource with a token in the query/cookie → the server creates
  a session in the same hub → sends `session_id` + `ConnectResult` as the first event.
  **Recovery synergy:** the native `Last-Event-ID` on EventSource auto-reconnect maps onto
  `StreamPosition` — the server resumes the session from the right position.
- SSE/HTTP-streaming/WebTransport extensions — additive, as a separate `impl Transport`.

### 4.1 Envelopes

```protobuf
syntax = "proto3";
package manifold.v1;

message Command {
  uint32 id = 1;              // correlation id, unique within a connection
  oneof method {
    ConnectRequest     connect     = 2;
    SubscribeRequest   subscribe   = 3;
    UnsubscribeRequest unsubscribe = 4;
    PublishRequest     publish     = 5;
    PresenceRequest    presence    = 6;
    HistoryRequest     history     = 7;
    PingRequest        ping        = 8;
    RefreshRequest     refresh     = 9;   // renew the CONNECTION with a new JWT
    SubRefreshRequest  sub_refresh = 10;  // renew a SUBSCRIPTION with a new sub-token
  }
}

message Reply {
  uint32 id = 1;              // 0 ⇒ asynchronous Push
  Error  error = 2;
  oneof payload {
    ConnectResult     connect      = 3;
    SubscribeResult   subscribe    = 4;
    UnsubscribeResult unsubscribe  = 5;
    PublishResult     publish      = 6;
    PresenceResult    presence     = 7;
    HistoryResult     history      = 8;
    PongResult        pong         = 9;
    Push              push         = 10;
  }
}

message Error {
  uint32 code = 1;            // stable machine-readable code
  string message = 2;
  bool   temporary = 3;       // true ⇒ it makes sense for the client to retry (backoff)
}
```

### 4.2 Client→server commands

```protobuf
message ConnectRequest {
  string token = 1;                  // connection JWT (claim channels[])
  map<string, SubscribeRequest> subs = 2;  // batch restore of subscriptions in 1 RTT (reconnect)
  map<string, string> headers = 3;
  string name = 4;                   // SDK name for debugging
}
message SubscribeRequest {
  string channel = 1;                // "chat:room:42"
  string token = 2;                  // opt. separate sub-token
  bool   recover = 3;
  StreamPosition position = 4;       // which offset/epoch to recover from
}
message UnsubscribeRequest { string channel = 1; }
message PublishRequest {
  string channel = 1;
  bytes  data = 2;
  bool   transient = 3;   // fire-and-forget: bypasses history/offset (typing, ephemeral signals)
}
message PresenceRequest { string channel = 1; }
message HistoryRequest {
  string channel = 1; int32 limit = 2; StreamPosition since = 3; bool reverse = 4;
}
message RefreshRequest    { string token = 1; }
message SubRefreshRequest { string channel = 1; string token = 2; }
message PingRequest {}
```

### 4.3 Results and asynchronous pushes

```protobuf
message ConnectResult {
  string client = 1;                 // connection id
  uint32 ping_interval_ms = 2;
  uint32 expires_in_s = 3;           // 0 = never expires
  bytes  data = 4;
  map<string, SubscribeResult> subs = 5;   // result of each restored subscription
  string session = 6;                // opt. id for server-side resume
}
message SubscribeResult {
  bool recoverable = 1;
  StreamPosition position = 2;
  bool recovered = 3;
  repeated Publication publications = 4;  // delivery of missed messages
  bool positioned = 5;
}
message UnsubscribeResult {}
message PublishResult {}
message PresenceResult { map<string, ClientInfo> presence = 1; }
message HistoryResult { repeated Publication publications = 1; StreamPosition position = 2; }
message PongResult {}

message Push {
  string channel = 1;
  oneof event {
    Publication pub         = 2;
    Join        join        = 3;
    Leave       leave       = 4;
    Unsubscribe unsubscribe = 5;     // the server forcibly unsubscribed
    Disconnect  disconnect  = 6;     // the server is closing the connection
  }
}

message Publication {
  bytes  data = 1;                   // application payload (the engine does not parse it)
  uint64 offset = 2;                 // position in the channel stream (recovery)
  ClientInfo info = 3;
  map<string, string> tags = 4;
}
message Join  { ClientInfo info = 1; }
message Leave { ClientInfo info = 1; }
message Unsubscribe { uint32 code = 1; string reason = 2; }
message Disconnect  { uint32 code = 1; string reason = 2; bool reconnect = 3; }

message ClientInfo { string user = 1; string client = 2; bytes conn_info = 3; bytes chan_info = 4; }

// Recovery foundation
message StreamPosition {
  uint64 offset = 1;                 // monotonic number of the last message
  string epoch = 2;                  // stream lifetime marker; a change ⇒ history was reset
}
```

### 4.4 Protocol decisions

- `id`-correlation; `id = 0` is reserved for `Push`. One WS frame = one `Command`/`Reply`
  (batching can be added later as a separate type without breaking this).
- `data` — always `bytes`: the engine is universal, the payload is up to the clients.
- `Error.temporary` drives SDK retries.
- **Token expiration — variant B (per-connection refresh):** via the `getToken()` callback the SDK
  fetches a new token from the external backend and sends it in `RefreshRequest` / `SubRefreshRequest`.
  The connection/subscription lives on — without a reconnect, without presence noise, without a reconnect storm.
- **Versioning:** the major version is in the proto package (`manifold.v1`); negotiation via the WS subprotocol
  (`Sec-WebSocket-Protocol: manifold.v1`) / SSE query (`?v=1`). Within a major — only additive
  changes (field numbers are not reused). The SDK must **safely skip unknown**
  `oneof` variants of `Push`. `protocol_version` in `ConnectRequest` — for diagnostics/soft gates;
  a major mismatch → `Disconnect{reconnect:false}`.

---

## 5. Authentication and permissions

The Centrifugo model: tokens are issued by the **external backend**, the engine only verifies the signature.

### 5.1 Connection JWT with capability patterns

In the connection JWT — a `channels` claim with a list of allowed **glob patterns** and permissions:

```json
{
  "sub": "user-123",
  "channels": [
    { "match": "news:*",      "allow": ["sub"] },
    { "match": "chat:room:*", "allow": ["sub", "pub", "presence"] },
    { "match": "user:123:**", "allow": ["sub", "history"] }
  ]
}
```

- On `subscribe` the engine matches the requested channel against the patterns; a match → it lets it through.
- Dynamism is preserved: the client freely subscribes/unsubscribes **within the granted patterns**,
  without a trip to the backend on every subscription.
- `allow` maps onto methods: `subscribe`→`sub`, `publish`→`pub`, `presence`→`presence`,
  `history`→`history`.

### 5.2 Glob semantics

- The segment separator is `:`. Namespace = the **first segment** (`chat:room:42` → ns `chat`).
- `*` — **one segment**: `news:*` catches `news:sports`, but not `news:sports:football`.
- `**` — **globstar** (any number of segments): `news:**` catches both.

### 5.3 Separation of responsibility namespace ↔ JWT

Orthogonal:

- **The namespace sets the gate** — whether an action is allowed and **whether a token is required**.
- **The JWT capability grants the right** to a specific user when the gate requires a token.

Per-action access modes (`subscribe`/`publish`/`presence`/`history`):

| Mode | Who can |
|---|---|
| `off` | No client (e.g. publish — only the Server API) |
| `public` | Anyone, without a token |
| `token` | Only with a matching pattern in the JWT (or a sub-token) |

### 5.4 Connection protection layer (before JWT)

Before the token check, the network layer works (`[server.security]`, `[server.conn_limits]`):

- **Origin allowlist** — protection against CSWSH (the browser sends `Origin` on a WS upgrade).
- **CORS** — for the HTTP API/SSE (cross-origin browser requests).
- **trusted_proxies** — the correct client IP from `X-Forwarded-For` behind an LB.
- **Connection limits** (per node, locally): `max_connections[_per_ip]`, `connect_rate_per_ip`
  (anti-flood), `handshake_timeout` (anti slow-loris), `idle_timeout`.
- **write_buffer_limit** — disconnect a slow consumer (protecting node memory in fan-out).
- `require_subprotocol`, `ip_allow/deny`, opt. `[server.tls]`.
- Global `max_connections_per_user` (across the cluster) — via Redis, deferred.

### Validation chain for `subscribe chat:room:42`:

```
1. namespace = "chat" → find it in the config (none → unknown_namespace, if strict)
2. access.subscribe == token ⇒ a token is required
3. find in JWT.channels a pattern matching the channel, with allow ⊇ ["sub"]
4. check max_subscribers, rate_limit.subscribe
5. OK → subscribe; if history_size>0 — return StreamPosition / recovery
```

---

## 6. Configuration (`config.toml`)

TOML (native to Rust via `serde` + `toml`). `defaults` and each `namespaces.<name>` —
one `NamespaceConfig` type; unset fields are inherited from `defaults`.

```toml
[server]
node_name = "manifold-1"
log_level = "info"

[server.ws]
listen   = "0.0.0.0:8000"
path     = "/connection/websocket"
max_message_size = "65536"
ping_interval    = "25s"

[server.sse]                     # SSE fallback (for networks that cut WS); shares the HTTP server with ws
enabled       = true
path          = "/connection/sse"      # downstream (EventSource, GET)
emit_path     = "/connection/sse/emit" # upstream (Command, POST, X-Session-Id)
keepalive     = "25s"

[server.security]                # protection at the handshake/network level (BEFORE JWT)
allowed_origins        = ["https://app.example.com", "https://*.example.com"]  # CSWSH; empty=do not check
cors_allowed_origins   = ["https://app.example.com"]   # CORS for the HTTP API/SSE
cors_allow_credentials = true
trusted_proxies        = ["10.0.0.0/8", "127.0.0.1/32"]  # who to trust in X-Forwarded-For
ip_allow               = []      # empty = all; ip_deny takes priority
ip_deny                = []

[server.conn_limits]             # connection limits (locally on the node)
max_connections          = 0     # 0 = no limit (per node)
max_connections_per_ip   = 100
max_connections_per_user = 0     # 0 = no limit; global (Redis) — later
connect_rate_per_ip      = { rate = 10, burst = 20 }   # anti connection-flood
handshake_timeout        = "5s"  # must send a valid Connect in time (anti slow-loris)
idle_timeout             = "60s" # no ping/activity → close
write_buffer_limit       = "1MB" # overflowed the outgoing buffer → disconnect (slow consumer)
require_subprotocol      = true  # require Sec-WebSocket-Protocol: manifold.v1

[server.tls]                     # opt.; usually TLS is terminated at the LB/proxy
enabled     = false
cert_path   = "/etc/manifold/tls/cert.pem"
key_path    = "/etc/manifold/tls/key.pem"
min_version = "1.2"

[server.http_api]
listen = "0.0.0.0:8001"
path   = "/api"

[server.grpc_api]                # required, not optional
listen = "0.0.0.0:8002"

[server.admin]
listen   = "127.0.0.1:8003"      # default — localhost
enabled  = true
password = "${ADMIN_PASSWORD}"   # empty + public listen ⇒ refuses to start

[server.health]                  # health/readiness for k8s/LB
listen = "0.0.0.0:8004"          # separate port; /health (liveness), /ready (readiness)

[redis]
url             = "redis://127.0.0.1:6379"
prefix          = "manifold"
idempotency_ttl = "5m"
node_heartbeat  = "5s"           # the node writes a heartbeat to Redis → info aggregates the cluster

[shutdown]                       # graceful drain on deploy/scale-down
drain_timeout    = "30s"         # wait for connections to drain before stopping
reconnect_advice = true          # broadcast Disconnect{reconnect:true}

[events]                         # opt. backend notifications about lifecycle (NOT authorization)
enabled  = false
endpoint = "https://app.example.com/manifold/events"
types    = ["connected", "disconnected", "subscribed", "unsubscribed"]
transport = "http"               # http (batch webhook) | grpc (stream)

[telemetry]
log_format      = "json"         # json | text
tracing_enabled = false          # OpenTelemetry (OTLP)
otlp_endpoint   = "http://localhost:4317"

[auth.jwt]
algorithm      = "HS256"
hmac_secret    = "${JWT_HMAC_SECRET}"
# or: algorithm = "RS256", jwks_url = "https://app.example.com/.well-known/jwks.json"
audience       = "manifold"
channels_claim = "channels"

[[api_keys]]
key   = "${API_KEY_BACKEND}"
allow = ["publish","broadcast","presence","history","subscribe",
         "unsubscribe","disconnect","channels","info"]
[[api_keys]]
key   = "${API_KEY_PUBLISHER}"
allow = ["publish","broadcast"]

[defaults]
presence        = false
join_leave      = false
history_size    = 0              # 0 ⇒ the channel is NOT recoverable
history_ttl     = "0s"
max_subscribers = 0              # 0 = no limit
name_max_len    = 255
[defaults.access]
subscribe = "token"
publish   = "off"
presence  = "token"
history   = "token"
strict_namespaces = true         # channel with an unknown ns → reject

[limits]                          # global per-connection ceilings (locally on the node)
max_channels_per_connection = 1000
max_commands_per_second     = 100

[namespaces.news]
history_size = 100
history_ttl  = "10m"
[namespaces.news.access]
subscribe = "public"
publish   = "off"
presence  = "public"
history   = "public"

[namespaces.chat]
presence        = true
join_leave      = true
history_size    = 300
history_ttl     = "24h"
max_subscribers = 5000
[namespaces.chat.access]
subscribe = "token"
publish   = "token"
presence  = "token"
history   = "token"
[namespaces.chat.rate_limit]
publish   = { rate = 20, burst = 40, scope = "client" }  # "20/s" = { rate=20, burst=20 }
subscribe = { rate = 10, burst = 10, scope = "client" }  # scope: client(local) | channel | user(Redis)

[namespaces.user]
history_size = 50
history_ttl  = "72h"
[namespaces.user.access]
subscribe = "token"
publish   = "off"
presence  = "off"
history   = "token"
```

> **There is no archive (DB) in the MVP.** History and recovery are entirely on Redis. The `[archive]`
> section and durable backends (rqlite/sqlite/Postgres) — a future extension behind a `HistoryStore` trait,
> not implemented in the MVP.

**Specifics:** secrets via `${ENV}` (expanded at load time), durations as strings
(`"10m"`), `api_keys` — an array of tables. **Hot-reload** on `SIGHUP` / Server API `reload` →
atomic swap of `Arc<Config>`, live connections are not dropped.

---

## 7. Hub and recovery

### 7.1 What lives where

**In each node's memory (only local routing):**

```rust
connections: DashMap<ClientId, ConnHandle>
//   ConnHandle = { tx: mpsc::Sender<Reply>, user_id, granted_patterns, subs: HashSet<Channel> }
channels: DashMap<Channel, HashSet<ClientId>>   // channel → LOCAL subscribers
```

Each connection has a writer task reading `Reply` from its own `mpsc`. The hub never
writes to the socket directly.

**In Redis (shared cluster state):** pub/sub (fan-out between nodes), per-channel stream
(history + `offset`), `epoch`, presence hash, control channel, idempotency cache.

### 7.2 offset and epoch

- `offset` — a monotonic message counter **within a channel**.
- `epoch` — a random string generated when the channel stream is created; it changes when the
  stream is lost. The client compares `epoch`: a mismatch ⇒ recovery is impossible, a full refetch is needed.

### 7.3 Publish into a recoverable channel (atomic, Lua in Redis)

```
1. offset = INCR seq:{channel}
2. XADD hist:{channel} MAXLEN ~ N  { offset, data }
3. PUBLISH ch:{channel}  Publication{ offset, epoch, data }
```

Nodes with local subscribers receive the `PUBLISH` and do a local fan-out. A node's subscription
to the Redis channel is **lazy**: the first local subscriber → `SUBSCRIBE`, the last one left →
`UNSUBSCRIBE`.

### 7.4 Recovery without races

```
1. SUBSCRIBE to live FIRST; buffer incoming live publications (do not deliver)
2. compare epoch: mismatch → recovered=false, return the current position
3. read hist:{channel} with offset > N (XRANGE), no more than the last N
     - a gap larger than the stored history → recovered=false (the client does a refetch)
4. merge the missed ones + the live buffer, dedup by offset, order
5. return in SubscribeResult.publications, recovered=true; afterward live via regular Push
```

Guarantee: no gaps, no duplicates (live is subscribed before reading history, dedup by `offset`).

### 7.5 Node failure

Connections drop → clients on reconnect land on another node and do a recover.
It works because history and `offset` are in Redis, not in the node's memory.

### 7.6 Presence

`presence:{channel}` — a Redis hash `clientId → ClientInfo` with a per-entry TTL (heartbeat by
`ping_interval`). Unsubscribe/disconnect → removal. `Join`/`Leave` are broadcast by the same `PUBLISH`.
The TTL protects against "ghosts" on a hard node failure.

### 7.7 Delivery guarantees

- **Recoverable channels** (`history_size > 0`): effectively **at-least-once** (the client detects
  a gap by `offset`, duplicates are cut off).
- **Non-recoverable channels**: **at-most-once** (pure fan-out, no storage).

### 7.8 Ephemeral publications (transient)

`PublishRequest.transient = true` → the publication is broadcast to subscribers but **bypasses** the Lua
history path: it does not increment `offset`, is not written to `hist:{channel}`. At-most-once, not part of
recovery. For typing indicators, cursors, "ephemeral" signals — even in a recoverable namespace,
so as not to clutter history or burn `offset`.

### 7.9 Scaling fan-out (hot channels)

- **Local history cache on the node** (a ring buffer of recent publications): on a mass simultaneous
  `subscribe` the node serves the recovery window from memory, without an `XRANGE` to Redis per
  subscriber → no stampede on Redis. (An optimization behind the seam, not an MVP blocker.)
- **Ceiling of a very hot channel**: Redis publishes once per node, but the local fan-out on the node
  (N subscribers) is a natural CPU/memory ceiling. Mitigated by the number of nodes, write batching, and
  `write_buffer_limit`. Super-hot channels (millions) — shard at the application level
  (internal sharding would break ordering).

### 7.10 Durability boundary (notifications, guaranteed delivery)

The engine is **live + short recovery** (the `history_size`/`history_ttl` window in Redis), **not** a
durable inbox. When offline longer than the window `recovered=false` for a channel → the application
**reads the missed messages from its own backend** (its DB is the system of record), the engine continues
live. "Guaranteed delivery" of notifications is achieved by this pairing, not by storage in the engine. The
mobile push itself (APNs/FCM when there is no WS) is out of scope; the backend triggers it on
`[events].disconnected` + a "is the user online?" query (see Server API).

### 7.11 Fast reconnect without loss

Environments like Cloudflare cut WS periodically (~every 100s) → there are many reconnects. The goal:
a reconnect is cheap, lossless, without presence flap.

- **The SDK remembers subscriptions** (the source of truth): a registry `{ channel → {last_position, sub_token?} }`,
  `last_position` is updated by incoming publications. The server is **stateless** per session —
  a reconnect survives even a node restart. (Server-side resume by `session` — an opt. optimization.)
- **Restore in 1 RTT:** on a reconnect the SDK sends **one** `ConnectRequest` with a `subs` map
  (channel → recover + position). The server authenticates once, restores all subscriptions, and
  runs recovery for each channel; `ConnectResult.subs` carries the result of each. Instead of
  `1 connect + N subscribe` round-trips.
- **JWT is reused:** in 30–100s the token has not expired → the reconnect does **not** go to the application
  backend for a token. A reconnect storm does not hit the token backend.
- **Damping presence flap:** on a transport break a `Leave` is **not** sent immediately — the presence entry
  lives by TTL (e.g. 60s) > the reconnect interval. A reconnect within the window updates the entry,
  `Join`/`Leave` are not generated. `Leave` — only on an explicit unsubscribe/`disconnect` or by TTL.
- **Losslessness boundary:** a break wider than the channel history or a changed `epoch` → `recovered=false`
  for the channel, the SDK does a clean re-subscribe + a "refetch" signal to the application (controlled
  degradation, not silent loss).
- **Anti-storm:** jittered backoff in the SDK spreads out synchronous reconnects (the CDN cuts many at once).

### 7.12 Archive (long history) — out of MVP

In the MVP **there is no DB**: both recovery and history are entirely on Redis (`history_size`/`history_ttl`).
Long history (audit, large ranges) — a different use case; a **seam** is laid behind a `HistoryStore`
trait, but not implemented. When needed — a durable backend
(rqlite/sqlite/Postgres/libSQL) and one async archiver reading Redis streams in batches is added, **without
hub changes** (`offset` is already authoritative from the moment of publication).

---

## 8. Server API (publishing and management)

The trusted server-to-server side. Both transports — **thin adapters over a single
`ApiService`** in `core` (one logic implementation).

### 8.1 Transports and auth

- **HTTP/JSON** (`POST /api`) — simple integration from any language.
- **gRPC** (tonic) — low latency, streaming; **required in the MVP**.
- Auth — an **API key** (not JWT): HTTP header `Authorization: apikey <secret>` /
  gRPC metadata, checked by an interceptor. Keys and permissions — in the config.

### 8.2 Methods

| Method | Purpose |
|---|---|
| `publish` | Into one channel → `{offset, epoch}` |
| `broadcast` | One message into many channels |
| `presence` / `presence_stats` | Full list / only counters |
| `history` / `history_remove` | Read / clear history |
| `subscribe` / `unsubscribe` | Server-initiated sub/unsub of a user |
| `disconnect` | Kick a user (by `user_id` / `client_id`) |
| `user_online` | Whether a user has active connections + their count (push-vs-realtime, across the cluster) |
| `channels` | Active channels (by glob) |
| `info` | Nodes, metrics |
| `batch` | A batch of commands in one RTT |
| `PublishStream` (gRPC bidi) | A stream of publications over one connection (high-throughput) |

### 8.3 Semantics

- **A single path into the hub:** publish from the Server API goes through the same Lua script as a client
  publish. Server-side and client-side publications are indistinguishable to subscribers.
- **Idempotency:** an optional `idempotency_key`, a `key → result` cache in Redis with a TTL.
  A retry (HTTP or gRPC stream) → the same `{offset}`, without re-publishing.
- **Control channel:** `subscribe`/`unsubscribe`/`disconnect` are addressed to a connection on any
  node via a separate Redis pub/sub. The node owning the connection performs the action.

---

## 9. Admin / monitoring UI

React + Mantine. A third, separate access perimeter (besides the client JWT and the API keys).

### 9.1 Authentication

`[server.admin].password` → `POST /admin/login` → an admin session (httpOnly cookie). Default —
bound to `127.0.0.1`; an empty password on a public interface ⇒ refuses to start.

The login method is behind the `AdminAuth` seam (`[server.admin].auth = "password" | "oidc"`). In the MVP —
**password**; OIDC (SSO via Google/Okta/Keycloak, PKCE flow + claims mapping) — an additive
implementation for the future, the session cookie is issued the same way.

### 9.2 Sections and Mantine components

| Section | Components | Source |
|---|---|---|
| Overview | `AppShell`, `Card`, `RingProgress`, `@mantine/charts` | `info` + the metrics stream |
| Channels | `mantine-datatable`, glob `TextInput`, `Drawer` | `channels`, `presence`, `history` |
| Connections | `mantine-datatable`, `ActionIcon`, `Modal` | `info`, `disconnect` |
| Publish | `@mantine/form`, `JsonInput` | `publish` / `broadcast` |
| Namespaces | `@mantine/code-highlight` (TOML) | config + `reload` |
| Metrics | `@mantine/charts` (`AreaChart`/`LineChart`) | `$metrics` via WS |

### 9.3 Dogfooding + external monitoring

- **Live data through the engine itself:** a reserved system namespace `$` (`$metrics`,
  `$node:events`), accessible **only** to an admin session (hardcoded). The admin client
  subscribes via the regular SDK — the product tests itself.
- **Prometheus** `/metrics` (text exposition) — for Grafana/Alertmanager; the React UI is a quick
  glance, not a replacement for the observability stack.

### 9.4 Build

Vite statics, served by axum itself on `[server.admin].listen` — a single binary (engine +
admin). Charts — by default `@mantine/charts` (Recharts); for high-frequency live charts the
fallback is `uPlot` (an optimization for later).

---

## 10. Documentation

**docmd** — a zero-config, framework-free SSG (~18kb JS), Markdown-in → static HTML, built-in
fuzzy search, container syntax (callouts/tabs/cards), themes, i18n. It does not conflict with
Vite/React (a separate Node CLI).

- **Autogen:** `proto/*.proto` and config structures → `.md` (protocol, server-api,
  config-reference); docmd picks them up alongside the handwritten guides. A CI run keeps the
  docs in sync with the code.
- **Trade-off:** there is no built-in live playground. If needed — a public route
  `/playground` in `web/` (the same SDK), linked from the docs. The content is not locked in (plain Markdown),
  migration to another SSG is cheap.

---

## 11. Client SDK (`packages/client-ts`)

A first-class package — "the library on the frontend". It carries all the client logic so that the
application code works at a high level (`subscribe`/`publish`/`on`) without knowing the protocol.

**Responsibilities:**

- **Transport**: WebSocket (default) with auto-fallback to SSE; behind a common interface.
- **Encoding**: protobuf-es (types from `packages/proto-gen`).
- **Subscription registry** — the source of truth: `{ channel → {last_position, sub_token?} }`,
  `last_position` is updated by incoming publications.
- **Reconnect**: jittered backoff, restoring all subscriptions in 1 RTT via `ConnectRequest.subs`,
  reuse of the cached JWT.
- **Recovery**: per-channel catch-up by `offset`/`epoch`; on `recovered=false` — a `needRefetch` signal.
- **Tokens**: the `getToken()` callback (connection) and `getSubToken(channel)` (private channels);
  renewal via `RefreshRequest`/`SubRefreshRequest` (variant B, without a reconnect).
- **API**: `connect()`, `newSubscription(channel)`, `sub.on('publication'|'join'|'leave')`,
  `sub.subscribe/unsubscribe()`, `publish/presence/history`.
- **Version**: negotiates `manifold.v1`; safely skips unknown `Push` variants.

The same package is reused by the admin (`web/`) for live `$metrics` and the opt. public `/playground`.

## 12. Operations (deploy / shutdown / observability)

- **Graceful shutdown / drain** (`[shutdown]`): on SIGTERM the node stops accepting new connections,
  `/ready` returns 503 (the LB takes it out), live ones are sent `Disconnect{reconnect:true}` → clients
  reconnect to other nodes (restore in 1 RTT, lossless), then — exit on
  `drain_timeout`. A deploy without a mass hard break.
- **Health/readiness** (`[server.health]`): `/health` (liveness) and `/ready` (readiness; accounts for
  drain and Redis availability) on a separate port for k8s/LB.
- **Node registry** (`redis.node_heartbeat`): each node writes a heartbeat to Redis; the Server API `info`
  and the admin Overview aggregate the **whole cluster**, not a single node.
- **Lifecycle events** (`[events]`, opt.): `connected`/`disconnected`/`subscribed`/
  `unsubscribed` → to the backend (HTTP batch webhook or gRPC stream) for analytics/cleanup. This is **not**
  authorization (that is on the JWT), but notifications.
- **Observability** (`[telemetry]`): structured logs (`json`), Prometheus `/metrics`, opt.
  OpenTelemetry tracing (OTLP).
- **Redis pub/sub sharding** (behind a `Broker` trait): at large volumes a single pub/sub is a
  bottleneck; the plan — Redis 7 `SPUBLISH`/`SSUBSCRIBE` (sharded). The seam is laid, implementation
  as needed.

## 13. Implementation plan (by dependencies)

1. **Core:** WS connect, JWT auth, sub/unsub by glob patterns, in-memory hub (single node).
   + health/readiness, graceful shutdown from the first step.
2. **Client SDK** (`client-ts`): transport, subscription registry, reconnect — needed to test the core.
3. **Broker abstraction + Redis** → multi-node (pub/sub fan-out) + node registry (heartbeat).
4. **SSE transport** (fallback) in the server and the SDK.
5. **Server API** (HTTP + gRPC), a unified `ApiService`, idempotency, control channel.
6. **Presence** (join/leave, online list, TTL, flap damping).
7. **History + recovery** (Redis Streams, `offset`/`epoch`, restore in 1 RTT without races).
8. **Lifecycle events** (`[events]`) + observability (Prometheus, opt. tracing).
9. **Admin UI** (Mantine) + `$metrics` + Prometheus.
10. **Docs** (docmd) + autogen from `.proto`/config.

---

## 14. Resolved questions

- ~~The exact `rate_limit` format and where to count.~~ **Resolved:** a token bucket `{rate, burst, scope}`;
  in the MVP only `scope = client` (local, in the node's memory) + the `[limits]` section per connection.
  Global `scope = channel/user` (Redis) — in the config schema, implementation deferred. On
  exceeding — `Error{rate_limited, temporary}`, not a disconnect.
- ~~Transport fallbacks (SSE/HTTP-streaming) — whether they are needed.~~ **Resolved:** WebSocket + SSE in the MVP,
  both behind a `Transport` trait. HTTP-streaming/WebTransport — additively later.
- ~~Protocol versioning.~~ **Resolved:** the major in the proto package (`manifold.v1`) + negotiation
  via the WS subprotocol (`Sec-WebSocket-Protocol: manifold.v1`) / SSE query (`?v=1`); additive
  changes within a major (Protobuf compatibility); the SDK safely skips unknown
  `oneof` variants; `protocol_version` in `ConnectRequest` for diagnostics; a major mismatch →
  `Disconnect{reconnect:false}` + `Error{temporary:false}`.
- ~~History backend: only Redis or a durable store for long-term retention.~~ **Resolved for the MVP:**
  **no DB** — recovery and history are entirely on Redis. A durable archive (rqlite/sqlite/Postgres) —
  a future extension behind a `HistoryStore` trait, not implemented in the MVP.
- Protocol versioning: how the client and server negotiate the `.proto` version.
- ~~OIDC for the admin UI (instead of a password).~~ **Resolved:** in the MVP — a password; OIDC behind the `AdminAuth` seam
  (`auth = "password" | "oidc"`), additively later.
```
