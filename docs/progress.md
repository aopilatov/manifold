---
title: Progress
description: Implementation status by stage
---

# Implementation progress

Stages follow the plan in the [architecture](/architecture) (section 13). Statuses:
✅ done · 🚧 in progress · ⬜ not started.

| # | Stage | Status |
|---|------|--------|
| 1 | Single-node core (WS connect, JWT, sub/unsub by glob, in-memory hub) + health/shutdown | ✅ |
| 2 | Client SDK (`client-ts`): transport, subscription registry, reconnect | ✅ |
| 3 | Broker abstraction + Redis → multi-node (pub/sub fan-out) | ✅ |
| 4 | SSE transport (fallback) in server and SDK | ✅ |
| 5 | Server API (HTTP + gRPC), unified `ApiService`, idempotency, control channel | ✅ |
| 6 | Presence (join/leave, online list, TTL) | ✅ |
| 7 | History + recovery (Redis Streams, offset/epoch) | ✅ |
| 8 | Lifecycle events + observability (Prometheus, tracing) | ✅ |
| 9 | Admin UI (Mantine) + Prometheus | ✅ |
| 10 | Docs (docmd) + autogen from `.proto`/config | ✅ |

> Presence/history (6, 7) were partially done **in-memory** in stage 1 (single node); the full
> multi-node version on Redis — stage 3+.

---

## Stage 1 — Single-node core ✅

**Implemented (working logic, not a skeleton):**

| Component | File | What it does |
|---|---|---|
| JWT validation | [`core/auth.rs`](https://github.com) | HMAC HS256/384/512, claims with capability patterns |
| Glob + access | `core/auth.rs`, `core/namespace.rs` | `*`/`**` matching, namespace↔JWT chain |
| Hub | `core/hub.rs` | offset/epoch, history (ring-buffer), presence, local fan-out |
| Orchestrator | `core/api.rs` | connect (restore subs in 1 RTT), subscribe/unsubscribe/publish/presence/history, cleanup |
| WS transport | `server/ws.rs` | handshake → JWT → writer task → Command→Reply loop (protobuf) |
| Boot | `server/main.rs` | config → Hub → ApiService → router + health |

**Tests (10, all green):**

- `publish_fans_out_to_subscribers` — a publication arrives with `offset=1`
- `subscribe_denied_without_grant` — rejected without the required pattern in the JWT
- `public_namespace_subscribes_without_token` — `news` (public) without a token
- `client_publish_to_public_feed_is_denied` — `publish=off` blocks client-side publishing
- `transient_publish_skips_history` — typing is not written to history
- `recovery_returns_missed_publications` — catch-up of missed offsets, `recovered=true`
- + 3 glob tests + `config.toml` parsing

**Smoke:** the binary starts, `/health` and `/ready` → 200.

**Remaining as a skeleton (`TODO`) for the next stages:**

- WS: Origin/subprotocol check, `handshake_timeout`, `write_buffer_limit`, ping/pong timeouts
- `RefreshRequest` / `SubRefreshRequest` (variant B)
- graceful shutdown (SIGTERM → drain → `Disconnect{reconnect:true}`)

**Known limitation (lifted in stage 2):** the real WS round-trip is now covered by an e2e test
SDK↔server (`packages/client-ts/test/e2e.mjs`). The core Cargo tests still go through `ApiService`.

---

## Stage 2 — Client SDK ✅

**Implemented:**

| Package | What was done |
|---|---|
| `packages/proto-gen` | protobuf-es types generated from `proto/manifold.proto` (buf), built into `dist` |
| `packages/client-ts` | full SDK: WS transport, protobuf codec, Command↔Reply correlation by id |

**SDK capabilities** (`ManifoldClient` / `Subscription`):

- Subscription registry as the source of truth; restoring **all subscriptions in 1 RTT** via `Connect.subs`.
- Reconnect with jittered backoff (`jitteredDelay`), reuse of the cached JWT.
- Recovery: `recover`/`position`, catch-up of missed messages, position update by `offset`.
- Token refresh on a timer (variant B), ping by `pingIntervalMs`.
- API: `connect`, `newSubscription`, `sub.on(publication|join|leave|...)`, `subscribe`/`unsubscribe`/`publish`/`presence`.
- Safe skipping of unknown push variants (via `switch` over `case`).

**Tests:**

- `test/backoff.test.ts` — jitter bounds and distribution (2 tests, ✔).
- `test/e2e.mjs` — **live round-trip against the Rust server**: connect → subscribe → publish →
  receive `hello-e2e` → presence `[smoke-1]`. ✔

**Side effect:** the server got WebSocket-subprotocol negotiation `manifold.v1`
(`ws.protocols([...])`) — part of `require_subprotocol`; without it undici-WebSocket dropped the connection.

---

## Stage 3 — Redis broker / multi-node ✅

Channel state (offset/epoch/history/presence) was moved out of the hub into `Broker`. Two
implementations behind one trait; the hub is now just local routing.

| Implementation | Where | What |
|---|---|---|
| `MemoryBroker` | `broker/memory.rs` | single node, everything in memory (for `redis.enabled=false` and tests) |
| `RedisBroker` | `broker/redis_broker.rs` | multi-node: Lua publish, pub/sub fan-out, presence in Redis |

**RedisBroker:**

- **publish** — Lua atomically `INCR seq` + `XADD hist` (id = `offset-0`), then `PUBLISH` the serialized
  Push to `ch:{channel}`. offset/epoch — from Redis (cluster-global).
- **fan-out** — a background task holds the pub/sub and `PSUBSCRIBE {prefix}:ch:*`, delivering what arrives
  (including from other nodes) to local subscribers via the `Delivery` trait (inversion: the broker does not know the hub).
- **recovery** — `XRANGE` from `(offset-0`, epoch comparison.
- **presence** — ZSET `pz:{ch}` (score = expire_at) + HASH `ph:{ch}`; TTL and cleanup of expired entries.

**Delivery inversion:** `Delivery` is declared in the broker, implemented in core (`HubDelivery` → `hub.fan_out`),
so the broker does not depend on the hub. Broker choice — by `[redis].enabled` in the config.

**Tests (live Redis, skipped if unavailable):**

- `cross_node_fanout` — publish on node A → delivered on node B ✔
- `recovery_via_redis` — `XRANGE` catch-up of offsets 2,3; foreign epoch → `recovered=false` ✔
- `presence_shared_across_nodes` — presence shared across nodes ✔
- **`test/e2e_multinode.mjs`** — two real server processes + SDK: publish via node-1 →
  receipt by a subscriber on node-2. **E2E MULTINODE OK** ✔

**Side effect:** in core `ApiService` became async, added `ApiService::in_memory(cfg)` for a single node.

**Deliberate simplifications (TODO for later stages):**

- **PSUBSCRIBE-all** instead of lazy per-channel subscription (each node receives all publications and
  filters locally). Lazy `SUBSCRIBE`/`UNSUBSCRIBE` by first/last subscriber — an optimization.
- **Recovery boundary:** no explicit deduplication at the "history ↔ live" seam (a single publication may be
  duplicated on subscribe); strict no-gap/dedup — a refinement.
- **Presence flap:** `Leave` is sent immediately on a break (TTL protects the list from "ghosts", but
  damping the `Leave` flap with a deferred timer — TODO).
- **idempotency / control channel** (for Server API disconnect) — stage 5.

---

## Stage 4 — SSE transport ✅

A fallback for networks that cut WS. A split session, reusing the same hub/ApiService.

**Server** (`server/sse.rs`):

- `GET /connection/sse?token=JWT` — downstream (EventSource): authentication, session in the hub,
  streaming `Reply` as **base64(protobuf)** in `data:`. First event — `ConnectResult` (carries session_id).
- `POST /connection/sse/emit` (`X-Session-Id`, body — protobuf `Command`) — upstream; the reply goes
  back down the SSE of the same session (via `tx` in `ConnHandle`).
- Session teardown on a break — `CleanupGuard` (Drop → `api.cleanup`).
- Enabled by `[server.sse].enabled`; shares the listener with WS.

**SDK** (`client-ts`): the transport was extracted behind a `Transport` interface (`transport.ts`):

- `WsTransport` — a bidirectional socket (connect — via the Connect command, batch subscription restore).
- `SseTransport` — EventSource (down) + `fetch` POST (up); connect runs a GET, subscriptions
  are restored individually. `transport: "ws" | "sse"` in the options.
- `client.ts` became transport-agnostic.

**Test:** `test/e2e_sse.mjs` — against a real server over SSE: connect → subscribe → publish →
receive `hello-sse`. **E2E SSE OK** ✔ (in Node — an `EventSource` polyfill from undici; in the browser the global one).

**Simplifications (TODO):** no native resume via `Last-Event-ID` (reconnect — the common client one);
`require_subprotocol`/origin checks for SSE are not applied separately.

---

## Stage 5 — Server API (HTTP + gRPC) ✅

The trusted server-to-server side. Both transports — thin wrappers over `ApiService::api_*`.
Auth — API key (`Authorization: apikey <key>` + method `allow`).

**Methods:** `publish` (idempotent), `broadcast`, `presence`, `presence_stats`, `history`,
`channels`, `info`, `disconnect`, `unsubscribe`, `user_online`. (`history_remove`, server-side
`subscribe`, `batch`, `publish_stream` — `TODO` stubs.)

| Layer | Where |
|---|---|
| HTTP/JSON | `server/http_api.rs` (`POST /api/<method>`, `data` — base64) |
| gRPC | `server/grpc_api.rs` (tonic, service `manifold.v1.ServerApi`) |
| Idempotency | `Broker.idempotency_get/put` (Redis `SET EX` / memory) |
| Control channel | `Broker.control_publish` + `Delivery.control` (Redis `{prefix}:control`) |

**Idempotency:** `idempotency_key` → cached position; a repeated publish does not create a
new offset. **Control channel:** `disconnect`/`unsubscribe` are addressed to a connection on any node —
the command flies into Redis, the node owning the connection executes it.

**Tests (live Redis, 2 nodes + SDK):**

- `test/e2e_serverapi.mjs` — HTTP publish on node-1 → receipt by a subscriber on node-2; HTTP presence
  sees the subscriber; **HTTP disconnect by user → cross-node break** ✔
- `test/e2e_grpc.mjs` — gRPC `Publish` → WS subscriber receives the message ✔
- Idempotency (two publishes with one key → one offset) and `401` on a wrong key — verified ✔

**Simplifications (TODO):** `user_online`/`info` — local (without cluster aggregation via the node registry);
gRPC methods `subscribe`/`batch`/`publish_stream`/`history_remove` — stubs.

---

## Stage 8 — Lifecycle events + observability ✅

**Lifecycle events** (`[events]`): `connected`/`disconnected`/`subscribed`/`unsubscribed`
→ the application backend (NOT authorization). Behind the `EventSink` trait (core), HTTP implementation
`HttpEventSink` (server, reqwest, fire-and-forget POST, filter by types).

**Observability:**

- **Prometheus** `/metrics` (on the health port): `manifold_connections`, `manifold_channels` (gauge),
  `manifold_messages_published_total`, `manifold_subscriptions_total`,
  `manifold_connections_opened/closed_total` (counter). Counters — atomic in `core/metrics.rs`,
  with no dependency on Prometheus; the exposition is built in the server.
- **JSON logs** — via `[telemetry].log_format = "json"`.

**Test:** `test/e2e_events.mjs` — a webhook receiver + client (connect→subscribe→unsubscribe→
disconnect): all 4 events received, `/metrics` contains the counters. ✔

**Simplifications (TODO):** OTLP tracing (`tracing_enabled`/`otlp_endpoint`) is not wired up — heavy
dependencies, deferred; events — per-event POST (without batching/retries).

---

## Stage 9 — Admin UI ✅

The third access perimeter: password → session (admin JWT in an httpOnly cookie). The backend — thin wrappers
over `ApiService::api_*`; static `web/dist` is served by the same server.

**Backend** (`server/admin.rs`): `POST /admin/login`, `/admin/me`, `/admin/info` (metrics),
`/admin/channels`, `/admin/presence`, `/admin/publish`, `/admin/disconnect`. Session — a JWT
signed with the password; guard on all endpoints; insecure-default (empty password on a public
interface → refuses to start).

**Frontend** (`web/`, React + Mantine, Vite): login → AppShell with sections
**Overview** (live metrics, polling `/admin/info` every 2s), **Channels** (list + presence on click),
**Publish** (form). Client — `web/src/api.ts` (`credentials: include`).

**Verified:**

- Backend (curl): `401` without a session, login `200` + cookie, wrong password `401`, `info`/`channels`/
  `publish` under a session, static `/` → `200 text/html`. ✔
- UI: `tsc` + `vite build` (779 modules) ✔; **visually via Preview** — login → dashboard
  (6 metric cards, navbar) → Publish section. ✔

**Simplifications (TODO):** live metrics via **polling `/admin/info`**, not dogfooding through
`$metrics`-WS; the Connections/Namespaces/Metrics-charts sections are not done; SPA deep-link fallback
returns `404` (the app is state-based, loads from `/`); OIDC — behind the `auth = "password"|"oidc"` seam.

---

## Stage 10 — Documentation (autogen) ✅

The reference docs are generated from sources of truth and built by docmd into static output.

**Generator** (`docs/generate.mjs`, protobufjs reflection + a config parser):

- `proto/manifold.proto` → `protocol.md` (client messages with field/oneof tables) +
  `server-api.md` (gRPC service `ServerApi` + `*Api*` messages).
- `config.toml` → `config-reference.md` (sections + key/example/description from inline comments).

**Pipeline:** `pnpm docs:gen` (generation only) / `docs:build` (gen → docmd build) /
`docs:dev`. docmd builds **6 pages** + full-text search into `dist-docs/`.

**Verified:** `docs:build` → "Build complete. Generated 6 pages" ✔; **visually via Preview** —
the docmd site (dark theme, navbar, autogen page "Protocol" with the `Command` table, search,
table of contents). ✔

**Simplifications (TODO):** descriptions in `config-reference` come from inline comments (some keys
without a description); no deep documentation of semantics (handwritten guides — separately).

---

## Summary

All 10 stages are implemented and verified (unit/integration tests + live e2e + visual
screenshots). The engine works end-to-end: single node and multi-node (Redis), WS and SSE, client SDK,
Server API (HTTP+gRPC), events/metrics, admin UI, auto-documentation. (Lua publish INCR+XADD+PUBLISH, lazy
SUBSCRIBE/UNSUBSCRIBE, XRANGE recovery, presence hash with TTL, control pub/sub, idempotency).
Move history/presence out of the in-memory hub into the broker.
