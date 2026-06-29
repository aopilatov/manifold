# Manifold

A configurable realtime engine (WebSocket pub/sub) — "like Centrifugo, but more configurable".
A standalone server that runs independently. Monorepo: Rust backend + React frontend + docs.

> Full design — [`docs/architecture.md`](docs/architecture.md).

## Install

### Docker

Multi-arch image (amd64 + arm64) on Docker Hub: **[saxikopilatov/manifold](https://hub.docker.com/r/saxikopilatov/manifold)**.

```bash
docker pull saxikopilatov/manifold:1.0.0

docker run -d --name manifold \
  -p 8000:8000 -p 8001:8001 -p 8002:8002 -p 8004:8004 \
  -e JWT_HMAC_SECRET=change-me -e ADMIN_PASSWORD=change-me \
  -v "$(pwd)/config.toml:/app/config.toml" \
  saxikopilatov/manifold:1.0.0
```

Ports: `8000` WebSocket · `8001` HTTP Server API · `8002` gRPC Server API · `8004` health/metrics.
The admin UI (`8003`) binds to `127.0.0.1` by default — see the security notes in the design doc.

Engine + Redis together (multi-node) via Compose:

```bash
docker compose up        # builds from the Dockerfile and starts redis + server
```

### Client SDK (npm)

**[manifold-client](https://www.npmjs.com/package/manifold-client)** — TypeScript SDK: WS/SSE transport, reconnect, recovery, subscription registry.

```bash
npm install manifold-client
```

```ts
import { ManifoldClient } from "manifold-client";

const client = new ManifoldClient({
  url: "ws://localhost:8000/connection/websocket",
  getToken: async () => "<JWT issued by your backend>",
});

await client.connect();

const sub = client.newSubscription("chat:room:42");
sub.on("publication", (p) => console.log(new TextDecoder().decode(p.data)));
await sub.subscribe();

await sub.publish(new TextEncoder().encode("hello"));
```

> In the browser `WebSocket`/`EventSource` are global. In Node, `WebSocket` is global (Node ≥ 22);
> for the SSE transport polyfill `EventSource` (e.g. from `undici`). The generated protobuf types
> live in **[manifold-proto-gen](https://www.npmjs.com/package/manifold-proto-gen)** (a dependency of the SDK).

## Structure

```
manifold/
├── crates/
│   ├── server/      # axum: WS/SSE, Server API (HTTP+gRPC), admin, health
│   ├── core/        # config, auth (JWT+glob), hub, namespace, ApiService
│   ├── broker/      # trait Broker + Redis implementation
│   ├── protocol/    # prost/tonic types from proto/
│   └── loadtest/    # load/stress test binary
├── proto/           # manifold.proto — the single contract
├── packages/
│   ├── client-ts/   # client SDK (reconnect, recovery, subscription registry)
│   └── proto-gen/   # protobuf-es types (shared by client-ts and web)
├── web/             # admin UI (React + Mantine)
├── docs/            # documentation (docmd) + architecture.md
├── config.toml      # engine config
├── justfile         # orchestration
└── docker-compose.yml
```

## Requirements

- Rust ≥ 1.80, `cargo`
- Node ≥ 20, `pnpm`
- Docker (Redis)
- `just` (optional)

## Quick start

```bash
just infra-up        # Redis
just server          # engine (cargo run -p manifold-server)
just web             # admin UI (Vite)
just docs            # documentation (docmd)
```

## Stack

Rust · axum · tokio · Protobuf · Redis · React · Mantine · docmd
