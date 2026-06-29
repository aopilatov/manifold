---
title: Manifold
description: Configurable realtime engine (WebSocket pub/sub)
---

# Manifold

A standalone realtime engine server (pub/sub over WebSocket): "like Centrifugo, but more
configurable". Runs independently. Monorepo: Rust backend + React frontend + Markdown docs.

- **[Architecture](/architecture)** — the full design document.

## Install

### Docker

Multi-arch image (amd64 + arm64): **[saxikopilatov/manifold](https://hub.docker.com/r/saxikopilatov/manifold)**.

```bash
docker pull saxikopilatov/manifold:1.0.0

docker run -d --name manifold \
  -p 8000:8000 -p 8001:8001 -p 8002:8002 -p 8004:8004 \
  -e JWT_HMAC_SECRET=change-me -e ADMIN_PASSWORD=change-me \
  -v "$(pwd)/config.toml:/app/config.toml" \
  saxikopilatov/manifold:1.0.0
```

Ports: `8000` WebSocket · `8001` HTTP Server API · `8002` gRPC Server API · `8004` health/metrics
(admin UI `8003` binds to `127.0.0.1` by default). Engine + Redis together: `docker compose up`.

### Client SDK (npm)

**[manifold-client](https://www.npmjs.com/package/manifold-client)** — TypeScript SDK (WS/SSE, reconnect, recovery).

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

> Browser: `WebSocket`/`EventSource` are global. Node: `WebSocket` is global (≥ 22); polyfill
> `EventSource` (from `undici`) for the SSE transport. Protobuf types:
> **[manifold-proto-gen](https://www.npmjs.com/package/manifold-proto-gen)**.

## Quick start (dev)

```bash
just dev        # bring up Redis + engine + admin + docs
```

## Parts

- `crates/` — Rust engine (server, core, broker, protocol)
- `packages/client-ts` — client SDK
- `web/` — admin UI (React + Mantine)
- `docs/` — this documentation (docmd)
