---
title: Manifold
description: Configurable realtime engine (WebSocket pub/sub)
---

# Manifold

A standalone realtime engine server (pub/sub over WebSocket): "like Centrifugo, but more
configurable". Runs independently. Monorepo: Rust backend + React frontend + Markdown docs.

- **[Architecture](/architecture)** — the full design document.

## Quick start (dev)

```bash
just dev        # bring up Redis + engine + admin + docs
```

## Parts

- `crates/` — Rust engine (server, core, broker, protocol)
- `packages/client-ts` — client SDK
- `web/` — admin UI (React + Mantine)
- `docs/` — this documentation (docmd)
