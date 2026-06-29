# Socket

A configurable realtime engine (WebSocket pub/sub) — "like Centrifugo, but more configurable".
A standalone server that runs independently. Monorepo: Rust backend + React frontend + docs.

> Full design — [`docs/architecture.md`](docs/architecture.md).

## Structure

```
socket/
├── crates/
│   ├── server/      # axum: WS/SSE, Server API (HTTP+gRPC), admin, health
│   ├── core/        # config, auth (JWT+glob), hub, namespace, ApiService
│   ├── broker/      # trait Broker + Redis implementation
│   └── protocol/    # prost/tonic types from proto/
├── proto/           # socket.proto — the single contract
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
just server          # engine (cargo run -p socket-server)
just web             # admin UI (Vite)
just docs            # documentation (docmd)
```

## Stack

Rust · axum · tokio · Protobuf · Redis · React · Mantine · docmd
