# Socket

Настраиваемый realtime-движок (WebSocket pub/sub) — «как Centrifugo, но настраиваемее».
Самостоятельный сервер, запускается независимо. Монорепо: Rust-бэкенд + React-фронтенд + доки.

> Полный дизайн — [`docs/architecture.md`](docs/architecture.md).

## Структура

```
socket/
├── crates/
│   ├── server/      # axum: WS/SSE, Server API (HTTP+gRPC), admin, health
│   ├── core/        # конфиг, auth (JWT+glob), hub, namespace, ApiService
│   ├── broker/      # trait Broker + Redis-реализация
│   └── protocol/    # prost/tonic типы из proto/
├── proto/           # socket.proto — единый контракт
├── packages/
│   ├── client-ts/   # клиентский SDK (реконнект, recovery, реестр подписок)
│   └── proto-gen/   # protobuf-es типы (общие для client-ts и web)
├── web/             # admin UI (React + Mantine)
├── docs/            # документация (docmd) + architecture.md
├── config.toml      # конфиг движка
├── justfile         # оркестрация
└── docker-compose.yml
```

## Требования

- Rust ≥ 1.80, `cargo`
- Node ≥ 20, `pnpm`
- Docker (Redis)
- `just` (опционально)

## Быстрый старт

```bash
just infra-up        # Redis
just server          # движок (cargo run -p socket-server)
just web             # admin UI (Vite)
just docs            # документация (docmd)
```

## Статус

Этап 1 (ядро одной ноды) реализован и протестирован; остальное — скелет с `TODO`.
Полный статус по этапам — [`docs/progress.md`](docs/progress.md). План реализации —
[`docs/architecture.md`](docs/architecture.md) (раздел 13).

## Стек

Rust · axum · tokio · Protobuf · Redis · React · Mantine · docmd
