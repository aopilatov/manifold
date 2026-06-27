---
title: Прогресс
description: Статус реализации по этапам
---

# Прогресс реализации

Этапы — по плану из [архитектуры](/architecture) (раздел 13). Статусы:
✅ готово · 🚧 в работе · ⬜ не начато.

| # | Этап | Статус |
|---|------|--------|
| 1 | Ядро одной ноды (WS-коннект, JWT, sub/unsub по glob, in-memory hub) + health/shutdown | ✅ |
| 2 | Клиентский SDK (`client-ts`): транспорт, реестр подписок, реконнект | ✅ |
| 3 | Broker-абстракция + Redis → мультинода (pub/sub fan-out) + реестр нод | ⬜ |
| 4 | SSE-транспорт (фолбэк) в сервере и SDK | ⬜ |
| 5 | Server API (HTTP + gRPC), единый `ApiService`, идемпотентность, control-канал | ⬜ |
| 6 | Presence (join/leave, список онлайн, TTL, гашение флапа) | 🚧 |
| 7 | История + recovery (Redis Streams, offset/epoch, 1 RTT без гонок) | 🚧 |
| 8 | События жизненного цикла + observability (Prometheus, tracing) | ⬜ |
| 9 | Admin UI (Mantine) + `$metrics` + Prometheus | ⬜ |
| 10 | Docs (docmd) + автоген из `.proto`/config | 🚧 |

> Presence/история (6, 7) частично сделаны **in-memory** на этапе 1 (одна нода); полноценная
> мультинодовая версия на Redis — этап 3+.

---

## Этап 1 — Ядро одной ноды ✅

**Реализовано (рабочая логика, не скелет):**

| Компонент | Файл | Что делает |
|---|---|---|
| JWT-валидация | [`core/auth.rs`](https://github.com) | HMAC HS256/384/512, claims с capability-паттернами |
| Glob + доступ | `core/auth.rs`, `core/namespace.rs` | `*`/`**` матчинг, цепочка namespace↔JWT |
| Hub | `core/hub.rs` | offset/epoch, история (ring-buffer), presence, локальный fan-out |
| Оркестратор | `core/api.rs` | connect (восстановление subs за 1 RTT), subscribe/unsubscribe/publish/presence/history, cleanup |
| WS-транспорт | `server/ws.rs` | handshake → JWT → writer-задача → цикл Command→Reply (protobuf) |
| Boot | `server/main.rs` | конфиг → Hub → ApiService → роутер + health |

**Тесты (10, все зелёные):**

- `publish_fans_out_to_subscribers` — публикация доходит с `offset=1`
- `subscribe_denied_without_grant` — отказ без нужного паттерна в JWT
- `public_namespace_subscribes_without_token` — `news` (public) без токена
- `client_publish_to_public_feed_is_denied` — `publish=off` режет клиентскую публикацию
- `transient_publish_skips_history` — typing не пишется в историю
- `recovery_returns_missed_publications` — догон пропущенных offset, `recovered=true`
- + 3 glob-теста + парсинг `config.toml`

**Smoke:** бинарь стартует, `/health` и `/ready` → 200.

**Осталось скелетом (`TODO`) для следующих этапов:**

- WS: проверка Origin/subprotocol, `handshake_timeout`, `write_buffer_limit`, ping/pong-таймауты
- `RefreshRequest` / `SubRefreshRequest` (вариант B)
- graceful shutdown (SIGTERM → drain → `Disconnect{reconnect:true}`)

**Известное ограничение (снято на этапе 2):** реальный WS round-trip теперь покрыт e2e-тестом
SDK↔сервер (`packages/client-ts/test/e2e.mjs`). Cargo-тесты ядра по-прежнему через `ApiService`.

---

## Этап 2 — Клиентский SDK ✅

**Реализовано:**

| Пакет | Что сделано |
|---|---|
| `packages/proto-gen` | protobuf-es типы сгенерированы из `proto/socket.proto` (buf), собираются в `dist` |
| `packages/client-ts` | полный SDK: транспорт WS, кодек protobuf, корреляция Command↔Reply по id |

**Возможности SDK** (`SocketClient` / `Subscription`):

- Реестр подписок как источник истины; восстановление **всех подписок за 1 RTT** через `Connect.subs`.
- Реконнект с джиттер-бэкоффом (`jitteredDelay`), переиспользование кэшированного JWT.
- Recovery: `recover`/`position`, догон пропущенного, обновление позиции по `offset`.
- Refresh токена по таймеру (вариант B), ping по `pingIntervalMs`.
- API: `connect`, `newSubscription`, `sub.on(publication|join|leave|...)`, `subscribe`/`unsubscribe`/`publish`/`presence`.
- Безопасный скип неизвестных push-вариантов (через `switch` по `case`).

**Тесты:**

- `test/backoff.test.ts` — границы и распределение джиттера (2 теста, ✔).
- `test/e2e.mjs` — **живой round-trip против Rust-сервера**: connect → subscribe → publish →
  приём `hello-e2e` → presence `[smoke-1]`. ✔

**Побочно:** в сервере реализовано согласование WebSocket-subprotocol `socket.v1`
(`ws.protocols([...])`) — часть `require_subprotocol`; без него undici-WebSocket рвал коннект.

---

## Этап 3 — Redis-брокер / мультинода ⬜

_Не начат._ Реализовать `Broker` для Redis (Lua-публикация INCR+XADD+PUBLISH, ленивая
SUBSCRIBE/UNSUBSCRIBE, XRANGE recovery, presence-хэш с TTL, control pub/sub, idempotency).
Перенести историю/presence из in-memory hub в брокер.
