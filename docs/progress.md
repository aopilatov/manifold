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
| 3 | Broker-абстракция + Redis → мультинода (pub/sub fan-out) | ✅ |
| 4 | SSE-транспорт (фолбэк) в сервере и SDK | ✅ |
| 5 | Server API (HTTP + gRPC), единый `ApiService`, идемпотентность, control-канал | ✅ |
| 6 | Presence (join/leave, список онлайн, TTL) | ✅ |
| 7 | История + recovery (Redis Streams, offset/epoch) | ✅ |
| 8 | События жизненного цикла + observability (Prometheus, tracing) | ✅ |
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

## Этап 3 — Redis-брокер / мультинода ✅

Состояние каналов (offset/epoch/история/presence) вынесено из hub в `Broker`. Две реализации
за одним трейтом; hub теперь — только локальная маршрутизация.

| Реализация | Где | Что |
|---|---|---|
| `MemoryBroker` | `broker/memory.rs` | одна нода, всё в памяти (для `redis.enabled=false` и тестов) |
| `RedisBroker` | `broker/redis_broker.rs` | мультинода: Lua-публикация, pub/sub fan-out, presence в Redis |

**RedisBroker:**

- **publish** — Lua атомарно `INCR seq` + `XADD hist` (id = `offset-0`), затем `PUBLISH` сериализованного
  Push на `ch:{channel}`. offset/epoch — из Redis (глобальные на кластер).
- **fan-out** — фоновая задача держит pub/sub и `PSUBSCRIBE {prefix}:ch:*`, отдаёт пришедшее
  (в т.ч. с других нод) локальным подписчикам через трейт `Delivery` (инверсия: брокер не знает hub).
- **recovery** — `XRANGE` с `(offset-0`, сверка epoch.
- **presence** — ZSET `pz:{ch}` (score = expire_at) + HASH `ph:{ch}`; TTL и очистка протухших.

**Инверсия доставки:** `Delivery` объявлен в брокере, реализован в core (`HubDelivery` → `hub.fan_out`),
чтобы брокер не зависел от hub. Выбор брокера — по `[redis].enabled` в конфиге.

**Тесты (живой Redis, пропускаются если недоступен):**

- `cross_node_fanout` — publish на ноде A → доставлено на ноду B ✔
- `recovery_via_redis` — `XRANGE` догон offset 2,3; чужая epoch → `recovered=false` ✔
- `presence_shared_across_nodes` — presence общий между нодами ✔
- **`test/e2e_multinode.mjs`** — два реальных процесса сервера + SDK: публикация через node-1 →
  приём подписчиком на node-2. **E2E MULTINODE OK** ✔

**Побочно:** в core `ApiService` стал async, добавлен `ApiService::in_memory(cfg)` для одной ноды.

**Осознанные упрощения (TODO след. этапов):**

- **PSUBSCRIBE-all** вместо ленивой per-channel подписки (каждая нода получает все публикации и
  фильтрует локально). Ленивый `SUBSCRIBE`/`UNSUBSCRIBE` по первому/последнему подписчику — оптимизация.
- **Recovery boundary:** нет явной дедупликации на стыке «история ↔ live» (возможен дубль одной
  публикации при подписке); строгий no-gap/dedup — рефайнмент.
- **Presence flap:** `Leave` шлётся сразу при разрыве (TTL защищает список от «призраков», но
  гашение флапа `Leave` отложенным таймером — TODO).
- **idempotency / control-канал** (для Server API disconnect) — этап 5.

---

## Этап 4 — SSE-транспорт ✅

Фолбэк для сетей, режущих WS. Расщеплённая сессия, переиспользует тот же hub/ApiService.

**Сервер** (`server/sse.rs`):

- `GET /connection/sse?token=JWT` — downstream (EventSource): аутентификация, сессия в hub,
  стрим `Reply` как **base64(protobuf)** в `data:`. Первое событие — `ConnectResult` (несёт session_id).
- `POST /connection/sse/emit` (`X-Session-Id`, тело — protobuf `Command`) — upstream; ответ уходит
  вниз по SSE той же сессии (через `tx` в `ConnHandle`).
- Снятие сессии при разрыве — `CleanupGuard` (Drop → `api.cleanup`).
- Включается `[server.sse].enabled`; делит слушатель с WS.

**SDK** (`client-ts`): транспорт вынесен за интерфейс `Transport` (`transport.ts`):

- `WsTransport` — двунаправленный сокет (коннект — Connect-командой, батч-восстановление подписок).
- `SseTransport` — EventSource (вниз) + `fetch` POST (вверх); коннект выполняет GET, подписки
  восстанавливаются индивидуально. `transport: "ws" | "sse"` в опциях.
- `client.ts` стал транспорт-агностичным.

**Тест:** `test/e2e_sse.mjs` — против реального сервера через SSE: connect → subscribe → publish →
приём `hello-sse`. **E2E SSE OK** ✔ (в Node — полифилл `EventSource` из undici; в браузере глобальный).

**Упрощения (TODO):** нет нативного resume по `Last-Event-ID` (реконнект — общий клиентский);
`require_subprotocol`/origin-проверки для SSE отдельно не применяются.

---

## Этап 5 — Server API (HTTP + gRPC) ✅

Доверенная server-to-server сторона. Оба транспорта — тонкие обёртки над `ApiService::api_*`.
Auth — API-ключ (`Authorization: apikey <key>` + `allow` метода).

**Методы:** `publish` (идемпотентный), `broadcast`, `presence`, `presence_stats`, `history`,
`channels`, `info`, `disconnect`, `unsubscribe`, `user_online`. (`history_remove`, server-side
`subscribe`, `batch`, `publish_stream` — заглушки `TODO`.)

| Слой | Где |
|---|---|
| HTTP/JSON | `server/http_api.rs` (`POST /api/<method>`, `data` — base64) |
| gRPC | `server/grpc_api.rs` (tonic, сервис `socket.v1.ServerApi`) |
| Идемпотентность | `Broker.idempotency_get/put` (Redis `SET EX` / память) |
| Control-канал | `Broker.control_publish` + `Delivery.control` (Redis `{prefix}:control`) |

**Идемпотентность:** `idempotency_key` → закэшированная позиция; повторный publish не создаёт
новый offset. **Control-канал:** `disconnect`/`unsubscribe` адресуются соединению на любой ноде —
команда летит в Redis, нода-владелец её исполняет.

**Тесты (живой Redis, 2 ноды + SDK):**

- `test/e2e_serverapi.mjs` — HTTP publish на node-1 → приём подписчиком на node-2; HTTP presence
  видит подписчика; **HTTP disconnect по user → кросс-нодовый разрыв** ✔
- `test/e2e_grpc.mjs` — gRPC `Publish` → WS-подписчик получает сообщение ✔
- Идемпотентность (два publish с одним ключом → один offset) и `401` на неверный ключ — проверены ✔

**Упрощения (TODO):** `user_online`/`info` — локальные (без кластерной агрегации через реестр нод);
gRPC-методы `subscribe`/`batch`/`publish_stream`/`history_remove` — заглушки.

---

## Этап 8 — События жизненного цикла + observability ✅

**События жизненного цикла** (`[events]`): `connected`/`disconnected`/`subscribed`/`unsubscribed`
→ прикладной бэкенд (НЕ авторизация). За трейтом `EventSink` (core), HTTP-реализация
`HttpEventSink` (server, reqwest, fire-and-forget POST, фильтр по типам).

**Observability:**

- **Prometheus** `/metrics` (на health-порту): `socket_connections`, `socket_channels` (gauge),
  `socket_messages_published_total`, `socket_subscriptions_total`,
  `socket_connections_opened/closed_total` (counter). Счётчики — атомарные в `core/metrics.rs`,
  без зависимости от Prometheus; экспозиция формируется в server.
- **JSON-логи** — по `[telemetry].log_format = "json"`.

**Тест:** `test/e2e_events.mjs` — приёмник вебхуков + клиент (connect→subscribe→unsubscribe→
disconnect): получены все 4 события, `/metrics` содержит счётчики. ✔

**Упрощения (TODO):** OTLP-трейсинг (`tracing_enabled`/`otlp_endpoint`) не подключён — тяжёлые
зависимости, отложено; события — per-event POST (без батчинга/ретраев).

---

## Этап 9 — Admin UI ⬜

_Не начат._ См. [архитектуру, раздел 9](/architecture). Каркас `web/` (Mantine) уже есть. (Lua-публикация INCR+XADD+PUBLISH, ленивая
SUBSCRIBE/UNSUBSCRIBE, XRANGE recovery, presence-хэш с TTL, control pub/sub, idempotency).
Перенести историю/presence из in-memory hub в брокер.
