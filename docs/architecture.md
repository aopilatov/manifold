---
title: Архитектура
description: Дизайн-документ движка реалтайма (WebSocket pub/sub)
---

# Socket — дизайн-документ

Самостоятельный сервер-движок реалтайма (pub/sub поверх WebSocket): «как Centrifugo,
но настраиваемее». Деплоится и запускается независимо, не встраивается как библиотека.
Монорепо: Rust-бэкенд + React-фронтенд (admin) + Markdown-документация.

---

## 1. Цели и не-цели

**Цели:**

- Публичные каналы — подписка **без токена**, с динамической под/отпиской на подканалы.
- Приватные каналы — то же самое, но с авторизацией подписки.
- Гибкая модель прав: список разрешённых **паттернов каналов** прямо в JWT.
- Мультинода с первого дня (горизонтальное масштабирование).
- Presence, история сообщений и довосстановление (recovery).
- Server API для публикации из прикладного бэкенда (HTTP **и** gRPC).
- Admin/monitoring UI.

**Не-цели:**

- Движок **не** хранит пользователей и **не** выдаёт токены — это делает внешний (проектный) бэкенд.
- Движок **не** парсит прикладной payload сообщений (`data` всегда `bytes`).

---

## 2. Технологический стек

| Часть | Стек |
|---|---|
| **Backend** | Rust: `axum` + `tokio`; свой протокол на **Protobuf** (`prost`); gRPC через `tonic` |
| **Брокер** | **Redis** (pub/sub + история + presence), скрыт за trait `Broker` (позже — NATS) |
| **Client SDK** | TS-пакет (`client-ts`): транспорт, реконнект, реестр подписок, recovery, protobuf |
| **Frontend (admin)** | Vite + React + TS + **Mantine**; live-данные через `protobuf-es`; зависит от `client-ts` |
| **Docs** | **docmd** (zero-config SSG, framework-free, Markdown-in → static HTML) |
| **Монорепо** | Cargo workspace + Vite + docmd; оркестрация через `just` |

Единого «фуллстек Rust+React» фреймворка не существует — стандарт здесь монорепо из
независимых частей, связанных общим контрактом `.proto`.

---

## 3. Структура репозитория

```
socket/
├── Cargo.toml               # cargo workspace
├── proto/                   # .proto — ЕДИНЫЙ контракт (источник истины)
├── crates/
│   ├── server/              # axum: WS-сервер, HTTP API, gRPC API, раздача admin-статики
│   │   ├── ws/              # клиентский WebSocket-протокол
│   │   ├── http_api/        # Server API: HTTP/JSON адаптер
│   │   └── grpc_api/        # Server API: gRPC (tonic) адаптер
│   ├── core/                # ApiService, hub, реестр каналов, glob-матчинг, auth
│   ├── protocol/            # prost-сгенерированные типы из proto/
│   └── broker/              # trait Broker + Redis-реализация
├── packages/
│   ├── client-ts/           # КЛИЕНТСКИЙ SDK (TS): реконнект, реестр подписок, recovery, protobuf
│   └── proto-gen/           # сгенерированные protobuf-es типы (общие для client-ts и web)
├── web/                     # React + Mantine admin UI (зависит от client-ts)
├── docs/                    # docmd: Markdown-документация (+ автоген из proto/config)
├── config.toml              # конфиг сервера
├── justfile                 # dev/build/codegen
└── docker-compose.yml       # server + redis
```

**Кодоген типов:** `proto/*.proto` → Rust (`prost`) и TS (`protobuf-es`). Фронт и бэк
физически не разъезжаются по схеме. Часть `.md`-доков тоже генерится из `.proto` и
config-структур.

---

## 4. Протокол (клиент ⇆ сервер)

Бинарный, на Protobuf. Летят `Command` (от клиента) и `Reply` (от сервера).
`Reply` — это либо ответ на команду (тот же `id`), либо асинхронный `Push` (`id = 0`).

### Транспорты (за trait `Transport`)

Ядро (сессия/hub/recovery/auth) работает с `Stream<Command>` + `Sink<Reply>` и транспорт
не знает. В MVP — два:

```rust
trait Transport {
    fn commands(&mut self) -> impl Stream<Item = Command>;  // вверх (client→server)
    fn replies(&mut self)  -> impl Sink<Reply>;             // вниз (server→client)
}
```

- **WebSocket** — один двунаправленный сокет, сырые бинарные кадры.
- **SSE** (фолбэк для сетей, режущих WS) — расщеплённая сессия:
  `GET /connection/sse` (EventSource, downstream) + `POST /connection/sse/emit`
  (Command вверх, коррелируется `X-Session-Id`). SSE текстовый → кадры как
  **base64(protobuf)**. Установка: EventSource с токеном в query/cookie → сервер создаёт
  сессию в том же hub → первым событием шлёт `session_id` + `ConnectResult`.
  **Recovery-синергия:** нативный `Last-Event-ID` при авто-реконнекте EventSource ложится на
  `StreamPosition` — сервер досылает сессию с нужной позиции.
- SSE/HTTP-streaming/WebTransport-расширения — аддитивны, отдельным `impl Transport`.

### 4.1 Конверты

```protobuf
syntax = "proto3";
package socket.v1;

message Command {
  uint32 id = 1;              // корреляционный id, уникален в рамках соединения
  oneof method {
    ConnectRequest     connect     = 2;
    SubscribeRequest   subscribe   = 3;
    UnsubscribeRequest unsubscribe = 4;
    PublishRequest     publish     = 5;
    PresenceRequest    presence    = 6;
    HistoryRequest     history     = 7;
    PingRequest        ping        = 8;
    RefreshRequest     refresh     = 9;   // продление СОЕДИНЕНИЯ новым JWT
    SubRefreshRequest  sub_refresh = 10;  // продление ПОДПИСКИ новым sub-токеном
  }
}

message Reply {
  uint32 id = 1;              // 0 ⇒ асинхронный Push
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
  uint32 code = 1;            // стабильный машинный код
  string message = 2;
  bool   temporary = 3;       // true ⇒ клиенту имеет смысл повторить (backoff)
}
```

### 4.2 Команды клиент→сервер

```protobuf
message ConnectRequest {
  string token = 1;                  // connection JWT (claim channels[])
  map<string, SubscribeRequest> subs = 2;  // батч-восстановление подписок за 1 RTT (реконнект)
  map<string, string> headers = 3;
  string name = 4;                   // имя SDK для отладки
}
message SubscribeRequest {
  string channel = 1;                // "chat:room:42"
  string token = 2;                  // опц. отдельный sub-токен
  bool   recover = 3;
  StreamPosition position = 4;       // с какого offset/epoch восстанавливать
}
message UnsubscribeRequest { string channel = 1; }
message PublishRequest {
  string channel = 1;
  bytes  data = 2;
  bool   transient = 3;   // fire-and-forget: минует историю/offset (typing, эфемерные сигналы)
}
message PresenceRequest { string channel = 1; }
message HistoryRequest {
  string channel = 1; int32 limit = 2; StreamPosition since = 3; bool reverse = 4;
}
message RefreshRequest    { string token = 1; }
message SubRefreshRequest { string channel = 1; string token = 2; }
message PingRequest {}
```

### 4.3 Результаты и асинхронные пуши

```protobuf
message ConnectResult {
  string client = 1;                 // id соединения
  uint32 ping_interval_ms = 2;
  uint32 expires_in_s = 3;           // 0 = бессрочно
  bytes  data = 4;
  map<string, SubscribeResult> subs = 5;   // результат каждой восстановленной подписки
  string session = 6;                // опц. id для server-side resume
}
message SubscribeResult {
  bool recoverable = 1;
  StreamPosition position = 2;
  bool recovered = 3;
  repeated Publication publications = 4;  // досылка пропущенного
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
    Unsubscribe unsubscribe = 5;     // сервер принудительно отписал
    Disconnect  disconnect  = 6;     // сервер закрывает соединение
  }
}

message Publication {
  bytes  data = 1;                   // прикладной payload (движок не парсит)
  uint64 offset = 2;                 // позиция в потоке канала (recovery)
  ClientInfo info = 3;
  map<string, string> tags = 4;
}
message Join  { ClientInfo info = 1; }
message Leave { ClientInfo info = 1; }
message Unsubscribe { uint32 code = 1; string reason = 2; }
message Disconnect  { uint32 code = 1; string reason = 2; bool reconnect = 3; }

message ClientInfo { string user = 1; string client = 2; bytes conn_info = 3; bytes chan_info = 4; }

// Фундамент recovery
message StreamPosition {
  uint64 offset = 1;                 // монотонный номер последнего сообщения
  string epoch = 2;                  // метка жизни потока; смена ⇒ история сброшена
}
```

### 4.4 Решения протокола

- `id`-корреляция; `id = 0` зарезервирован под `Push`. Один WS-кадр = один `Command`/`Reply`
  (батчинг можно добавить позже отдельным типом, не ломая это).
- `data` — всегда `bytes`: движок универсален, payload — на усмотрение клиентов.
- `Error.temporary` управляет ретраями SDK.
- **Истечение токенов — вариант B (refresh по соединению):** SDK через колбэк `getToken()`
  берёт у внешнего бэкенда новый токен и шлёт его в `RefreshRequest` / `SubRefreshRequest`.
  Соединение/подписка живут дальше — без реконнекта, без presence-шума, без reconnect storm.
- **Версионирование:** мажор — в пакете proto (`socket.v1`); согласование через WS-subprotocol
  (`Sec-WebSocket-Protocol: socket.v1`) / SSE-квери (`?v=1`). Внутри мажора — только аддитивные
  изменения (номера полей не переиспользуем). SDK обязан **безопасно скипать неизвестные**
  `oneof`-варианты `Push`. `protocol_version` в `ConnectRequest` — для диагностики/мягких гейтов;
  несовпадение мажора → `Disconnect{reconnect:false}`.

---

## 5. Аутентификация и права

Модель Centrifugo: токены выдаёт **внешний бэкенд**, движок только проверяет подпись.

### 5.1 Connection JWT с capability-паттернами

В connection JWT — claim `channels` со списком разрешённых **glob-паттернов** и прав:

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

- На `subscribe` движок матчит запрошенный канал против паттернов; совпало → пускает.
- Динамика сохраняется: клиент свободно под/отписывается **в пределах выданных паттернов**,
  без похода в бэкенд на каждую подписку.
- `allow` маппится на методы: `subscribe`→`sub`, `publish`→`pub`, `presence`→`presence`,
  `history`→`history`.

### 5.2 Glob-семантика

- Разделитель сегментов — `:`. Namespace = **первый сегмент** (`chat:room:42` → ns `chat`).
- `*` — **один сегмент**: `news:*` ловит `news:sports`, но не `news:sports:football`.
- `**` — **globstar** (любое число сегментов): `news:**` ловит оба.

### 5.3 Разделение ответственности namespace ↔ JWT

Ортогонально:

- **Namespace задаёт ворота** — разрешено ли действие и **нужен ли токен**.
- **JWT-capability выдаёт право** конкретному юзеру, когда ворота требуют токен.

Режимы доступа на действие (`subscribe`/`publish`/`presence`/`history`):

| Режим | Кто может |
|---|---|
| `off` | Никто из клиентов (например, publish — только Server API) |
| `public` | Любой, без токена |
| `token` | Только при совпадающем паттерне в JWT (или sub-токене) |

### 5.4 Слой защиты соединения (до JWT)

До проверки токена работает сетевой слой (`[server.security]`, `[server.conn_limits]`):

- **Origin allowlist** — защита от CSWSH (браузер шлёт `Origin` при WS-апгрейде).
- **CORS** — для HTTP-API/SSE (кросс-доменные браузерные запросы).
- **trusted_proxies** — корректный client-IP из `X-Forwarded-For` за LB.
- **Лимиты соединений** (на ноду, локально): `max_connections[_per_ip]`, `connect_rate_per_ip`
  (анти-флуд), `handshake_timeout` (анти slow-loris), `idle_timeout`.
- **write_buffer_limit** — дисконнект медленного потребителя (защита памяти ноды в fan-out).
- `require_subprotocol`, `ip_allow/deny`, опц. `[server.tls]`.
- Глобальный `max_connections_per_user` (по кластеру) — через Redis, отложено.

### Цепочка проверки `subscribe chat:room:42`:

```
1. namespace = "chat" → найти в конфиге (нет → unknown_namespace, если strict)
2. access.subscribe == token ⇒ нужен токен
3. найти в JWT.channels паттерн, матчащий канал, с allow ⊇ ["sub"]
4. проверить max_subscribers, rate_limit.subscribe
5. ОК → подписка; при history_size>0 — отдать StreamPosition / recovery
```

---

## 6. Конфигурация (`config.toml`)

TOML (нативно для Rust через `serde` + `toml`). `defaults` и каждый `namespaces.<имя>` —
один тип `NamespaceConfig`; незаданные поля наследуются из `defaults`.

```toml
[server]
node_name = "socket-1"
log_level = "info"

[server.ws]
listen   = "0.0.0.0:8000"
path     = "/connection/websocket"
max_message_size = "65536"
ping_interval    = "25s"

[server.sse]                     # SSE-фолбэк (для сетей, режущих WS); делит HTTP-сервер с ws
enabled       = true
path          = "/connection/sse"      # downstream (EventSource, GET)
emit_path     = "/connection/sse/emit" # upstream (Command, POST, X-Session-Id)
keepalive     = "25s"

[server.security]                # защита на уровне рукопожатия/сети (ДО JWT)
allowed_origins        = ["https://app.example.com", "https://*.example.com"]  # CSWSH; пусто=не проверять
cors_allowed_origins   = ["https://app.example.com"]   # CORS для HTTP-API/SSE
cors_allow_credentials = true
trusted_proxies        = ["10.0.0.0/8", "127.0.0.1/32"]  # кому верить в X-Forwarded-For
ip_allow               = []      # пусто = все; ip_deny приоритетнее
ip_deny                = []

[server.conn_limits]             # лимиты соединений (локально на ноде)
max_connections          = 0     # 0 = без лимита (на ноду)
max_connections_per_ip   = 100
max_connections_per_user = 0     # 0 = без лимита; глобально (Redis) — позже
connect_rate_per_ip      = { rate = 10, burst = 20 }   # анти connection-flood
handshake_timeout        = "5s"  # успеть прислать валидный Connect (анти slow-loris)
idle_timeout             = "60s" # нет ping/активности → закрыть
write_buffer_limit       = "1MB" # переполнил исходящий буфер → дисконнект (медленный потребитель)
require_subprotocol      = true  # требовать Sec-WebSocket-Protocol: socket.v1

[server.tls]                     # опц.; обычно TLS терминируется на LB/прокси
enabled     = false
cert_path   = "/etc/socket/tls/cert.pem"
key_path    = "/etc/socket/tls/key.pem"
min_version = "1.2"

[server.http_api]
listen = "0.0.0.0:8001"
path   = "/api"

[server.grpc_api]                # обязателен, не опционален
listen = "0.0.0.0:8002"

[server.admin]
listen   = "127.0.0.1:8003"      # дефолт — localhost
enabled  = true
password = "${ADMIN_PASSWORD}"   # пусто + публичный listen ⇒ отказ старта

[server.health]                  # health/readiness для k8s/LB
listen = "0.0.0.0:8004"          # отдельный порт; /health (liveness), /ready (readiness)

[redis]
url             = "redis://127.0.0.1:6379"
prefix          = "socket"
idempotency_ttl = "5m"
node_heartbeat  = "5s"           # нода пишет heartbeat в Redis → info агрегирует кластер

[shutdown]                       # graceful drain на деплое/скейл-дауне
drain_timeout    = "30s"         # ждать слива соединений перед остановкой
reconnect_advice = true          # рассылать Disconnect{reconnect:true}

[events]                         # опц. уведомления бэкенда о жизненном цикле (НЕ авторизация)
enabled  = false
endpoint = "https://app.example.com/socket/events"
types    = ["connected", "disconnected", "subscribed", "unsubscribed"]
transport = "http"               # http (батч-вебхук) | grpc (стрим)

[telemetry]
log_format      = "json"         # json | text
tracing_enabled = false          # OpenTelemetry (OTLP)
otlp_endpoint   = "http://localhost:4317"

[auth.jwt]
algorithm      = "HS256"
hmac_secret    = "${JWT_HMAC_SECRET}"
# либо: algorithm = "RS256", jwks_url = "https://app.example.com/.well-known/jwks.json"
audience       = "socket"
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
history_size    = 0              # 0 ⇒ канал НЕ recoverable
history_ttl     = "0s"
max_subscribers = 0              # 0 = без лимита
name_max_len    = 255
[defaults.access]
subscribe = "token"
publish   = "off"
presence  = "token"
history   = "token"
strict_namespaces = true         # канал без известного ns → отказ

[limits]                          # глобальные потолки на соединение (локально на ноде)
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
subscribe = { rate = 10, burst = 10, scope = "client" }  # scope: client(лок.) | channel | user(Redis)

[namespaces.user]
history_size = 50
history_ttl  = "72h"
[namespaces.user.access]
subscribe = "token"
publish   = "off"
presence  = "off"
history   = "token"
```

> **Архив (БД) в MVP отсутствует.** История и recovery полностью на Redis. Секция `[archive]`
> и durable-бэкенды (rqlite/sqlite/Postgres) — будущее расширение за trait `HistoryStore`,
> в MVP не реализуется.

**Особенности:** секреты через `${ENV}` (раскрытие при загрузке), длительности строками
(`"10m"`), `api_keys` — массив таблиц. **Hot-reload** по `SIGHUP` / Server API `reload` →
атомарная подмена `Arc<Config>`, живые соединения не рвутся.

---

## 7. Hub и recovery

### 7.1 Что где лежит

**В памяти каждой ноды (только локальная маршрутизация):**

```rust
connections: DashMap<ClientId, ConnHandle>
//   ConnHandle = { tx: mpsc::Sender<Reply>, user_id, granted_patterns, subs: HashSet<Channel> }
channels: DashMap<Channel, HashSet<ClientId>>   // канал → ЛОКАЛЬНЫЕ подписчики
```

На каждое соединение — writer-задача, читающая `Reply` из своего `mpsc`. Хаб никогда не
пишет в сокет напрямую.

**В Redis (общее состояние кластера):** pub/sub (fan-out между нодами), per-channel stream
(история + `offset`), `epoch`, presence-хэш, control-канал, кэш идемпотентности.

### 7.2 offset и epoch

- `offset` — монотонный счётчик сообщений **в канале**.
- `epoch` — случайная строка, генерится при создании потока канала; меняется при потере
  потока. Клиент сравнивает `epoch`: не совпал ⇒ recovery невозможен, нужен полный refetch.

### 7.3 Publish в recoverable-канал (атомарно, Lua в Redis)

```
1. offset = INCR seq:{channel}
2. XADD hist:{channel} MAXLEN ~ N  { offset, data }
3. PUBLISH ch:{channel}  Publication{ offset, epoch, data }
```

Ноды с локальными подписчиками получают `PUBLISH` и делают локальный fan-out. Подписка
ноды на Redis-канал **ленивая**: первый локальный подписчик → `SUBSCRIBE`, последний ушёл →
`UNSUBSCRIBE`.

### 7.4 Recovery без гонок

```
1. SUBSCRIBE на live ПЕРВЫМ; входящие live-публикации буферизовать (не отдавать)
2. сравнить epoch: не совпал → recovered=false, отдать текущий position
3. прочитать hist:{channel} с offset > N (XRANGE), не больше N последних
     - разрыв больше хранимой истории → recovered=false (клиент делает refetch)
4. слить пропущенное + буфер live, дедуп по offset, упорядочить
5. отдать в SubscribeResult.publications, recovered=true; дальше live обычными Push
```

Гарантия: ни дыр, ни дублей (live подписан раньше чтения истории, дедуп по `offset`).

### 7.5 Падение ноды

Соединения оборвались → клиенты по reconnect попадают на другую ноду и делают recover.
Работает, потому что история и `offset` — в Redis, а не в памяти ноды.

### 7.6 Presence

`presence:{channel}` — Redis-хэш `clientId → ClientInfo` с TTL на запись (heartbeat по
`ping_interval`). Отписка/дисконнект → удаление. `Join`/`Leave` рассылаются тем же `PUBLISH`.
TTL защищает от «призраков» при жёстком падении ноды.

### 7.7 Гарантии доставки

- **Recoverable-каналы** (`history_size > 0`): фактически **at-least-once** (клиент детектит
  разрыв по `offset`, дубли отсекаются).
- **Не-recoverable-каналы**: **at-most-once** (чистый fan-out, без хранения).

### 7.8 Эфемерные публикации (transient)

`PublishRequest.transient = true` → публикация рассылается подписчикам, но **минует** Lua-путь
истории: не инкрементит `offset`, не пишется в `hist:{channel}`. At-most-once, не участвует в
recovery. Для typing-индикаторов, курсоров, «эфемерных» сигналов — даже в recoverable-namespace,
чтобы не засорять историю и не жечь `offset`.

### 7.9 Масштабирование fan-out (горячие каналы)

- **Локальный history-кэш на ноде** (ring-buffer свежих публикаций): при массовом одновременном
  `subscribe` нода отдаёт recovery-окно из памяти, не делая `XRANGE` к Redis на каждого
  подписчика → нет стампеда на Redis. (Оптимизация за швом, не MVP-блокер.)
- **Потолок очень горячего канала**: Redis публикует раз на ноду, но локальный fan-out на ноде
  (N подписчиков) — естественный потолок CPU/памяти. Смягчается числом нод, батчингом записи и
  `write_buffer_limit`. Сверхгорячие каналы (миллионы) — шардировать на уровне приложения
  (внутренний шардинг ломал бы ordering).

### 7.10 Граница durability (нотификации, гарантированная доставка)

Движок — это **live + короткий recovery** (окно `history_size`/`history_ttl` в Redis), **не**
durable-инбокс. При офлайне дольше окна `recovered=false` по каналу → приложение **дочитывает
пропущенное из своего бэкенда** (его БД — система записи), движок продолжает live. «Гарантированная
доставка» нотификаций достигается этой связкой, а не хранением в движке. Сам мобильный push
(APNs/FCM при отсутствии WS) — вне scope; бэкенд триггерит его по `[events].disconnected` +
запросу «юзер онлайн?» (см. Server API).

### 7.11 Быстрый реконнект без потерь

Окружения вроде Cloudflare рвут WS периодически (~каждые 100с) → реконнектов много. Цель:
реконнект дёшев, без потерь, без presence-флапа.

- **Подписки помнит SDK** (источник истины): реестр `{ channel → {last_position, sub_token?} }`,
  `last_position` обновляется по приходящим публикациям. Сервер по сессии **stateless** —
  реконнект переживает даже рестарт ноды. (Server-side resume по `session` — опц. оптимизация.)
- **Восстановление за 1 RTT:** на реконнекте SDK шлёт **один** `ConnectRequest` с картой `subs`
  (канал → recover + position). Сервер делает auth один раз, восстанавливает все подписки и
  гонит recovery по каждому каналу; `ConnectResult.subs` несёт результат каждой. Вместо
  `1 connect + N subscribe` round-trip'ов.
- **JWT переиспользуется:** за 30–100с токен не протух → реконнект **не ходит** в прикладной
  бэкенд за токеном. Шторм реконнектов не бьёт по бэкенду токенов.
- **Гашение presence-флапа:** при обрыве транспорта `Leave` **не** шлётся сразу — запись presence
  живёт по TTL (например, 60с) > интервала реконнекта. Реконнект внутри окна обновляет запись,
  `Join`/`Leave` не генерятся. `Leave` — только при явной отписке/`disconnect` или по TTL.
- **Граница losslessness:** разрыв шире истории канала или сменился `epoch` → `recovered=false`
  по каналу, SDK делает чистую переподписку + сигнал «refetch» приложению (контролируемая
  деградация, не молчаливая потеря).
- **Анти-шторм:** jittered backoff в SDK размазывает синхронные реконнекты (CDN рвёт многих разом).

### 7.12 Архив (долгая история) — вне MVP

В MVP **БД нет**: и recovery, и история целиком на Redis (`history_size`/`history_ttl`).
Долгая история (аудит, большие диапазоны) — другой юзкейс; заложен **шов** за trait
`HistoryStore`, но не реализуется. Когда понадобится — добавляется durable-бэкенд
(rqlite/sqlite/Postgres/libSQL) и один async-архиватор, читающий Redis-стримы батчами, **без
правок hub** (`offset` уже авторитетен с момента публикации).

---

## 8. Server API (публикация и управление)

Доверенная server-to-server сторона. Оба транспорта — **тонкие адаптеры над единым
`ApiService`** в `core` (одна реализация логики).

### 8.1 Транспорты и auth

- **HTTP/JSON** (`POST /api`) — простая интеграция из любого языка.
- **gRPC** (tonic) — низкая latency, стриминг; **обязателен в MVP**.
- Auth — **API-ключ** (не JWT): HTTP-заголовок `Authorization: apikey <secret>` /
  gRPC-metadata, проверка интерсептором. Ключи и права — в конфиге.

### 8.2 Методы

| Метод | Назначение |
|---|---|
| `publish` | В один канал → `{offset, epoch}` |
| `broadcast` | Одно сообщение в много каналов |
| `presence` / `presence_stats` | Полный список / только счётчики |
| `history` / `history_remove` | Чтение / очистка истории |
| `subscribe` / `unsubscribe` | Сервер-инициированная под/отписка юзера |
| `disconnect` | Кик юзера (по `user_id` / `client_id`) |
| `user_online` | Есть ли активные соединения у юзера + их число (push-vs-realtime, по кластеру) |
| `channels` | Активные каналы (по glob) |
| `info` | Ноды, метрики |
| `batch` | Пачка команд за один RTT |
| `PublishStream` (gRPC bidi) | Поток публикаций в одном соединении (high-throughput) |

### 8.3 Семантика

- **Единый путь в hub:** publish из Server API идёт через тот же Lua-скрипт, что и клиентский
  publish. Серверная и клиентская публикация неотличимы для подписчиков.
- **Идемпотентность:** опциональный `idempotency_key`, кэш `key → result` в Redis с TTL.
  Ретрай (HTTP или gRPC-стрим) → тот же `{offset}`, без повторной публикации.
- **Control-канал:** `subscribe`/`unsubscribe`/`disconnect` адресуются соединению на любой
  ноде через отдельный Redis pub/sub. Нода-владелец соединения выполняет действие.

---

## 9. Admin / monitoring UI

React + Mantine. Третий, отдельный контур доступа (помимо клиентского JWT и API-ключей).

### 9.1 Аутентификация

`[server.admin].password` → `POST /admin/login` → admin-сессия (httpOnly-cookie). Дефолт —
бинд на `127.0.0.1`; пустой пароль на публичном интерфейсе ⇒ отказ старта.

Способ входа — за швом `AdminAuth` (`[server.admin].auth = "password" | "oidc"`). В MVP —
**пароль**; OIDC (SSO через Google/Okta/Keycloak, PKCE-flow + маппинг claims) — аддитивная
реализация на будущее, сессия-cookie выдаётся одинаково.

### 9.2 Разделы и компоненты Mantine

| Раздел | Компоненты | Источник |
|---|---|---|
| Overview | `AppShell`, `Card`, `RingProgress`, `@mantine/charts` | `info` + поток метрик |
| Channels | `mantine-datatable`, glob-`TextInput`, `Drawer` | `channels`, `presence`, `history` |
| Connections | `mantine-datatable`, `ActionIcon`, `Modal` | `info`, `disconnect` |
| Publish | `@mantine/form`, `JsonInput` | `publish` / `broadcast` |
| Namespaces | `@mantine/code-highlight` (TOML) | конфиг + `reload` |
| Metrics | `@mantine/charts` (`AreaChart`/`LineChart`) | `$metrics` через WS |

### 9.3 Догфудинг + внешний мониторинг

- **Live-данные через сам движок:** зарезервированный системный namespace `$` (`$metrics`,
  `$node:events`), доступный **только** admin-сессии (жёстко в коде). Admin-клиент
  подписывается обычным SDK — продукт тестирует сам себя.
- **Prometheus** `/metrics` (text exposition) — для Grafana/Alertmanager; React-UI — быстрый
  взгляд, не замена observability-стеку.

### 9.4 Сборка

Vite-статика, раздаётся самим axum по `[server.admin].listen` — один бинарь (движок +
админка). Графики — дефолт `@mantine/charts` (Recharts); для высокочастотных живых графиков
запасной вариант — `uPlot` (оптимизация на потом).

---

## 10. Документация

**docmd** — zero-config, framework-free SSG (~18kb JS), Markdown-in → static HTML, встроенный
fuzzy-поиск, container-синтаксис (callouts/tabs/cards), темы, i18n. Не конфликтует с
Vite/React (отдельный Node-CLI).

- **Автоген:** `proto/*.proto` и config-структуры → `.md` (protocol, server-api,
  config-reference); docmd подхватывает наравне с рукописными гайдами. Прогон в CI держит
  доки синхронными с кодом.
- **Компромисс:** нет встроенного живого playground. При необходимости — публичный роут
  `/playground` в `web/` (тот же SDK), ссылки из доков. Контент не залочен (обычный Markdown),
  миграция на другой SSG дешёвая.

---

## 11. Клиентский SDK (`packages/client-ts`)

Первоклассный пакет — «библиотека на фронте». Несёт всю клиентскую логику, чтобы прикладной
код работал на высоком уровне (`subscribe`/`publish`/`on`), не зная протокола.

**Ответственности:**

- **Транспорт**: WebSocket (дефолт) с авто-фолбэком на SSE; за общим интерфейсом.
- **Кодирование**: protobuf-es (типы из `packages/proto-gen`).
- **Реестр подписок** — источник истины: `{ channel → {last_position, sub_token?} }`,
  `last_position` обновляется по приходящим публикациям.
- **Реконнект**: jittered backoff, восстановление всех подписок за 1 RTT через `ConnectRequest.subs`,
  переиспользование кэшированного JWT.
- **Recovery**: per-channel догон по `offset`/`epoch`; при `recovered=false` — сигнал `needRefetch`.
- **Токены**: колбэк `getToken()` (connection) и `getSubToken(channel)` (приватные каналы);
  продление через `RefreshRequest`/`SubRefreshRequest` (вариант B, без реконнекта).
- **API**: `connect()`, `newSubscription(channel)`, `sub.on('publication'|'join'|'leave')`,
  `sub.subscribe/unsubscribe()`, `publish/presence/history`.
- **Версия**: согласует `socket.v1`; безопасно скипает неизвестные `Push`-варианты.

Тот же пакет переиспользует админка (`web/`) для live-`$metrics` и опц. публичный `/playground`.

## 12. Операционка (deploy / shutdown / observability)

- **Graceful shutdown / дренаж** (`[shutdown]`): по SIGTERM нода перестаёт принимать новые коннекты,
  `/ready` отдаёт 503 (LB выводит её), живым рассылается `Disconnect{reconnect:true}` → клиенты
  переподключаются на другие ноды (восстановление за 1 RTT, без потерь), затем — выход по
  `drain_timeout`. Деплой без массового жёсткого разрыва.
- **Health/readiness** (`[server.health]`): `/health` (liveness) и `/ready` (readiness; учитывает
  дренаж и доступность Redis) на отдельном порту для k8s/LB.
- **Реестр нод** (`redis.node_heartbeat`): каждая нода пишет heartbeat в Redis; Server API `info`
  и admin Overview агрегируют **весь кластер**, а не одну ноду.
- **События жизненного цикла** (`[events]`, опц.): `connected`/`disconnected`/`subscribed`/
  `unsubscribed` → бэкенду (HTTP-батч-вебхук или gRPC-стрим) для аналитики/очистки. Это **не**
  авторизация (она на JWT), а уведомления.
- **Observability** (`[telemetry]`): structured logs (`json`), Prometheus `/metrics`, опц.
  OpenTelemetry-трейсинг (OTLP).
- **Шардирование Redis pub/sub** (за trait `Broker`): на больших объёмах один pub/sub —
  бутылочное горлышко; план — Redis 7 `SPUBLISH`/`SSUBSCRIBE` (sharded). Шов заложен, реализация
  по необходимости.

## 13. План реализации (по зависимостям)

1. **Ядро:** WS-коннект, JWT-auth, sub/unsub по glob-паттернам, in-memory hub (одна нода).
   + health/readiness, graceful shutdown с первого шага.
2. **Клиентский SDK** (`client-ts`): транспорт, реестр подписок, реконнект — нужен для проверки ядра.
3. **Broker-абстракция + Redis** → мультинода (pub/sub fan-out) + реестр нод (heartbeat).
4. **SSE-транспорт** (фолбэк) в сервере и SDK.
5. **Server API** (HTTP + gRPC), единый `ApiService`, идемпотентность, control-канал.
6. **Presence** (join/leave, список онлайн, TTL, гашение флапа).
7. **История + recovery** (Redis Streams, `offset`/`epoch`, восстановление за 1 RTT без гонок).
8. **События жизненного цикла** (`[events]`) + observability (Prometheus, опц. tracing).
9. **Admin UI** (Mantine) + `$metrics` + Prometheus.
10. **Docs** (docmd) + автоген из `.proto`/config.

---

## 14. Решённые вопросы

- ~~Точный формат `rate_limit` и где считать.~~ **Решено:** токен-бакет `{rate, burst, scope}`;
  в MVP только `scope = client` (локальный, в памяти ноды) + секция `[limits]` на соединение.
  Глобальные `scope = channel/user` (Redis) — в схеме конфига, реализация отложена. При
  превышении — `Error{rate_limited, temporary}`, не дисконнект.
- ~~Транспорт-фолбэки (SSE/HTTP-streaming) — нужны ли.~~ **Решено:** WebSocket + SSE в MVP,
  оба за trait `Transport`. HTTP-streaming/WebTransport — аддитивно позже.
- ~~Версионирование протокола.~~ **Решено:** мажор в пакете proto (`socket.v1`) + согласование
  через WS-subprotocol (`Sec-WebSocket-Protocol: socket.v1`) / SSE-квери (`?v=1`); аддитивные
  изменения внутри мажора (Protobuf-совместимость); SDK безопасно скипает неизвестные
  `oneof`-варианты; `protocol_version` в `ConnectRequest` для диагностики; несовпадение мажора →
  `Disconnect{reconnect:false}` + `Error{temporary:false}`.
- ~~Бэкенд истории: только Redis или durable-стор для долгого хранения.~~ **Решено для MVP:**
  **БД нет** — recovery и история целиком на Redis. Durable-архив (rqlite/sqlite/Postgres) —
  будущее расширение за trait `HistoryStore`, в MVP не реализуется.
- Версионирование протокола: как клиент и сервер согласуют версию `.proto`.
- ~~OIDC для admin UI (вместо пароля).~~ **Решено:** в MVP — пароль; OIDC за швом `AdminAuth`
  (`auth = "password" | "oidc"`), аддитивно позже.
```
