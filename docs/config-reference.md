---
title: Справочник конфига
description: Все ключи config.toml
---

> Сгенерировано автоматически из источников истины. Не редактировать вручную.

Источник — `config.toml`. Секреты задаются через `${ENV_VAR}`.

| Ключ | Пример | Описание |
|---|---|---|
| `strict_namespaces` | `true` | канал без известного namespace → отказ |

## `[server]`

| Ключ | Пример | Описание |
|---|---|---|
| `node_name` | `"socket-1"` |  |
| `log_level` | `"info"` |  |

## `[server.ws]`

| Ключ | Пример | Описание |
|---|---|---|
| `listen` | `"0.0.0.0:8000"` |  |
| `path` | `"/connection/websocket"` |  |
| `max_message_size` | `"65536"` |  |
| `ping_interval` | `"25s"` |  |

## `[server.sse]`

| Ключ | Пример | Описание |
|---|---|---|
| `enabled` | `true` |  |
| `path` | `"/connection/sse"` |  |
| `emit_path` | `"/connection/sse/emit"` |  |

## `[server.http_api]`

| Ключ | Пример | Описание |
|---|---|---|
| `listen` | `"0.0.0.0:8001"` |  |
| `path` | `"/api"` |  |

## `[server.grpc_api]`

| Ключ | Пример | Описание |
|---|---|---|
| `listen` | `"0.0.0.0:8002"` |  |

## `[server.admin]`

| Ключ | Пример | Описание |
|---|---|---|
| `listen` | `"127.0.0.1:8003"` |  |
| `enabled` | `true` |  |
| `auth` | `"password"` | password \| oidc |
| `password` | `"${ADMIN_PASSWORD}"` |  |

## `[server.health]`

| Ключ | Пример | Описание |
|---|---|---|
| `listen` | `"0.0.0.0:8004"` | /health, /ready |

## `[server.security]`

| Ключ | Пример | Описание |
|---|---|---|
| `allowed_origins` | `["https://app.example.com"]` |  |
| `cors_allowed_origins` | `["https://app.example.com"]` |  |
| `cors_allow_credentials` | `true` |  |
| `trusted_proxies` | `["10.0.0.0/8", "127.0.0.1/32"]` |  |
| `ip_allow` | `[]` |  |
| `ip_deny` | `[]` |  |

## `[server.conn_limits]`

| Ключ | Пример | Описание |
|---|---|---|
| `max_connections` | `0` |  |
| `max_connections_per_ip` | `100` |  |
| `max_connections_per_user` | `0` |  |
| `require_subprotocol` | `true` |  |

## `[server.tls]`

| Ключ | Пример | Описание |
|---|---|---|
| `enabled` | `false` |  |
| `cert_path` | `"/etc/socket/tls/cert.pem"` |  |
| `key_path` | `"/etc/socket/tls/key.pem"` |  |

## `[redis]`

| Ключ | Пример | Описание |
|---|---|---|
| `enabled` | `false` | true → мультинода (RedisBroker); false → одна нода (память) |
| `url` | `"redis://127.0.0.1:6379"` |  |
| `prefix` | `"socket"` |  |
| `idempotency_ttl` | `"5m"` |  |

## `[auth.jwt]`

| Ключ | Пример | Описание |
|---|---|---|
| `algorithm` | `"HS256"` |  |
| `hmac_secret` | `"${JWT_HMAC_SECRET}"` |  |
| `audience` | `"socket"` |  |
| `channels_claim` | `"channels"` |  |

## `[api_keys]`

| Ключ | Пример | Описание |
|---|---|---|
| `key` | `"${API_KEY_BACKEND}"` |  |
| `allow` | `["publish","broadcast","presence","history","subscribe",` |  |

## `[api_keys]`

| Ключ | Пример | Описание |
|---|---|---|
| `key` | `"${API_KEY_PUBLISHER}"` |  |
| `allow` | `["publish","broadcast"]` |  |

## `[defaults]`

| Ключ | Пример | Описание |
|---|---|---|
| `presence` | `false` |  |
| `join_leave` | `false` |  |
| `history_size` | `0` |  |
| `history_ttl` | `"0s"` |  |
| `max_subscribers` | `0` |  |
| `name_max_len` | `255` |  |

## `[defaults.access]`

| Ключ | Пример | Описание |
|---|---|---|
| `subscribe` | `"token"` |  |
| `publish` | `"off"` |  |
| `presence` | `"token"` |  |
| `history` | `"token"` |  |

## `[limits]`

| Ключ | Пример | Описание |
|---|---|---|
| `max_channels_per_connection` | `1000` |  |
| `max_commands_per_second` | `100` |  |

## `[namespaces.news]`

| Ключ | Пример | Описание |
|---|---|---|
| `history_size` | `100` |  |
| `history_ttl` | `"10m"` |  |

## `[namespaces.news.access]`

| Ключ | Пример | Описание |
|---|---|---|
| `subscribe` | `"public"` |  |
| `publish` | `"off"` |  |
| `presence` | `"public"` |  |
| `history` | `"public"` |  |

## `[namespaces.chat]`

| Ключ | Пример | Описание |
|---|---|---|
| `presence` | `true` |  |
| `join_leave` | `true` |  |
| `history_size` | `300` |  |
| `history_ttl` | `"24h"` |  |
| `max_subscribers` | `5000` |  |

## `[namespaces.chat.access]`

| Ключ | Пример | Описание |
|---|---|---|
| `subscribe` | `"token"` |  |
| `publish` | `"token"` |  |
| `presence` | `"token"` |  |
| `history` | `"token"` |  |

## `[namespaces.chat.rate_limit]`

| Ключ | Пример | Описание |
|---|---|---|
| `publish` | `{ rate = 20, burst = 40, scope = "client" }` |  |
| `subscribe` | `{ rate = 10, burst = 10, scope = "client" }` |  |

## `[namespaces.user]`

| Ключ | Пример | Описание |
|---|---|---|
| `history_size` | `50` |  |
| `history_ttl` | `"72h"` |  |

## `[namespaces.user.access]`

| Ключ | Пример | Описание |
|---|---|---|
| `subscribe` | `"token"` |  |
| `publish` | `"off"` |  |
| `presence` | `"off"` |  |
| `history` | `"token"` |  |

## `[shutdown]`

| Ключ | Пример | Описание |
|---|---|---|
| `drain_timeout` | `"30s"` |  |
| `reconnect_advice` | `true` |  |

## `[events]`

| Ключ | Пример | Описание |
|---|---|---|
| `enabled` | `false` |  |
| `endpoint` | `"https://app.example.com/socket/events"` |  |
| `types` | `["connected", "disconnected", "subscribed", "unsubscribed"]` |  |
| `transport` | `"http"` |  |

## `[telemetry]`

| Ключ | Пример | Описание |
|---|---|---|
| `log_format` | `"json"` |  |
| `tracing_enabled` | `false` |  |
| `otlp_endpoint` | `"http://localhost:4317"` |  |

