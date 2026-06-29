---
title: Config reference
description: All config.toml keys
---

> Generated automatically from the sources of truth. Do not edit manually.

Source — `config.toml`. Secrets are set via `${ENV_VAR}`.

| Key | Example | Description |
|---|---|---|
| `strict_namespaces` | `true` | channel with an unknown namespace → reject |

## `[server]`

| Key | Example | Description |
|---|---|---|
| `node_name` | `"socket-1"` |  |
| `log_level` | `"info"` |  |

## `[server.ws]`

| Key | Example | Description |
|---|---|---|
| `listen` | `"0.0.0.0:8000"` |  |
| `path` | `"/connection/websocket"` |  |
| `max_message_size` | `"65536"` |  |
| `ping_interval` | `"25s"` |  |

## `[server.sse]`

| Key | Example | Description |
|---|---|---|
| `enabled` | `true` |  |
| `path` | `"/connection/sse"` |  |
| `emit_path` | `"/connection/sse/emit"` |  |

## `[server.http_api]`

| Key | Example | Description |
|---|---|---|
| `listen` | `"0.0.0.0:8001"` |  |
| `path` | `"/api"` |  |

## `[server.grpc_api]`

| Key | Example | Description |
|---|---|---|
| `listen` | `"0.0.0.0:8002"` |  |

## `[server.admin]`

| Key | Example | Description |
|---|---|---|
| `listen` | `"127.0.0.1:8003"` |  |
| `enabled` | `true` |  |
| `auth` | `"password"` | password \| oidc |
| `password` | `"${ADMIN_PASSWORD}"` |  |

## `[server.health]`

| Key | Example | Description |
|---|---|---|
| `listen` | `"0.0.0.0:8004"` | /health, /ready |

## `[server.security]`

| Key | Example | Description |
|---|---|---|
| `allowed_origins` | `["https://app.example.com"]` |  |
| `cors_allowed_origins` | `["https://app.example.com"]` |  |
| `cors_allow_credentials` | `true` |  |
| `trusted_proxies` | `["10.0.0.0/8", "127.0.0.1/32"]` |  |
| `ip_allow` | `[]` |  |
| `ip_deny` | `[]` |  |

## `[server.conn_limits]`

| Key | Example | Description |
|---|---|---|
| `max_connections` | `0` |  |
| `max_connections_per_ip` | `100` |  |
| `max_connections_per_user` | `0` |  |
| `require_subprotocol` | `true` |  |

## `[server.tls]`

| Key | Example | Description |
|---|---|---|
| `enabled` | `false` |  |
| `cert_path` | `"/etc/socket/tls/cert.pem"` |  |
| `key_path` | `"/etc/socket/tls/key.pem"` |  |

## `[redis]`

| Key | Example | Description |
|---|---|---|
| `enabled` | `false` | true → multi-node (RedisBroker); false → single node (in-memory) |
| `url` | `"redis://127.0.0.1:6379"` |  |
| `prefix` | `"socket"` |  |
| `idempotency_ttl` | `"5m"` |  |

## `[auth.jwt]`

| Key | Example | Description |
|---|---|---|
| `algorithm` | `"HS256"` |  |
| `hmac_secret` | `"${JWT_HMAC_SECRET}"` |  |
| `audience` | `"socket"` |  |
| `channels_claim` | `"channels"` |  |

## `[api_keys]`

| Key | Example | Description |
|---|---|---|
| `key` | `"${API_KEY_BACKEND}"` |  |
| `allow` | `["publish","broadcast","presence","history","subscribe",` |  |

## `[api_keys]`

| Key | Example | Description |
|---|---|---|
| `key` | `"${API_KEY_PUBLISHER}"` |  |
| `allow` | `["publish","broadcast"]` |  |

## `[defaults]`

| Key | Example | Description |
|---|---|---|
| `presence` | `false` |  |
| `join_leave` | `false` |  |
| `history_size` | `0` |  |
| `history_ttl` | `"0s"` |  |
| `max_subscribers` | `0` |  |
| `name_max_len` | `255` |  |

## `[defaults.access]`

| Key | Example | Description |
|---|---|---|
| `subscribe` | `"token"` |  |
| `publish` | `"off"` |  |
| `presence` | `"token"` |  |
| `history` | `"token"` |  |

## `[limits]`

| Key | Example | Description |
|---|---|---|
| `max_channels_per_connection` | `1000` |  |
| `max_commands_per_second` | `100` |  |

## `[namespaces.news]`

| Key | Example | Description |
|---|---|---|
| `history_size` | `100` |  |
| `history_ttl` | `"10m"` |  |

## `[namespaces.news.access]`

| Key | Example | Description |
|---|---|---|
| `subscribe` | `"public"` |  |
| `publish` | `"off"` |  |
| `presence` | `"public"` |  |
| `history` | `"public"` |  |

## `[namespaces.chat]`

| Key | Example | Description |
|---|---|---|
| `presence` | `true` |  |
| `join_leave` | `true` |  |
| `history_size` | `300` |  |
| `history_ttl` | `"24h"` |  |
| `max_subscribers` | `5000` |  |

## `[namespaces.chat.access]`

| Key | Example | Description |
|---|---|---|
| `subscribe` | `"token"` |  |
| `publish` | `"token"` |  |
| `presence` | `"token"` |  |
| `history` | `"token"` |  |

## `[namespaces.chat.rate_limit]`

| Key | Example | Description |
|---|---|---|
| `publish` | `{ rate = 20, burst = 40, scope = "client" }` |  |
| `subscribe` | `{ rate = 10, burst = 10, scope = "client" }` |  |

## `[namespaces.user]`

| Key | Example | Description |
|---|---|---|
| `history_size` | `50` |  |
| `history_ttl` | `"72h"` |  |

## `[namespaces.user.access]`

| Key | Example | Description |
|---|---|---|
| `subscribe` | `"token"` |  |
| `publish` | `"off"` |  |
| `presence` | `"off"` |  |
| `history` | `"token"` |  |

## `[shutdown]`

| Key | Example | Description |
|---|---|---|
| `drain_timeout` | `"30s"` |  |
| `reconnect_advice` | `true` |  |

## `[events]`

| Key | Example | Description |
|---|---|---|
| `enabled` | `false` |  |
| `endpoint` | `"https://app.example.com/socket/events"` |  |
| `types` | `["connected", "disconnected", "subscribed", "unsubscribed"]` |  |
| `transport` | `"http"` |  |

## `[telemetry]`

| Key | Example | Description |
|---|---|---|
| `log_format` | `"json"` |  |
| `tracing_enabled` | `false` |  |
| `otlp_endpoint` | `"http://localhost:4317"` |  |

