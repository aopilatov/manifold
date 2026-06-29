---
title: Протокол
description: Клиентский WebSocket/SSE-протокол (Protobuf)
---

> Сгенерировано автоматически из источников истины. Не редактировать вручную.

Бинарный protobuf. Пакет `socket.v1`. Клиент шлёт `Command`, сервер отвечает `Reply` (тот же `id`) либо асинхронным `Push` (`id = 0`).

## Command

| Поле | Тип | № | oneof |
|---|---|---|---|
| `id` | `uint32` | 1 |  |
| `connect` | `ConnectRequest` | 2 | `method` |
| `subscribe` | `SubscribeRequest` | 3 | `method` |
| `unsubscribe` | `UnsubscribeRequest` | 4 | `method` |
| `publish` | `PublishRequest` | 5 | `method` |
| `presence` | `PresenceRequest` | 6 | `method` |
| `history` | `HistoryRequest` | 7 | `method` |
| `ping` | `PingRequest` | 8 | `method` |
| `refresh` | `RefreshRequest` | 9 | `method` |
| `sub_refresh` | `SubRefreshRequest` | 10 | `method` |

## Reply

| Поле | Тип | № | oneof |
|---|---|---|---|
| `id` | `uint32` | 1 |  |
| `error` | `Error` | 2 |  |
| `connect` | `ConnectResult` | 3 | `payload` |
| `subscribe` | `SubscribeResult` | 4 | `payload` |
| `unsubscribe` | `UnsubscribeResult` | 5 | `payload` |
| `publish` | `PublishResult` | 6 | `payload` |
| `presence` | `PresenceResult` | 7 | `payload` |
| `history` | `HistoryResult` | 8 | `payload` |
| `pong` | `PongResult` | 9 | `payload` |
| `push` | `Push` | 10 | `payload` |

## Error

| Поле | Тип | № | oneof |
|---|---|---|---|
| `code` | `uint32` | 1 |  |
| `message` | `string` | 2 |  |
| `temporary` | `bool` | 3 |  |

## ConnectRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `token` | `string` | 1 |  |
| `subs` | `map<string, SubscribeRequest>` | 2 |  |
| `headers` | `map<string, string>` | 3 |  |
| `name` | `string` | 4 |  |
| `protocol_version` | `uint32` | 5 |  |

## SubscribeRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `token` | `string` | 2 |  |
| `recover` | `bool` | 3 |  |
| `position` | `StreamPosition` | 4 |  |

## UnsubscribeRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

## PublishRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `data` | `bytes` | 2 |  |
| `transient` | `bool` | 3 |  |

## PresenceRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

## HistoryRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `limit` | `int32` | 2 |  |
| `since` | `StreamPosition` | 3 |  |
| `reverse` | `bool` | 4 |  |

## RefreshRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `token` | `string` | 1 |  |

## SubRefreshRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `token` | `string` | 2 |  |

## PingRequest

_(пустое сообщение)_
## ConnectResult

| Поле | Тип | № | oneof |
|---|---|---|---|
| `client` | `string` | 1 |  |
| `ping_interval_ms` | `uint32` | 2 |  |
| `expires_in_s` | `uint32` | 3 |  |
| `data` | `bytes` | 4 |  |
| `subs` | `map<string, SubscribeResult>` | 5 |  |
| `session` | `string` | 6 |  |

## SubscribeResult

| Поле | Тип | № | oneof |
|---|---|---|---|
| `recoverable` | `bool` | 1 |  |
| `position` | `StreamPosition` | 2 |  |
| `recovered` | `bool` | 3 |  |
| `publications` | `repeated Publication` | 4 |  |
| `positioned` | `bool` | 5 |  |

## UnsubscribeResult

_(пустое сообщение)_
## PublishResult

| Поле | Тип | № | oneof |
|---|---|---|---|
| `position` | `StreamPosition` | 1 |  |

## PresenceResult

| Поле | Тип | № | oneof |
|---|---|---|---|
| `presence` | `map<string, ClientInfo>` | 1 |  |

## HistoryResult

| Поле | Тип | № | oneof |
|---|---|---|---|
| `publications` | `repeated Publication` | 1 |  |
| `position` | `StreamPosition` | 2 |  |

## PongResult

_(пустое сообщение)_
## Push

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `pub` | `Publication` | 2 | `event` |
| `join` | `Join` | 3 | `event` |
| `leave` | `Leave` | 4 | `event` |
| `unsubscribe` | `Unsubscribe` | 5 | `event` |
| `disconnect` | `Disconnect` | 6 | `event` |

## Publication

| Поле | Тип | № | oneof |
|---|---|---|---|
| `data` | `bytes` | 1 |  |
| `offset` | `uint64` | 2 |  |
| `info` | `ClientInfo` | 3 |  |
| `tags` | `map<string, string>` | 4 |  |

## Join

| Поле | Тип | № | oneof |
|---|---|---|---|
| `info` | `ClientInfo` | 1 |  |

## Leave

| Поле | Тип | № | oneof |
|---|---|---|---|
| `info` | `ClientInfo` | 1 |  |

## Unsubscribe

| Поле | Тип | № | oneof |
|---|---|---|---|
| `code` | `uint32` | 1 |  |
| `reason` | `string` | 2 |  |

## Disconnect

| Поле | Тип | № | oneof |
|---|---|---|---|
| `code` | `uint32` | 1 |  |
| `reason` | `string` | 2 |  |
| `reconnect` | `bool` | 3 |  |

## ClientInfo

| Поле | Тип | № | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |
| `client` | `string` | 2 |  |
| `conn_info` | `bytes` | 3 |  |
| `chan_info` | `bytes` | 4 |  |

## StreamPosition

| Поле | Тип | № | oneof |
|---|---|---|---|
| `offset` | `uint64` | 1 |  |
| `epoch` | `string` | 2 |  |

