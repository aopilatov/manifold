---
title: Server API
description: Серверный API (HTTP + gRPC) для публикации из бэкенда
---

> Сгенерировано автоматически из источников истины. Не редактировать вручную.

Доверенная server-to-server сторона. gRPC-сервис ниже; HTTP/JSON — те же методы на `POST /api/<method>`. Auth — `Authorization: apikey <key>`.

## Сервис `ServerApi`

| Метод | Запрос | Ответ | Стрим |
|---|---|---|---|
| `Publish` | `PublishApiRequest` | `PublishApiResponse` | — |
| `Broadcast` | `BroadcastApiRequest` | `BroadcastApiResponse` | — |
| `Presence` | `PresenceApiRequest` | `PresenceApiResponse` | — |
| `PresenceStats` | `PresenceStatsApiRequest` | `PresenceStatsApiResponse` | — |
| `History` | `HistoryApiRequest` | `HistoryApiResponse` | — |
| `HistoryRemove` | `HistoryRemoveApiRequest` | `HistoryRemoveApiResponse` | — |
| `Subscribe` | `SubscribeApiRequest` | `SubscribeApiResponse` | — |
| `Unsubscribe` | `UnsubscribeApiRequest` | `UnsubscribeApiResponse` | — |
| `Disconnect` | `DisconnectApiRequest` | `DisconnectApiResponse` | — |
| `UserOnline` | `UserOnlineApiRequest` | `UserOnlineApiResponse` | — |
| `Channels` | `ChannelsApiRequest` | `ChannelsApiResponse` | — |
| `Info` | `InfoApiRequest` | `InfoApiResponse` | — |
| `Batch` | `BatchApiRequest` | `BatchApiResponse` | — |
| `PublishStream` | `PublishApiRequest` | `PublishApiResponse` | ↑↓ |

## Сообщения

### PublishApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `data` | `bytes` | 2 |  |
| `idempotency_key` | `string` | 3 |  |
| `tags` | `map<string, string>` | 4 |  |

### PublishApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `offset` | `uint64` | 2 |  |
| `epoch` | `string` | 3 |  |

### BroadcastApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channels` | `repeated string` | 1 |  |
| `data` | `bytes` | 2 |  |
| `idempotency_key` | `string` | 3 |  |

### BroadcastApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `responses` | `map<string, PublishApiResponse>` | 2 |  |

### PresenceApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

### PresenceApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `presence` | `map<string, ClientInfo>` | 2 |  |

### PresenceStatsApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

### PresenceStatsApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `num_clients` | `uint32` | 2 |  |
| `num_users` | `uint32` | 3 |  |

### HistoryApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `limit` | `int32` | 2 |  |
| `since` | `StreamPosition` | 3 |  |
| `reverse` | `bool` | 4 |  |

### HistoryApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `publications` | `repeated Publication` | 2 |  |
| `position` | `StreamPosition` | 3 |  |

### HistoryRemoveApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

### HistoryRemoveApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### SubscribeApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |
| `channel` | `string` | 2 |  |

### SubscribeApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### UnsubscribeApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |
| `channel` | `string` | 2 |  |

### UnsubscribeApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### DisconnectApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |
| `client` | `string` | 2 |  |
| `code` | `uint32` | 3 |  |
| `reason` | `string` | 4 |  |

### DisconnectApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### UserOnlineApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |

### UserOnlineApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `online` | `bool` | 2 |  |
| `num_connections` | `uint32` | 3 |  |

### ChannelsApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `pattern` | `string` | 1 |  |

### ChannelsApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `channels` | `repeated string` | 2 |  |

### InfoApiRequest

_(пустое сообщение)_
### InfoApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `nodes` | `repeated NodeInfo` | 2 |  |

### NodeInfo

| Поле | Тип | № | oneof |
|---|---|---|---|
| `name` | `string` | 1 |  |
| `num_clients` | `uint32` | 2 |  |
| `num_channels` | `uint32` | 3 |  |
| `uptime_s` | `uint64` | 4 |  |

### BatchApiRequest

| Поле | Тип | № | oneof |
|---|---|---|---|
| `commands` | `repeated Command` | 1 |  |

### BatchApiResponse

| Поле | Тип | № | oneof |
|---|---|---|---|
| `replies` | `repeated Reply` | 1 |  |

