---
title: Server API
description: Server-side API (HTTP + gRPC) for publishing from the backend
---

> Generated automatically from the sources of truth. Do not edit manually.

Trusted server-to-server side. The gRPC service is below; HTTP/JSON exposes the same methods at `POST /api/<method>`. Auth — `Authorization: apikey <key>`.

## Service `ServerApi`

| Method | Request | Response | Stream |
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

## Messages

### PublishApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `data` | `bytes` | 2 |  |
| `idempotency_key` | `string` | 3 |  |
| `tags` | `map<string, string>` | 4 |  |

### PublishApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `offset` | `uint64` | 2 |  |
| `epoch` | `string` | 3 |  |

### BroadcastApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `channels` | `repeated string` | 1 |  |
| `data` | `bytes` | 2 |  |
| `idempotency_key` | `string` | 3 |  |

### BroadcastApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `responses` | `map<string, PublishApiResponse>` | 2 |  |

### PresenceApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

### PresenceApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `presence` | `map<string, ClientInfo>` | 2 |  |

### PresenceStatsApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

### PresenceStatsApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `num_clients` | `uint32` | 2 |  |
| `num_users` | `uint32` | 3 |  |

### HistoryApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |
| `limit` | `int32` | 2 |  |
| `since` | `StreamPosition` | 3 |  |
| `reverse` | `bool` | 4 |  |

### HistoryApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `publications` | `repeated Publication` | 2 |  |
| `position` | `StreamPosition` | 3 |  |

### HistoryRemoveApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `channel` | `string` | 1 |  |

### HistoryRemoveApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### SubscribeApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |
| `channel` | `string` | 2 |  |

### SubscribeApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### UnsubscribeApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |
| `channel` | `string` | 2 |  |

### UnsubscribeApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### DisconnectApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |
| `client` | `string` | 2 |  |
| `code` | `uint32` | 3 |  |
| `reason` | `string` | 4 |  |

### DisconnectApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |

### UserOnlineApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `user` | `string` | 1 |  |

### UserOnlineApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `online` | `bool` | 2 |  |
| `num_connections` | `uint32` | 3 |  |

### ChannelsApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `pattern` | `string` | 1 |  |

### ChannelsApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `channels` | `repeated string` | 2 |  |

### InfoApiRequest

_(empty message)_
### InfoApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `error` | `Error` | 1 |  |
| `nodes` | `repeated NodeInfo` | 2 |  |

### NodeInfo

| Field | Type | # | oneof |
|---|---|---|---|
| `name` | `string` | 1 |  |
| `num_clients` | `uint32` | 2 |  |
| `num_channels` | `uint32` | 3 |  |
| `uptime_s` | `uint64` | 4 |  |

### BatchApiRequest

| Field | Type | # | oneof |
|---|---|---|---|
| `commands` | `repeated Command` | 1 |  |

### BatchApiResponse

| Field | Type | # | oneof |
|---|---|---|---|
| `replies` | `repeated Reply` | 1 |  |

