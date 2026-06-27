---
title: Socket
description: Настраиваемый realtime-движок (WebSocket pub/sub)
---

# Socket

Самостоятельный сервер-движок реалтайма (pub/sub поверх WebSocket): «как Centrifugo, но
настраиваемее». Запускается независимо. Монорепо: Rust-бэкенд + React-фронтенд + Markdown-доки.

- **[Архитектура](/architecture)** — полный дизайн-документ.

## Быстрый старт (dev)

```bash
just dev        # поднять Redis + движок + admin + docs
```

## Части

- `crates/` — Rust-движок (server, core, broker, protocol)
- `packages/client-ts` — клиентский SDK
- `web/` — admin UI (React + Mantine)
- `docs/` — эта документация (docmd)
