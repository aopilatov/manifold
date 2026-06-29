# Оркестрация монорепо. `just` — https://github.com/casey/just

# Список команд
default:
    @just --list

# Поднять инфраструктуру (Redis)
infra-up:
    docker compose up -d redis

infra-down:
    docker compose down

# Сгенерировать protobuf-типы для TS (Rust генерит build.rs автоматически)
proto-gen:
    pnpm proto:gen

# Запустить движок (Rust)
server:
    SOCKET_CONFIG=config.toml cargo run -p socket-server

# Admin UI (Vite dev)
web:
    pnpm web:dev

# Сгенерировать справочные доки из proto/config
docs-gen:
    pnpm docs:gen

# Документация (автоген + docmd dev)
docs:
    pnpm docs:dev

# Полный dev-цикл: инфра + движок + admin + docs
dev: infra-up
    @echo "Redis поднят. Запусти в отдельных терминалах: just server | just web | just docs"

# Сборка всего
build:
    cargo build --release
    pnpm web:build
    pnpm docs:build

# Тесты Rust
test:
    cargo test

# Проверка (clippy + fmt)
check:
    cargo clippy --all-targets -- -D warnings
    cargo fmt --check
