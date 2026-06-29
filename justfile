# Monorepo orchestration. `just` — https://github.com/casey/just

# List commands
default:
    @just --list

# Bring up infrastructure (Redis)
infra-up:
    docker compose up -d redis

infra-down:
    docker compose down

# Generate protobuf types for TS (Rust generates via build.rs automatically)
proto-gen:
    pnpm proto:gen

# Run the engine (Rust)
server:
    SOCKET_CONFIG=config.toml cargo run -p socket-server

# Admin UI (Vite dev)
web:
    pnpm web:dev

# Generate reference docs from proto/config
docs-gen:
    pnpm docs:gen

# Documentation (autogen + docmd dev)
docs:
    pnpm docs:dev

# Full dev cycle: infra + engine + admin + docs
dev: infra-up
    @echo "Redis is up. Run in separate terminals: just server | just web | just docs"

# Build everything
build:
    cargo build --release
    pnpm web:build
    pnpm docs:build

# Rust tests
test:
    cargo test

# Checks (clippy + fmt)
check:
    cargo clippy --all-targets -- -D warnings
    cargo fmt --check
