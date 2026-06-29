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
    MANIFOLD_CONFIG=config.toml cargo run -p manifold-server

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

# Start a load-test-tuned server (public namespaces, no presence/history)
loadtest-server:
    MANIFOLD_CONFIG=config.loadtest.toml cargo run --release -p manifold-server

# Run the load generator. Raise the fd limit first: `ulimit -n 200000`.
# Example: just loadtest --subscribers 2000 --messages 5000000 --rate 400000
loadtest *ARGS:
    cargo run --release -p manifold-loadtest -- {{ARGS}}

# Checks (clippy + fmt)
check:
    cargo clippy --all-targets -- -D warnings
    cargo fmt --check
