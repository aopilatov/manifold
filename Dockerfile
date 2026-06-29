# syntax=docker/dockerfile:1

# ──────────────────────────────────────────────────────────────
# Stage 1 — web: build the admin UI static bundle (web/dist)
# ──────────────────────────────────────────────────────────────
FROM docker.io/library/node:24-bookworm-slim AS web
RUN corepack enable
WORKDIR /src

# Install deps first (cache-friendly): copy manifests, then install.
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml ./
COPY packages/proto-gen/package.json ./packages/proto-gen/
COPY packages/client-ts/package.json ./packages/client-ts/
COPY web/package.json ./web/
RUN pnpm install --no-frozen-lockfile

# Sources needed for the TS pipeline.
COPY proto ./proto
COPY packages ./packages
COPY web ./web

# proto-gen (buf) → client SDK → admin UI.
RUN pnpm --filter manifold-proto-gen generate \
 && pnpm --filter manifold-proto-gen build \
 && pnpm --filter manifold-client build \
 && pnpm --filter manifold-web build

# ──────────────────────────────────────────────────────────────
# Stage 2 — build: compile the Rust server.
# Official Rust image (Debian, glibc 2.36) — forward-compatible with UBI10 glibc (2.39).
# Avoids the current UBI dev-package glibc/gconv version skew. The runtime image stays ubi10-micro.
# ──────────────────────────────────────────────────────────────
FROM docker.io/library/rust:1-bookworm AS build

# cmake is needed by aws-lc-rs/ring; gcc/make/perl already ship in the rust base image.
RUN apt-get update && apt-get install -y --no-install-recommends cmake \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY proto ./proto

# protoc is vendored by the protocol crate's build.rs (protoc-bin-vendored) — no system protoc.
RUN cargo build --release -p manifold-server \
 && strip target/release/manifold-server

# ──────────────────────────────────────────────────────────────
# Stage 3 — runtime: ubi10-micro (no package manager, minimal surface)
# ──────────────────────────────────────────────────────────────
FROM registry.access.redhat.com/ubi10/ubi-micro:latest AS runtime
WORKDIR /app

# ubi10-micro already ships glibc + libgcc_s + the dynamic loader, so the glibc-dynamic
# binary just runs. Only need a CA bundle for outbound HTTPS (event webhooks over rustls).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /src/target/release/manifold-server /app/manifold-server
COPY --from=web   /src/web/dist                     /app/web/dist
COPY config.toml /app/config.toml

ENV MANIFOLD_CONFIG=/app/config.toml \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# ws, http-api, grpc-api, admin, health/metrics
EXPOSE 8000 8001 8002 8003 8004

# Run as non-root (numeric UID works without /etc/passwd entry).
USER 1001

ENTRYPOINT ["/app/manifold-server"]
