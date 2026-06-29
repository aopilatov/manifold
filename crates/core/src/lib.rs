//! Engine core: config, auth (JWT + glob), hub (registry of connections/channels), namespace policy,
//! a single `ApiService` (backing both WS commands and the HTTP/gRPC Server API).

pub mod api;
pub mod auth;
pub mod config;
pub mod delivery;
pub mod events;
pub mod hub;
pub mod metrics;
pub mod namespace;

pub use config::Config;
