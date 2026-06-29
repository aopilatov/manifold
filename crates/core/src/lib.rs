//! Ядро движка: конфиг, auth (JWT + glob), hub (реестр соединений/каналов), namespace-политика,
//! единый `ApiService` (за ним стоят и WS-команды, и HTTP/gRPC Server API).

pub mod api;
pub mod auth;
pub mod config;
pub mod delivery;
pub mod events;
pub mod hub;
pub mod metrics;
pub mod namespace;

pub use config::Config;
