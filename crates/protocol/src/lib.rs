//! Сгенерированные из `proto/socket.proto` типы протокола и gRPC Server API.
//!
//! `socket.v1` — мажорная версия протокола (см. design-doc, версионирование).

pub mod v1 {
    tonic::include_proto!("socket.v1");
}

pub use v1::*;

/// Текущая мажорная версия клиентского протокола.
pub const PROTOCOL_VERSION: u32 = 1;
