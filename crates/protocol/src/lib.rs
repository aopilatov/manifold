//! Protocol types and the gRPC Server API generated from `proto/manifold.proto`.
//!
//! `manifold.v1` — the major protocol version (see design doc, versioning).

pub mod v1 {
    tonic::include_proto!("manifold.v1");
}

pub use v1::*;

/// Current major version of the client protocol.
pub const PROTOCOL_VERSION: u32 = 1;
