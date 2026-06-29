//! Loading and the model for `config.toml`. `defaults` and each `namespaces.<name>` share the
//! same type [`NamespaceConfig`]; unset namespace fields are inherited from `defaults`.

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: Server,
    pub redis: Redis,
    pub auth: Auth,
    #[serde(default)]
    pub api_keys: Vec<ApiKey>,
    pub defaults: NamespaceConfig,
    #[serde(default)]
    pub namespaces: HashMap<String, NamespaceConfig>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub shutdown: Shutdown,
    #[serde(default)]
    pub events: Events,
    #[serde(default)]
    pub telemetry: Telemetry,
    #[serde(default = "default_true")]
    pub strict_namespaces: bool,
}

#[derive(Debug, Deserialize)]
pub struct Server {
    pub node_name: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub ws: WsConfig,
    #[serde(default)]
    pub sse: SseConfig,
    pub http_api: Listen,
    pub grpc_api: Listen,
    pub admin: AdminConfig,
    pub health: Listen,
    #[serde(default)]
    pub security: Security,
    #[serde(default)]
    pub conn_limits: ConnLimits,
    #[serde(default)]
    pub tls: Tls,
}

#[derive(Debug, Deserialize)]
pub struct Listen {
    pub listen: String,
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WsConfig {
    pub listen: String,
    pub path: String,
    pub max_message_size: String,
    #[serde(with = "humantime_serde")]
    pub ping_interval: Duration,
}

#[derive(Debug, Default, Deserialize)]
pub struct SseConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub emit_path: String,
}

#[derive(Debug, Deserialize)]
pub struct AdminConfig {
    pub listen: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub auth: AdminAuthKind,
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AdminAuthKind {
    #[default]
    Password,
    Oidc,
}

#[derive(Debug, Default, Deserialize)]
pub struct Security {
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,
    #[serde(default)]
    pub cors_allow_credentials: bool,
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
    #[serde(default)]
    pub ip_allow: Vec<String>,
    #[serde(default)]
    pub ip_deny: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ConnLimits {
    #[serde(default)]
    pub max_connections: u32,
    #[serde(default)]
    pub max_connections_per_ip: u32,
    #[serde(default)]
    pub max_connections_per_user: u32,
    #[serde(default)]
    pub require_subprotocol: bool,
    // connect_rate_per_ip, handshake_timeout, idle_timeout, write_buffer_limit — TODO(parse)
}

#[derive(Debug, Default, Deserialize)]
pub struct Tls {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
}

#[derive(Debug, Deserialize)]
pub struct Redis {
    pub url: String,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default)]
    pub idempotency_ttl: Option<String>,
    /// true → multi-node via RedisBroker; false → single node (MemoryBroker).
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct Auth {
    pub jwt: Jwt,
}

#[derive(Debug, Deserialize)]
pub struct Jwt {
    pub algorithm: String,
    #[serde(default)]
    pub hmac_secret: Option<String>,
    #[serde(default)]
    pub jwks_url: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default = "default_channels_claim")]
    pub channels_claim: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiKey {
    pub key: String,
    #[serde(default)]
    pub allow: Vec<String>,
}

/// Per-namespace policy (also used for `defaults`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct NamespaceConfig {
    #[serde(default)]
    pub access: Access,
    #[serde(default)]
    pub presence: bool,
    #[serde(default)]
    pub join_leave: bool,
    #[serde(default)]
    pub history_size: usize,
    #[serde(default)]
    pub history_ttl: Option<String>,
    #[serde(default)]
    pub max_subscribers: u32,
    #[serde(default)]
    pub name_max_len: Option<usize>,
    #[serde(default)]
    pub rate_limit: Option<RateLimit>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Access {
    #[serde(default)]
    pub subscribe: AccessMode,
    #[serde(default)]
    pub publish: AccessMode,
    #[serde(default)]
    pub presence: AccessMode,
    #[serde(default)]
    pub history: AccessMode,
}

impl Default for Access {
    fn default() -> Self {
        Self {
            subscribe: AccessMode::Token,
            publish: AccessMode::Off,
            presence: AccessMode::Token,
            history: AccessMode::Token,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccessMode {
    Off,
    Public,
    #[default]
    Token,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimit {
    #[serde(default)]
    pub publish: Option<Bucket>,
    #[serde(default)]
    pub subscribe: Option<Bucket>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Bucket {
    pub rate: u32,
    pub burst: u32,
    #[serde(default)]
    pub scope: BucketScope,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BucketScope {
    #[default]
    Client,
    Channel,
    User,
}

#[derive(Debug, Default, Deserialize)]
pub struct Limits {
    #[serde(default)]
    pub max_channels_per_connection: u32,
    #[serde(default)]
    pub max_commands_per_second: u32,
}

#[derive(Debug, Default, Deserialize)]
pub struct Shutdown {
    #[serde(default)]
    pub drain_timeout: Option<String>,
    #[serde(default = "default_true")]
    pub reconnect_advice: bool,
}

#[derive(Debug, Default, Deserialize)]
pub struct Events {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub types: Vec<String>,
    #[serde(default)]
    pub transport: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Telemetry {
    #[serde(default)]
    pub log_format: Option<String>,
    #[serde(default)]
    pub tracing_enabled: bool,
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
}

fn default_true() -> bool { true }
fn default_log_level() -> String { "info".into() }
fn default_prefix() -> String { "socket".into() }
fn default_channels_claim() -> String { "channels".into() }

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] toml::de::Error),
}

impl Config {
    /// Load from a file, expanding `${ENV_VAR}`.
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        let expanded = expand_env(&raw);
        Ok(toml::from_str(&expanded)?)
    }

    /// Effective namespace: namespace values layered over `defaults`.
    /// TODO(impl): full merge of unset fields from defaults.
    pub fn namespace(&self, channel: &str) -> &NamespaceConfig {
        let ns = channel.split(':').next().unwrap_or("");
        self.namespaces.get(ns).unwrap_or(&self.defaults)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_repo_config() {
        // config.toml at the repo root ↔ serde structs haven't drifted.
        let cfg = Config::load(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config.toml"))
            .expect("config.toml should parse");
        assert_eq!(cfg.server.node_name, "socket-1");
        assert!(cfg.namespaces.contains_key("chat"));
        assert_eq!(cfg.namespace("news:sports").history_size, 100);
        assert_eq!(cfg.namespace("chat:room:1").access.publish, AccessMode::Token);
    }
}

/// Minimal expansion of `${VAR}` from the environment.
fn expand_env(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next();
            let mut var = String::new();
            for c2 in chars.by_ref() {
                if c2 == '}' { break; }
                var.push(c2);
            }
            out.push_str(&std::env::var(&var).unwrap_or_default());
        } else {
            out.push(c);
        }
    }
    out
}
