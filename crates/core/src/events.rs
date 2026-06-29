//! Connection lifecycle events → application backend (optional, `[events]`).
//! This is NOT authorization (that's on the JWT) — notifications for analytics/cleanup.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct LifecycleEvent {
    #[serde(rename = "type")]
    pub kind: String, // connected | disconnected | subscribed | unsubscribed
    pub node: String,
    pub user: String,
    pub client: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

/// Where events go. The HTTP webhook implementation lives in the server crate.
pub trait EventSink: Send + Sync {
    fn emit(&self, event: LifecycleEvent);
}

/// By default — nowhere.
pub struct NoopSink;
impl EventSink for NoopSink {
    fn emit(&self, _event: LifecycleEvent) {}
}
