//! События жизненного цикла соединений → прикладной бэкенд (опц., `[events]`).
//! Это НЕ авторизация (она на JWT) — уведомления для аналитики/очистки.

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

/// Куда уходят события. Реализация HTTP-вебхука — в server-крейте.
pub trait EventSink: Send + Sync {
    fn emit(&self, event: LifecycleEvent);
}

/// По умолчанию — никуда.
pub struct NoopSink;
impl EventSink for NoopSink {
    fn emit(&self, _event: LifecycleEvent) {}
}
