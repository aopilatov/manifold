//! Metric counters (atomic, with no Prometheus dependency). The exposition format lives in server.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub messages_published: AtomicU64,
    pub subscriptions: AtomicU64,
    pub unsubscriptions: AtomicU64,
    pub connections_opened: AtomicU64,
    pub connections_closed: AtomicU64,
}

impl Metrics {
    #[inline]
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn val(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}
