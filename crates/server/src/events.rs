//! HTTP webhook for lifecycle events (stage 8). Fire-and-forget POST to `[events].endpoint`.

use std::collections::HashSet;

use socket_core::events::{EventSink, LifecycleEvent};

pub struct HttpEventSink {
    client: reqwest::Client,
    endpoint: String,
    types: HashSet<String>,
}

impl HttpEventSink {
    pub fn new(endpoint: String, types: Vec<String>) -> Self {
        Self { client: reqwest::Client::new(), endpoint, types: types.into_iter().collect() }
    }
}

impl EventSink for HttpEventSink {
    fn emit(&self, event: LifecycleEvent) {
        // filter by configured types (empty = all)
        if !self.types.is_empty() && !self.types.contains(&event.kind) {
            return;
        }
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        tokio::spawn(async move {
            let _ = client.post(&endpoint).json(&event).send().await;
        });
    }
}
