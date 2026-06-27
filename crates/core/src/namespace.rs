//! Проверка доступа: namespace задаёт «ворота» (нужен ли токен), JWT — право юзеру.

use crate::auth::Claims;
use crate::config::{AccessMode, Config};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Subscribe,
    Publish,
    Presence,
    History,
}

impl Action {
    /// Имя права в JWT.allow.
    pub fn allow_key(self) -> &'static str {
        match self {
            Action::Subscribe => "sub",
            Action::Publish => "pub",
            Action::Presence => "presence",
            Action::History => "history",
        }
    }
}

#[derive(Debug)]
pub enum Decision {
    Allow,
    Deny(&'static str),
}

/// Цепочка проверки действия на канале (см. design-doc, раздел 5).
pub fn check(cfg: &Config, claims: Option<&Claims>, channel: &str, action: Action) -> Decision {
    let ns_name = channel.split(':').next().unwrap_or("");
    if cfg.strict_namespaces && !cfg.namespaces.contains_key(ns_name) {
        return Decision::Deny("unknown_namespace");
    }
    let ns = cfg.namespace(channel);

    let mode = match action {
        Action::Subscribe => ns.access.subscribe,
        Action::Publish => ns.access.publish,
        Action::Presence => ns.access.presence,
        Action::History => ns.access.history,
    };

    match mode {
        AccessMode::Off => Decision::Deny("action_off"),
        AccessMode::Public => Decision::Allow,
        AccessMode::Token => match claims {
            Some(c) if c.allows(channel, action.allow_key()) => Decision::Allow,
            Some(_) => Decision::Deny("not_permitted"),
            None => Decision::Deny("token_required"),
        },
    }
}
