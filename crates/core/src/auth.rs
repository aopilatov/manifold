//! Аутентификация клиента: проверка connection JWT и capability-паттернов (glob `*`/`**`).
//!
//! Токены выдаёт ВНЕШНИЙ бэкенд — движок только проверяет подпись и матчит каналы.

use serde::Deserialize;

/// Claims connection JWT. Поле `channels` — список разрешённых glob-паттернов с правами.
#[derive(Debug, Deserialize)]
pub struct Claims {
    pub sub: String,
    #[serde(default)]
    pub exp: Option<u64>,
    #[serde(default)]
    pub channels: Vec<ChannelGrant>,
}

#[derive(Debug, Deserialize)]
pub struct ChannelGrant {
    #[serde(rename = "match")]
    pub pattern: String,
    #[serde(default)]
    pub allow: Vec<String>, // "sub" | "pub" | "presence" | "history"
}

impl Claims {
    /// Разрешено ли действие `action` на канале `channel` хотя бы одним грантом.
    pub fn allows(&self, channel: &str, action: &str) -> bool {
        self.channels.iter().any(|g| {
            g.allow.iter().any(|a| a == action) && glob_match(&g.pattern, channel)
        })
    }
}

/// Glob-матчинг по сегментам, разделитель `:`.
/// `*`  — ровно один сегмент; `**` — любое число сегментов (globstar).
pub fn glob_match(pattern: &str, channel: &str) -> bool {
    let p: Vec<&str> = pattern.split(':').collect();
    let c: Vec<&str> = channel.split(':').collect();
    seg_match(&p, &c)
}

fn seg_match(p: &[&str], c: &[&str]) -> bool {
    match (p.first(), c.first()) {
        (None, None) => true,
        (Some(&"**"), _) => {
            // globstar: матчит 0..N сегментов
            if p.len() == 1 {
                return true;
            }
            (0..=c.len()).any(|i| seg_match(&p[1..], &c[i..]))
        }
        (Some(&"*"), Some(_)) => seg_match(&p[1..], &c[1..]),
        (Some(ps), Some(cs)) if ps == cs => seg_match(&p[1..], &c[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn star_one_segment() {
        assert!(glob_match("news:*", "news:sports"));
        assert!(!glob_match("news:*", "news:sports:football"));
    }

    #[test]
    fn globstar_any() {
        assert!(glob_match("news:**", "news:sports"));
        assert!(glob_match("news:**", "news:sports:football"));
        assert!(glob_match("user:123:**", "user:123:notifications:push"));
    }

    #[test]
    fn exact() {
        assert!(glob_match("chat:room:42", "chat:room:42"));
        assert!(!glob_match("chat:room:42", "chat:room:43"));
    }
}
