//! Аутентификация клиента: проверка connection JWT и capability-паттернов (glob `*`/`**`).
//!
//! Токены выдаёт ВНЕШНИЙ бэкенд — движок только проверяет подпись и матчит каналы.

use crate::config::Jwt;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

/// Claims connection JWT. Поле `channels` — список разрешённых glob-паттернов с правами.
#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    pub sub: String,
    #[serde(default)]
    pub exp: Option<u64>,
    #[serde(default)]
    pub channels: Vec<ChannelGrant>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelGrant {
    #[serde(rename = "match")]
    pub pattern: String,
    #[serde(default)]
    pub allow: Vec<String>, // "sub" | "pub" | "presence" | "history"
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("unsupported algorithm: {0}")]
    UnsupportedAlg(String),
    #[error("missing hmac_secret")]
    MissingSecret,
    #[error("invalid token: {0}")]
    Invalid(String),
}

/// Проверка подписи connection JWT (этап 1: HMAC HS256/384/512; RSA/JWKS — TODO).
pub fn validate_jwt(token: &str, cfg: &Jwt) -> Result<Claims, AuthError> {
    let alg = match cfg.algorithm.as_str() {
        "HS256" => Algorithm::HS256,
        "HS384" => Algorithm::HS384,
        "HS512" => Algorithm::HS512,
        other => return Err(AuthError::UnsupportedAlg(other.into())),
    };
    let secret = cfg.hmac_secret.as_ref().ok_or(AuthError::MissingSecret)?;

    let mut val = Validation::new(alg);
    val.required_spec_claims.clear(); // exp не обязателен (вариант B: refresh по соединению)
    match &cfg.audience {
        Some(aud) => val.set_audience(&[aud]),
        None => val.validate_aud = false,
    }

    let key = DecodingKey::from_secret(secret.as_bytes());
    decode::<Claims>(token, &key, &val)
        .map(|d| d.claims)
        .map_err(|e| AuthError::Invalid(e.to_string()))
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
