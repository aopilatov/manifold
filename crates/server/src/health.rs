//! Health/readiness для k8s/LB. `/ready` учитывает дренаж и доступность Redis.

use axum::http::StatusCode;

pub async fn liveness() -> StatusCode {
    StatusCode::OK
}

pub async fn readiness() -> StatusCode {
    // TODO(impl): 503 при дренаже (graceful shutdown) или недоступном Redis.
    StatusCode::OK
}
