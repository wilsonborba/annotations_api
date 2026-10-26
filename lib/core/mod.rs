use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub const ROOM_SECONDS: i64 = 900;
pub const UPLOAD_BYTES: i64 = 50_000_000;
pub const CACHE_SECONDS: u64 = 120;
#[derive(Clone)]
pub struct Settings {
    pub database_url: String,
    pub couch_url: String,
    pub couch_database: String,
    pub couch_user: String,
    pub couch_password: String,
    pub redis_url: String,
    pub fsm_url: String,
    pub fsm_key: String,
    pub host: String,
    pub port: u16,
}
impl Settings {
    pub fn load() -> Self {
        fn env(k: &str, d: &str) -> String {
            std::env::var(k).unwrap_or_else(|_| d.into())
        }
        Self {
            database_url: env("DATABASE_URL", ""),
            couch_url: env("COUCHDB_URL", ""),
            couch_database: env("COUCHDB_DATABASE", "annotations_documents"),
            couch_user: env("COUCHDB_USER", ""),
            couch_password: env("COUCHDB_PASSWORD", ""),
            redis_url: env("REDIS_URL", ""),
            fsm_url: env("FSM_URL", ""),
            fsm_key: env("FSM_API_KEY", ""),
            host: env("HOST", "127.0.0.1"),
            port: env("PORT", "8111").parse().unwrap_or(8111),
        }
    }
}
#[derive(Debug)]
pub struct Error(pub StatusCode, pub String);
pub type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn bad(s: impl Into<String>) -> Self {
        Self(StatusCode::UNPROCESSABLE_ENTITY, s.into())
    }
    pub fn missing() -> Self {
        Self(StatusCode::NOT_FOUND, "Content is unavailable".into())
    }
    pub fn conflict(s: impl Into<String>) -> Self {
        Self(StatusCode::CONFLICT, s.into())
    }
    pub fn unavailable() -> Self {
        Self(
            StatusCode::SERVICE_UNAVAILABLE,
            "Storage is unavailable".into(),
        )
    }
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        tracing::error!(error=%e,"Relational storage operation failed");
        Self::unavailable()
    }
}
impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        tracing::warn!(error=%e.status().map(|s|s.as_u16()).unwrap_or(0),"Remote storage operation failed");
        Self::unavailable()
    }
}
pub fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
pub fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}
pub fn slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 100
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
