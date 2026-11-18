pub mod logs;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub const ROOM_SECONDS: i64 = 900;
pub const UPLOAD_BYTES: i64 = 50_000_000;
pub const CACHE_SECONDS: u64 = 120;

/// Limits for v2 note documents (`note-original`, `note-typed`, `instant-room`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NoteLimits {
    /// Items in one document, nested ink of typed handwriting blocks included.
    pub max_items: usize,
    /// Raw input samples in one stroke.
    pub max_samples_per_stroke: usize,
    /// Editable nodes in one stroke or shape path.
    pub max_nodes_per_stroke: usize,
    /// Size of the serialized document body in bytes.
    pub max_document_bytes: usize,
}
/// Default note limits. The document byte limit stays below the CouchDB 3 default
/// `max_document_size` (8,000,000 bytes) and leaves room for the stored envelope.
pub const NOTE_LIMITS: NoteLimits = NoteLimits {
    max_items: 10_000,
    max_samples_per_stroke: 10_000,
    max_nodes_per_stroke: 1_000,
    max_document_bytes: 7_500_000,
};
/// Retry and cleanup bounds of the note save pipeline and its reconciliation task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SaveSettings {
    /// A started save older than this is treated as stopped by the task.
    pub grace_seconds: i64,
    /// Operations and revisions that one task run handles at most, each.
    pub batch: i64,
    /// Task attempts for one stopped operation before it waits for an operator.
    pub max_attempts: i64,
    /// Age after which a revision that is not current, not published and not a
    /// source of another revision is removed.
    pub revision_retention_seconds: i64,
}
/// Default save bounds. A save normally ends in seconds; the grace period leaves
/// room for slow uploads of large documents before the task cleans up.
pub const SAVE_SETTINGS: SaveSettings = SaveSettings {
    grace_seconds: 900,
    batch: 100,
    max_attempts: 5,
    revision_retention_seconds: 86_400,
};
/// Limits of the owner note API (`/me/notes`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NoteApiLimits {
    /// Characters in a note title.
    pub max_title_chars: usize,
    /// Tags on one note.
    pub max_tags: usize,
    /// Characters in one tag name.
    pub max_tag_chars: usize,
    /// Projects named in one create request.
    pub max_projects: usize,
    /// Characters in a project name.
    pub max_project_name_chars: usize,
    /// List page size when the request gives no limit.
    pub default_page: i64,
    /// Largest list page size and largest sync batch.
    pub max_page: i64,
}
/// Default owner note API limits.
pub const NOTE_API_LIMITS: NoteApiLimits = NoteApiLimits {
    max_title_chars: 200,
    max_tags: 20,
    max_tag_chars: 50,
    max_projects: 50,
    max_project_name_chars: 100,
    default_page: 50,
    max_page: 100,
};
/// Limits of public discovery (`/notes`, `/authors`, `/projects`) and its search index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoveryLimits {
    /// Characters of the short summary taken from the published Typed text.
    pub summary_chars: usize,
    /// Characters in the search text `q`.
    pub max_query_chars: usize,
    /// Characters in one search term; longer words are not indexed.
    pub max_term_chars: usize,
    /// Distinct search terms of one published note.
    pub max_terms: usize,
}
/// Default discovery limits. The summary limit matches the CHECK of `note_search.summary`.
pub const DISCOVERY_LIMITS: DiscoveryLimits = DiscoveryLimits {
    summary_chars: 200,
    max_query_chars: 200,
    max_term_chars: 100,
    max_terms: 20_000,
};
/// Limits of owner managed files (`/me/files`). Each purpose has its own byte limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileLimits {
    /// Note attachments: images on paper, file cards and previews.
    pub attachment_bytes: i64,
    /// Transcription input files.
    pub transcription_bytes: i64,
    /// Content job input files (PDF, image, audio, video).
    pub content_job_bytes: i64,
    /// Characters in a file name.
    pub max_filename_chars: usize,
}
/// Default file limits. Every value stays below the FSM default upload limit
/// (`FSM_MAX_UPLOAD_BYTES`, 104,857,600 bytes). The attachment limit keeps the
/// existing 50 MB image limit (`UPLOAD_BYTES`).
pub const FILE_LIMITS: FileLimits = FileLimits {
    attachment_bytes: UPLOAD_BYTES,
    transcription_bytes: 20_000_000,
    content_job_bytes: 100_000_000,
    max_filename_chars: 255,
};
/// Bounds of the managed file cleanup task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileSettings {
    /// An upload that holds `uploading` longer than this is treated as stopped.
    pub grace_seconds: i64,
    /// Queue rows and stopped uploads that one task run handles at most, each.
    pub batch: i64,
    /// FSM delete attempts for one item before it waits for an operator.
    pub max_attempts: i64,
    /// Wait before the next attempt after a failed FSM delete.
    pub retry_seconds: i64,
}
/// Default cleanup bounds, the same as the note save task.
pub const FILE_SETTINGS: FileSettings = FileSettings {
    grace_seconds: 900,
    batch: 100,
    max_attempts: 5,
    retry_seconds: 30,
};
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
    /// Folder for upload staging before FSM. Empty means the system temp folder.
    pub upload_staging_dir: String,
    pub host: String,
    pub port: u16,
    pub development: bool,
}
impl Settings {
    pub fn development(&self) -> bool {
        self.development
    }
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
            upload_staging_dir: env("UPLOAD_STAGING_DIR", ""),
            host: env("HOST", "127.0.0.1"),
            port: env("PORT", "8111").parse().unwrap_or(8111),
            development: env("DEVELOPMENT_MODE", "false").parse().unwrap_or(false),
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
