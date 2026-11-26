pub mod logs;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

/// Bounds for a metadata fetch, across all redirect hops and DNS lookups.
#[derive(Clone, Copy, Debug)]
pub struct LinkPreviewLimits {
    pub bytes: usize,
    pub redirects: usize,
    pub timeout: std::time::Duration,
}
pub const LINK_PREVIEW_LIMITS: LinkPreviewLimits = LinkPreviewLimits {
    bytes: 512_000,
    redirects: 3,
    timeout: std::time::Duration::from_secs(5),
};

pub const ROOM_SECONDS: i64 = 900;
/// Attempts to merge an independent Instant write into a concurrently changed room.
pub const ROOM_CAS_ATTEMPTS: usize = 16;
pub const UPLOAD_BYTES: i64 = 50_000_000;
pub const CACHE_SECONDS: u64 = 120;
/// Shared storage request bound, also used before sweeping uncertain temporary uploads.
pub const STORAGE_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
/// Default note limits (annotations#19, decision 32a104ee983b48ca995a7e7820839020):
/// at least 30 dense handwritten pages in one note. A stored revision keeps its
/// body as one gzip attachment of its CouchDB document, so the uncompressed
/// document may be larger than the CouchDB document size.
pub const NOTE_LIMITS: NoteLimits = NoteLimits {
    max_items: 50_000,
    max_samples_per_stroke: 10_000,
    max_nodes_per_stroke: 1_000,
    max_document_bytes: 64 * 1024 * 1024,
};
/// The invisible placement grid of the editor (B2.1): one cell in page units.
/// Structured blocks sit on whole cells; ink and shape points never snap.
pub const GRID_CELL: f64 = 8.0;
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
/// Bounds of transcription runs (`/me/transcriptions`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TranscriptionLimits {
    /// Managed transcription files that one run names.
    pub max_file_refs: usize,
    /// Events that one read of the event stream sends at most.
    pub event_batch: i64,
    /// Wait between two checks for new events of an unfinished run.
    pub event_poll: std::time::Duration,
    /// Keep-alive comment interval of the event stream.
    pub keep_alive: std::time::Duration,
}
/// Default transcription bounds. The keep-alive matches the log stream.
pub const TRANSCRIPTION_LIMITS: TranscriptionLimits = TranscriptionLimits {
    max_file_refs: 20,
    event_batch: 100,
    event_poll: std::time::Duration::from_secs(1),
    keep_alive: std::time::Duration::from_secs(15),
};
/// Bounds of correction feedback (`POST /me/transcriptions/{run_id}/feedback`) and of
/// the training command line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrainingLimits {
    /// Size of one feedback request body in bytes.
    pub max_request_bytes: usize,
    /// Events in one request.
    pub max_events: usize,
    /// Characters in the model suggestion or in the user correction.
    pub max_text_chars: usize,
    /// Ink item ids of the run that one event names.
    pub max_source_items: usize,
    /// Strokes in one event.
    pub max_strokes: usize,
    /// Samples in one stroke.
    pub max_samples_per_stroke: usize,
    /// Largest x or y of a sample, in integer 1/100 page units.
    pub max_coordinate: i64,
    /// Largest sample time, in ms from the stroke start.
    pub max_sample_ms: i64,
    /// Rows that one export or remove step reads at most.
    pub batch: i64,
    /// Representative `received` sample size shown in `training review`.
    pub review_sample: i64,
    /// Concurrent feedback requests allowed per account.
    pub max_concurrent_per_account: usize,
    /// Feedback requests allowed per account inside one rate window.
    pub max_requests_per_window: usize,
    /// Rate window duration in seconds.
    pub rate_window_seconds: u64,
    /// Safety expiry for an in-flight concurrency slot in Redis.
    pub inflight_ttl_seconds: u64,
}
/// Default feedback bounds. One request stays far below the note body limit; the
/// sample limit matches `NOTE_LIMITS`; pressure and tilt use the note stroke ranges.
pub const TRAINING_LIMITS: TrainingLimits = TrainingLimits {
    max_request_bytes: 2_000_000,
    max_events: 100,
    max_text_chars: 10_000,
    max_source_items: 1_000,
    max_strokes: 500,
    max_samples_per_stroke: NOTE_LIMITS.max_samples_per_stroke,
    max_coordinate: 100_000_000,
    max_sample_ms: 600_000,
    batch: 200,
    review_sample: 20,
    max_concurrent_per_account: 4,
    max_requests_per_window: 60,
    rate_window_seconds: 60,
    inflight_ttl_seconds: 60,
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
    /// Optional Cortex API base URL. Empty leaves Cortex content jobs off.
    pub cortex_url: String,
    /// Optional Plane Slim LAN base URL. Empty disables task card routes.
    pub plane_url: String,
    /// Optional central gateway base URL for waitlist forwarding.
    pub gateway_url: String,
    /// Optional Reports backend base URL for owner item verification & cascade delete.
    pub reports_url: String,
    /// Server-held Plane Slim API key. Never exposed to clients or logs.
    pub plane_api_key: String,
    /// Plane Slim workspace slug.
    pub plane_workspace: String,
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
            cortex_url: env("CORTEX_URL", ""),
            plane_url: env("PLANE_URL", ""),
            gateway_url: env("GATEWAY_URL", ""),
            reports_url: env("REPORTS_URL", ""),
            plane_api_key: env("PLANE_API_KEY", ""),
            plane_workspace: env("PLANE_WORKSPACE", "asodya"),
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
