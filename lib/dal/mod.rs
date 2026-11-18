pub mod cache;
pub mod cortex;
pub mod documents;
pub mod execution;
pub mod files;
pub mod links;
pub mod models;
pub mod rooms;
pub mod training;
pub mod transcriptions;
pub mod transfers;
use crate::core::{Error, Result, Settings};
use sqlx::{AnyPool, any::AnyPoolOptions};
use std::sync::Arc;
/// Migrations in apply order for SQLite fixtures. 005, 008, 012, 013, 018 and 019 use their tested SQLite equivalents.
pub const SQLITE_MIGRATIONS: [&str; 21] = [
    include_str!("../../migrations/001_schema.sql"),
    include_str!("../../migrations/002_media.sql"),
    include_str!("../../migrations/003_owner_links.sql"),
    include_str!("../../migrations/004_v2_notes.sql"),
    include_str!("../../tests/sqlite/005_v2_note_changes.sql"),
    include_str!("../../migrations/006_v2_projects_tags.sql"),
    include_str!("../../migrations/007_v2_files_operations.sql"),
    include_str!("../../tests/sqlite/008_v2_save_operations.sql"),
    include_str!("../../migrations/009_v2_note_titles.sql"),
    include_str!("../../migrations/010_v2_publication_titles.sql"),
    include_str!("../../migrations/011_v2_project_listing.sql"),
    include_str!("../../tests/sqlite/012_v2_public_discovery.sql"),
    include_str!("../../tests/sqlite/013_v2_managed_files.sql"),
    include_str!("../../migrations/014_v2_model_registry.sql"),
    include_str!("../../migrations/015_v2_model_downloads.sql"),
    include_str!("../../migrations/016_v2_transcription_runs.sql"),
    include_str!("../../migrations/017_v2_training_feedback.sql"),
    include_str!("../../tests/sqlite/018_v2_file_previews.sql"),
    include_str!("../../tests/sqlite/019_v2_model_withdrawn.sql"),
    include_str!("../../migrations/020_v2_study_links.sql"),
    include_str!("../../migrations/021_v2_content_jobs.sql"),
];
#[derive(Clone)]
pub struct Store {
    pub db: AnyPool,
    pub docs: documents::Documents,
    pub cache: cache::Cache,
    pub rooms: rooms::Rooms,
    pub transfers: transfers::Transfers,
    pub files: files::Files,
    pub training_limits: training::FeedbackLimiter,
    /// Optional Cortex API client. Disabled by default when CORTEX_URL is empty.
    pub cortex: Arc<dyn cortex::CortexClient>,
    /// Cloud transcription execution. No server runtime exists yet.
    pub execution: Arc<dyn execution::CloudExecution>,
    pub http: reqwest::Client,
}
impl Store {
    pub async fn connect(settings: Settings) -> Result<Self> {
        sqlx::any::install_default_drivers();
        let db = AnyPoolOptions::new()
            .max_connections(5)
            .connect(&settings.database_url)
            .await?;
        let http = reqwest::Client::builder()
            .timeout(crate::core::STORAGE_REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::unavailable())?;
        let redis = if settings.redis_url.is_empty() {
            None
        } else {
            Some(
                redis::Client::open(settings.redis_url.as_str())
                    .map_err(|_| Error::unavailable())?,
            )
        };
        Ok(Self {
            db,
            docs: documents::Documents::remote(settings.clone(), http.clone()),
            cache: cache::Cache::new(redis.clone()),
            rooms: rooms::Rooms::new(redis.clone()),
            transfers: transfers::Transfers::new(redis.clone()),
            files: files::Files::new(settings.clone(), http.clone()),
            training_limits: training::FeedbackLimiter::new(redis),
            cortex: cortex::HttpCortex::client(settings.cortex_url, http.clone()),
            execution: execution::none(),
            http,
        })
    }
    pub async fn fixture() -> Self {
        sqlx::any::install_default_drivers();
        let db = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&db)
            .await
            .unwrap();
        for migration in SQLITE_MIGRATIONS {
            sqlx::raw_sql(migration).execute(&db).await.unwrap();
        }
        Self {
            db,
            docs: documents::Documents::Memory(Arc::default()),
            cache: cache::Cache::fixture(),
            rooms: rooms::Rooms::fixture(),
            transfers: transfers::Transfers::fixture(),
            files: files::Files::fixture(),
            training_limits: training::FeedbackLimiter::fixture(),
            cortex: Arc::new(cortex::DisabledCortex),
            execution: execution::none(),
            http: reqwest::Client::new(),
        }
    }
    /// True when the relational store is PostgreSQL. SQLite stands in for it in tests;
    /// the few statements that need a PostgreSQL type (the search index) branch on this.
    pub fn postgres(&self) -> bool {
        self.db
            .connect_options()
            .database_url
            .scheme()
            .starts_with("postgres")
    }
    pub async fn ready(&self) -> Result<()> {
        sqlx::query("SELECT id FROM posts LIMIT 1")
            .fetch_optional(&self.db)
            .await?;
        self.docs.ready().await
    }
}
