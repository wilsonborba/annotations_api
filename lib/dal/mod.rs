pub mod cache;
pub mod documents;
pub mod files;
pub mod rooms;
use crate::core::{Error, Result, Settings};
use sqlx::{AnyPool, any::AnyPoolOptions};
use std::sync::Arc;
#[derive(Clone)]
pub struct Store {
    pub db: AnyPool,
    pub docs: documents::Documents,
    pub cache: cache::Cache,
    pub rooms: rooms::Rooms,
    pub files: files::Files,
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
            .timeout(std::time::Duration::from_secs(30))
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
            rooms: rooms::Rooms::new(redis),
            files: files::Files::new(settings, http.clone()),
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
        sqlx::raw_sql(include_str!("../../migrations/001_schema.sql"))
            .execute(&db)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../../migrations/002_media.sql"))
            .execute(&db)
            .await
            .unwrap();
        Self {
            db,
            docs: documents::Documents::Memory(Arc::default()),
            cache: cache::Cache::fixture(),
            rooms: rooms::Rooms::fixture(),
            files: files::Files::fixture(),
            http: reqwest::Client::new(),
        }
    }
    pub async fn ready(&self) -> Result<()> {
        sqlx::query("SELECT id FROM posts LIMIT 1")
            .fetch_optional(&self.db)
            .await?;
        self.docs.ready().await
    }
}
