//! Relational model metadata. No artifact bytes, FSM requests or training data access.
use crate::domain::{
    models::{
        Download, DownloadContent, Listing, Manifest, PublicArtifact, PublicManifest, Result,
    },
    notes::NoteError,
};
use axum::http::StatusCode;
use sqlx::{Any, AnyPool, Row, Transaction, any::AnyRow};

pub async fn register(db: &AnyPool, m: &Manifest) -> Result<()> {
    let mut tx = db.begin().await?;
    let result = sqlx::query("INSERT INTO model_releases(version,status,code_revision,dataset_snapshot_id,source_checkpoint,created_at) VALUES($1,'draft',$2,$3,$4,$5)")
        .bind(&m.release_id).bind(&m.code_revision).bind(&m.dataset_snapshot_id)
        .bind(&m.source_checkpoint).bind(chrono::Utc::now().timestamp()).execute(&mut *tx).await;
    match result {
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            return Err(NoteError::new(
                StatusCode::CONFLICT,
                "model_release_exists",
                "Model releases are immutable; use a new release id",
            ));
        }
        other => {
            other?;
        }
    }
    for a in &m.artifacts {
        sqlx::query("INSERT INTO model_artifacts(version,platform,fsm_reference,size,sha256) VALUES($1,$2,$3,$4,$5)")
            .bind(&m.release_id).bind(&a.platform).bind(&a.fsm_reference).bind(a.size).bind(&a.sha256).execute(&mut *tx).await?;
    }
    for lang in &m.languages {
        sqlx::query("INSERT INTO model_languages(version,language) VALUES($1,$2)")
            .bind(&m.release_id)
            .bind(lang)
            .execute(&mut *tx)
            .await?;
        for (name, value) in &m.metrics[lang] {
            sqlx::query(
                "INSERT INTO model_metrics(version,language,name,value) VALUES($1,$2,$3,$4)",
            )
            .bind(&m.release_id)
            .bind(lang)
            .bind(name)
            .bind(value)
            .execute(&mut *tx)
            .await?;
        }
    }
    for capability in &m.capabilities {
        sqlx::query("INSERT INTO model_capabilities(version,capability) VALUES($1,$2)")
            .bind(&m.release_id)
            .bind(capability)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}
pub async fn lock_default(db: &AnyPool) -> Result<Transaction<'static, Any>> {
    let mut tx = db.begin().await?;
    // An UPDATE obtains a row lock on PostgreSQL and a writer lock on SQLite.
    sqlx::query("UPDATE model_default SET id=id WHERE id=1")
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}
pub async fn pointers(tx: &mut Transaction<'_, Any>) -> Result<(Option<String>, Option<String>)> {
    let row = sqlx::query("SELECT current_version,previous_version FROM model_default WHERE id=1")
        .fetch_one(&mut **tx)
        .await?;
    Ok((
        row.try_get("current_version")?,
        row.try_get("previous_version")?,
    ))
}
pub async fn release_status(
    tx: &mut Transaction<'_, Any>,
    version: &str,
) -> Result<Option<String>> {
    Ok(
        sqlx::query("SELECT status FROM model_releases WHERE version=$1")
            .bind(version)
            .fetch_optional(&mut **tx)
            .await?
            .map(|r| r.get("status")),
    )
}
pub async fn switch(
    tx: &mut Transaction<'_, Any>,
    target: &str,
    previous: Option<&str>,
    replaced_status: &str,
) -> Result<()> {
    sqlx::query("UPDATE model_releases SET status=$1 WHERE status='active'")
        .bind(replaced_status)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE model_releases SET status='active' WHERE version=$1")
        .bind(target)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE model_default SET current_version=$1,previous_version=$2 WHERE id=1")
        .bind(target)
        .bind(previous)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub async fn mark_withdrawn(tx: &mut Transaction<'_, Any>, version: &str) -> Result<()> {
    sqlx::query("UPDATE model_releases SET status='withdrawn' WHERE version=$1")
        .bind(version)
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "UPDATE model_default SET previous_version=NULL WHERE id=1 AND previous_version=$1",
    )
    .bind(version)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
async fn public(db: &AnyPool, row: AnyRow) -> Result<PublicManifest> {
    let version: String = row.try_get("version")?;
    let artifacts = sqlx::query(
        "SELECT platform,size,sha256 FROM model_artifacts WHERE version=$1 ORDER BY platform",
    )
    .bind(&version)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| {
        Ok(PublicArtifact {
            platform: row.try_get("platform")?,
            size: row.try_get("size")?,
            sha256: row.try_get("sha256")?,
        })
    })
    .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?;
    let languages =
        sqlx::query("SELECT language FROM model_languages WHERE version=$1 ORDER BY language")
            .bind(&version)
            .fetch_all(db)
            .await?
            .into_iter()
            .map(|r| r.try_get("language"))
            .collect::<std::result::Result<Vec<String>, _>>()?;
    let capabilities = sqlx::query(
        "SELECT capability FROM model_capabilities WHERE version=$1 ORDER BY capability",
    )
    .bind(&version)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|r| r.try_get("capability"))
    .collect::<std::result::Result<Vec<String>, _>>()?;
    let current: Option<String> = row.try_get("current_version")?;
    Ok(PublicManifest {
        is_default: current.as_deref() == Some(&version),
        version,
        status: row.try_get("status")?,
        created_at: row.try_get("created_at")?,
        artifacts,
        languages,
        capabilities,
    })
}
pub async fn list(db: &AnyPool, platform: Option<&str>, language: Option<&str>) -> Result<Listing> {
    // Capture statuses and pointer in one SQL snapshot. Child metadata is immutable.
    let rows = sqlx::query("SELECT r.version,r.status,r.created_at,d.current_version FROM model_releases r CROSS JOIN model_default d WHERE r.status IN ('active','retired') AND ($1 IS NULL OR EXISTS(SELECT 1 FROM model_artifacts a WHERE a.version=r.version AND a.platform=$1)) AND ($2 IS NULL OR EXISTS(SELECT 1 FROM model_languages l WHERE l.version=r.version AND l.language=$2)) ORDER BY r.created_at DESC,r.version")
        .bind(platform).bind(language).fetch_all(db).await?;
    let mut default_version = None;
    let mut models = Vec::new();
    for row in rows {
        default_version = row.try_get("current_version")?;
        models.push(public(db, row).await?);
    }
    // An empty compatible list has no compatible default.
    if !models.iter().any(|m| m.is_default) {
        default_version = None;
    }
    Ok(Listing {
        default_version,
        models,
    })
}
pub enum DetailRow {
    Missing,
    Withdrawn,
    Manifest(PublicManifest),
}
pub async fn detail(db: &AnyPool, version: &str) -> Result<DetailRow> {
    let row = sqlx::query("SELECT r.version,r.status,r.created_at,d.current_version FROM model_releases r CROSS JOIN model_default d WHERE r.version=$1 AND r.status <> 'draft'")
        .bind(version).fetch_optional(db).await?;
    match row {
        Some(row) if row.try_get::<String, _>("status")? == "withdrawn" => Ok(DetailRow::Withdrawn),
        Some(row) => Ok(DetailRow::Manifest(public(db, row).await?)),
        None => Ok(DetailRow::Missing),
    }
}

pub enum ArtifactCheck {
    Missing,
    Withdrawn,
    Ready,
}
/// Check whether the release is served (active or retired) and has an artifact for the platform.
pub async fn served_artifact(db: &AnyPool, version: &str, platform: &str) -> Result<ArtifactCheck> {
    let row = sqlx::query("SELECT r.status FROM model_artifacts a JOIN model_releases r ON r.version=a.version WHERE a.version=$1 AND a.platform=$2 AND r.status <> 'draft'")
        .bind(version).bind(platform).fetch_optional(db).await?;
    match row {
        Some(row) if row.try_get::<String, _>("status")? == "withdrawn" => {
            Ok(ArtifactCheck::Withdrawn)
        }
        Some(_) => Ok(ArtifactCheck::Ready),
        None => Ok(ArtifactCheck::Missing),
    }
}
const DOWNLOAD: &str = "SELECT d.download_id,d.version,d.platform,d.created_at,a.size,a.sha256,a.fsm_reference,r.status FROM model_downloads d JOIN model_artifacts a ON a.version=d.version AND a.platform=d.platform JOIN model_releases r ON r.version=d.version";
fn to_download(row: &AnyRow) -> std::result::Result<Download, sqlx::Error> {
    Ok(Download {
        download_id: row.try_get("download_id")?,
        model_version: row.try_get("version")?,
        platform: row.try_get("platform")?,
        size: row.try_get("size")?,
        sha256: row.try_get("sha256")?,
        model_status: row.try_get("status")?,
        created_at: row.try_get("created_at")?,
    })
}
/// Insert the download unless the account already has one for this version and platform,
/// then return the stored row. Concurrent retries converge on one id.
pub async fn create_download(
    db: &AnyPool,
    id: &str,
    owner: &str,
    version: &str,
    platform: &str,
    now: i64,
) -> Result<Download> {
    let mut tx = db.begin().await?;
    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO model_downloads(download_id,owner_id,version,platform,created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(owner_id,version,platform) DO NOTHING")
        .bind(id).bind(owner).bind(version).bind(platform).bind(now).execute(&mut *tx).await?;
    let row = sqlx::query(&format!(
        "{DOWNLOAD} WHERE d.owner_id=$1 AND d.version=$2 AND d.platform=$3"
    ))
    .bind(owner)
    .bind(version)
    .bind(platform)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(to_download(&row)?)
}
pub enum OwnDownload {
    Missing,
    Withdrawn,
    Row(AnyRow),
}
async fn own(db: &AnyPool, owner: &str, id: &str) -> Result<OwnDownload> {
    // Drafts are never served; the owner filter hides other accounts' ids.
    let row = sqlx::query(&format!(
        "{DOWNLOAD} WHERE d.download_id=$1 AND d.owner_id=$2 AND r.status <> 'draft'"
    ))
    .bind(id)
    .bind(owner)
    .fetch_optional(db)
    .await?;
    match row {
        Some(row) if row.try_get::<String, _>("status")? == "withdrawn" => {
            Ok(OwnDownload::Withdrawn)
        }
        Some(row) => Ok(OwnDownload::Row(row)),
        None => Ok(OwnDownload::Missing),
    }
}
pub async fn download(db: &AnyPool, owner: &str, id: &str) -> Result<OwnDownload> {
    own(db, owner, id).await
}
pub fn download_from_row(row: &AnyRow) -> Result<Download> {
    Ok(to_download(row)?)
}
pub fn content_from_row(row: &AnyRow) -> Result<DownloadContent> {
    Ok(DownloadContent {
        fsm_reference: row.try_get("fsm_reference")?,
        size: row.try_get("size")?,
        sha256: row.try_get("sha256")?,
    })
}
