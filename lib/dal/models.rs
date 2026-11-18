//! Relational model metadata. No artifact bytes, FSM requests or training data access.
use crate::domain::{
    models::{Listing, Manifest, PublicArtifact, PublicManifest, Result},
    notes::NoteError,
};
use axum::http::StatusCode;
use sqlx::{Any, AnyPool, Row, Transaction, any::AnyRow};
use std::collections::BTreeMap;

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
pub async fn exists(tx: &mut Transaction<'_, Any>, version: &str) -> Result<bool> {
    Ok(
        sqlx::query("SELECT version FROM model_releases WHERE version=$1")
            .bind(version)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
    )
}
pub async fn switch(
    tx: &mut Transaction<'_, Any>,
    current: Option<&str>,
    target: &str,
) -> Result<()> {
    sqlx::query("UPDATE model_releases SET status='retired' WHERE status='active'")
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE model_releases SET status='active' WHERE version=$1")
        .bind(target)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE model_default SET current_version=$1,previous_version=$2 WHERE id=1")
        .bind(target)
        .bind(current)
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
    let mut metrics: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for row in sqlx::query(
        "SELECT language,name,value FROM model_metrics WHERE version=$1 ORDER BY language,name",
    )
    .bind(&version)
    .fetch_all(db)
    .await?
    {
        metrics
            .entry(row.try_get("language")?)
            .or_default()
            .insert(row.try_get("name")?, row.try_get("value")?);
    }
    let current: Option<String> = row.try_get("current_version")?;
    Ok(PublicManifest {
        is_default: current.as_deref() == Some(&version),
        version,
        status: row.try_get("status")?,
        created_at: row.try_get("created_at")?,
        artifacts,
        languages,
        capabilities,
        metrics,
    })
}
pub async fn list(db: &AnyPool, platform: Option<&str>, language: Option<&str>) -> Result<Listing> {
    // Capture statuses and pointer in one SQL snapshot. Child metadata is immutable.
    let rows = sqlx::query("SELECT r.version,r.status,r.created_at,d.current_version FROM model_releases r CROSS JOIN model_default d WHERE r.status <> 'draft' AND ($1 IS NULL OR EXISTS(SELECT 1 FROM model_artifacts a WHERE a.version=r.version AND a.platform=$1)) AND ($2 IS NULL OR EXISTS(SELECT 1 FROM model_languages l WHERE l.version=r.version AND l.language=$2)) ORDER BY r.created_at DESC,r.version")
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
pub async fn detail(db: &AnyPool, version: &str) -> Result<Option<PublicManifest>> {
    let row = sqlx::query("SELECT r.version,r.status,r.created_at,d.current_version FROM model_releases r CROSS JOIN model_default d WHERE r.version=$1 AND r.status <> 'draft'")
        .bind(version).fetch_optional(db).await?;
    match row {
        Some(row) => Ok(Some(public(db, row).await?)),
        None => Ok(None),
    }
}
