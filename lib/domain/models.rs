//! Owned release metadata and manual promotion. The public DTO is a strict whitelist:
//! versions, checksums, sizes, languages and capabilities. Provenance, FSM references
//! and evaluation metrics stay on the server.
use crate::{dal::models as repository, domain::notes::NoteError};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::AnyPool;
use std::collections::{BTreeMap, HashSet};

pub type Result<T> = std::result::Result<T, NoteError>;

/// Server input only. Provenance and FSM references never reach a public manifest.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub release_id: String,
    pub code_revision: String,
    pub dataset_snapshot_id: String,
    pub source_checkpoint: String,
    pub artifacts: Vec<Artifact>,
    pub languages: Vec<String>,
    pub capabilities: Vec<String>,
    pub metrics: BTreeMap<String, BTreeMap<String, f64>>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub platform: String,
    pub fsm_reference: String,
    pub size: i64,
    pub sha256: String,
}
#[derive(Debug, Serialize)]
pub struct PublicArtifact {
    pub platform: String,
    pub size: i64,
    pub sha256: String,
}
#[derive(Debug, Serialize)]
pub struct PublicManifest {
    pub version: String,
    pub status: String,
    pub is_default: bool,
    pub created_at: i64,
    pub artifacts: Vec<PublicArtifact>,
    pub languages: Vec<String>,
    pub capabilities: Vec<String>,
}
#[derive(Debug, Serialize)]
pub struct Listing {
    pub default_version: Option<String>,
    pub models: Vec<PublicManifest>,
}

fn invalid() -> NoteError {
    NoteError::invalid("invalid_model_manifest", "Model manifest is invalid")
}
pub fn token(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn platform(s: &str) -> bool {
    matches!(s, "android" | "ios" | "macos" | "linux")
}
fn language(s: &str) -> bool {
    matches!(s, "en" | "pt-BR" | "th")
}
fn distinct(values: &[String]) -> bool {
    values.iter().collect::<HashSet<_>>().len() == values.len()
}
impl Manifest {
    pub fn validate(&self) -> Result<()> {
        if !token(&self.release_id, 128)
            || [
                &self.code_revision,
                &self.dataset_snapshot_id,
                &self.source_checkpoint,
            ]
            .iter()
            .any(|s| s.trim().is_empty() || s.len() > 4096 || s.chars().any(char::is_control))
            || self.artifacts.is_empty()
            || self.artifacts.len() > 4
            || self.languages.is_empty()
            || self.languages.len() > 3
            || !distinct(&self.languages)
            || self.languages.iter().any(|s| !language(s))
            || self.capabilities.is_empty()
            || self.capabilities.len() > 32
            || !distinct(&self.capabilities)
            || self.capabilities.iter().any(|s| !token(s, 64))
            || self.metrics.len() != self.languages.len()
        {
            return Err(invalid());
        }
        let mut platforms = HashSet::new();
        for artifact in &self.artifacts {
            if !platform(&artifact.platform)
                || !platforms.insert(&artifact.platform)
                || artifact.size <= 0
                || artifact.fsm_reference.trim().is_empty()
                || artifact.fsm_reference.len() > 4096
                || artifact.fsm_reference.chars().any(char::is_control)
                || artifact.sha256.len() != 64
                || !artifact
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(invalid());
            }
        }
        for lang in &self.languages {
            let metrics = self.metrics.get(lang).ok_or_else(invalid)?;
            if metrics.is_empty()
                || metrics.len() > 32
                || metrics
                    .iter()
                    .any(|(name, value)| !token(name, 64) || !value.is_finite() || *value < 0.0)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

pub async fn register(db: &AnyPool, manifest: Manifest) -> Result<String> {
    manifest.validate()?;
    repository::register(db, &manifest).await?;
    Ok(manifest.release_id)
}
fn conflict(code: &str, message: &str) -> NoteError {
    NoteError::new(StatusCode::CONFLICT, code, message)
}
fn withdrawn_error() -> NoteError {
    NoteError::new(
        StatusCode::GONE,
        "model_withdrawn",
        "Model release has been withdrawn",
    )
}
pub async fn activate(db: &AnyPool, version: &str) -> Result<String> {
    if !token(version, 128) {
        return Err(invalid());
    }
    promote(db, Some(version)).await
}
pub async fn rollback(db: &AnyPool) -> Result<String> {
    promote(db, None).await
}
/// Mark a non-active release as `withdrawn` so it is never listed or downloaded.
pub async fn withdraw(db: &AnyPool, version: &str) -> Result<String> {
    if !token(version, 128) {
        return Err(invalid());
    }
    let mut tx = repository::lock_default(db).await?;
    let status = repository::release_status(&mut tx, version)
        .await?
        .ok_or_else(|| {
            NoteError::new(
                StatusCode::NOT_FOUND,
                "model_not_found",
                "Model release is unavailable",
            )
        })?;
    if status == "active" {
        return Err(conflict(
            "model_active",
            "Roll back or activate another release before withdrawing the active release",
        ));
    }
    if status != "withdrawn" {
        repository::mark_withdrawn(&mut tx, version).await?;
    }
    tx.commit().await?;
    Ok(version.to_owned())
}
async fn promote(db: &AnyPool, requested: Option<&str>) -> Result<String> {
    let mut tx = repository::lock_default(db).await?;
    let (current, previous) = repository::pointers(&mut tx).await?;
    let target = requested.or(previous.as_deref()).ok_or_else(|| {
        conflict(
            "model_rollback_unavailable",
            "No previous valid model release is available",
        )
    })?;
    // Releases are immutable after registration; withdrawn releases cannot be activated.
    let target_status = repository::release_status(&mut tx, target)
        .await?
        .ok_or_else(|| {
            NoteError::new(
                StatusCode::NOT_FOUND,
                "model_not_found",
                "Model release is unavailable",
            )
        })?;
    if target_status == "withdrawn" {
        return Err(withdrawn_error());
    }
    if current.as_deref() != Some(target) {
        // Activate keeps the replaced release as retired and sets it as the rollback target.
        // Rollback marks the left release as withdrawn and clears the rollback target.
        let (next_previous, replaced_status) = if requested.is_some() {
            (current.as_deref(), "retired")
        } else {
            (None, "withdrawn")
        };
        repository::switch(&mut tx, target, next_previous, replaced_status).await?;
    }
    tx.commit().await?;
    Ok(target.to_owned())
}

pub async fn list(
    db: &AnyPool,
    platform_filter: Option<&str>,
    language_filter: Option<&str>,
) -> Result<Listing> {
    if platform_filter.is_some_and(|s| !platform(s))
        || language_filter.is_some_and(|s| !language(s))
    {
        return Err(NoteError::invalid(
            "invalid_model_filter",
            "Unsupported platform or language",
        ));
    }
    repository::list(db, platform_filter, language_filter).await
}
pub async fn detail(db: &AnyPool, version: &str) -> Result<PublicManifest> {
    if !token(version, 128) {
        return Err(NoteError::new(
            StatusCode::NOT_FOUND,
            "model_not_found",
            "Model release is unavailable",
        ));
    }
    match repository::detail(db, version).await? {
        repository::DetailRow::Manifest(m) => Ok(m),
        repository::DetailRow::Withdrawn => Err(withdrawn_error()),
        repository::DetailRow::Missing => Err(NoteError::new(
            StatusCode::NOT_FOUND,
            "model_not_found",
            "Model release is unavailable",
        )),
    }
}

/// `POST /me/model_downloads` body. The account comes from the session, never the body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadRequest {
    pub model_version: String,
    pub platform: String,
}
/// A logical download bound to one account, release and platform. It names the shared
/// artifact by checksum and size only; the FSM reference never leaves the server.
#[derive(Debug, Serialize)]
pub struct Download {
    pub download_id: String,
    pub model_version: String,
    pub platform: String,
    pub size: i64,
    pub sha256: String,
    pub model_status: String,
    pub created_at: i64,
}
/// Server-side read target of a download. Not serializable on purpose.
pub struct DownloadContent {
    pub fsm_reference: String,
    pub size: i64,
    pub sha256: String,
}
fn download_not_found() -> NoteError {
    NoteError::new(
        StatusCode::NOT_FOUND,
        "model_download_not_found",
        "Model download is unavailable",
    )
}
impl DownloadRequest {
    pub fn parse(body: &[u8]) -> Result<Self> {
        let input: Self = serde_json::from_slice(body).map_err(|_| {
            NoteError::invalid(
                "invalid_model_download",
                "Send model_version and platform only",
            )
        })?;
        if !token(&input.model_version, 128) || !platform(&input.platform) {
            return Err(NoteError::invalid(
                "invalid_model_download",
                "Unsupported model version or platform",
            ));
        }
        Ok(input)
    }
}
/// Create or reuse the caller's download of a served (active or retired) release artifact.
/// Returns the download and true when this call created it.
pub async fn create_download(
    db: &AnyPool,
    owner: &str,
    input: &DownloadRequest,
) -> Result<(Download, bool)> {
    match repository::served_artifact(db, &input.model_version, &input.platform).await? {
        repository::ArtifactCheck::Ready => {}
        repository::ArtifactCheck::Withdrawn => return Err(withdrawn_error()),
        repository::ArtifactCheck::Missing => {
            return Err(NoteError::new(
                StatusCode::NOT_FOUND,
                "model_not_found",
                "Model release is unavailable",
            ));
        }
    }
    let id = crate::core::id();
    let download = repository::create_download(
        db,
        &id,
        owner,
        &input.model_version,
        &input.platform,
        chrono::Utc::now().timestamp(),
    )
    .await?;
    let created = download.download_id == id;
    Ok((download, created))
}
/// The caller's own download. Another account's id reads as not found.
pub async fn download(db: &AnyPool, owner: &str, download_id: &str) -> Result<Download> {
    if !token(download_id, 64) {
        return Err(download_not_found());
    }
    match repository::download(db, owner, download_id).await? {
        repository::OwnDownload::Row(row) => repository::download_from_row(&row),
        repository::OwnDownload::Withdrawn => Err(withdrawn_error()),
        repository::OwnDownload::Missing => Err(download_not_found()),
    }
}
pub async fn download_content(
    db: &AnyPool,
    owner: &str,
    download_id: &str,
) -> Result<DownloadContent> {
    if !token(download_id, 64) {
        return Err(download_not_found());
    }
    match repository::download(db, owner, download_id).await? {
        repository::OwnDownload::Row(row) => repository::content_from_row(&row),
        repository::OwnDownload::Withdrawn => Err(withdrawn_error()),
        repository::OwnDownload::Missing => Err(download_not_found()),
    }
}
