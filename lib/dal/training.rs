//! Relational state of correction feedback (`training_feedback`). The validated events
//! live in CouchDB; these rows hold their ids, stage, status and provenance links.
//! No business rule here.
use crate::domain::notes::NoteError;
use sqlx::{AnyPool, Row, any::AnyRow};

type Result<T> = std::result::Result<T, NoteError>;

/// One event to store.
pub struct NewFeedback {
    pub feedback_id: String,
    pub run_id: String,
    pub owner_id: String,
    pub event_id: String,
    pub request_hash: String,
    pub content_hash: String,
    pub segment_id: String,
    pub stage: String,
    pub source_note_id: Option<String>,
    pub document_id: String,
    pub schema_version: i64,
    pub created_at: i64,
}
/// A stored event of a run, for replays.
pub struct StoredEvent {
    pub request_hash: String,
    pub stage: String,
    pub created_at: i64,
}
/// The result of one insert of a batch.
pub enum Inserted {
    /// The row is new.
    New,
    /// A concurrent request stored the same event first; the new document is unused.
    Replay(StoredEvent),
}
/// A stored example with its run provenance, for the export.
pub struct ExportRow {
    pub feedback_id: String,
    pub run_id: String,
    pub owner_id: String,
    pub event_id: String,
    pub segment_id: String,
    pub stage: String,
    pub status: String,
    pub source_note_id: Option<String>,
    pub document_id: String,
    pub schema_version: i64,
    pub created_at: i64,
    pub model_version: String,
    pub platform: String,
    pub language: String,
    pub mode: String,
    pub source_revision_id: Option<String>,
    pub source_hash: Option<String>,
}
/// Which examples a removal selects.
pub enum Selection<'a> {
    Run(&'a str),
    Note(&'a str),
    Account(&'a str),
}

/// The CouchDB id of the selected ink of a run, when the run has ink.
pub async fn ink_document(db: &AnyPool, run_id: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query("SELECT document_id FROM transcription_run_inks WHERE run_id=$1")
            .bind(run_id)
            .fetch_optional(db)
            .await?
            .map(|r| r.get("document_id")),
    )
}
fn to_stored(row: &AnyRow) -> std::result::Result<StoredEvent, sqlx::Error> {
    Ok(StoredEvent {
        request_hash: row.try_get("request_hash")?,
        stage: row.try_get("stage")?,
        created_at: row.try_get("created_at")?,
    })
}
/// The stored event with this id in the run.
pub async fn event(db: &AnyPool, run_id: &str, event_id: &str) -> Result<Option<StoredEvent>> {
    let row = sqlx::query(
        "SELECT request_hash,stage,created_at FROM training_feedback WHERE run_id=$1 AND event_id=$2",
    )
    .bind(run_id)
    .bind(event_id)
    .fetch_optional(db)
    .await?;
    Ok(match row {
        Some(row) => Some(to_stored(&row)?),
        None => None,
    })
}
/// Insert the events in one transaction. An event whose content equals an earlier
/// example of the same run is stored as `flagged`, all others as `received`.
/// Returns `Err(event_id)` and writes nothing when a concurrent request stored one of
/// the event ids with a different request.
pub async fn insert(
    db: &AnyPool,
    rows: &[NewFeedback],
) -> Result<std::result::Result<Vec<Inserted>, String>> {
    let mut tx = db.begin().await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let copy =
            sqlx::query("SELECT 1 FROM training_feedback WHERE run_id=$1 AND content_hash=$2")
                .bind(&row.run_id)
                .bind(&row.content_hash)
                .fetch_optional(&mut *tx)
                .await?
                .is_some();
        let status = if copy { "flagged" } else { "received" };
        let added = sqlx::query("INSERT INTO training_feedback(feedback_id,run_id,owner_id,event_id,request_hash,content_hash,segment_id,stage,status,source_note_id,document_id,schema_name,schema_version,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'training-feedback',$12,$13) ON CONFLICT(run_id,event_id) DO NOTHING")
            .bind(&row.feedback_id).bind(&row.run_id).bind(&row.owner_id).bind(&row.event_id)
            .bind(&row.request_hash).bind(&row.content_hash).bind(&row.segment_id).bind(&row.stage)
            .bind(status).bind(&row.source_note_id).bind(&row.document_id).bind(row.schema_version)
            .bind(row.created_at).execute(&mut *tx).await?.rows_affected();
        if added == 1 {
            out.push(Inserted::New);
            continue;
        }
        let found = sqlx::query(
            "SELECT request_hash,stage,created_at FROM training_feedback WHERE run_id=$1 AND event_id=$2",
        )
        .bind(&row.run_id)
        .bind(&row.event_id)
        .fetch_one(&mut *tx)
        .await?;
        let found = to_stored(&found)?;
        if found.request_hash != row.request_hash {
            return Ok(Err(row.event_id.clone()));
        }
        out.push(Inserted::Replay(found));
    }
    tx.commit().await?;
    Ok(Ok(out))
}

const EXPORT: &str = "SELECT f.feedback_id,f.run_id,f.owner_id,f.event_id,f.segment_id,f.stage,f.status,f.source_note_id,f.document_id,f.schema_version,f.created_at,d.version,d.platform,t.language,t.mode,t.source_revision_id,t.source_hash FROM training_feedback f JOIN transcription_runs t ON t.run_id=f.run_id JOIN model_downloads d ON d.download_id=t.download_id";
/// Up to `limit` examples after `after` (feedback id order). Flagged examples only
/// when `include_flagged` is true.
pub async fn export(
    db: &AnyPool,
    after: &str,
    limit: i64,
    include_flagged: bool,
) -> Result<Vec<ExportRow>> {
    let statuses = if include_flagged {
        "('received','approved_for_training','flagged')"
    } else {
        "('received','approved_for_training')"
    };
    let rows = sqlx::query(&format!(
        "{EXPORT} WHERE f.feedback_id>$1 AND f.status IN {statuses} ORDER BY f.feedback_id LIMIT $2"
    ))
    .bind(after)
    .bind(limit)
    .fetch_all(db)
    .await?;
    rows.iter()
        .map(|r| {
            Ok(ExportRow {
                feedback_id: r.try_get("feedback_id")?,
                run_id: r.try_get("run_id")?,
                owner_id: r.try_get("owner_id")?,
                event_id: r.try_get("event_id")?,
                segment_id: r.try_get("segment_id")?,
                stage: r.try_get("stage")?,
                status: r.try_get("status")?,
                source_note_id: r.try_get("source_note_id")?,
                document_id: r.try_get("document_id")?,
                schema_version: r.try_get("schema_version")?,
                created_at: r.try_get("created_at")?,
                model_version: r.try_get("version")?,
                platform: r.try_get("platform")?,
                language: r.try_get("language")?,
                mode: r.try_get("mode")?,
                source_revision_id: r.try_get("source_revision_id")?,
                source_hash: r.try_get("source_hash")?,
            })
        })
        .collect()
}
/// Up to `limit` selected examples: (feedback id, document id).
pub async fn selected(
    db: &AnyPool,
    selection: &Selection<'_>,
    limit: i64,
) -> Result<Vec<(String, String)>> {
    let (column, value) = match selection {
        Selection::Run(v) => ("run_id", v),
        Selection::Note(v) => ("source_note_id", v),
        Selection::Account(v) => ("owner_id", v),
    };
    Ok(sqlx::query(&format!(
        "SELECT feedback_id,document_id FROM training_feedback WHERE {column}=$1 ORDER BY feedback_id LIMIT $2"
    ))
    .bind(*value)
    .bind(limit)
    .fetch_all(db)
    .await?
    .iter()
    .map(|r| (r.get("feedback_id"), r.get("document_id")))
    .collect())
}
/// Remove one example row.
pub async fn remove(db: &AnyPool, feedback_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM training_feedback WHERE feedback_id=$1")
        .bind(feedback_id)
        .execute(db)
        .await?;
    Ok(())
}
