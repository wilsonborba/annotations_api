//! Relational state of transcription runs. Ink and result documents live in CouchDB;
//! these rows hold their ids only. No business rule here.
use crate::domain::notes::NoteError;
use sqlx::{AnyPool, Row, any::AnyRow};

type Result<T> = std::result::Result<T, NoteError>;

/// A run with its model release and the state of its source note.
pub struct RunRow {
    pub run_id: String,
    pub download_id: String,
    pub model_version: String,
    pub platform: String,
    pub language: String,
    pub mode: String,
    pub status: String,
    pub source_kind: String,
    pub source_revision_id: Option<String>,
    pub source_hash: Option<String>,
    /// The note of the source revision, when it still exists.
    pub source_note_id: Option<String>,
    /// The current Original revision of that note.
    pub current_original: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
pub struct NewRun<'a> {
    pub run_id: &'a str,
    pub owner_id: &'a str,
    pub download_id: &'a str,
    pub language: &'a str,
    pub mode: &'a str,
    pub source_kind: &'a str,
    pub source_revision_id: Option<&'a str>,
    pub source_hash: Option<&'a str>,
    pub request_hash: &'a str,
    /// CouchDB id and schema version of the selected ink document.
    pub ink: Option<(&'a str, i64)>,
    pub file_ids: &'a [String],
    pub now: i64,
}
pub struct ResultRow {
    pub event_id: String,
    pub request_hash: String,
    pub document_id: String,
    pub schema_version: i64,
    pub runtime_name: String,
    pub runtime_version: String,
    pub elapsed_ms: i64,
    pub created_at: i64,
}
pub struct EventRow {
    pub seq: i64,
    pub stage: String,
    pub created_at: i64,
}

const RUN: &str = "SELECT t.run_id,t.download_id,t.language,t.mode,t.status,t.source_kind,t.source_revision_id,t.source_hash,t.created_at,t.updated_at,d.version,d.platform,r.note_id AS source_note_id,c.revision_id AS current_original FROM transcription_runs t JOIN model_downloads d ON d.download_id=t.download_id LEFT JOIN note_revisions r ON r.revision_id=t.source_revision_id LEFT JOIN note_current_revisions c ON c.note_id=r.note_id AND c.representation='original'";
fn to_run(row: &AnyRow) -> std::result::Result<RunRow, sqlx::Error> {
    Ok(RunRow {
        run_id: row.try_get("run_id")?,
        download_id: row.try_get("download_id")?,
        model_version: row.try_get("version")?,
        platform: row.try_get("platform")?,
        language: row.try_get("language")?,
        mode: row.try_get("mode")?,
        status: row.try_get("status")?,
        source_kind: row.try_get("source_kind")?,
        source_revision_id: row.try_get("source_revision_id")?,
        source_hash: row.try_get("source_hash")?,
        source_note_id: row.try_get("source_note_id")?,
        current_original: row.try_get("current_original")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}
/// The run of this owner. Another account's run reads as absent.
pub async fn run(db: &AnyPool, owner: &str, run_id: &str) -> Result<Option<RunRow>> {
    let row = sqlx::query(&format!("{RUN} WHERE t.run_id=$1 AND t.owner_id=$2"))
        .bind(run_id)
        .bind(owner)
        .fetch_optional(db)
        .await?;
    Ok(match row {
        Some(row) => Some(to_run(&row)?),
        None => None,
    })
}
/// The live (not cancelled) run of an identical earlier request.
pub async fn reusable(db: &AnyPool, owner: &str, request_hash: &str) -> Result<Option<String>> {
    Ok(sqlx::query(
        "SELECT run_id FROM transcription_runs WHERE owner_id=$1 AND request_hash=$2 AND status <> 'cancelled'",
    )
    .bind(owner)
    .bind(request_hash)
    .fetch_optional(db)
    .await?
    .map(|r| r.get("run_id")))
}
/// True when the release lists the language.
pub async fn release_language(db: &AnyPool, version: &str, language: &str) -> Result<bool> {
    Ok(
        sqlx::query("SELECT 1 FROM model_languages WHERE version=$1 AND language=$2")
            .bind(version)
            .bind(language)
            .fetch_optional(db)
            .await?
            .is_some(),
    )
}
/// True when the revision is an Original revision of a note of this owner.
pub async fn own_original(db: &AnyPool, owner: &str, revision_id: &str) -> Result<bool> {
    Ok(sqlx::query("SELECT 1 FROM note_revisions r JOIN notes n ON n.note_id=r.note_id WHERE r.revision_id=$1 AND n.owner_id=$2 AND r.representation='original'")
        .bind(revision_id).bind(owner).fetch_optional(db).await?.is_some())
}
/// True when the file is a ready transcription input of this owner.
pub async fn ready_input_file(db: &AnyPool, owner: &str, file_id: &str) -> Result<bool> {
    Ok(sqlx::query("SELECT 1 FROM files WHERE file_id=$1 AND owner_id=$2 AND status='ready' AND purpose='transcription' AND fsm_reference IS NOT NULL")
        .bind(file_id).bind(owner).fetch_optional(db).await?.is_some())
}
async fn add_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    run_id: &str,
    stage: &str,
    now: i64,
) -> Result<()> {
    sqlx::query("INSERT INTO transcription_run_events(run_id,seq,stage,created_at) SELECT $1,COALESCE(MAX(seq),0)+1,$2,$3 FROM transcription_run_events WHERE run_id=$1")
        .bind(run_id).bind(stage).bind(now).execute(&mut **tx).await?;
    Ok(())
}
/// Insert a run with its inputs and its `registered` event in one transaction.
/// Returns false when a live run of the same request exists (a concurrent retry).
pub async fn insert(db: &AnyPool, run: &NewRun<'_>) -> Result<bool> {
    let mut tx = db.begin().await?;
    let inserted = sqlx::query("INSERT INTO transcription_runs(run_id,owner_id,download_id,language,mode,status,source_kind,source_revision_id,source_hash,request_hash,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'registered',$6,$7,$8,$9,$10,$10)")
        .bind(run.run_id).bind(run.owner_id).bind(run.download_id).bind(run.language)
        .bind(run.mode).bind(run.source_kind).bind(run.source_revision_id).bind(run.source_hash)
        .bind(run.request_hash).bind(run.now).execute(&mut *tx).await;
    match inserted {
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => return Ok(false),
        other => {
            other?;
        }
    }
    if let Some((document_id, version)) = run.ink {
        sqlx::query("INSERT INTO transcription_run_inks(run_id,document_id,schema_name,schema_version) VALUES($1,$2,'note-original',$3)")
            .bind(run.run_id).bind(document_id).bind(version).execute(&mut *tx).await?;
    }
    for file_id in run.file_ids {
        // The reference keeps the input file from release while the run exists.
        let added = sqlx::query("INSERT INTO file_references(file_id,reference_kind,reference_id) SELECT file_id,'transcription',$2 FROM files WHERE file_id=$1 AND owner_id=$3 AND status='ready' AND purpose='transcription'")
            .bind(file_id).bind(run.run_id).bind(run.owner_id).execute(&mut *tx).await?.rows_affected();
        if added != 1 {
            return Err(NoteError::invalid(
                "unknown_file",
                "A file reference is not a ready transcription file of this account",
            ));
        }
    }
    add_event(&mut tx, run.run_id, "registered", run.now).await?;
    tx.commit().await?;
    Ok(true)
}
pub async fn file_ids(db: &AnyPool, run_id: &str) -> Result<Vec<String>> {
    Ok(sqlx::query("SELECT file_id FROM file_references WHERE reference_kind='transcription' AND reference_id=$1 ORDER BY file_id")
        .bind(run_id).fetch_all(db).await?.iter().map(|r| r.get("file_id")).collect())
}
/// The schema version of the selected ink document, when the run has ink.
pub async fn ink_version(db: &AnyPool, run_id: &str) -> Result<Option<i64>> {
    Ok(
        sqlx::query("SELECT schema_version FROM transcription_run_inks WHERE run_id=$1")
            .bind(run_id)
            .fetch_optional(db)
            .await?
            .map(|r| r.get("schema_version")),
    )
}
pub async fn result(db: &AnyPool, run_id: &str) -> Result<Option<ResultRow>> {
    let row = sqlx::query("SELECT event_id,request_hash,document_id,schema_version,runtime_name,runtime_version,elapsed_ms,created_at FROM transcription_results WHERE run_id=$1")
        .bind(run_id).fetch_optional(db).await?;
    Ok(match row {
        Some(r) => Some(ResultRow {
            event_id: r.try_get("event_id")?,
            request_hash: r.try_get("request_hash")?,
            document_id: r.try_get("document_id")?,
            schema_version: r.try_get("schema_version")?,
            runtime_name: r.try_get("runtime_name")?,
            runtime_version: r.try_get("runtime_version")?,
            elapsed_ms: r.try_get("elapsed_ms")?,
            created_at: r.try_get("created_at")?,
        }),
        None => None,
    })
}
/// Store the one result of a registered run and its `result_received` event.
/// Returns false when the run is no longer `registered`; nothing is written then.
pub async fn complete(db: &AnyPool, run_id: &str, result: &ResultRow) -> Result<bool> {
    let mut tx = db.begin().await?;
    let changed = sqlx::query("UPDATE transcription_runs SET status='completed',updated_at=$1 WHERE run_id=$2 AND status='registered'")
        .bind(result.created_at).bind(run_id).execute(&mut *tx).await?.rows_affected();
    if changed != 1 {
        return Ok(false);
    }
    sqlx::query("INSERT INTO transcription_results(run_id,event_id,request_hash,document_id,schema_name,schema_version,runtime_name,runtime_version,elapsed_ms,created_at) VALUES($1,$2,$3,$4,'note-typed',$5,$6,$7,$8,$9)")
        .bind(run_id).bind(&result.event_id).bind(&result.request_hash).bind(&result.document_id)
        .bind(result.schema_version).bind(&result.runtime_name).bind(&result.runtime_version)
        .bind(result.elapsed_ms).bind(result.created_at).execute(&mut *tx).await?;
    add_event(&mut tx, run_id, "result_received", result.created_at).await?;
    tx.commit().await?;
    Ok(true)
}
/// Stop a registered run. Inputs, events and results stay. False when not `registered`.
pub async fn cancel(db: &AnyPool, run_id: &str, now: i64) -> Result<bool> {
    let mut tx = db.begin().await?;
    let changed = sqlx::query("UPDATE transcription_runs SET status='cancelled',updated_at=$1 WHERE run_id=$2 AND status='registered'")
        .bind(now).bind(run_id).execute(&mut *tx).await?.rows_affected();
    if changed != 1 {
        return Ok(false);
    }
    add_event(&mut tx, run_id, "cancelled", now).await?;
    tx.commit().await?;
    Ok(true)
}
pub async fn events(db: &AnyPool, run_id: &str, after: i64, limit: i64) -> Result<Vec<EventRow>> {
    let rows = sqlx::query("SELECT seq,stage,created_at FROM transcription_run_events WHERE run_id=$1 AND seq>$2 ORDER BY seq LIMIT $3")
        .bind(run_id).bind(after).bind(limit).fetch_all(db).await?;
    rows.iter()
        .map(|r| {
            Ok(EventRow {
                seq: r.try_get("seq")?,
                stage: r.try_get("stage")?,
                created_at: r.try_get("created_at")?,
            })
        })
        .collect()
}
pub async fn last_seq(db: &AnyPool, run_id: &str) -> Result<i64> {
    Ok(sqlx::query(
        "SELECT COALESCE(MAX(seq),0) AS seq FROM transcription_run_events WHERE run_id=$1",
    )
    .bind(run_id)
    .fetch_one(db)
    .await?
    .try_get("seq")?)
}
