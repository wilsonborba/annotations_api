//! Durable optional Cortex processing jobs (`/me/content_jobs`).
//!
//! Default off: requires `CORTEX_URL` to be configured. Jobs start only on explicit
//! owner request, persist state and event cursors, survive refresh/reopen, and save
//! completed or partial output as a private reviewable draft before deleting the
//! temporary Cortex chat.
use crate::{
    core::{self, FILE_LIMITS},
    dal::{Store, cortex::OutputStatus},
    domain::{
        files,
        notes::{NoteError, allow, parse_body},
    },
};
use axum::http::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;

type R<T> = std::result::Result<T, NoteError>;

#[derive(Clone, Debug)]
pub struct JobEvent {
    pub seq: i64,
    pub stage: String,
    pub created_at: i64,
}

fn token(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic())
}

fn terminal(status: &str) -> bool {
    matches!(status, "completed" | "partial" | "cancelled" | "failed")
}

fn is_valid_kind(kind: &str) -> bool {
    matches!(kind, "extraction" | "study_notes")
}

/// `POST /me/content_jobs`: start or resume a durable Cortex processing job.
pub async fn create(store: &Store, owner: &str, raw: &[u8]) -> R<(Value, bool)> {
    if !store.cortex.is_configured() {
        return Err(NoteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "cortex_unavailable",
            "Optional Cortex processing is disabled",
        ));
    }
    let body = parse_body(raw)?;
    allow(
        &body,
        &[
            "client_job_id",
            "kind",
            "text",
            "file_ids",
            "source_note_id",
            "source_revision_id",
        ],
        "",
    )?;
    let client_job_id = body
        .get("client_job_id")
        .and_then(Value::as_str)
        .filter(|s| token(s))
        .ok_or_else(|| {
            NoteError::invalid(
                "invalid_client_job_id",
                "client_job_id uses 1 to 128 visible ASCII characters",
            )
        })?
        .to_string();
    let kind = body
        .get("kind")
        .and_then(Value::as_str)
        .filter(|s| is_valid_kind(s))
        .ok_or_else(|| {
            NoteError::invalid(
                "invalid_content_job_kind",
                "kind must be extraction or study_notes",
            )
        })?
        .to_string();
    let text = match body.get("text") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => {
            if s.len() > 100_000 {
                return Err(NoteError::invalid("text_too_long", "text is too long"));
            }
            s.clone()
        }
        Some(_) => return Err(NoteError::invalid("invalid_type", "text must be a string")),
    };
    let mut file_ids = Vec::new();
    if let Some(val) = body.get("file_ids") {
        let list = val
            .as_array()
            .ok_or_else(|| NoteError::invalid("invalid_type", "file_ids must be an array"))?;
        if list.len() > 20 {
            return Err(NoteError::invalid(
                "too_many_files",
                "A content job accepts at most 20 files",
            ));
        }
        for item in list {
            let id = item
                .as_str()
                .filter(|s| token(s))
                .ok_or_else(|| NoteError::invalid("invalid_file_id", "Invalid file_id"))?;
            if !file_ids.iter().any(|known| known == id) {
                file_ids.push(id.to_string());
            }
        }
    }
    if text.trim().is_empty() && file_ids.is_empty() {
        return Err(NoteError::invalid(
            "empty_content_job",
            "A content job needs text or at least one file_id",
        ));
    }
    let source_note_id = match body.get("source_note_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if token(s) => Some(s.clone()),
        _ => {
            return Err(NoteError::invalid(
                "invalid_reference",
                "Invalid source_note_id",
            ));
        }
    };
    let source_revision_id = match body.get("source_revision_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if token(s) => Some(s.clone()),
        _ => {
            return Err(NoteError::invalid(
                "invalid_reference",
                "Invalid source_revision_id",
            ));
        }
    };

    let canonical = json!({
        "kind": kind,
        "text": text,
        "file_ids": file_ids,
        "source_note_id": source_note_id,
        "source_revision_id": source_revision_id,
    });
    let request_hash = format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()));

    if let Some(existing) = sqlx::query(
        "SELECT job_id,request_hash FROM content_jobs WHERE owner_id=$1 AND client_job_id=$2",
    )
    .bind(owner)
    .bind(&client_job_id)
    .fetch_optional(&store.db)
    .await?
    {
        let existing_id: String = existing.get("job_id");
        let existing_hash: String = existing.get("request_hash");
        if existing_hash != request_hash {
            return Err(NoteError::new(
                StatusCode::CONFLICT,
                "content_job_conflict",
                "client_job_id already names a different content job",
            ));
        }
        return Ok((get(store, owner, &existing_id).await?, false));
    }

    for fid in &file_ids {
        let file = files::get(store, owner, fid).await?;
        if file.status != "ready" {
            return Err(NoteError::new(
                StatusCode::CONFLICT,
                "file_not_ready",
                "All job files must be ready",
            ));
        }
    }
    if let Some(note_id) = &source_note_id {
        let owned = sqlx::query("SELECT 1 FROM notes WHERE note_id=$1 AND owner_id=$2")
            .bind(note_id)
            .bind(owner)
            .fetch_optional(&store.db)
            .await?
            .is_some();
        if !owned {
            return Err(NoteError::not_found());
        }
    }
    if let Some(rev_id) = &source_revision_id {
        let owned = sqlx::query(
            "SELECT 1 FROM note_revisions r JOIN notes n ON n.note_id=r.note_id WHERE r.revision_id=$1 AND n.owner_id=$2",
        )
        .bind(rev_id)
        .bind(owner)
        .fetch_optional(&store.db)
        .await?
        .is_some();
        if !owned {
            return Err(NoteError::not_found());
        }
    }

    let now = core::now();
    let job_id = core::id();
    let mut tx = store.db.begin().await?;
    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    let inserted = sqlx::query(
        "INSERT INTO content_jobs(job_id,owner_id,client_job_id,source_note_id,source_revision_id,kind,request_hash,prompt_text,status,event_cursor,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'queued',1,$9,$9) ON CONFLICT(owner_id,client_job_id) DO NOTHING",
    )
    .bind(&job_id)
    .bind(owner)
    .bind(&client_job_id)
    .bind(&source_note_id)
    .bind(&source_revision_id)
    .bind(&kind)
    .bind(&request_hash)
    .bind(&text)
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if inserted == 0 {
        tx.rollback().await?;
        let existing = sqlx::query(
            "SELECT job_id,request_hash FROM content_jobs WHERE owner_id=$1 AND client_job_id=$2",
        )
        .bind(owner)
        .bind(&client_job_id)
        .fetch_one(&store.db)
        .await?;
        let existing_id: String = existing.get("job_id");
        let existing_hash: String = existing.get("request_hash");
        if existing_hash != request_hash {
            return Err(NoteError::new(
                StatusCode::CONFLICT,
                "content_job_conflict",
                "client_job_id already names a different content job",
            ));
        }
        return Ok((get(store, owner, &existing_id).await?, false));
    }
    for (idx, fid) in file_ids.iter().enumerate() {
        sqlx::query("INSERT INTO content_job_files(job_id,file_id,position) VALUES($1,$2,$3)")
            .bind(&job_id)
            .bind(fid)
            .bind(idx as i64)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO file_references(file_id,reference_kind,reference_id) VALUES($1,'content_job',$2) ON CONFLICT DO NOTHING",
        )
        .bind(fid)
        .bind(&job_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "INSERT INTO content_job_events(job_id,seq,stage,created_at) VALUES($1,1,'queued',$2)",
    )
    .bind(&job_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let bg_store = store.clone();
    let bg_owner = owner.to_owned();
    let bg_job = job_id.clone();
    tokio::spawn(async move {
        let _ = run_job(&bg_store, &bg_owner, &bg_job).await;
    });

    Ok((get(store, owner, &job_id).await?, true))
}

async fn append_event(store: &Store, job_id: &str, stage: &str) -> R<()> {
    let now = core::now();
    let mut tx = store.db.begin().await?;
    let next_seq: i64 = sqlx::query(
        "SELECT COALESCE(MAX(seq),0)+1 AS next_seq FROM content_job_events WHERE job_id=$1",
    )
    .bind(job_id)
    .fetch_one(&mut *tx)
    .await?
    .get("next_seq");
    sqlx::query("INSERT INTO content_job_events(job_id,seq,stage,created_at) VALUES($1,$2,$3,$4)")
        .bind(job_id)
        .bind(next_seq)
        .bind(stage)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE content_jobs SET event_cursor=$1,updated_at=$2 WHERE job_id=$3")
        .bind(next_seq)
        .bind(now)
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn release_files(store: &Store, job_id: &str) -> R<()> {
    sqlx::query(
        "DELETE FROM file_references WHERE reference_kind='content_job' AND reference_id=$1",
    )
    .bind(job_id)
    .execute(&store.db)
    .await?;
    Ok(())
}

async fn run_job(store: &Store, owner: &str, job_id: &str) -> R<()> {
    let row =
        sqlx::query("SELECT status,prompt_text FROM content_jobs WHERE job_id=$1 AND owner_id=$2")
            .bind(job_id)
            .bind(owner)
            .fetch_optional(&store.db)
            .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let status: String = row.get("status");
    if terminal(&status) {
        return Ok(());
    }
    let prompt_text: String = row.get("prompt_text");

    let file_rows =
        sqlx::query("SELECT file_id FROM content_job_files WHERE job_id=$1 ORDER BY position")
            .bind(job_id)
            .fetch_all(&store.db)
            .await?;

    let mut cortex_file_ids = Vec::new();
    if !file_rows.is_empty() {
        append_event(store, job_id, "uploading").await?;
    }
    for frow in file_rows {
        let fid: String = frow.get("file_id");
        let dl = match files::download(store, owner, &fid).await {
            Ok(d) => d,
            Err(e) => {
                return fail_job(store, job_id, &e.code, &e.message).await;
            }
        };
        let stream = match store.files.read(dl.fsm_reference()).await {
            Ok(s) => s,
            Err(_) => {
                return fail_job(
                    store,
                    job_id,
                    "storage_unavailable",
                    "Cannot read file bytes",
                )
                .await;
            }
        };
        let bytes = match axum::body::to_bytes(stream, FILE_LIMITS.content_job_bytes as usize).await
        {
            Ok(b) => b.to_vec(),
            Err(_) => {
                return fail_job(
                    store,
                    job_id,
                    "storage_unavailable",
                    "Cannot stream file bytes",
                )
                .await;
            }
        };
        let cortex_fid = match store
            .cortex
            .upload_file(owner, &dl.file.filename, dl.served_type(), bytes)
            .await
        {
            Ok(id) => id,
            Err(_) => {
                return fail_job(store, job_id, "cortex_unavailable", "Cortex upload failed").await;
            }
        };
        sqlx::query(
            "UPDATE content_job_files SET cortex_file_id=$1 WHERE job_id=$2 AND file_id=$3",
        )
        .bind(&cortex_fid)
        .bind(job_id)
        .bind(&fid)
        .execute(&store.db)
        .await?;
        cortex_file_ids.push(cortex_fid);
    }

    // Check if cancelled during file upload.
    let current_status: String = sqlx::query("SELECT status FROM content_jobs WHERE job_id=$1")
        .bind(job_id)
        .fetch_one(&store.db)
        .await?
        .get("status");
    if terminal(&current_status) {
        release_files(store, job_id).await?;
        return Ok(());
    }

    let (session_id, output_id) = match store
        .cortex
        .start_chat(owner, &prompt_text, cortex_file_ids)
        .await
    {
        Ok(ids) => ids,
        Err(_) => {
            return fail_job(store, job_id, "cortex_unavailable", "Cortex start failed").await;
        }
    };
    sqlx::query(
        "UPDATE content_jobs SET status='running',cortex_session_id=$1,cortex_output_id=$2,updated_at=$3 WHERE job_id=$4 AND status='queued'",
    )
    .bind(&session_id)
    .bind(&output_id)
    .bind(core::now())
    .bind(job_id)
    .execute(&store.db)
    .await?;
    append_event(store, job_id, "running").await?;

    let mut final_out = None;
    for _ in 0..120 {
        let cur: String = sqlx::query("SELECT status FROM content_jobs WHERE job_id=$1")
            .bind(job_id)
            .fetch_one(&store.db)
            .await?
            .get("status");
        if terminal(&cur) {
            release_files(store, job_id).await?;
            return Ok(());
        }
        match store
            .cortex
            .get_output(owner, &session_id, &output_id)
            .await
        {
            Ok(out) if out.status != OutputStatus::Running => {
                final_out = Some(out);
                break;
            }
            Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            Err(_) => {
                return fail_job(store, job_id, "cortex_unavailable", "Cortex poll failed").await;
            }
        }
    }

    let Some(out) = final_out else {
        return fail_job(store, job_id, "cortex_timeout", "Cortex job timed out").await;
    };

    let (next_status, err_code, err_msg) = match out.status {
        OutputStatus::Completed => ("completed", None, None),
        OutputStatus::Partial => ("partial", out.error_code, out.error_message),
        OutputStatus::Cancelled => ("cancelled", None, None),
        OutputStatus::Failed => (
            "failed",
            out.error_code.or_else(|| Some("cortex_failed".into())),
            out.error_message
                .or_else(|| Some("Cortex processing failed".into())),
        ),
        OutputStatus::Running => ("failed", Some("cortex_timeout".into()), None),
    };

    // Save the draft output durably BEFORE deleting the temporary Cortex chat.
    sqlx::query(
        "UPDATE content_jobs SET status=$1,draft_text=$2,draft_saved=1,error_code=$3,error_message=$4,updated_at=$5 WHERE job_id=$6 AND status IN ('queued','running')",
    )
    .bind(next_status)
    .bind(&out.text)
    .bind(&err_code)
    .bind(&err_msg)
    .bind(core::now())
    .bind(job_id)
    .execute(&store.db)
    .await?;
    append_event(store, job_id, next_status).await?;

    if store.cortex.delete_chat(owner, &session_id).await.is_ok() {
        sqlx::query("UPDATE content_jobs SET cortex_cleaned=1,updated_at=$1 WHERE job_id=$2")
            .bind(core::now())
            .bind(job_id)
            .execute(&store.db)
            .await?;
    }
    release_files(store, job_id).await?;
    Ok(())
}

async fn fail_job(store: &Store, job_id: &str, code: &str, message: &str) -> R<()> {
    sqlx::query(
        "UPDATE content_jobs SET status='failed',error_code=$1,error_message=$2,updated_at=$3 WHERE job_id=$4 AND status IN ('queued','running')",
    )
    .bind(code)
    .bind(message)
    .bind(core::now())
    .bind(job_id)
    .execute(&store.db)
    .await?;
    append_event(store, job_id, "failed").await?;
    release_files(store, job_id).await?;
    Ok(())
}

/// `GET /me/content_jobs/{job_id}`: read durable job state and reviewable draft.
pub async fn get(store: &Store, owner: &str, job_id: &str) -> R<Value> {
    if !token(job_id) {
        return Err(NoteError::not_found());
    }
    let row = sqlx::query(
        "SELECT job_id,client_job_id,source_note_id,source_revision_id,kind,status,cortex_session_id,cortex_output_id,event_cursor,draft_text,draft_saved,cortex_cleaned,error_code,error_message,created_at,updated_at FROM content_jobs WHERE job_id=$1 AND owner_id=$2",
    )
    .bind(job_id)
    .bind(owner)
    .fetch_optional(&store.db)
    .await?
    .ok_or_else(NoteError::not_found)?;
    let file_ids: Vec<String> =
        sqlx::query("SELECT file_id FROM content_job_files WHERE job_id=$1 ORDER BY position")
            .bind(job_id)
            .fetch_all(&store.db)
            .await?
            .into_iter()
            .map(|r| r.get("file_id"))
            .collect();
    let error = match (
        row.try_get::<Option<String>, _>("error_code")?,
        row.try_get::<Option<String>, _>("error_message")?,
    ) {
        (Some(code), msg) => json!({"code": code, "message": msg}),
        _ => Value::Null,
    };
    Ok(json!({
        "job_id": row.get::<String, _>("job_id"),
        "client_job_id": row.get::<String, _>("client_job_id"),
        "kind": row.get::<String, _>("kind"),
        "status": row.get::<String, _>("status"),
        "source_note_id": row.try_get::<Option<String>, _>("source_note_id")?,
        "source_revision_id": row.try_get::<Option<String>, _>("source_revision_id")?,
        "cortex_session_id": row.try_get::<Option<String>, _>("cortex_session_id")?,
        "cortex_output_id": row.try_get::<Option<String>, _>("cortex_output_id")?,
        "event_cursor": row.get::<i64, _>("event_cursor"),
        "draft_text": row.get::<String, _>("draft_text"),
        "draft_saved": row.get::<i64, _>("draft_saved") == 1,
        "cortex_cleaned": row.get::<i64, _>("cortex_cleaned") == 1,
        "file_ids": file_ids,
        "error": error,
        "created_at": row.get::<i64, _>("created_at"),
        "updated_at": row.get::<i64, _>("updated_at"),
    }))
}

pub fn cursor(raw: Option<&str>) -> R<i64> {
    match raw {
        None | Some("") => Ok(0),
        Some(v) => v.parse::<i64>().ok().filter(|n| *n >= 0).ok_or_else(|| {
            NoteError::invalid("invalid_cursor", "after must be a non-negative integer")
        }),
    }
}

pub async fn check_events(store: &Store, owner: &str, job_id: &str) -> R<()> {
    get(store, owner, job_id).await.map(|_| ())
}

pub async fn events(
    store: &Store,
    owner: &str,
    job_id: &str,
    after: i64,
) -> R<(Vec<JobEvent>, bool)> {
    let job = get(store, owner, job_id).await?;
    let finished = terminal(job["status"].as_str().unwrap_or(""));
    let rows = sqlx::query(
        "SELECT seq,stage,created_at FROM content_job_events WHERE job_id=$1 AND seq>$2 ORDER BY seq",
    )
    .bind(job_id)
    .bind(after)
    .fetch_all(&store.db)
    .await?;
    let list = rows
        .into_iter()
        .map(|r| JobEvent {
            seq: r.get("seq"),
            stage: r.get("stage"),
            created_at: r.get("created_at"),
        })
        .collect();
    Ok((list, finished))
}

/// `DELETE /me/content_jobs/{job_id}/execution`: cancel a running job, preserve any
/// partial output as a draft before cleaning up the temporary Cortex chat, and leave
/// the source note unchanged.
pub async fn cancel(store: &Store, owner: &str, job_id: &str) -> R<Value> {
    let current = get(store, owner, job_id).await?;
    let status = current["status"].as_str().unwrap_or("");
    if terminal(status) {
        return Ok(current);
    }
    let session_id = current["cortex_session_id"].as_str().map(str::to_owned);
    let output_id = current["cortex_output_id"].as_str().map(str::to_owned);
    let mut partial_text = String::new();
    if let (Some(sid), Some(oid)) = (&session_id, &output_id)
        && let Ok(out) = store.cortex.cancel_output(owner, sid, oid).await
    {
        partial_text = out.text;
    }
    let draft_saved = i64::from(!partial_text.is_empty());
    let changed = sqlx::query(
        "UPDATE content_jobs SET status='cancelled',draft_text=$1,draft_saved=$2,updated_at=$3 WHERE job_id=$4 AND owner_id=$5 AND status IN ('queued','running')",
    )
    .bind(&partial_text)
    .bind(draft_saved)
    .bind(core::now())
    .bind(job_id)
    .bind(owner)
    .execute(&store.db)
    .await?
    .rows_affected();
    if changed == 1 {
        append_event(store, job_id, "cancelled").await?;
        if let Some(sid) = session_id
            && store.cortex.delete_chat(owner, &sid).await.is_ok()
        {
            sqlx::query("UPDATE content_jobs SET cortex_cleaned=1,updated_at=$1 WHERE job_id=$2")
                .bind(core::now())
                .bind(job_id)
                .execute(&store.db)
                .await?;
        }
        release_files(store, job_id).await?;
    }
    get(store, owner, job_id).await
}
