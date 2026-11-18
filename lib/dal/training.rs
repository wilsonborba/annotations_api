//! Relational state of correction feedback (`training_feedback`). The validated events
//! live in CouchDB; these rows hold their ids, stage, status and provenance links.
//! No business rule here.
use crate::{
    core::{self, TRAINING_LIMITS},
    domain::notes::NoteError,
};
use axum::http::StatusCode;
use sqlx::{AnyPool, Row, any::AnyRow};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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
/// Which examples `training approve` or `training flag` selects.
pub enum ReviewTarget<'a> {
    Run(&'a str),
    Event(&'a str),
    RunEvent(&'a str, &'a str),
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
fn to_export(r: &AnyRow) -> std::result::Result<ExportRow, sqlx::Error> {
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
}
/// Up to `limit` examples after `after` (feedback id order). Defaults to
/// `approved_for_training` only; `include_received` and `include_flagged` add those statuses.
pub async fn export(
    db: &AnyPool,
    after: &str,
    limit: i64,
    include_received: bool,
    include_flagged: bool,
) -> Result<Vec<ExportRow>> {
    let mut allowed = vec!["'approved_for_training'"];
    if include_received {
        allowed.push("'received'");
    }
    if include_flagged {
        allowed.push("'flagged'");
    }
    let statuses = format!("({})", allowed.join(","));
    let rows = sqlx::query(&format!(
        "{EXPORT} WHERE f.feedback_id>$1 AND f.status IN {statuses} ORDER BY f.feedback_id LIMIT $2"
    ))
    .bind(after)
    .bind(limit)
    .fetch_all(db)
    .await?;
    rows.iter()
        .map(to_export)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}
/// Flagged examples for administrator review, oldest first.
pub async fn review_flagged(db: &AnyPool, limit: i64) -> Result<Vec<ExportRow>> {
    let rows = sqlx::query(&format!(
        "{EXPORT} WHERE f.status='flagged' ORDER BY f.created_at,f.feedback_id LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(db)
    .await?;
    rows.iter()
        .map(to_export)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}
/// A representative sample of `received` examples, balanced round-robin across accounts.
pub async fn review_received_sample(db: &AnyPool, limit: i64) -> Result<Vec<ExportRow>> {
    let rows = sqlx::query(
        "SELECT s.* FROM (SELECT f.feedback_id,f.run_id,f.owner_id,f.event_id,f.segment_id,f.stage,f.status,f.source_note_id,f.document_id,f.schema_version,f.created_at,d.version,d.platform,t.language,t.mode,t.source_revision_id,t.source_hash,ROW_NUMBER() OVER (PARTITION BY f.owner_id ORDER BY f.created_at,f.feedback_id) AS rn FROM training_feedback f JOIN transcription_runs t ON t.run_id=f.run_id JOIN model_downloads d ON d.download_id=t.download_id WHERE f.status='received') s ORDER BY s.rn,s.created_at,s.feedback_id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(db)
    .await?;
    rows.iter()
        .map(to_export)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}
/// Move matching examples to `status` (`approved_for_training` or `flagged`).
/// Returns the number of rows updated.
pub async fn set_status(db: &AnyPool, target: &ReviewTarget<'_>, status: &str) -> Result<usize> {
    let affected = match target {
        ReviewTarget::Run(run_id) => {
            sqlx::query("UPDATE training_feedback SET status=$1 WHERE run_id=$2 AND status<>$1")
                .bind(status)
                .bind(*run_id)
                .execute(db)
                .await?
                .rows_affected()
        }
        ReviewTarget::RunEvent(run_id, event_id) => {
            sqlx::query("UPDATE training_feedback SET status=$1 WHERE run_id=$2 AND (feedback_id=$3 OR event_id=$3) AND status<>$1")
                .bind(status)
                .bind(*run_id)
                .bind(*event_id)
                .execute(db)
                .await?
                .rows_affected()
        }
        ReviewTarget::Event(id) => {
            let exact = sqlx::query(
                "UPDATE training_feedback SET status=$1 WHERE feedback_id=$2 AND status<>$1",
            )
            .bind(status)
            .bind(*id)
            .execute(db)
            .await?
            .rows_affected();
            if exact > 0 {
                exact
            } else {
                sqlx::query(
                    "UPDATE training_feedback SET status=$1 WHERE event_id=$2 AND status<>$1",
                )
                .bind(status)
                .bind(*id)
                .execute(db)
                .await?
                .rows_affected()
            }
        }
    };
    Ok(affected as usize)
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

const ACQUIRE_LIMIT: &str = r#"
local rate = tonumber(redis.call('GET', KEYS[1]) or '0')
if rate >= tonumber(ARGV[1]) then return 1 end
local inflight = tonumber(redis.call('GET', KEYS[2]) or '0')
if inflight >= tonumber(ARGV[3]) then return 2 end
local next_rate = redis.call('INCR', KEYS[1])
if next_rate == 1 then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2])) end
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
return 0
"#;

const RELEASE_LIMIT: &str = r#"
local left = tonumber(redis.call('DECR', KEYS[1]) or '0')
if left <= 0 then redis.call('DEL', KEYS[1]) end
return 1
"#;

#[derive(Default)]
pub struct MemoryLimits {
    inflight: HashMap<String, usize>,
    windows: HashMap<String, (i64, usize)>,
}

/// Per-account rate and concurrency limiter for training feedback, matching the
/// Redis/Memory/Unavailable pattern of `Transfers`.
#[derive(Clone)]
pub enum FeedbackLimiter {
    Redis(redis::Client),
    Memory(Arc<Mutex<MemoryLimits>>),
    Unavailable,
}

impl FeedbackLimiter {
    pub fn new(redis: Option<redis::Client>) -> Self {
        redis.map(Self::Redis).unwrap_or(Self::Unavailable)
    }
    pub fn fixture() -> Self {
        Self::Memory(Arc::default())
    }
    /// Acquire one rate-window slot and one concurrency slot for `owner`.
    pub async fn acquire(&self, owner: &str, now: i64) -> Result<FeedbackPermit> {
        match self {
            Self::Unavailable => Err(NoteError::from(core::Error::unavailable())),
            Self::Memory(state) => {
                let mut guard = state.lock().unwrap();
                let inflight = guard.inflight.get(owner).copied().unwrap_or(0);
                if inflight >= TRAINING_LIMITS.max_concurrent_per_account {
                    return Err(rate_limited(
                        "Too many concurrent feedback requests for this account",
                    ));
                }
                let window = TRAINING_LIMITS.rate_window_seconds as i64;
                let (start, count) = guard.windows.entry(owner.to_owned()).or_insert((now, 0));
                if now - *start >= window {
                    *start = now;
                    *count = 0;
                }
                if *count >= TRAINING_LIMITS.max_requests_per_window {
                    return Err(rate_limited("Too many feedback requests for this account"));
                }
                *count += 1;
                *guard.inflight.entry(owner.to_owned()).or_insert(0) += 1;
                Ok(FeedbackPermit {
                    limiter: self.clone(),
                    owner: owner.to_owned(),
                    released: false,
                })
            }
            Self::Redis(client) => {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| NoteError::from(core::Error::unavailable()))?;
                let code: i64 = redis::Script::new(ACQUIRE_LIMIT)
                    .key(format!("annotations:training:rate:{owner}"))
                    .key(format!("annotations:training:inflight:{owner}"))
                    .arg(TRAINING_LIMITS.max_requests_per_window)
                    .arg(TRAINING_LIMITS.rate_window_seconds)
                    .arg(TRAINING_LIMITS.max_concurrent_per_account)
                    .arg(TRAINING_LIMITS.inflight_ttl_seconds)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(|_| NoteError::from(core::Error::unavailable()))?;
                match code {
                    0 => Ok(FeedbackPermit {
                        limiter: self.clone(),
                        owner: owner.to_owned(),
                        released: false,
                    }),
                    1 => Err(rate_limited("Too many feedback requests for this account")),
                    _ => Err(rate_limited(
                        "Too many concurrent feedback requests for this account",
                    )),
                }
            }
        }
    }
    async fn release_slot(&self, owner: &str) {
        match self {
            Self::Unavailable => {}
            Self::Memory(state) => {
                let mut guard = state.lock().unwrap();
                if let Some(n) = guard.inflight.get_mut(owner) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        guard.inflight.remove(owner);
                    }
                }
            }
            Self::Redis(client) => {
                if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                    let _: redis::RedisResult<i64> = redis::Script::new(RELEASE_LIMIT)
                        .key(format!("annotations:training:inflight:{owner}"))
                        .invoke_async(&mut conn)
                        .await;
                }
            }
        }
    }
}

fn rate_limited(message: &str) -> NoteError {
    NoteError::new(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limit_exceeded",
        message,
    )
}

/// Releases the per-account concurrency slot when dropped or explicitly released.
pub struct FeedbackPermit {
    limiter: FeedbackLimiter,
    owner: String,
    released: bool,
}
impl FeedbackPermit {
    pub async fn release(mut self) {
        self.released = true;
        self.limiter.release_slot(&self.owner).await;
    }
}
impl Drop for FeedbackPermit {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let limiter = self.limiter.clone();
        let owner = std::mem::take(&mut self.owner);
        if let FeedbackLimiter::Memory(_) = &limiter {
            if let FeedbackLimiter::Memory(state) = &limiter {
                let mut guard = state.lock().unwrap();
                if let Some(n) = guard.inflight.get_mut(&owner) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        guard.inflight.remove(&owner);
                    }
                }
            }
        } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                limiter.release_slot(&owner).await;
            });
        }
    }
}
