//! Server-owned transcription runs (`/me/transcriptions`).
//!
//! The server owns the run id, the account binding, the model download and version,
//! the source and the input references, and the stages it received. A local run takes
//! one result from its owner; a cloud run gets its result from the server only. Run
//! checks give traceable, account-bound submissions with replay protection. They are
//! not proof that honest local inference happened.
//!
//! A completed run is a saved private draft. It is never published; acceptance into
//! the note is a separate typed save that names the run (`notes::replace`).
//! Cancel stops unfinished execution and deletes nothing.
use crate::{
    core::{self, NOTE_LIMITS, TRANSCRIPTION_LIMITS},
    dal::{
        Store,
        transcriptions::{self as repository, EventRow, NewRun, ResultRow, RunRow},
    },
    domain::{
        models::{self, token},
        note_documents::{self, FileScope},
        note_saves::{canonical, sha_hex},
        notes::{NoteError, allow, parse_body, wrong_type},
    },
};
use axum::http::StatusCode;
use serde_json::{Map, Value, json};

type R<T> = std::result::Result<T, NoteError>;

fn not_found() -> NoteError {
    NoteError::new(
        StatusCode::NOT_FOUND,
        "transcription_not_found",
        "Transcription run is unavailable",
    )
}
fn conflict(code: &str, message: &str) -> NoteError {
    NoteError::new(StatusCode::CONFLICT, code, message)
}
fn missing(field: &str) -> NoteError {
    NoteError::invalid(
        "missing_field",
        format!("A required field is missing: {field}"),
    )
}
fn text<'a>(body: &'a Map<String, Value>, field: &str, max: usize) -> R<&'a str> {
    match body.get(field) {
        None | Some(Value::Null) => Err(missing(field)),
        Some(Value::String(s)) if token(s, max) => Ok(s),
        Some(_) => Err(wrong_type(field)),
    }
}
/// A client source hash or event id: 1 to 128 visible ASCII characters.
pub(crate) fn visible(s: &str) -> bool {
    (1..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_graphic())
}
fn optional_visible<'a>(body: &'a Map<String, Value>, field: &str) -> R<Option<&'a str>> {
    match body.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if visible(s) => Ok(Some(s)),
        Some(_) => Err(wrong_type(field)),
    }
}
pub(crate) fn request_hash(value: &Value) -> String {
    let mut out = String::new();
    canonical(value, &mut out);
    sha_hex(out.as_bytes())
}

/// The source of a run: a saved Original revision, or the client hash of a note that
/// is not synced. Exactly one is given.
enum Source<'a> {
    Revision(&'a str),
    Hash(&'a str),
}
fn source(body: &Map<String, Value>) -> R<Source<'_>> {
    match (
        optional_visible(body, "source_revision_id")?,
        optional_visible(body, "source_hash")?,
    ) {
        (Some(r), None) => Ok(Source::Revision(r)),
        (None, Some(h)) => Ok(Source::Hash(h)),
        _ => Err(NoteError::invalid(
            "invalid_source",
            "Send exactly one of source_revision_id and source_hash",
        )),
    }
}
fn schema_version(map: &Map<String, Value>, field: &str) -> R<Option<i64>> {
    match map.get("schema_version") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .filter(|v| *v > 0)
            .map(Some)
            .ok_or_else(|| wrong_type(field)),
    }
}

/// `POST /me/transcriptions`. Returns the run and true when this call created it.
/// An identical request of the same account reuses its live run (retry or reopen).
pub async fn create(store: &Store, owner: &str, raw: &[u8]) -> R<(Value, bool)> {
    let body = parse_body(raw)?;
    allow(
        &body,
        &[
            "download_id",
            "model_version",
            "language",
            "mode",
            "source_revision_id",
            "source_hash",
            "ink",
            "file_refs",
        ],
        "",
    )?;
    let download_id = text(&body, "download_id", 64)?;
    let model_version = text(&body, "model_version", 128)?;
    let language = text(&body, "language", 16)?;
    let mode = match body.get("mode").and_then(Value::as_str) {
        Some(m @ ("local" | "cloud")) => m,
        Some(_) => return Err(wrong_type("mode")),
        None => return Err(missing("mode")),
    };
    let source = source(&body)?;
    let ink = match body.get("ink") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let map = v.as_object().ok_or_else(|| wrong_type("ink"))?;
            allow(map, &["schema_version", "document"], "ink.")?;
            let document = map.get("document").ok_or_else(|| missing("ink.document"))?;
            Some((schema_version(map, "ink.schema_version")?, document))
        }
    };
    let mut file_ids: Vec<String> = vec![];
    if let Some(list) = body.get("file_refs").filter(|v| !v.is_null()) {
        let list = list.as_array().ok_or_else(|| wrong_type("file_refs"))?;
        for (i, id) in list.iter().enumerate() {
            let id = id
                .as_str()
                .filter(|s| visible(s))
                .ok_or_else(|| wrong_type(&format!("file_refs[{i}]")))?;
            if !file_ids.iter().any(|known| known == id) {
                file_ids.push(id.to_string());
            }
        }
    }
    if file_ids.len() > TRANSCRIPTION_LIMITS.max_file_refs {
        return Err(NoteError::invalid(
            "too_many_file_refs",
            format!(
                "A run names at most {} files",
                TRANSCRIPTION_LIMITS.max_file_refs
            ),
        ));
    }
    if ink.is_none() && file_ids.is_empty() {
        return Err(NoteError::invalid(
            "input_required",
            "Send the selected ink or authorized file_refs",
        ));
    }

    // The download must be the caller's and must match the requested version.
    let download = models::download(&store.db, owner, download_id).await?;
    if download.model_version != model_version {
        return Err(NoteError::invalid(
            "model_version_mismatch",
            "The model version does not match the download",
        ));
    }
    if !repository::release_language(&store.db, model_version, language).await? {
        return Err(NoteError::invalid(
            "unsupported_language",
            "The model release does not support this language",
        ));
    }
    if mode == "cloud" && !store.execution.supports(model_version) {
        return Err(NoteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "cloud_execution_unavailable",
            "Cloud execution is unavailable for this model release; use local mode",
        ));
    }
    if let Source::Revision(revision_id) = source
        && !repository::own_original(&store.db, owner, revision_id).await?
    {
        return Err(NoteError::new(
            StatusCode::NOT_FOUND,
            "source_not_found",
            "The source revision is unavailable",
        ));
    }
    for file_id in &file_ids {
        if !repository::ready_input_file(&store.db, owner, file_id).await? {
            return Err(NoteError::invalid(
                "unknown_file",
                "A file reference is not a ready transcription file of this account",
            ));
        }
    }
    let ink = match ink {
        None => None,
        Some((version, document)) => {
            let version = note_documents::validate_document(
                store,
                "note-original",
                version,
                FileScope::Owner(owner),
                document,
                &NOTE_LIMITS,
            )
            .await
            .map_err(|e| NoteError::from(core::Error::from(e)))?;
            let items = document["items"].as_array().map_or(&[][..], |v| v);
            if items.is_empty()
                || items
                    .iter()
                    .any(|i| !matches!(i["kind"].as_str(), Some("stroke" | "shape")))
            {
                return Err(NoteError::invalid(
                    "invalid_ink",
                    "The selected ink has stroke and shape items only",
                ));
            }
            Some((version, document))
        }
    };
    let mut sorted = file_ids.clone();
    sorted.sort();
    let hash = request_hash(&json!({
        "download_id": download_id,
        "language": language,
        "mode": mode,
        "source": match source {
            Source::Revision(r) => json!({"revision": r}),
            Source::Hash(h) => json!({"hash": h}),
        },
        "ink": ink.map(|(v, d)| json!({"schema_version": v, "document": d})),
        "file_refs": sorted,
    }));
    if let Some(run_id) = repository::reusable(&store.db, owner, &hash).await? {
        return Ok((get(store, owner, &run_id).await?, false));
    }
    let run_id = core::id();
    let ink_document_id = format!("transcription-ink-{run_id}");
    if let Some((version, document)) = ink {
        store
            .docs
            .put_immutable(
                &ink_document_id,
                &json!({
                    "type": "transcription_ink",
                    "run_id": run_id,
                    "schema_name": "note-original",
                    "schema_version": version,
                    "body": document,
                }),
            )
            .await
            .map_err(NoteError::from)?;
    }
    let (source_kind, source_revision_id, source_hash) = match source {
        Source::Revision(r) => ("revision", Some(r), None),
        Source::Hash(h) => ("hash", None, Some(h)),
    };
    let inserted = repository::insert(
        &store.db,
        &NewRun {
            run_id: &run_id,
            owner_id: owner,
            download_id,
            language,
            mode,
            source_kind,
            source_revision_id,
            source_hash,
            request_hash: &hash,
            ink: ink.map(|(v, _)| (ink_document_id.as_str(), v)),
            file_ids: &file_ids,
            now: core::now(),
        },
    )
    .await;
    if !matches!(inserted, Ok(true)) && ink.is_some() {
        // Nothing references the document of a run that was not stored.
        let _ = store.docs.delete(&ink_document_id).await;
    }
    if !inserted? {
        let run_id = repository::reusable(&store.db, owner, &hash)
            .await?
            .ok_or_else(|| NoteError::from(core::Error::unavailable()))?;
        return Ok((get(store, owner, &run_id).await?, false));
    }
    Ok((get(store, owner, &run_id).await?, true))
}

pub(crate) async fn own(store: &Store, owner: &str, run_id: &str) -> R<RunRow> {
    if !visible(run_id) {
        return Err(not_found());
    }
    repository::run(&store.db, owner, run_id)
        .await?
        .ok_or_else(not_found)
}
/// True when the run's source revision is gone or is no longer the current Original.
/// A run with a client source hash has no server copy to compare; it is never stale here.
fn stale(run: &RunRow) -> bool {
    run.source_kind == "revision"
        && (run.source_revision_id.is_none() || run.source_revision_id != run.current_original)
}
async fn to_json(store: &Store, run: &RunRow) -> R<Value> {
    let result = match repository::result(&store.db, &run.run_id).await? {
        None => Value::Null,
        Some(r) => {
            let document = store
                .docs
                .get(&r.document_id)
                .await
                .map_err(NoteError::from)?;
            json!({
                "event_id": r.event_id,
                "schema_version": r.schema_version,
                "document": document["body"],
                "runtime": {
                    "name": r.runtime_name,
                    "version": r.runtime_version,
                    "elapsed_ms": r.elapsed_ms,
                },
                "created_at": r.created_at,
            })
        }
    };
    Ok(json!({
        "run_id": run.run_id,
        "download_id": run.download_id,
        "model_version": run.model_version,
        "platform": run.platform,
        "language": run.language,
        "mode": run.mode,
        "status": run.status,
        "source_note_id": run.source_note_id,
        "source_revision_id": run.source_revision_id,
        "source_hash": run.source_hash,
        "source_stale": stale(run),
        "inputs": {
            "ink_schema_version": repository::ink_version(&store.db, &run.run_id).await?,
            "file_refs": repository::file_ids(&store.db, &run.run_id).await?,
        },
        "last_event_id": repository::last_seq(&store.db, &run.run_id).await?,
        "result": result,
        "published": false,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    }))
}

/// `GET /me/transcriptions/{run_id}`: state, stages and the saved private draft.
pub async fn get(store: &Store, owner: &str, run_id: &str) -> R<Value> {
    let run = own(store, owner, run_id).await?;
    to_json(store, &run).await
}

/// `PUT /me/transcriptions/{run_id}/result`. Accepted once per local run. A retry with
/// the same event id and the same body returns the first result (`false`).
pub async fn submit(store: &Store, owner: &str, run_id: &str, raw: &[u8]) -> R<(Value, bool)> {
    let run = own(store, owner, run_id).await?;
    let body = parse_body(raw)?;
    allow(
        &body,
        &[
            "event_id",
            "source_revision_id",
            "source_hash",
            "schema_version",
            "document",
            "runtime",
        ],
        "",
    )?;
    let event_id = optional_visible(&body, "event_id")?.ok_or_else(|| missing("event_id"))?;
    if run.mode != "local" {
        return Err(conflict(
            "cloud_result_server_owned",
            "Cloud run results are written by the server only",
        ));
    }
    let hash = request_hash(&Value::Object(body.clone()));
    if let Some(found) = repository::result(&store.db, &run.run_id).await? {
        return replay(store, &run, &found, event_id, &hash).await;
    }
    if run.status == "cancelled" {
        return Err(conflict("run_cancelled", "The run was cancelled"));
    }
    let matches = match source(&body)? {
        Source::Revision(r) => {
            run.source_kind == "revision" && run.source_revision_id.as_deref() == Some(r)
        }
        Source::Hash(h) => run.source_kind == "hash" && run.source_hash.as_deref() == Some(h),
    };
    if !matches {
        return Err(conflict(
            "source_mismatch",
            "The result source does not match the source of the run",
        ));
    }
    let runtime = body
        .get("runtime")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("runtime"))?;
    allow(runtime, &["name", "version", "elapsed_ms"], "runtime.")?;
    let runtime_name = text(runtime, "name", 64).map_err(|_| wrong_type("runtime.name"))?;
    let runtime_version =
        text(runtime, "version", 64).map_err(|_| wrong_type("runtime.version"))?;
    let elapsed_ms = runtime
        .get("elapsed_ms")
        .and_then(Value::as_i64)
        .filter(|v| *v >= 0)
        .ok_or_else(|| wrong_type("runtime.elapsed_ms"))?;
    let document = body.get("document").ok_or_else(|| missing("document"))?;
    let version = note_documents::validate_document(
        store,
        "note-typed",
        schema_version(&body, "schema_version")?,
        FileScope::Owner(owner),
        document,
        &NOTE_LIMITS,
    )
    .await
    .map_err(|e| NoteError::from(core::Error::from(e)))?;
    let document_id = format!("transcription-result-{}", core::id());
    store
        .docs
        .put_immutable(
            &document_id,
            &json!({
                "type": "transcription_result",
                "run_id": run.run_id,
                "schema_name": "note-typed",
                "schema_version": version,
                "body": document,
            }),
        )
        .await
        .map_err(NoteError::from)?;
    let row = ResultRow {
        event_id: event_id.to_string(),
        request_hash: hash.clone(),
        document_id: document_id.clone(),
        schema_version: version,
        runtime_name: runtime_name.to_string(),
        runtime_version: runtime_version.to_string(),
        elapsed_ms,
        created_at: core::now(),
    };
    let stored = repository::complete(&store.db, &run.run_id, &row).await;
    if !matches!(stored, Ok(true)) {
        let _ = store.docs.delete(&document_id).await;
    }
    if stored? {
        return Ok((get(store, owner, &run.run_id).await?, true));
    }
    // The run changed meanwhile: a concurrent result or a cancel.
    let run = own(store, owner, &run.run_id).await?;
    match repository::result(&store.db, &run.run_id).await? {
        Some(found) => replay(store, &run, &found, event_id, &hash).await,
        None => Err(conflict("run_cancelled", "The run was cancelled")),
    }
}
async fn replay(
    store: &Store,
    run: &RunRow,
    found: &ResultRow,
    event_id: &str,
    hash: &str,
) -> R<(Value, bool)> {
    if found.event_id != event_id {
        return Err(conflict(
            "result_exists",
            "The run already has its result; a run accepts one result",
        ));
    }
    if found.request_hash != hash {
        return Err(conflict(
            "event_id_reused",
            "The event id was already used with a different result",
        ));
    }
    Ok((to_json(store, run).await?, false))
}

/// `DELETE /me/transcriptions/{run_id}/execution`. Stops an unfinished run. A completed
/// or cancelled run stays as it is; inputs, events and the saved draft are never deleted.
pub async fn cancel(store: &Store, owner: &str, run_id: &str) -> R<Value> {
    let run = own(store, owner, run_id).await?;
    if run.status == "registered" {
        repository::cancel(&store.db, &run.run_id, core::now()).await?;
    }
    get(store, owner, &run.run_id).await
}

/// One server-side stage of a run, for the event stream.
pub struct Event {
    pub id: i64,
    pub stage: String,
    pub data: Value,
}
/// The `after` cursor: the last event id that the client received (0 or more).
pub fn cursor(raw: Option<&str>) -> R<i64> {
    match raw {
        None => Ok(0),
        Some(s) => s
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            .ok_or_else(|| NoteError::invalid("invalid_cursor", "after must be an event id")),
    }
}
/// Check that the caller owns the run before a stream starts.
pub async fn check_events(store: &Store, owner: &str, run_id: &str) -> R<()> {
    own(store, owner, run_id).await.map(|_| ())
}
/// Events after the cursor and whether the run has finished. The status is read first,
/// so a finished run never hides its last stage.
pub async fn events(store: &Store, owner: &str, run_id: &str, after: i64) -> R<(Vec<Event>, bool)> {
    let run = own(store, owner, run_id).await?;
    let finished = run.status != "registered";
    let rows = repository::events(
        &store.db,
        &run.run_id,
        after,
        TRANSCRIPTION_LIMITS.event_batch,
    )
    .await?;
    let more = rows.len() as i64 == TRANSCRIPTION_LIMITS.event_batch;
    let events = rows
        .into_iter()
        .map(|EventRow { seq, stage, created_at }| Event {
            id: seq,
            data: json!({"event_id": seq, "run_id": run.run_id, "stage": stage, "created_at": created_at}),
            stage,
        })
        .collect();
    Ok((events, finished && !more))
}

/// The Original revision that an accepted typed save names through `transcription_run_id`.
/// The run must be a completed run of the caller with a saved source revision of this note.
/// The save itself refuses a source that is no longer the current Original.
pub async fn accepted_source(store: &Store, owner: &str, run_id: &str, note_id: &str) -> R<String> {
    let run = own(store, owner, run_id).await?;
    if run.status != "completed" {
        return Err(conflict(
            "transcription_not_completed",
            "Only a completed transcription run can be accepted",
        ));
    }
    match (&run.source_revision_id, &run.source_note_id) {
        (Some(revision), Some(note)) if note == note_id => Ok(revision.clone()),
        _ => Err(NoteError::invalid(
            "transcription_source_mismatch",
            "The transcription run has no saved source revision of this note",
        )),
    }
}
