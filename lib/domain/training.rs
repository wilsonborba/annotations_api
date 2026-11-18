//! Correction feedback of transcription runs as untrusted training input, and the
//! administrator export and removal of collected examples.
//!
//! Feedback is part of the integrated transcription flow: there is no consent toggle,
//! opt-out or anonymous intake. Each event is validated against the published
//! `training-feedback` schema and strict bounds before it reaches CouchDB. Text is data:
//! it is stored as sent, never executed and never filtered by phrase rules.
//!
//! A stored event is `received`. A second copy of an earlier example of the same run is
//! `flagged`. Nothing here sets `approved_for_training`; review decides that outside the
//! request path. Intermediate edits and accepted results keep their own stage.
//! Examples stay until an administrator removes them; removing a note keeps them.
use crate::{
    core::{self, TRAINING_LIMITS},
    dal::{
        Store,
        training::{self as repository, ExportRow, Inserted, NewFeedback, Selection},
    },
    domain::{
        notes::{NoteError, allow, parse_body},
        schemas,
        transcriptions::{own, request_hash, visible},
    },
};
use axum::http::StatusCode;
use serde_json::{Map, Value, json};
use std::collections::HashSet;

type R<T> = std::result::Result<T, NoteError>;

/// The schema name of one feedback event.
pub const SCHEMA: &str = "training-feedback";
/// Schema versions that this server can validate.
pub fn supports(version: i64) -> bool {
    version == 1
}
/// Pressure (0 to 1000) and tilt (mrad) use the ranges of note strokes.
const PRESSURE_MAX: i64 = 1000;
const TILT_MAX: i64 = 1571;

fn invalid(code: &str, message: &str, path: &str) -> NoteError {
    NoteError::invalid(code, format!("{message}: {path}"))
}
fn wrong(path: &str) -> NoteError {
    invalid("invalid_type", "The value has the wrong type", path)
}

/// Strict access to one JSON object with its path.
struct Obj<'a> {
    map: &'a Map<String, Value>,
    path: String,
}
impl<'a> Obj<'a> {
    fn new(value: &'a Value, path: &str) -> R<Self> {
        match value.as_object() {
            Some(map) => Ok(Self {
                map,
                path: path.into(),
            }),
            None => Err(wrong(path)),
        }
    }
    fn at(&self, key: &str) -> String {
        if self.path.is_empty() {
            key.into()
        } else {
            format!("{}.{key}", self.path)
        }
    }
    fn allow(&self, keys: &[&str]) -> R<()> {
        allow(self.map, keys, &self.at(""))
    }
    fn opt(&self, key: &str) -> Option<&'a Value> {
        self.map.get(key).filter(|v| !v.is_null())
    }
    fn req(&self, key: &str) -> R<&'a Value> {
        self.opt(key).ok_or_else(|| {
            invalid(
                "missing_field",
                "A required field is missing",
                &self.at(key),
            )
        })
    }
    fn str(&self, key: &str) -> R<&'a str> {
        self.req(key)?.as_str().ok_or_else(|| wrong(&self.at(key)))
    }
    fn arr(&self, key: &str) -> R<Option<&'a Vec<Value>>> {
        match self.opt(key) {
            None => Ok(None),
            Some(v) => v.as_array().map(Some).ok_or_else(|| wrong(&self.at(key))),
        }
    }
    /// A reference: 1 to 128 visible ASCII characters.
    fn reference(&self, key: &str) -> R<&'a str> {
        let s = self.str(key)?;
        if !visible(s) {
            return Err(invalid(
                "invalid_reference",
                "References use 1 to 128 visible ASCII characters",
                &self.at(key),
            ));
        }
        Ok(s)
    }
    /// Text is data: any characters except control characters other than tab and new
    /// line, up to the character limit. Content is never filtered by phrases.
    fn text(&self, key: &str) -> R<&'a str> {
        let s = self.str(key)?;
        if s.chars().count() > TRAINING_LIMITS.max_text_chars {
            return Err(invalid(
                "text_too_long",
                "The text is longer than allowed",
                &self.at(key),
            ));
        }
        if s.chars().any(|c| c.is_control() && c != '\n' && c != '\t') {
            return Err(invalid(
                "invalid_text",
                "Text contains a control character",
                &self.at(key),
            ));
        }
        Ok(s)
    }
    /// An array of integers. A float, another type or a number that is not finite fails.
    fn ints(&self, key: &str) -> R<Option<Vec<i64>>> {
        let Some(list) = self.arr(key)? else {
            return Ok(None);
        };
        list.iter()
            .enumerate()
            .map(|(i, v)| {
                v.as_i64()
                    .ok_or_else(|| wrong(&format!("{}[{i}]", self.at(key))))
            })
            .collect::<R<Vec<i64>>>()
            .map(Some)
    }
}

/// One stroke of the corrected segment, stored column by column like note strokes:
/// x and y in integer 1/100 page units, t in ms from the stroke start, optional
/// pressure (0 to 1000) and tilt (mrad).
fn stroke(value: &Value, path: &str) -> R<()> {
    let s = Obj::new(value, path)?;
    s.allow(&["x", "y", "t", "pressure", "tilt"])?;
    let range =
        |code: &str, at: String| invalid(code, "The number is outside the allowed range", &at);
    let xs = s
        .ints("x")?
        .ok_or_else(|| invalid("missing_field", "A required field is missing", &s.at("x")))?;
    if xs.is_empty() {
        return Err(invalid(
            "empty_array",
            "The list must not be empty",
            &s.at("x"),
        ));
    }
    if xs.len() > TRAINING_LIMITS.max_samples_per_stroke {
        return Err(invalid(
            "too_many_samples",
            "The stroke has more samples than allowed",
            path,
        ));
    }
    let column = |key: &str, required: bool| -> R<Option<Vec<i64>>> {
        match s.ints(key)? {
            None if required => Err(invalid(
                "missing_field",
                "A required field is missing",
                &s.at(key),
            )),
            Some(v) if v.len() != xs.len() => Err(invalid(
                "invalid_count",
                "The list has the wrong number of entries",
                &s.at(key),
            )),
            other => Ok(other),
        }
    };
    let ys = column("y", true)?.unwrap_or_default();
    let ts = column("t", true)?.unwrap_or_default();
    for (key, values) in [("x", &xs), ("y", &ys)] {
        if let Some(i) = values
            .iter()
            .position(|v| !(0..=TRAINING_LIMITS.max_coordinate).contains(v))
        {
            return Err(range("out_of_range", format!("{}[{i}]", s.at(key))));
        }
    }
    for (i, t) in ts.iter().enumerate() {
        if (i == 0 && *t != 0) || (i > 0 && *t < ts[i - 1]) {
            return Err(invalid(
                "invalid_sample_order",
                "Sample times start at 0 and never decrease",
                &format!("{}[{i}]", s.at("t")),
            ));
        }
        if *t > TRAINING_LIMITS.max_sample_ms {
            return Err(range("out_of_range", format!("{}[{i}]", s.at("t"))));
        }
    }
    for (key, max) in [("pressure", PRESSURE_MAX), ("tilt", TILT_MAX)] {
        if let Some(values) = column(key, false)?
            && let Some(i) = values.iter().position(|v| !(0..=max).contains(v))
        {
            return Err(range("out_of_range", format!("{}[{i}]", s.at(key))));
        }
    }
    Ok(())
}

/// A validated event.
struct Example<'a> {
    value: &'a Value,
    event_id: &'a str,
    segment_id: &'a str,
    source_item_ids: Vec<&'a str>,
    stage: &'a str,
    language: &'a str,
    model_version: &'a str,
    path: String,
}
fn example<'a>(value: &'a Value, path: String) -> R<Example<'a>> {
    let e = Obj::new(value, &path)?;
    e.allow(&[
        "event_id",
        "segment_id",
        "source_item_ids",
        "strokes",
        "suggestion",
        "correction",
        "stage",
        "language",
        "model_version",
    ])?;
    let event_id = e.reference("event_id")?;
    let segment_id = e.reference("segment_id")?;
    let mut source_item_ids = vec![];
    if let Some(list) = e.arr("source_item_ids")? {
        if list.len() > TRAINING_LIMITS.max_source_items {
            return Err(invalid(
                "too_many_source_items",
                "The event names more source items than allowed",
                &e.at("source_item_ids"),
            ));
        }
        for (i, id) in list.iter().enumerate() {
            let at = format!("{}[{i}]", e.at("source_item_ids"));
            let id = id.as_str().ok_or_else(|| wrong(&at))?;
            if !visible(id) {
                return Err(invalid(
                    "invalid_reference",
                    "References use 1 to 128 visible ASCII characters",
                    &at,
                ));
            }
            source_item_ids.push(id);
        }
    }
    if let Some(list) = e.arr("strokes")? {
        if list.len() > TRAINING_LIMITS.max_strokes {
            return Err(invalid(
                "too_many_strokes",
                "The event has more strokes than allowed",
                &e.at("strokes"),
            ));
        }
        for (i, s) in list.iter().enumerate() {
            stroke(s, &format!("{}[{i}]", e.at("strokes")))?;
        }
    }
    e.text("suggestion")?;
    e.text("correction")?;
    let stage = e.str("stage")?;
    if !matches!(stage, "intermediate" | "accepted") {
        return Err(invalid(
            "invalid_variant",
            "stage must be intermediate or accepted",
            &e.at("stage"),
        ));
    }
    Ok(Example {
        value,
        event_id,
        segment_id,
        source_item_ids,
        stage,
        language: e.str("language")?,
        model_version: e.str("model_version")?,
        path,
    })
}

fn not_completed() -> NoteError {
    NoteError::new(
        StatusCode::CONFLICT,
        "transcription_not_completed",
        "Feedback needs a completed transcription run",
    )
}
fn reused() -> NoteError {
    NoteError::new(
        StatusCode::CONFLICT,
        "event_id_reused",
        "The event id was already used with a different event",
    )
}

/// `POST /me/transcriptions/{run_id}/feedback`. Stores every new event of the request
/// and returns the run's view of all events of the request, plus true when at least one
/// event is new. A retry of a stored event returns it again; a stored event id with a
/// different event fails and stores nothing.
pub async fn submit(store: &Store, owner: &str, run_id: &str, raw: &[u8]) -> R<(Value, bool)> {
    if raw.len() > TRAINING_LIMITS.max_request_bytes {
        return Err(NoteError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "The request body is larger than allowed",
        ));
    }
    let body = parse_body(raw)?;
    allow(&body, &["schema_version", "events"], "")?;
    let requested = match body.get("schema_version") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_i64()
                .filter(|v| *v > 0)
                .ok_or_else(|| wrong("schema_version"))?,
        ),
    };
    let list = match body.get("events") {
        None | Some(Value::Null) => {
            return Err(invalid(
                "missing_field",
                "A required field is missing",
                "events",
            ));
        }
        Some(Value::Array(list)) => list,
        Some(_) => return Err(wrong("events")),
    };
    if list.is_empty() {
        return Err(invalid(
            "empty_array",
            "The list must not be empty",
            "events",
        ));
    }
    if list.len() > TRAINING_LIMITS.max_events {
        return Err(NoteError::invalid(
            "too_many_events",
            format!(
                "A request sends at most {} events",
                TRAINING_LIMITS.max_events
            ),
        ));
    }
    let mut examples: Vec<Example> = vec![];
    for (i, value) in list.iter().enumerate() {
        let next = example(value, format!("events[{i}]"))?;
        match examples.iter().find(|e| e.event_id == next.event_id) {
            // The same event twice in one request counts once.
            Some(known) if known.value == next.value => {}
            Some(_) => {
                return Err(invalid(
                    "duplicate_event_id",
                    "The request sends one event id with different events",
                    &format!("{}.event_id", next.path),
                ));
            }
            None => examples.push(next),
        }
    }

    // The published schema version and its registered fields.
    let (version, fields) = schemas::published_fields(store, SCHEMA, requested)
        .await
        .map_err(|e| {
            if e.0.is_server_error() {
                NoteError::from(e)
            } else {
                NoteError::invalid(
                    "schema_not_published",
                    "A registered published schema version is required",
                )
            }
        })?;
    if !supports(version) {
        return Err(NoteError::invalid(
            "unsupported_schema_version",
            "No validator exists for this schema version",
        ));
    }
    for e in &examples {
        if let Err((code, path)) = schemas::check_fields(&fields, e.value) {
            let at = if path.is_empty() {
                e.path.clone()
            } else {
                format!("{}.{path}", e.path)
            };
            return Err(invalid(code, "The event does not match the schema", &at));
        }
    }

    // The run must be a completed run of the caller, and every event must match it.
    let run = own(store, owner, run_id).await?;
    if run.status != "completed" {
        return Err(not_completed());
    }
    for e in &examples {
        if e.language != run.language {
            return Err(invalid(
                "language_mismatch",
                "The language does not match the run",
                &format!("{}.language", e.path),
            ));
        }
        if e.model_version != run.model_version {
            return Err(invalid(
                "model_version_mismatch",
                "The model version does not match the run",
                &format!("{}.model_version", e.path),
            ));
        }
    }
    if examples.iter().any(|e| !e.source_item_ids.is_empty()) {
        let items: HashSet<String> = match repository::ink_document(&store.db, &run.run_id).await? {
            None => HashSet::new(),
            Some(id) => {
                let document = store.docs.get(&id).await.map_err(NoteError::from)?;
                document["body"]["items"]
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|i| i["id"].as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default()
            }
        };
        for e in &examples {
            if let Some(i) = e.source_item_ids.iter().position(|id| !items.contains(*id)) {
                return Err(invalid(
                    "unknown_source_item",
                    "A source item is not an ink item of the run",
                    &format!("{}.source_item_ids[{i}]", e.path),
                ));
            }
        }
    }

    // Replays and conflicts before any write.
    let mut stored = vec![];
    let mut fresh = vec![];
    for e in &examples {
        let hash = request_hash(&json!({"schema_version": version, "event": e.value}));
        match repository::event(&store.db, &run.run_id, e.event_id).await? {
            Some(found) if found.request_hash != hash => return Err(reused()),
            Some(found) => stored.push(json!({
                "event_id": e.event_id, "stage": found.stage, "created_at": found.created_at,
            })),
            None => fresh.push((e, hash)),
        }
    }
    if fresh.is_empty() {
        return Ok((response(&run.run_id, stored), false));
    }

    let now = core::now();
    let mut rows = Vec::with_capacity(fresh.len());
    for (e, hash) in &fresh {
        let mut content = e.value.as_object().cloned().unwrap_or_default();
        content.remove("event_id");
        let feedback_id = core::id();
        let document_id = format!("training-feedback-{feedback_id}");
        store
            .docs
            .put_immutable(
                &document_id,
                &json!({
                    "type": "training_feedback",
                    "run_id": run.run_id,
                    "event_id": e.event_id,
                    "schema_name": SCHEMA,
                    "schema_version": version,
                    "body": e.value,
                }),
            )
            .await
            .map_err(NoteError::from)?;
        rows.push(NewFeedback {
            feedback_id,
            run_id: run.run_id.clone(),
            owner_id: owner.to_string(),
            event_id: e.event_id.to_string(),
            request_hash: hash.clone(),
            content_hash: request_hash(&Value::Object(content)),
            segment_id: e.segment_id.to_string(),
            stage: e.stage.to_string(),
            source_note_id: run.source_note_id.clone(),
            document_id,
            schema_version: version,
            created_at: now,
        });
    }
    let inserted = match repository::insert(&store.db, &rows).await {
        Ok(Ok(inserted)) => inserted,
        failed => {
            // Nothing references the documents of rows that were not stored.
            for row in &rows {
                let _ = store.docs.delete(&row.document_id).await;
            }
            return Err(failed.err().unwrap_or_else(reused));
        }
    };
    let mut created = false;
    for (row, result) in rows.iter().zip(inserted) {
        match result {
            Inserted::New => {
                created = true;
                stored.push(json!({
                    "event_id": row.event_id, "stage": row.stage, "created_at": row.created_at,
                }));
            }
            Inserted::Replay(found) => {
                let _ = store.docs.delete(&row.document_id).await;
                stored.push(json!({
                    "event_id": row.event_id, "stage": found.stage, "created_at": found.created_at,
                }));
            }
        }
    }
    Ok((response(&run.run_id, stored), created))
}
fn response(run_id: &str, mut events: Vec<Value>) -> Value {
    events.sort_by(|a, b| a["event_id"].as_str().cmp(&b["event_id"].as_str()));
    json!({"run_id": run_id, "events": events})
}

/// One exported example with its provenance, as one line of JSON.
async fn export_line(store: &Store, row: &ExportRow) -> R<Value> {
    let document = store
        .docs
        .get(&row.document_id)
        .await
        .map_err(NoteError::from)?;
    Ok(json!({
        "example_id": row.feedback_id,
        "event_id": row.event_id,
        "status": row.status,
        "stage": row.stage,
        "segment_id": row.segment_id,
        "schema_name": SCHEMA,
        "schema_version": row.schema_version,
        "received_at": row.created_at,
        "provenance": {
            "account_id": row.owner_id,
            "run_id": row.run_id,
            "run_mode": row.mode,
            "language": row.language,
            "model_version": row.model_version,
            "platform": row.platform,
            "source_note_id": row.source_note_id,
            "source_revision_id": row.source_revision_id,
            "source_hash": row.source_hash,
        },
        "event": document["body"],
    }))
}

/// `training export`: write received and approved examples (and flagged ones when
/// `include_flagged` is true) as JSON Lines. Returns the number of examples written.
pub async fn export(
    store: &Store,
    include_flagged: bool,
    out: &mut dyn std::io::Write,
) -> R<usize> {
    let mut after = String::new();
    let mut count = 0;
    loop {
        let rows =
            repository::export(&store.db, &after, TRAINING_LIMITS.batch, include_flagged).await?;
        for row in &rows {
            let line = export_line(store, row).await?;
            writeln!(out, "{line}").map_err(|_| {
                NoteError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "export_write_failed",
                    "Cannot write the export output",
                )
            })?;
            count += 1;
        }
        match rows.last() {
            Some(last) if rows.len() as i64 == TRAINING_LIMITS.batch => {
                after = last.feedback_id.clone()
            }
            _ => return Ok(count),
        }
    }
}

/// `training remove`: delete the selected examples, their CouchDB documents and rows,
/// and nothing else. Returns the number of examples removed.
pub async fn remove(store: &Store, selection: Selection<'_>) -> R<usize> {
    let value = match &selection {
        Selection::Run(v) | Selection::Note(v) | Selection::Account(v) => *v,
    };
    if !visible(value) {
        return Err(NoteError::invalid(
            "invalid_reference",
            "The selection uses 1 to 128 visible ASCII characters",
        ));
    }
    let mut count = 0;
    loop {
        let rows = repository::selected(&store.db, &selection, TRAINING_LIMITS.batch).await?;
        if rows.is_empty() {
            return Ok(count);
        }
        for (feedback_id, document_id) in rows {
            // The document goes first; a failed removal keeps the row for a retry.
            match store.docs.delete(&document_id).await {
                Ok(()) => {}
                Err(e) if e.0 == StatusCode::NOT_FOUND => {}
                Err(e) => return Err(e.into()),
            }
            repository::remove(&store.db, &feedback_id).await?;
            count += 1;
        }
    }
}
