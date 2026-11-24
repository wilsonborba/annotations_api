//! Owner working copies of notes (`/me/notes`): create, read, list, sync feed,
//! replace, change and delete.
//!
//! The owner always comes from the gateway identity. Every query filters by that
//! owner, so another owner's note is reported as not found. Every write goes through
//! the crash-safe save pipeline (`note_saves`); delete is one SQL transaction that
//! queues the CouchDB documents for durable removal.
use crate::{
    core::{self, Error, NOTE_API_LIMITS, NOTE_LIMITS, SAVE_SETTINGS},
    dal::Store,
    domain::{
        names,
        note_saves::{
            self, Content, Metadata, Representation, RepresentationInput, SaveError, SaveRequest,
            SaveResult, Target,
        },
        publication, transcriptions,
    },
};
use axum::http::StatusCode;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;

/// Largest request body: two complete documents and their metadata.
pub const MAX_BODY_BYTES: usize = 2 * NOTE_LIMITS.max_document_bytes + 1024 * 1024;

/// A rejected request. `code` is stable and machine-readable; `message` never
/// carries note content.
#[derive(Debug, PartialEq, Eq)]
pub struct NoteError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    /// The current note revision, on a stale If-Match (409).
    pub current_revision: Option<i64>,
}
impl NoteError {
    pub fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            current_revision: None,
        }
    }
    pub(crate) fn invalid(code: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, code, message)
    }
    pub(crate) fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", "Content is unavailable")
    }
    fn conflict(current_revision: i64) -> Self {
        Self {
            current_revision: Some(current_revision),
            ..Self::new(
                StatusCode::CONFLICT,
                "revision_conflict",
                "The note changed since the given revision",
            )
        }
    }
    pub fn identity_required() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "identity_required",
            "A signed-in account is required",
        )
    }
    pub fn if_match_required() -> Self {
        Self::new(
            StatusCode::PRECONDITION_REQUIRED,
            "if_match_required",
            "Send the note revision in the If-Match header",
        )
    }
    pub fn invalid_if_match() -> Self {
        Self::invalid(
            "invalid_if_match",
            "If-Match must be one positive note revision",
        )
    }
    /// The JSON body of the error response.
    pub fn to_json(&self) -> Value {
        let mut out = json!({"error": self.message, "code": self.code});
        if let Some(revision) = self.current_revision {
            out["current_revision"] = json!(revision);
        }
        out
    }
}
impl From<Error> for NoteError {
    fn from(e: Error) -> Self {
        if e.0 == StatusCode::NOT_FOUND {
            return Self::not_found();
        }
        if e.0.is_server_error() {
            return Self::new(e.0, "storage_unavailable", "Storage is unavailable");
        }
        // Domain errors read "<code>: <message>"; keep the code machine-readable.
        let (code, message) = match e.1.split_once(": ") {
            Some((code, rest))
                if !code.is_empty()
                    && code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') =>
            {
                (code.to_string(), rest.to_string())
            }
            _ => ("invalid_content".to_string(), e.1.clone()),
        };
        Self {
            status: e.0,
            code,
            message,
            current_revision: None,
        }
    }
}
impl From<sqlx::Error> for NoteError {
    fn from(e: sqlx::Error) -> Self {
        Error::from(e).into()
    }
}
impl From<SaveError> for NoteError {
    fn from(e: SaveError) -> Self {
        match e {
            SaveError::Conflict { current_revision } => Self::conflict(current_revision),
            SaveError::NotFound => Self::not_found(),
            other => {
                let code = other.code();
                let mut out = NoteError::from(Error::from(other));
                if code != "invalid_content" {
                    out.code = code.into();
                    // Drop the note id that the pipeline adds to some messages.
                    if let Some((text, _)) = out.message.clone().split_once(" (") {
                        out.message = text.into();
                    }
                }
                out
            }
        }
    }
}
type R<T> = std::result::Result<T, NoteError>;

// Request parsing ---------------------------------------------------------------

pub(crate) fn parse_body(raw: &[u8]) -> R<Map<String, Value>> {
    if raw.len() > MAX_BODY_BYTES {
        return Err(NoteError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "The request body is larger than allowed",
        ));
    }
    let value: Value = serde_json::from_slice(raw).map_err(|e| {
        if e.to_string().starts_with("number out of range") {
            NoteError::invalid("non_finite_number", "Numbers must be finite")
        } else {
            NoteError::invalid("invalid_json", "The request body is not valid JSON")
        }
    })?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(NoteError::invalid(
            "invalid_type",
            "The request body must be a JSON object",
        )),
    }
}

/// Reject fields that the request does not define (for example `owner_id`).
pub(crate) fn allow(map: &Map<String, Value>, keys: &[&str], at: &str) -> R<()> {
    let mut unknown: Vec<&String> = map.keys().filter(|k| !keys.contains(&k.as_str())).collect();
    unknown.sort();
    match unknown.first() {
        Some(k) => Err(NoteError::invalid(
            "unknown_field",
            format!("The field is not allowed here: {at}{k}"),
        )),
        None => Ok(()),
    }
}

pub(crate) fn wrong_type(field: &str) -> NoteError {
    NoteError::invalid(
        "invalid_type",
        format!("The value has the wrong type: {field}"),
    )
}

pub(crate) fn representation_name(value: &str, field: &str) -> R<Representation> {
    Representation::parse(value).ok_or_else(|| {
        NoteError::invalid(
            "invalid_representation",
            format!("{field} must be original or typed"),
        )
    })
}

fn schema_version(map: &Map<String, Value>, field: &str) -> R<Option<i64>> {
    match map.get("schema_version") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .filter(|v| *v > 0)
            .map(Some)
            .ok_or_else(|| wrong_type(&format!("{field}schema_version"))),
    }
}

/// `{"schema_version"?: n, "document": {..}}` of one representation.
fn document_input(
    representation: Representation,
    value: &Value,
    field: &str,
) -> R<RepresentationInput> {
    let map = value.as_object().ok_or_else(|| wrong_type(field))?;
    allow(map, &["schema_version", "document"], &format!("{field}."))?;
    let document = map.get("document").ok_or_else(|| {
        NoteError::invalid(
            "missing_field",
            format!("A required field is missing: {field}.document"),
        )
    })?;
    Ok(RepresentationInput {
        representation,
        schema_version: schema_version(map, &format!("{field}."))?,
        content: Content::Document(document.clone()),
        source_revision_id: None,
    })
}

pub(crate) fn clean_text(value: &str, field: &str, max_chars: usize) -> R<String> {
    let text = value.trim();
    if text.chars().any(char::is_control) {
        return Err(NoteError::invalid(
            "invalid_text",
            format!("Text contains a control character: {field}"),
        ));
    }
    if text.chars().count() > max_chars {
        return Err(NoteError::invalid(
            "text_too_long",
            format!("{field} has more than {max_chars} characters"),
        ));
    }
    Ok(text.to_string())
}

/// `{"title"?: string | null, "tags"?: [string]}`. A null or empty title removes it.
fn metadata(value: &Value) -> R<Metadata> {
    let map = value.as_object().ok_or_else(|| wrong_type("metadata"))?;
    allow(map, &["title", "tags"], "metadata.")?;
    let limits = NOTE_API_LIMITS;
    let title = match map.get("title") {
        None => None,
        Some(Value::Null) => Some(String::new()),
        Some(Value::String(s)) => Some(clean_text(s, "metadata.title", limits.max_title_chars)?),
        Some(_) => return Err(wrong_type("metadata.title")),
    };
    let tags = match map.get("tags") {
        None => None,
        Some(Value::Array(list)) => {
            let mut tags: Vec<String> = vec![];
            let mut seen = vec![];
            for (i, tag) in list.iter().enumerate() {
                let field = format!("metadata.tags[{i}]");
                let name = clean_text(
                    tag.as_str().ok_or_else(|| wrong_type(&field))?,
                    &field,
                    limits.max_tag_chars,
                )?;
                let normalized = names::normalize(&name);
                if normalized.is_empty() {
                    return Err(NoteError::invalid(
                        "empty_tag",
                        format!("Tags must not be empty: {field}"),
                    ));
                }
                if !seen.contains(&normalized) {
                    seen.push(normalized);
                    tags.push(name);
                }
            }
            if tags.len() > limits.max_tags {
                return Err(NoteError::invalid(
                    "too_many_tags",
                    format!("A note has at most {} tags", limits.max_tags),
                ));
            }
            Some(tags)
        }
        Some(_) => return Err(wrong_type("metadata.tags")),
    };
    Ok(Metadata {
        title,
        tags,
        project_ids: None,
    })
}

pub(crate) fn token(s: &str) -> bool {
    (1..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_graphic())
}

fn sha_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The pipeline operation key of a change. Without an Idempotency-Key a change is
/// not replayed; If-Match still prevents a double write.
fn update_key(idempotency_key: Option<&str>) -> R<String> {
    match idempotency_key {
        Some(key) if token(key) => Ok(format!("u:{}", sha_hex(key.as_bytes()))),
        Some(_) => Err(NoteError::invalid(
            "invalid_operation_key",
            "The Idempotency-Key uses 1 to 128 visible ASCII characters",
        )),
        None => Ok(format!("u:{}", sha_hex(core::id().as_bytes()))),
    }
}

// Reads -------------------------------------------------------------------------

/// The views of `GET /me/notes/{note_id}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum View {
    Original,
    Typed,
    Both,
}
impl View {
    pub fn parse(value: Option<&str>) -> R<Self> {
        match value {
            None | Some("both") => Ok(Self::Both),
            Some("original") => Ok(Self::Original),
            Some("typed") => Ok(Self::Typed),
            Some(_) => Err(NoteError::invalid(
                "invalid_view",
                "view must be original, typed or both",
            )),
        }
    }
    fn includes(self, representation: &str) -> bool {
        match self {
            Self::Both => true,
            Self::Original => representation == "original",
            Self::Typed => representation == "typed",
        }
    }
}

pub(crate) const NOTE_COLUMNS: &str = "n.note_id,n.client_note_id,n.main_representation,n.revision,n.created_at,n.updated_at,COALESCE(t.title,'') AS title";

/// The summary of one note row of the owner: metadata, current revisions and
/// whether a publication exists. No document content.
pub(crate) async fn summary(store: &Store, owner: &str, row: &sqlx::any::AnyRow) -> R<Value> {
    let note_id: String = row.get("note_id");
    let tags: Vec<String> = sqlx::query("SELECT g.name FROM note_tags nt JOIN tags g ON g.tag_id=nt.tag_id WHERE nt.note_id=$1 ORDER BY g.normalized_name")
        .bind(&note_id).fetch_all(&store.db).await?
        .iter().map(|r| r.get("name")).collect();
    let projects: Vec<String> = sqlx::query(
        "SELECT project_id FROM project_notes WHERE note_id=$1 AND owner_id=$2 ORDER BY project_id",
    )
    .bind(&note_id)
    .bind(owner)
    .fetch_all(&store.db)
    .await?
    .iter()
    .map(|r| r.get("project_id"))
    .collect();
    let representations: Vec<Value> = sqlx::query("SELECT c.representation,c.revision_id,r.schema_version FROM note_current_revisions c JOIN note_revisions r ON r.revision_id=c.revision_id WHERE c.note_id=$1 ORDER BY c.representation")
        .bind(&note_id).fetch_all(&store.db).await?
        .iter()
        .map(|r| json!({"representation": r.get::<String,_>("representation"), "revision_id": r.get::<String,_>("revision_id"), "schema_version": r.get::<i64,_>("schema_version")}))
        .collect();
    let published = publication::is_published(store, &note_id).await?;
    let study_source = study_source_json(store, &note_id).await?;
    let title: String = row.get("title");
    Ok(json!({
        "note_id": note_id,
        "client_note_id": row.get::<String, _>("client_note_id"),
        "main_representation": row.get::<String, _>("main_representation"),
        "revision": row.get::<i64, _>("revision"),
        "title": if title.is_empty() { Value::Null } else { json!(title) },
        "tags": tags,
        "project_ids": projects,
        "representations": representations,
        "published": published,
        "study_source": study_source,
        "created_at": row.get::<i64, _>("created_at"),
        "updated_at": row.get::<i64, _>("updated_at"),
    }))
}

async fn study_source_json(store: &Store, note_id: &str) -> R<Value> {
    let row = sqlx::query(
        "SELECT source_note_id,source_title,source_author_id,source_author_name FROM note_study_sources WHERE note_id=$1",
    )
    .bind(note_id)
    .fetch_optional(&store.db)
    .await?;
    Ok(match row {
        None => Value::Null,
        Some(r) => {
            let title: String = r.try_get("source_title")?;
            let author_name: String = r.try_get("source_author_name")?;
            json!({
                "note_id": r.try_get::<Option<String>, _>("source_note_id")?,
                "title": if title.is_empty() { Value::Null } else { json!(title) },
                "author_id": r.try_get::<Option<String>, _>("source_author_id")?,
                "author_name": publication::author_name(author_name),
            })
        }
    })
}

async fn note_row(store: &Store, owner: &str, note_id: &str) -> R<sqlx::any::AnyRow> {
    sqlx::query(&format!("SELECT {NOTE_COLUMNS} FROM notes n LEFT JOIN note_titles t ON t.note_id=n.note_id WHERE n.note_id=$1 AND n.owner_id=$2"))
        .bind(note_id)
        .bind(owner)
        .fetch_optional(&store.db)
        .await?
        .ok_or_else(NoteError::not_found)
}

/// The current working copy: metadata, the documents of the chosen view and the
/// publication status.
pub async fn get(store: &Store, owner: &str, note_id: &str, view: View) -> R<Value> {
    let row = note_row(store, owner, note_id).await?;
    let mut out = summary(store, owner, &row).await?;
    let rows = sqlx::query("SELECT c.representation,r.document_id FROM note_current_revisions c JOIN note_revisions r ON r.revision_id=c.revision_id WHERE c.note_id=$1 ORDER BY c.representation")
        .bind(note_id).fetch_all(&store.db).await?;
    let mut representations = vec![];
    for (entry, row) in out["representations"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .zip(rows)
    {
        let name: String = row.get("representation");
        if !view.includes(&name) {
            continue;
        }
        let stored = store
            .docs
            .get(&row.get::<String, _>("document_id"))
            .await
            .map_err(|_| NoteError::from(Error::unavailable()))?;
        let mut entry = entry;
        entry["document"] = stored["body"].clone();
        representations.push(entry);
    }
    out["representations"] = Value::Array(representations);
    out["publication"] = publication::owner_block(store, note_id).await?;
    Ok(out)
}

/// Query of `GET /me/notes`. All values are raw query strings.
#[derive(Debug, Default)]
pub struct ListQuery {
    pub q: Option<String>,
    pub project_id: Option<String>,
    pub tag: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<String>,
    pub sync_cursor: Option<String>,
}

pub(crate) fn page_limit(raw: Option<&str>) -> R<i64> {
    let limits = NOTE_API_LIMITS;
    match raw {
        None => Ok(limits.default_page),
        Some(v) => v
            .parse::<i64>()
            .ok()
            .filter(|n| (1..=limits.max_page).contains(n))
            .ok_or_else(|| {
                NoteError::invalid(
                    "invalid_limit",
                    format!("limit must be from 1 to {}", limits.max_page),
                )
            }),
    }
}

/// A list cursor `"<updated_at>.<id>"`: the last item of the previous page.
pub(crate) fn keyset_cursor(raw: Option<&str>) -> R<(Option<i64>, String)> {
    match raw {
        None => Ok((None, String::new())),
        Some(c) => c
            .split_once('.')
            .and_then(|(t, id)| Some((t.parse::<i64>().ok()?, id)))
            .filter(|(_, id)| token(id))
            .map(|(t, id)| (Some(t), id.to_string()))
            .ok_or_else(|| NoteError::invalid("invalid_cursor", "The cursor is not valid")),
    }
}

fn like_pattern(q: &str) -> String {
    let mut out = String::from("%");
    for c in q.trim().to_lowercase().chars() {
        if matches!(c, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('%');
    out
}

/// List the owner's notes (newest change first), or read the change feed when
/// `sync_cursor` is given.
pub async fn list(store: &Store, owner: &str, query: &ListQuery) -> R<Value> {
    let limit = page_limit(query.limit.as_deref())?;
    if let Some(sync) = &query.sync_cursor {
        if query.q.is_some()
            || query.project_id.is_some()
            || query.tag.is_some()
            || query.cursor.is_some()
        {
            return Err(NoteError::invalid(
                "sync_cursor_with_filters",
                "sync_cursor cannot be combined with q, project_id, tag or cursor",
            ));
        }
        let after = sync
            .parse::<i64>()
            .ok()
            .filter(|n| *n >= 0)
            .ok_or_else(|| {
                NoteError::invalid("invalid_sync_cursor", "The sync cursor is not valid")
            })?;
        return changes(store, owner, after, limit).await;
    }
    // Read the feed position first: a change after it is in the next sync.
    let head: i64 =
        sqlx::query("SELECT COALESCE(MAX(sequence),0) AS head FROM note_changes WHERE owner_id=$1")
            .bind(owner)
            .fetch_one(&store.db)
            .await?
            .get("head");
    let (after_time, after_id) = keyset_cursor(query.cursor.as_deref())?;
    let q = query.q.as_deref().map(str::trim).unwrap_or("");
    let pattern = if q.is_empty() {
        String::new()
    } else {
        like_pattern(q)
    };
    let tag = query
        .tag
        .as_deref()
        .map(names::normalize)
        .unwrap_or_default();
    let rows = sqlx::query(&format!(
        "SELECT {NOTE_COLUMNS} FROM notes n LEFT JOIN note_titles t ON t.note_id=n.note_id \
         WHERE n.owner_id=$1 \
         AND ($2='' OR lower(COALESCE(t.title,'')) LIKE $2 ESCAPE '\\') \
         AND ($3='' OR EXISTS(SELECT 1 FROM project_notes p WHERE p.note_id=n.note_id AND p.owner_id=n.owner_id AND p.project_id=$3)) \
         AND ($4='' OR EXISTS(SELECT 1 FROM note_tags nt JOIN tags g ON g.tag_id=nt.tag_id WHERE nt.note_id=n.note_id AND g.normalized_name=$4)) \
         AND ($5=0 OR n.updated_at<$6 OR (n.updated_at=$6 AND n.note_id<$7)) \
         ORDER BY n.updated_at DESC,n.note_id DESC LIMIT $8"
    ))
    .bind(owner)
    .bind(&pattern)
    .bind(query.project_id.as_deref().unwrap_or(""))
    .bind(&tag)
    .bind(i64::from(after_time.is_some()))
    .bind(after_time.unwrap_or(0))
    .bind(&after_id)
    .bind(limit + 1)
    .fetch_all(&store.db)
    .await?;
    let more = rows.len() as i64 > limit;
    let mut items = vec![];
    let mut next = Value::Null;
    for row in rows.iter().take(limit as usize) {
        next = json!(format!(
            "{}.{}",
            row.get::<i64, _>("updated_at"),
            row.get::<String, _>("note_id")
        ));
        items.push(summary(store, owner, row).await?);
    }
    Ok(json!({
        "items": items,
        "next_cursor": if more { next } else { Value::Null },
        "sync_cursor": head.to_string(),
    }))
}

/// Changes after `after` in feed order: creates and changes (`upsert`) and
/// deletions (`delete`). Delete notices stay after the note row is gone.
async fn changes(store: &Store, owner: &str, after: i64, limit: i64) -> R<Value> {
    let rows = sqlx::query("SELECT sequence,note_id,change_kind,changed_at FROM note_changes WHERE owner_id=$1 AND sequence>$2 ORDER BY sequence LIMIT $3")
        .bind(owner).bind(after).bind(limit + 1).fetch_all(&store.db).await?;
    let more = rows.len() as i64 > limit;
    let mut last = after;
    let changes: Vec<Value> = rows
        .iter()
        .take(limit as usize)
        .map(|r| {
            last = r.get("sequence");
            json!({
                "sequence": last.to_string(),
                "note_id": r.get::<String, _>("note_id"),
                "change": r.get::<String, _>("change_kind"),
                "changed_at": r.get::<i64, _>("changed_at"),
            })
        })
        .collect();
    Ok(json!({
        "changes": changes,
        "next_sync_cursor": last.to_string(),
        "has_more": more,
    }))
}

// Writes ------------------------------------------------------------------------

/// The answer of a create: the new note, or the note that already has this
/// `client_note_id` (then `created` is false and nothing changed).
#[derive(Debug)]
pub struct Created {
    pub created: bool,
    pub body: Value,
    pub revision: i64,
}

fn save_body(result: &SaveResult) -> Value {
    json!({
        "note_id": result.note_id,
        "revision": result.revision,
        "revisions": result.revisions,
    })
}

async fn existing(store: &Store, owner: &str, client_note_id: &str) -> R<Option<Created>> {
    Ok(
        sqlx::query("SELECT note_id,revision FROM notes WHERE owner_id=$1 AND client_note_id=$2")
            .bind(owner)
            .bind(client_note_id)
            .fetch_optional(&store.db)
            .await?
            .map(|r| {
                let revision: i64 = r.get("revision");
                Created {
                    created: false,
                    revision,
                    body: json!({
                        "note_id": r.get::<String, _>("note_id"),
                        "client_note_id": client_note_id,
                        "revision": revision,
                        "created": false,
                    }),
                }
            }),
    )
}

/// Create a private working copy for the caller. The first create binds the note
/// to the caller's account. Idempotent by `client_note_id` per owner.
pub async fn create(store: &Store, owner: &str, raw: &[u8]) -> R<Created> {
    let body = parse_body(raw)?;
    allow(
        &body,
        &[
            "client_note_id",
            "main_representation",
            "metadata",
            "representations",
            "project_ids",
            "study_source_note_id",
        ],
        "",
    )?;
    let client_note_id = body
        .get("client_note_id")
        .and_then(Value::as_str)
        .filter(|s| token(s))
        .ok_or_else(|| {
            NoteError::invalid(
                "invalid_client_note_id",
                "The client note id uses 1 to 128 visible ASCII characters",
            )
        })?
        .to_string();
    let reps = body
        .get("representations")
        .and_then(Value::as_object)
        .ok_or_else(|| wrong_type("representations"))?;
    allow(reps, &["original", "typed"], "representations.")?;
    let mut representations = vec![];
    for (name, value) in reps {
        let representation = representation_name(name, "representations")?;
        representations.push(document_input(
            representation,
            value,
            &format!("representations.{name}"),
        )?);
    }
    if representations.is_empty() {
        return Err(NoteError::invalid(
            "representation_required",
            "A new note needs at least one representation",
        ));
    }
    let main_representation = match body.get("main_representation") {
        Some(Value::String(s)) => representation_name(s, "main_representation")?,
        Some(_) => return Err(wrong_type("main_representation")),
        None if representations.len() == 1 => representations[0].representation,
        None => {
            return Err(NoteError::invalid(
                "main_representation_required",
                "A note with two representations needs main_representation",
            ));
        }
    };
    let mut meta = match body.get("metadata") {
        None => Metadata::default(),
        Some(v) => metadata(v)?,
    };
    if let Some(list) = body.get("project_ids") {
        let list = list.as_array().ok_or_else(|| wrong_type("project_ids"))?;
        let mut ids: Vec<String> = vec![];
        for (i, id) in list.iter().enumerate() {
            let id = id
                .as_str()
                .filter(|s| token(s))
                .ok_or_else(|| wrong_type(&format!("project_ids[{i}]")))?;
            if !ids.iter().any(|known| known == id) {
                ids.push(id.to_string());
            }
        }
        if ids.len() > NOTE_API_LIMITS.max_projects {
            return Err(NoteError::invalid(
                "too_many_projects",
                format!(
                    "A create names at most {} projects",
                    NOTE_API_LIMITS.max_projects
                ),
            ));
        }
        meta.project_ids = Some(ids);
    }
    let study_source = match body.get("study_source_note_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) => {
            if !token(id) {
                return Err(NoteError::invalid(
                    "invalid_study_source",
                    "study_source_note_id uses 1 to 128 visible ASCII characters",
                ));
            }
            let row = sqlx::query(
                "SELECT n.note_id,COALESCE(pt.title,'') AS title,a.author_id,COALESCE(o.display_name,'') AS display_name FROM note_publications np JOIN notes n ON n.note_id=np.note_id LEFT JOIN note_publication_titles pt ON pt.note_id=np.note_id LEFT JOIN authors a ON a.owner_id=n.owner_id LEFT JOIN owners o ON o.id=n.owner_id WHERE np.note_id=$1 AND EXISTS(SELECT 1 FROM note_published_representations p WHERE p.note_id=np.note_id AND p.representation='typed')",
            )
            .bind(id)
            .fetch_optional(&store.db)
            .await?
            .ok_or_else(|| {
                NoteError::invalid(
                    "study_source_unavailable",
                    "The study source must be a published typed post",
                )
            })?;
            Some((
                row.try_get::<String, _>("note_id")?,
                row.try_get::<String, _>("title")?,
                row.try_get::<Option<String>, _>("author_id")?,
                row.try_get::<String, _>("display_name")?,
            ))
        }
        Some(_) => return Err(wrong_type("study_source_note_id")),
    };
    if let Some(found) = existing(store, owner, &client_note_id).await? {
        return Ok(found);
    }
    // Account binding: the gateway account becomes the owner of the new note.
    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(owner)
        .execute(&store.db)
        .await?;
    // A retry of the same request replays its result; another body for the same
    // client note id runs as its own operation and meets the existing note.
    let request_text = Value::Object(body.clone()).to_string();
    let request = SaveRequest {
        owner_id: owner.to_string(),
        operation_key: format!(
            "c:{}",
            sha_hex(format!("{client_note_id}\n{request_text}").as_bytes())
        ),
        target: Target::Create {
            client_note_id: client_note_id.clone(),
            main_representation,
        },
        representations,
        metadata: Some(meta),
    };
    match note_saves::save(store, &request).await {
        Ok(result) => {
            if let Some((source_id, title, author_id, author_name)) = &study_source {
                sqlx::query(
                    "INSERT INTO note_study_sources(note_id,source_note_id,source_title,source_author_id,source_author_name,created_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(note_id) DO NOTHING",
                )
                .bind(&result.note_id)
                .bind(source_id)
                .bind(title)
                .bind(author_id)
                .bind(author_name)
                .bind(core::now())
                .execute(&store.db)
                .await?;
            }
            let study_source = study_source_json(store, &result.note_id).await?;
            Ok(Created {
                created: true,
                revision: result.revision,
                body: {
                    let mut out = save_body(&result);
                    out["client_note_id"] = json!(client_note_id);
                    out["created"] = json!(true);
                    out["study_source"] = study_source;
                    out
                },
            })
        }
        Err(SaveError::NoteExists { .. }) => existing(store, owner, &client_note_id)
            .await?
            .ok_or_else(|| NoteError::from(Error::unavailable())),
        Err(e) => Err(e.into()),
    }
}

/// Store the owner's public display name from the gateway header, updating only when
/// it changes.
pub async fn sync_display_name(store: &Store, owner: &str, name: &str) -> R<()> {
    sqlx::query(
        "INSERT INTO owners(id,display_name) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET display_name=EXCLUDED.display_name WHERE owners.display_name<>EXCLUDED.display_name",
    )
    .bind(owner)
    .bind(name)
    .execute(&store.db)
    .await?;
    Ok(())
}

/// Replace one representation completely: `{"representation", "schema_version"?, "document",
/// "transcription_run_id"?}`. A typed save that names a completed transcription run of the
/// caller accepts that run: the new Typed revision records the run's Original source
/// revision, and the save fails when that revision is no longer the current Original.
pub async fn replace(
    store: &Store,
    owner: &str,
    note_id: &str,
    if_match: i64,
    idempotency_key: Option<&str>,
    raw: &[u8],
) -> R<(Value, i64)> {
    let body = parse_body(raw)?;
    allow(
        &body,
        &[
            "representation",
            "schema_version",
            "document",
            "transcription_run_id",
        ],
        "",
    )?;
    let representation = representation_name(
        body.get("representation")
            .and_then(Value::as_str)
            .unwrap_or(""),
        "representation",
    )?;
    let document = body.get("document").ok_or_else(|| {
        NoteError::invalid("missing_field", "A required field is missing: document")
    })?;
    let operation_key = update_key(idempotency_key)?;
    let source_revision_id = match body.get("transcription_run_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(run_id)) if representation == Representation::Typed => {
            Some(transcriptions::accepted_source(store, owner, run_id, note_id).await?)
        }
        Some(Value::String(_)) => {
            return Err(NoteError::invalid(
                "source_on_original",
                "Only a typed revision has a transcription source",
            ));
        }
        Some(_) => return Err(wrong_type("transcription_run_id")),
    };
    let request = SaveRequest {
        owner_id: owner.to_string(),
        operation_key,
        target: Target::Update {
            note_id: note_id.to_string(),
            if_match,
        },
        representations: vec![RepresentationInput {
            representation,
            schema_version: schema_version(&body, "")?,
            content: Content::Document(document.clone()),
            source_revision_id,
        }],
        metadata: None,
    };
    let result = note_saves::save(store, &request).await?;
    Ok((save_body(&result), result.revision))
}

/// Apply validated changes: `{"original"?: editor operation, "metadata"?: {..}}`.
/// The editor operation uses the steps of `editor_rules` (item add, replace,
/// update for move or resize, delete, page settings and batch).
pub async fn patch(
    store: &Store,
    owner: &str,
    note_id: &str,
    if_match: i64,
    idempotency_key: Option<&str>,
    raw: &[u8],
) -> R<(Value, i64)> {
    let body = parse_body(raw)?;
    if body.contains_key("typed") {
        return Err(NoteError::invalid(
            "typed_operations_unsupported",
            "Replace the typed representation with PUT",
        ));
    }
    allow(&body, &["original", "metadata"], "")?;
    let representations = match body.get("original") {
        None => vec![],
        Some(op) => vec![RepresentationInput {
            representation: Representation::Original,
            schema_version: None,
            content: Content::Operation(op.clone()),
            source_revision_id: None,
        }],
    };
    let meta = match body.get("metadata") {
        None => None,
        Some(v) => Some(metadata(v)?),
    };
    let request = SaveRequest {
        owner_id: owner.to_string(),
        operation_key: update_key(idempotency_key)?,
        target: Target::Update {
            note_id: note_id.to_string(),
            if_match,
        },
        representations,
        metadata: meta,
    };
    let result = note_saves::save(store, &request).await?;
    Ok((save_body(&result), result.revision))
}

/// Delete the cloud working copy with its revisions, publication, memberships,
/// tag links and file references. Shared files stay: only this note's references
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterReportReferenceInput {
    pub report_item_id: String,
    pub relation_kind: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct CreatedReportPreview {
    pub report_item_id: String,
    pub reused_by_note_ids: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ReportDeletePreview {
    pub note_id: String,
    pub created_items: Vec<CreatedReportPreview>,
    pub linked_items: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct FailedReportDeletion {
    pub report_item_id: String,
    pub error: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct DeleteNoteOutcome {
    pub note_deleted: bool,
    pub deleted_report_ids: Vec<String>,
    pub failed_report_deletions: Vec<FailedReportDeletion>,
    pub skipped_linked_count: usize,
}

pub async fn register_report_reference(
    store: &Store,
    owner: &str,
    note_id: &str,
    input: RegisterReportReferenceInput,
) -> R<Value> {
    let item_id = input.report_item_id.trim();
    if item_id.is_empty()
        || item_id.len() > 128
        || item_id
            .chars()
            .any(|c| c.is_control() || c == '/' || c == '\\')
    {
        return Err(NoteError::invalid(
            "invalid_report_item_id",
            "report_item_id is invalid",
        ));
    }
    if !matches!(input.relation_kind.as_str(), "created_for_note" | "linked") {
        return Err(NoteError::invalid(
            "invalid_relation_kind",
            "relation_kind must be created_for_note or linked",
        ));
    }
    let exists = sqlx::query("SELECT 1 FROM notes WHERE note_id=$1 AND owner_id=$2")
        .bind(note_id)
        .bind(owner)
        .fetch_optional(&store.db)
        .await?;
    if exists.is_none() {
        return Err(NoteError::not_found());
    }

    if input.relation_kind == "created_for_note" {
        store
            .reports
            .verify_owned_item(owner, item_id)
            .await
            .map_err(|err| match err.0 {
                axum::http::StatusCode::FORBIDDEN => NoteError::new(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    "Caller does not own this Reports item",
                ),
                axum::http::StatusCode::NOT_FOUND => NoteError::invalid(
                    "report_item_not_found",
                    "Reports item was not found for this owner",
                ),
                _ => NoteError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "reports_unavailable",
                    "Reports service could not verify item ownership",
                ),
            })?;

        let other_creator = sqlx::query(
            "SELECT note_id FROM note_report_references WHERE report_item_id=$1 AND relation_kind='created_for_note' AND note_id<>$2 LIMIT 1",
        )
        .bind(item_id)
        .bind(note_id)
        .fetch_optional(&store.db)
        .await?;
        if other_creator.is_some() {
            return Err(NoteError::invalid(
                "already_owned_by_other_note",
                "This Reports item is already recorded as created for another note",
            ));
        }
    }

    let now = core::now();
    sqlx::query(
        "INSERT INTO note_report_references(note_id,owner_id,report_item_id,relation_kind,created_at,updated_at)
         VALUES($1,$2,$3,$4,$5,$5)
         ON CONFLICT(note_id,report_item_id) DO UPDATE SET
            relation_kind = CASE
                WHEN note_report_references.relation_kind='created_for_note' THEN 'created_for_note'
                ELSE excluded.relation_kind
            END,
            updated_at = excluded.updated_at",
    )
    .bind(note_id)
    .bind(owner)
    .bind(item_id)
    .bind(&input.relation_kind)
    .bind(now)
    .execute(&store.db)
    .await?;

    Ok(json!({
        "note_id": note_id,
        "report_item_id": item_id,
        "relation_kind": input.relation_kind,
    }))
}

pub async fn report_delete_preview(
    store: &Store,
    owner: &str,
    note_id: &str,
) -> R<ReportDeletePreview> {
    let exists = sqlx::query("SELECT 1 FROM notes WHERE note_id=$1 AND owner_id=$2")
        .bind(note_id)
        .bind(owner)
        .fetch_optional(&store.db)
        .await?;
    if exists.is_none() {
        return Err(NoteError::not_found());
    }
    let rows = sqlx::query(
        "SELECT report_item_id, relation_kind FROM note_report_references WHERE note_id=$1 AND owner_id=$2 ORDER BY created_at ASC",
    )
    .bind(note_id)
    .bind(owner)
    .fetch_all(&store.db)
    .await?;

    let mut created_items = Vec::new();
    let mut linked_items = Vec::new();
    for r in rows {
        let item_id: String = r.get("report_item_id");
        let kind: String = r.get("relation_kind");
        if kind == "created_for_note" {
            let other_notes: Vec<String> = sqlx::query(
                "SELECT DISTINCT note_id FROM note_report_references WHERE report_item_id=$1 AND note_id<>$2 ORDER BY note_id ASC",
            )
            .bind(&item_id)
            .bind(note_id)
            .fetch_all(&store.db)
            .await?
            .into_iter()
            .map(|row| row.get("note_id"))
            .collect();
            created_items.push(CreatedReportPreview {
                report_item_id: item_id,
                reused_by_note_ids: other_notes,
            });
        } else {
            linked_items.push(item_id);
        }
    }
    Ok(ReportDeletePreview {
        note_id: note_id.to_string(),
        created_items,
        linked_items,
    })
}

pub async fn run_report_cleanup(
    store: &Store,
    owner: &str,
    note_id: &str,
    skipped_linked_count: usize,
) -> R<DeleteNoteOutcome> {
    let rows = sqlx::query(
        "SELECT report_item_id FROM note_report_cleanup WHERE note_id=$1 AND owner_id=$2 AND status IN ('pending','failed') ORDER BY report_item_id ASC",
    )
    .bind(note_id)
    .bind(owner)
    .fetch_all(&store.db)
    .await?;

    let mut deleted_report_ids = Vec::new();
    let mut failed_report_deletions = Vec::new();
    let now = core::now();
    for row in rows {
        let item_id: String = row.get("report_item_id");
        match store.reports.delete_owned_item(owner, &item_id).await {
            Ok(()) => {
                sqlx::query(
                    "UPDATE note_report_cleanup SET status='deleted', last_error='', updated_at=$1 WHERE note_id=$2 AND report_item_id=$3",
                )
                .bind(now)
                .bind(note_id)
                .bind(&item_id)
                .execute(&store.db)
                .await?;
                deleted_report_ids.push(item_id);
            }
            Err(err) => {
                let code = match err.0 {
                    axum::http::StatusCode::FORBIDDEN => "forbidden",
                    _ => "reports_unavailable",
                }
                .to_string();
                sqlx::query(
                    "UPDATE note_report_cleanup SET status='failed', attempts=attempts+1, last_error=$1, updated_at=$2 WHERE note_id=$3 AND report_item_id=$4",
                )
                .bind(&code)
                .bind(now)
                .bind(note_id)
                .bind(&item_id)
                .execute(&store.db)
                .await?;
                failed_report_deletions.push(FailedReportDeletion {
                    report_item_id: item_id,
                    error: code,
                });
            }
        }
    }
    Ok(DeleteNoteOutcome {
        note_deleted: true,
        deleted_report_ids,
        failed_report_deletions,
        skipped_linked_count,
    })
}

/// Deletes an owned note. With `delete_owned_reports=true`, cascades deletion
/// strictly to Reports items authoritatively recorded as created for this note
/// and owned by the caller. Linked/pre-existing or cross-owner items are skipped.
pub async fn delete(
    store: &Store,
    owner: &str,
    note_id: &str,
    if_match: i64,
    delete_owned_reports: Option<&str>,
) -> R<(bool, DeleteNoteOutcome)> {
    let cascade = match delete_owned_reports {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => {
            return Err(NoteError::invalid(
                "invalid_delete_owned_reports",
                "delete_owned_reports must be true or false",
            ));
        }
    };
    let now = core::now();
    let mut tx = store.db.begin().await?;
    // Lock the note row at the expected revision; a save in flight waits or fails.
    let locked = sqlx::query(
        "UPDATE notes SET updated_at=updated_at WHERE note_id=$1 AND owner_id=$2 AND revision=$3",
    )
    .bind(note_id)
    .bind(owner)
    .bind(if_match)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if locked != 1 {
        let current = sqlx::query("SELECT revision FROM notes WHERE note_id=$1 AND owner_id=$2")
            .bind(note_id)
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?;
        return Err(match current {
            Some(r) => NoteError::conflict(r.get("revision")),
            None => NoteError::not_found(),
        });
    }
    let documents: Vec<String> =
        sqlx::query("SELECT document_id FROM note_revisions WHERE note_id=$1")
            .bind(note_id)
            .fetch_all(&mut *tx)
            .await?
            .iter()
            .map(|r| r.get("document_id"))
            .collect();

    let ref_rows = sqlx::query(
        "SELECT owner_id, report_item_id, relation_kind FROM note_report_references WHERE note_id=$1",
    )
    .bind(note_id)
    .fetch_all(&mut *tx)
    .await?;

    let mut skipped_linked_count = 0usize;
    if cascade {
        for r in &ref_rows {
            let ref_owner: String = r.get("owner_id");
            let item_id: String = r.get("report_item_id");
            let kind: String = r.get("relation_kind");
            if kind == "created_for_note" && ref_owner == owner {
                sqlx::query(
                    "INSERT INTO note_report_cleanup(note_id,owner_id,report_item_id,status,attempts,last_error,updated_at)
                     VALUES($1,$2,$3,'pending',0,'',$4)
                     ON CONFLICT(note_id,report_item_id) DO NOTHING",
                )
                .bind(note_id)
                .bind(owner)
                .bind(&item_id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            } else {
                skipped_linked_count += 1;
            }
        }
    }
    sqlx::query("DELETE FROM note_report_references WHERE note_id=$1")
        .bind(note_id)
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM file_references WHERE reference_kind='note_revision' AND reference_id IN (SELECT revision_id FROM note_revisions WHERE note_id=$1)")
        .bind(note_id).execute(&mut *tx).await?;
    // Finished save operations of this note: a retry must not replay a deleted note.
    sqlx::query("DELETE FROM pending_operations WHERE operation_id IN (SELECT operation_id FROM note_save_operations WHERE note_id=$1 AND result_revision IS NOT NULL) OR (state='failed' AND operation_id IN (SELECT operation_id FROM note_save_operations WHERE note_id=$1))")
        .bind(note_id).execute(&mut *tx).await?;
    // Pointers and revisions go first, in reference order: PostgreSQL checks the
    // NO ACTION keys of revisions before a cascade two levels down has run.
    for sql in [
        "DELETE FROM note_publications WHERE note_id=$1",
        "DELETE FROM note_current_revisions WHERE note_id=$1",
        "DELETE FROM note_revisions WHERE note_id=$1 AND source_revision_id IS NOT NULL",
        "DELETE FROM note_revisions WHERE note_id=$1",
    ] {
        sqlx::query(sql).bind(note_id).execute(&mut *tx).await?;
    }
    // Cascades remove memberships, tag links and the title.
    sqlx::query("DELETE FROM notes WHERE note_id=$1 AND owner_id=$2")
        .bind(note_id)
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    for document_id in &documents {
        sqlx::query("INSERT INTO note_revision_cleanup(document_id,attempts,created_at) VALUES($1,0,$2) ON CONFLICT(document_id) DO NOTHING")
            .bind(document_id).bind(now).execute(&mut *tx).await?;
    }
    sqlx::query("INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES($1,$2,'delete',$3)")
        .bind(owner).bind(note_id).bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    // The documents are queued durably; the task retries what fails here.
    if note_saves::cleanup_pruned(store, now, SAVE_SETTINGS.batch, SAVE_SETTINGS.max_attempts)
        .await
        .is_err()
    {
        tracing::warn!("Deleted note document cleanup is pending; it will retry");
    }
    if cascade {
        let outcome = run_report_cleanup(store, owner, note_id, skipped_linked_count).await?;
        Ok((true, outcome))
    } else {
        Ok((
            false,
            DeleteNoteOutcome {
                note_deleted: true,
                deleted_report_ids: Vec::new(),
                failed_report_deletions: Vec::new(),
                skipped_linked_count: 0,
            },
        ))
    }
}
