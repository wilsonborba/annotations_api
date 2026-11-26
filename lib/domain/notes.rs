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
    pub mode: Option<String>,
    pub project: Option<String>,
    pub item: Option<Value>,
    pub operation_key: Option<String>,
    pub report_item_id: Option<String>,
    pub relation_kind: Option<String>,
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
pub struct ReusedReportWarning {
    pub report_item_id: String,
    pub reused_by_note_ids: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct DeleteNoteOutcome {
    pub note_deleted: bool,
    pub deleted_report_ids: Vec<String>,
    pub failed_report_deletions: Vec<FailedReportDeletion>,
    pub reused_warnings: Vec<ReusedReportWarning>,
    pub skipped_linked_count: usize,
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        let code = db_err.code().unwrap_or_default();
        if code == "23505" || code == "2067" || code == "1555" {
            return true;
        }
        if db_err.message().contains("UNIQUE constraint failed") {
            return true;
        }
    }
    false
}

fn map_reports_err(err: crate::core::Error, fallback_msg: &str) -> NoteError {
    match err.0 {
        axum::http::StatusCode::FORBIDDEN => {
            NoteError::new(StatusCode::FORBIDDEN, "forbidden", err.1)
        }
        axum::http::StatusCode::NOT_FOUND => NoteError::invalid(
            "report_item_not_found",
            "Reports item or project was not found",
        ),
        axum::http::StatusCode::UNPROCESSABLE_ENTITY | axum::http::StatusCode::BAD_REQUEST => {
            NoteError::invalid("invalid_report_item", &err.1)
        }
        _ => NoteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "reports_unavailable",
            fallback_msg,
        ),
    }
}

async fn lock_report_note(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    owner: &str,
    note_id: &str,
) -> R<()> {
    // The same row admission lock used by note deletion, including SQLite fixtures.
    let locked =
        sqlx::query("UPDATE notes SET updated_at=updated_at WHERE note_id=$1 AND owner_id=$2")
            .bind(note_id)
            .bind(owner)
            .execute(&mut **tx)
            .await?
            .rows_affected();
    if locked != 1 {
        return Err(NoteError::not_found());
    }
    Ok(())
}

fn validate_report_receipt(receipt: &sqlx::any::AnyRow, project: &str, digest: &str) -> R<()> {
    if receipt.get::<String, _>("project") != project
        || receipt.get::<String, _>("request_digest") != digest
    {
        return Err(NoteError::new(
            StatusCode::CONFLICT,
            "operation_key_conflict",
            "operation_key was already used with a different project or item payload",
        ));
    }
    Ok(())
}

pub async fn register_report_reference(
    store: &Store,
    owner: &str,
    note_id: &str,
    input: RegisterReportReferenceInput,
) -> R<Value> {
    let exists = sqlx::query("SELECT 1 FROM notes WHERE note_id=$1 AND owner_id=$2")
        .bind(note_id)
        .bind(owner)
        .fetch_optional(&store.db)
        .await?;
    if exists.is_none() {
        return Err(NoteError::not_found());
    }

    let mode = input.mode.as_deref().unwrap_or("link").trim();
    if mode == "create" {
        let project = input.project.as_deref().unwrap_or("").trim();
        if project.is_empty() {
            return Err(NoteError::invalid(
                "invalid_project",
                "project is required when mode is create",
            ));
        }
        let item_payload = input.item.ok_or_else(|| {
            NoteError::invalid("invalid_report_item", "item payload is required for create")
        })?;
        let requested_op_key = input
            .operation_key
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && s.len() <= 128)
            .map(str::to_string);
        let request_digest = {
            use sha2::{Digest, Sha256};
            let canonical = serde_json::to_vec(&item_payload).unwrap_or_default();
            let mut hasher = Sha256::new();
            hasher.update(project.as_bytes());
            hasher.update(b"\0");
            hasher.update(&canonical);
            hasher
                .finalize()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        let minted_idem_key = core::id();
        let now = core::now();
        let mut admission = store.db.begin().await?;
        lock_report_note(&mut admission, owner, note_id).await?;
        // A keyless retry can recover its unknown outcome using the same
        // validated request, without guessing or exposing the remote key.
        let op_key = if let Some(key) = requested_op_key {
            key
        } else {
            sqlx::query(
                "SELECT operation_key FROM note_report_create_receipts WHERE note_id=$1 AND owner_id=$2 AND project=$3 AND request_digest=$4 AND status='pending' ORDER BY updated_at, operation_key LIMIT 1",
            )
            .bind(note_id)
            .bind(owner)
            .bind(project)
            .bind(&request_digest)
            .fetch_optional(&mut *admission)
            .await?
            .map(|row| row.get("operation_key"))
            .unwrap_or_else(core::id)
        };
        sqlx::query(
            "INSERT INTO note_report_create_receipts(note_id,owner_id,operation_key,idempotency_key,project,request_digest,report_item_id,status,updated_at)
             VALUES($1,$2,$3,$4,$5,$6,'','pending',$7)
             ON CONFLICT(note_id,owner_id,operation_key) DO NOTHING",
        )
        .bind(note_id)
        .bind(owner)
        .bind(&op_key)
        .bind(&minted_idem_key)
        .bind(project)
        .bind(&request_digest)
        .bind(now)
        .execute(&mut *admission)
        .await?;
        let receipt_query =
            "SELECT idempotency_key, project, request_digest, report_item_id, status FROM note_report_create_receipts
             WHERE note_id=$1 AND owner_id=$2 AND operation_key=$3";
        let receipt = sqlx::query(receipt_query)
            .bind(note_id)
            .bind(owner)
            .bind(&op_key)
            .fetch_one(&mut *admission)
            .await?;
        validate_report_receipt(&receipt, project, &request_digest)?;
        if receipt.get::<String, _>("status") == "rejected" {
            sqlx::query(
                "UPDATE note_report_create_receipts SET status='pending', updated_at=$1 WHERE note_id=$2 AND owner_id=$3 AND operation_key=$4",
            )
            .bind(now)
            .bind(note_id)
            .bind(owner)
            .bind(&op_key)
            .execute(&mut *admission)
            .await?;
        }
        // Commit the recovery key before any remote effect can occur.
        admission.commit().await?;

        // Serialize the bounded remote request and local registration against
        // note deletion. A lost reply leaves the committed pending receipt.
        let mut tx = store.db.begin().await?;
        lock_report_note(&mut tx, owner, note_id).await?;
        let receipt = sqlx::query(receipt_query)
            .bind(note_id)
            .bind(owner)
            .bind(&op_key)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                NoteError::new(
                    StatusCode::CONFLICT,
                    "operation_key_conflict",
                    "The creation receipt changed; retry the request",
                )
            })?;
        validate_report_receipt(&receipt, project, &request_digest)?;
        if receipt.get::<String, _>("status") == "rejected" {
            return Err(NoteError::invalid(
                "invalid_report_item",
                "Reports rejected this request; retry the matching operation",
            ));
        }
        let server_idem_key: String = receipt.get("idempotency_key");
        let recorded = receipt.get::<String, _>("status") == "recorded";
        let recorded_item_id: String = receipt.get("report_item_id");
        let result = if recorded {
            // Reports deletes its idempotency row with the item. Never POST a
            // recorded operation again and accidentally recreate a deleted item.
            store
                .reports
                .owned_item_metadata(owner, &recorded_item_id)
                .await
        } else {
            store
                .reports
                .create_owned_item(owner, project, &server_idem_key, item_payload)
                .await
        };
        let meta = match result {
            Ok(meta) => meta,
            Err(error) => {
                if !recorded
                    && matches!(
                        error.0,
                        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
                    )
                {
                    // Preserve this operation's binding even after a definite
                    // validation rejection. It does not block note deletion.
                    sqlx::query(
                        "UPDATE note_report_create_receipts SET status='rejected' WHERE note_id=$1 AND owner_id=$2 AND operation_key=$3 AND status='pending'",
                    )
                    .bind(note_id)
                    .bind(owner)
                    .bind(&op_key)
                    .execute(&mut *tx)
                    .await?;
                    tx.commit().await?;
                }
                return Err(map_reports_err(
                    error,
                    "Reports service could not create item",
                ));
            }
        };
        if meta.project != project || (recorded && meta.id != recorded_item_id) {
            return Err(map_reports_err(
                Error::unavailable(),
                "Reports returned metadata for a different project",
            ));
        }
        let res = sqlx::query(
            "INSERT INTO note_report_references(note_id,owner_id,report_item_id,relation_kind,created_at,updated_at)
             VALUES($1,$2,$3,'created_for_note',$4,$4)
             ON CONFLICT(note_id,report_item_id) DO UPDATE SET updated_at = excluded.updated_at",
        )
        .bind(note_id)
        .bind(owner)
        .bind(&meta.id)
        .bind(now)
        .execute(&mut *tx)
        .await;
        if let Err(err) = res {
            if is_unique_violation(&err) {
                return Err(NoteError::invalid(
                    "already_owned_by_other_note",
                    "This Reports item is already recorded as created for another note",
                ));
            }
            return Err(err.into());
        }
        sqlx::query(
            "UPDATE note_report_create_receipts SET report_item_id=$1, status='recorded', updated_at=$2
             WHERE note_id=$3 AND owner_id=$4 AND operation_key=$5",
        )
        .bind(&meta.id)
        .bind(now)
        .bind(note_id)
        .bind(owner)
        .bind(&op_key)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(json!({
            "note_id": note_id,
            "report_item_id": meta.id,
            "relation_kind": "created_for_note",
            "operation_key": op_key,
            "item": meta,
        }));
    }

    if mode != "link" {
        return Err(NoteError::invalid(
            "invalid_mode",
            "mode must be create or link",
        ));
    }

    let item_id = input.report_item_id.as_deref().unwrap_or("").trim();
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
    let relation_kind = input.relation_kind.as_deref().unwrap_or("linked").trim();
    if relation_kind == "created_for_note" {
        return Err(NoteError::invalid(
            "provenance_required",
            "created_for_note requires server-verified creation through Notes (mode=create)",
        ));
    }
    if relation_kind != "linked" {
        return Err(NoteError::invalid(
            "invalid_relation_kind",
            "relation_kind must be linked for existing report_item_id registration",
        ));
    }

    let now = core::now();
    let mut tx = store.db.begin().await?;
    sqlx::query(
        "INSERT INTO note_report_references(note_id,owner_id,report_item_id,relation_kind,created_at,updated_at)
         VALUES($1,$2,$3,'linked',$4,$4)
         ON CONFLICT(note_id,report_item_id) DO UPDATE SET
            updated_at = excluded.updated_at",
    )
    .bind(note_id)
    .bind(owner)
    .bind(item_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(json!({
        "note_id": note_id,
        "report_item_id": item_id,
        "relation_kind": "linked",
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
            // Restrict reused_by_note_ids strictly to notes owned by the caller so
            // another owner's note UUID is never disclosed.
            let other_notes: Vec<String> = sqlx::query(
                "SELECT DISTINCT note_id FROM note_report_references WHERE report_item_id=$1 AND note_id<>$2 AND owner_id=$3 ORDER BY note_id ASC",
            )
            .bind(&item_id)
            .bind(note_id)
            .bind(owner)
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
    // A cleanup retry is admitted only after this caller's durable deletion
    // record or owner-bound cleanup record exists. The note row is intentionally
    // gone by the time a failed cascade is retried.
    let admitted = sqlx::query(
        "SELECT 1 FROM note_changes WHERE note_id=$1 AND owner_id=$2 AND change_kind='delete'
         UNION ALL
         SELECT 1 FROM note_report_cleanup WHERE note_id=$1 AND owner_id=$2
         LIMIT 1",
    )
    .bind(note_id)
    .bind(owner)
    .fetch_optional(&store.db)
    .await?
    .is_some();
    if !admitted {
        return Err(NoteError::not_found());
    }
    run_report_cleanup_with_warnings(store, owner, note_id, Vec::new(), skipped_linked_count).await
}

async fn run_report_cleanup_with_warnings(
    store: &Store,
    owner: &str,
    note_id: &str,
    reused_warnings: Vec<ReusedReportWarning>,
    skipped_linked_count: usize,
) -> R<DeleteNoteOutcome> {
    let rows = sqlx::query(
        "SELECT report_item_id, confirm_reused FROM note_report_cleanup WHERE note_id=$1 AND owner_id=$2 AND status IN ('pending','failed') ORDER BY report_item_id ASC",
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
        let confirm_reused: i64 = row.get("confirm_reused");
        match store
            .reports
            .delete_owned_item(owner, &item_id, confirm_reused != 0)
            .await
        {
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
                    axum::http::StatusCode::CONFLICT => "reused_confirmation_required",
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
        reused_warnings,
        skipped_linked_count,
    })
}

/// Deletes an owned note. With `delete_owned_reports=true`, cascades deletion
/// strictly to unreused `created_for_note` Reports items owned by the caller.
/// Reused `created_for_note` items are skipped with explicit `reused_warnings`
/// unless explicitly confirmed in `confirm_reused_report_ids`.
pub async fn delete(
    store: &Store,
    owner: &str,
    note_id: &str,
    if_match: i64,
    delete_owned_reports: Option<&str>,
    confirm_reused_report_ids: &[String],
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
    // A cascade must reconcile unknown creations before snapshotting its
    // deletion set. The existing keep-Reports choice may still delete the note.
    if cascade && sqlx::query(
        "SELECT 1 FROM note_report_create_receipts WHERE note_id=$1 AND owner_id=$2 AND status='pending' LIMIT 1",
    )
    .bind(note_id)
    .bind(owner)
    .fetch_optional(&mut *tx)
    .await?
    .is_some()
    {
        return Err(NoteError::new(
            StatusCode::CONFLICT,
            "report_creation_pending",
            "Retry the pending Reports creation before deleting this note",
        ));
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
    let mut reused_warnings = Vec::new();
    if cascade {
        for r in &ref_rows {
            let ref_owner: String = r.get("owner_id");
            let item_id: String = r.get("report_item_id");
            let kind: String = r.get("relation_kind");
            if kind == "created_for_note" && ref_owner == owner {
                let reused_elsewhere = sqlx::query(
                    "SELECT 1 FROM note_report_references WHERE report_item_id=$1 AND note_id<>$2 LIMIT 1",
                )
                .bind(&item_id)
                .bind(note_id)
                .fetch_optional(&mut *tx)
                .await?
                .is_some();
                if reused_elsewhere && !confirm_reused_report_ids.iter().any(|id| id == &item_id) {
                    let caller_reused_notes: Vec<String> = sqlx::query(
                        "SELECT DISTINCT note_id FROM note_report_references WHERE report_item_id=$1 AND note_id<>$2 AND owner_id=$3 ORDER BY note_id ASC",
                    )
                    .bind(&item_id)
                    .bind(note_id)
                    .bind(owner)
                    .fetch_all(&mut *tx)
                    .await?
                    .into_iter()
                    .map(|row| row.get("note_id"))
                    .collect();
                    reused_warnings.push(ReusedReportWarning {
                        report_item_id: item_id,
                        reused_by_note_ids: caller_reused_notes,
                    });
                    continue;
                }
                let confirm_flag: i64 = if reused_elsewhere { 1 } else { 0 };
                sqlx::query(
                    "INSERT INTO note_report_cleanup(note_id,owner_id,report_item_id,status,confirm_reused,attempts,last_error,updated_at)
                     VALUES($1,$2,$3,'pending',$4,0,'',$5)
                     ON CONFLICT(note_id,report_item_id) DO NOTHING",
                )
                .bind(note_id)
                .bind(owner)
                .bind(&item_id)
                .bind(confirm_flag)
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
        let outcome = run_report_cleanup_with_warnings(
            store,
            owner,
            note_id,
            reused_warnings,
            skipped_linked_count,
        )
        .await?;
        Ok((true, outcome))
    } else {
        Ok((
            false,
            DeleteNoteOutcome {
                note_deleted: true,
                deleted_report_ids: Vec::new(),
                failed_report_deletions: Vec::new(),
                reused_warnings: Vec::new(),
                skipped_linked_count: 0,
            },
        ))
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCertificationSourceInput {
    pub kind: Option<String>,
    pub file_id: Option<String>,
    pub revision_id: Option<String>,
    pub selection_kind: Option<String>,
    pub selection_spec: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCertificationConfigInput {
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub count: Option<i64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCertificationInput {
    pub title: String,
    pub description: Option<String>,
    pub expected_revision: Option<i64>,
    pub sources: Vec<PreparedCertificationSourceInput>,
    #[serde(default)]
    pub config: Option<PreparedCertificationConfigInput>,
}

const MAX_SELECTION_SPEC_BYTES: usize = 2000;
const MAX_SELECTION_RANGES: usize = 100;
const MAX_SELECTED_PDF_PAGES: u64 = 100;

fn normalize_mime_base(mime_type: &str) -> String {
    mime_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn parse_ascii_u64(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u64>().ok()
}

fn canonicalize_selection(kind: &str, spec: &str, mime_type: &str) -> R<String> {
    if !matches!(kind, "all" | "pages" | "passages" | "intervals") {
        return Err(NoteError::invalid(
            "invalid_selection",
            "Selection kind must be all, pages, passages, or intervals",
        ));
    }
    let trimmed = spec.trim();
    if trimmed.len() > MAX_SELECTION_SPEC_BYTES {
        return Err(NoteError::invalid(
            "invalid_selection",
            "Selection spec exceeds 2000 bytes",
        ));
    }
    if kind == "all" {
        if !trimmed.is_empty() {
            return Err(NoteError::invalid(
                "invalid_selection",
                "Selection spec must be empty when kind is all",
            ));
        }
        return Ok(String::new());
    }
    if trimmed.is_empty() {
        return Err(NoteError::invalid(
            "invalid_selection",
            "Selection spec is required when kind is not all",
        ));
    }
    match kind {
        "pages" => {
            if normalize_mime_base(mime_type) != "application/pdf" {
                return Err(NoteError::invalid(
                    "invalid_selection",
                    "pages selection requires application/pdf source",
                ));
            }
            let parts: Vec<&str> = trimmed.split(',').collect();
            if parts.len() > MAX_SELECTION_RANGES {
                return Err(NoteError::invalid(
                    "invalid_selection",
                    "Too many page ranges",
                ));
            }
            let mut ranges: Vec<(u32, u32)> = Vec::with_capacity(parts.len());
            let mut total_pages: u64 = 0;
            for raw_part in parts {
                let part = raw_part.trim();
                if part.is_empty() {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Empty page range segment",
                    ));
                }
                let (s, e) = if let Some((a, b)) = part.split_once('-') {
                    let sv = parse_ascii_u64(a)
                        .and_then(|v| u32::try_from(v).ok())
                        .ok_or_else(|| {
                            NoteError::invalid("invalid_selection", "Invalid page start")
                        })?;
                    let ev = parse_ascii_u64(b)
                        .and_then(|v| u32::try_from(v).ok())
                        .ok_or_else(|| {
                            NoteError::invalid("invalid_selection", "Invalid page end")
                        })?;
                    (sv, ev)
                } else {
                    let v = parse_ascii_u64(part)
                        .and_then(|v| u32::try_from(v).ok())
                        .ok_or_else(|| {
                            NoteError::invalid("invalid_selection", "Invalid page number")
                        })?;
                    (v, v)
                };
                if s == 0 || e < s || e > 100_000 {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Page range must be 1-based with start <= end",
                    ));
                }
                if let Some(&(_, prev_end)) = ranges.last()
                    && s <= prev_end
                {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Page ranges must be strictly increasing and disjoint",
                    ));
                }
                total_pages = total_pages.saturating_add(u64::from(e - s + 1));
                if total_pages > MAX_SELECTED_PDF_PAGES {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Total selected PDF pages must not exceed 100",
                    ));
                }
                ranges.push((s, e));
            }
            Ok(ranges
                .into_iter()
                .map(|(s, e)| {
                    if s == e {
                        s.to_string()
                    } else {
                        format!("{s}-{e}")
                    }
                })
                .collect::<Vec<_>>()
                .join(","))
        }
        "passages" => {
            if !normalize_mime_base(mime_type).starts_with("text/") {
                return Err(NoteError::invalid(
                    "invalid_selection",
                    "passages selection requires text/* source",
                ));
            }
            let parts: Vec<&str> = trimmed.split(',').collect();
            if parts.len() > MAX_SELECTION_RANGES {
                return Err(NoteError::invalid(
                    "invalid_selection",
                    "Too many passage ranges",
                ));
            }
            let mut ranges: Vec<(u64, u64)> = Vec::with_capacity(parts.len());
            for raw_part in parts {
                let part = raw_part.trim();
                let rest = part.strip_prefix("p1:").ok_or_else(|| {
                    NoteError::invalid("invalid_selection", "Passage range must use p1:start-end")
                })?;
                let (a, b) = rest.split_once('-').ok_or_else(|| {
                    NoteError::invalid("invalid_selection", "Passage range must use p1:start-end")
                })?;
                let s = parse_ascii_u64(a).ok_or_else(|| {
                    NoteError::invalid("invalid_selection", "Invalid passage start offset")
                })?;
                let e = parse_ascii_u64(b).ok_or_else(|| {
                    NoteError::invalid("invalid_selection", "Invalid passage end offset")
                })?;
                if e <= s {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Passage range must be half-open with end > start",
                    ));
                }
                if let Some(&(_, prev_end)) = ranges.last()
                    && s < prev_end
                {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Passage ranges must be sorted and non-overlapping",
                    ));
                }
                ranges.push((s, e));
            }
            Ok(ranges
                .into_iter()
                .map(|(s, e)| format!("p1:{s}-{e}"))
                .collect::<Vec<_>>()
                .join(","))
        }
        "intervals" => {
            if !normalize_mime_base(mime_type).starts_with("audio/") {
                return Err(NoteError::invalid(
                    "invalid_selection",
                    "intervals selection requires an audio source",
                ));
            }
            let body = trimmed.strip_prefix("ms:").ok_or_else(|| {
                NoteError::invalid("invalid_selection", "Intervals spec must start with ms:")
            })?;
            if body.is_empty() {
                return Err(NoteError::invalid(
                    "invalid_selection",
                    "Intervals spec requires at least one start-end range",
                ));
            }
            let parts: Vec<&str> = body.split(',').collect();
            if parts.len() > MAX_SELECTION_RANGES {
                return Err(NoteError::invalid(
                    "invalid_selection",
                    "Too many audio intervals",
                ));
            }
            let mut ranges: Vec<(u64, u64)> = Vec::with_capacity(parts.len());
            for raw_part in parts {
                let part = raw_part.trim();
                let (a, b) = part.split_once('-').ok_or_else(|| {
                    NoteError::invalid("invalid_selection", "Interval range must use start-end")
                })?;
                let s = parse_ascii_u64(a).ok_or_else(|| {
                    NoteError::invalid("invalid_selection", "Invalid interval start")
                })?;
                let e = parse_ascii_u64(b).ok_or_else(|| {
                    NoteError::invalid("invalid_selection", "Invalid interval end")
                })?;
                if e <= s {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Interval range must have end > start",
                    ));
                }
                if let Some(&(_, prev_end)) = ranges.last()
                    && s < prev_end
                {
                    return Err(NoteError::invalid(
                        "invalid_selection",
                        "Interval ranges must be sorted and non-overlapping",
                    ));
                }
                ranges.push((s, e));
            }
            let formatted = ranges
                .into_iter()
                .map(|(s, e)| format!("{s}-{e}"))
                .collect::<Vec<_>>()
                .join(",");
            Ok(format!("ms:{formatted}"))
        }
        _ => unreachable!(),
    }
}

fn collect_original_text(items: &[Value], out: &mut String) {
    for item in items {
        match item.get("kind").and_then(Value::as_str) {
            Some("text") => {
                let Some(spans) = item.get("spans").and_then(Value::as_array) else {
                    continue;
                };
                let mut line = String::new();
                for span in spans {
                    if let Some(text) = span.get("text").and_then(Value::as_str) {
                        line.push_str(text);
                    }
                }
                if !line.trim().is_empty() {
                    out.push_str(&line);
                    out.push('\n');
                }
            }
            Some("group") => {
                if let Some(children) = item.get("items").and_then(Value::as_array) {
                    collect_original_text(children, out);
                }
            }
            _ => {}
        }
    }
}

fn original_text(document: &Value) -> String {
    let mut out = String::new();
    if let Some(items) = document.get("items").and_then(Value::as_array) {
        collect_original_text(items, &mut out);
    }
    out
}

fn normalize_plain_text(raw: &str) -> String {
    let without_bom = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    without_bom
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\0', "")
}

enum PendingPreparedSource {
    Ready(Value),
    UploadNoteText {
        filename: String,
        bytes: Vec<u8>,
        sha256: String,
        selection_kind: String,
        selection_spec: String,
        external_ref: String,
    },
}

/// Server-to-server handoff from Notes to Certifications `POST /studies/prepared`.
/// Resolves owned ready `files` rows or materializes `note_original`/`note_typed`
/// text revisions as text-only source snapshots with exact saved revision provenance
/// (not entire note preservation). Non-text content (such as ink strokes, images, and cards)
/// is excluded from this text snapshot per H3 explicit eligible subset semantics.
/// Empty materializations are rejected with 422 `empty_note_source` before effects.
/// Validates expected_revision, checks selection grammar, and forwards `x-uuid` +
/// `CERTIFICATIONS_SERVICE_KEY` server-side.
pub async fn create_prepared_certification_study(
    store: &Store,
    owner: &str,
    note_id: &str,
    input: PreparedCertificationInput,
) -> R<Value> {
    let title = input.title.trim();
    if title.is_empty() || title.chars().count() > 200 {
        return Err(NoteError::invalid(
            "invalid_title",
            "title must be 1 to 200 characters",
        ));
    }
    if input.sources.is_empty() || input.sources.len() > 20 {
        return Err(NoteError::invalid(
            "invalid_sources",
            "Prepared study requires 1 to 20 sources",
        ));
    }
    let note_row = sqlx::query("SELECT revision FROM notes WHERE note_id=$1 AND owner_id=$2")
        .bind(note_id)
        .bind(owner)
        .fetch_optional(&store.db)
        .await?;
    let Some(note_row) = note_row else {
        return Err(NoteError::not_found());
    };
    let current_revision: i64 = note_row.get("revision");
    if let Some(expected_rev) = input.expected_revision
        && expected_rev != current_revision
    {
        return Err(NoteError {
            status: StatusCode::CONFLICT,
            code: "revision_conflict".into(),
            message: "Note revision changed; reload before creating study".into(),
            current_revision: Some(current_revision),
        });
    }

    let validated_config = if let Some(ref cfg) = input.config {
        let lang = match cfg.language.as_deref().map(str::trim) {
            Some("en") | None | Some("") => "en".to_string(),
            Some("pt-BR") => "pt-BR".to_string(),
            Some("th") => "th".to_string(),
            Some(other) => {
                return Err(NoteError::invalid(
                    "invalid_config",
                    format!("Unsupported language '{other}'. Supported languages: en, pt-BR, th"),
                ));
            }
        };
        let count = match cfg.count {
            Some(c) if (1..=20).contains(&c) => c,
            None => 10,
            Some(c) => {
                return Err(NoteError::invalid(
                    "invalid_config",
                    format!("Invalid question count {c}. Count must be between 1 and 20"),
                ));
            }
        };
        Some(json!({
            "language": lang,
            "count": count,
        }))
    } else {
        None
    };

    let mut pending_sources = Vec::with_capacity(input.sources.len());
    for s in &input.sources {
        let kind = match s.kind.as_deref().map(str::trim) {
            None if s.file_id.is_some() => "file",
            Some(k) => k,
            None => {
                return Err(NoteError::invalid(
                    "invalid_source",
                    "Source kind is required",
                ));
            }
        };
        let sel_kind = s
            .selection_kind
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("all");
        let sel_spec_raw = s.selection_spec.as_deref().unwrap_or("");

        match kind {
            "file" => {
                if s.revision_id.is_some() {
                    return Err(NoteError::invalid(
                        "invalid_source",
                        "revision_id is only allowed for note_original and note_typed sources",
                    ));
                }
                let fid = s.file_id.as_deref().map(str::trim).unwrap_or("");
                if fid.is_empty() {
                    return Err(NoteError::invalid("invalid_source", "file_id is required"));
                }
                let row = sqlx::query(
                    "SELECT filename, content_type, size_bytes, sha256, status, fsm_reference FROM files WHERE file_id=$1 AND owner_id=$2",
                )
                .bind(fid)
                .bind(owner)
                .fetch_optional(&store.db)
                .await?;
                let Some(r) = row else {
                    return Err(NoteError::invalid(
                        "file_not_found",
                        "Referenced file does not exist for this owner",
                    ));
                };
                let status: String = r.get("status");
                let fsm_ref: Option<String> = r.get("fsm_reference");
                let Some(fsm_ref) = fsm_ref.filter(|v| !v.trim().is_empty()) else {
                    return Err(NoteError::invalid(
                        "file_not_ready",
                        "Referenced file is not ready for Certifications handoff",
                    ));
                };
                if status != "ready" {
                    return Err(NoteError::invalid(
                        "file_not_ready",
                        "Referenced file is not ready for Certifications handoff",
                    ));
                }
                let filename: String = r.get("filename");
                let mime_type: String = r.get("content_type");
                let size_bytes: i64 = r.get("size_bytes");
                let sha256: String = r.get("sha256");
                let sel_spec = canonicalize_selection(sel_kind, sel_spec_raw, &mime_type)?;
                let file_ref = format!("annotations:file:{fid}");
                pending_sources.push(PendingPreparedSource::Ready(json!({
                    "filename": filename,
                    "mime_type": mime_type,
                    "size_bytes": size_bytes,
                    "sha256": sha256,
                    "fsm_ref": fsm_ref,
                    "selection_kind": sel_kind,
                    "selection_spec": sel_spec,
                    "external_ref": file_ref,
                })));
            }
            "note_original" | "note_typed" => {
                if s.file_id.is_some() {
                    return Err(NoteError::invalid(
                        "invalid_source",
                        "file_id is not allowed for note_original or note_typed sources",
                    ));
                }
                let representation = if kind == "note_original" {
                    "original"
                } else {
                    "typed"
                };
                let sel_spec = canonicalize_selection(sel_kind, sel_spec_raw, "text/plain")?;
                let rev_row = match s.revision_id.as_deref().map(str::trim) {
                    Some("") => {
                        return Err(NoteError::invalid(
                            "invalid_revision",
                            "revision_id must not be empty",
                        ));
                    }
                    Some(rev_id) => sqlx::query(
                        "SELECT revision_id, document_id FROM note_revisions WHERE revision_id=$1 AND note_id=$2 AND representation=$3",
                    )
                    .bind(rev_id)
                    .bind(note_id)
                    .bind(representation)
                    .fetch_optional(&store.db)
                    .await?,
                    None => sqlx::query(
                        "SELECT r.revision_id, r.document_id FROM note_current_revisions c JOIN note_revisions r ON r.revision_id=c.revision_id WHERE c.note_id=$1 AND c.representation=$2",
                    )
                    .bind(note_id)
                    .bind(representation)
                    .fetch_optional(&store.db)
                    .await?,
                };
                let Some(rev_row) = rev_row else {
                    return Err(NoteError::invalid(
                        "invalid_revision",
                        "Requested note representation revision does not exist for this note",
                    ));
                };
                let revision_id: String = rev_row.get("revision_id");
                let document_id: String = rev_row.get("document_id");
                let stored = store
                    .docs
                    .get(&document_id)
                    .await
                    .map_err(NoteError::from)?;
                let extracted = if representation == "typed" {
                    crate::domain::discovery::typed_text(&stored["body"])
                } else {
                    original_text(&stored["body"])
                };
                let normalized = normalize_plain_text(&extracted);
                if normalized.trim().is_empty() {
                    return Err(NoteError::invalid(
                        "empty_note_source",
                        "Note representation has no extractable text for Certification",
                    ));
                }
                let bytes = normalized.into_bytes();
                let sha256 = format!("{:x}", Sha256::digest(&bytes));
                let short_rev: String = revision_id.chars().take(12).collect();
                let filename = format!("note-{representation}-{short_rev}.txt");
                let note_ref = format!("annotations:note:{note_id}:{representation}:{revision_id}");

                let existing = sqlx::query(
                    "SELECT filename, content_type, size_bytes, sha256, fsm_reference FROM files WHERE owner_id=$1 AND sha256=$2 AND content_type='text/plain' AND status='ready' AND fsm_reference IS NOT NULL AND fsm_reference <> '' ORDER BY created_at DESC LIMIT 1",
                )
                .bind(owner)
                .bind(&sha256)
                .fetch_optional(&store.db)
                .await?;
                if let Some(r) = existing {
                    pending_sources.push(PendingPreparedSource::Ready(json!({
                        "filename": r.get::<String, _>("filename"),
                        "mime_type": r.get::<String, _>("content_type"),
                        "size_bytes": r.get::<i64, _>("size_bytes"),
                        "sha256": r.get::<String, _>("sha256"),
                        "fsm_ref": r.get::<String, _>("fsm_reference"),
                        "selection_kind": sel_kind,
                        "selection_spec": sel_spec,
                        "external_ref": note_ref,
                    })));
                } else {
                    pending_sources.push(PendingPreparedSource::UploadNoteText {
                        filename,
                        bytes,
                        sha256,
                        selection_kind: sel_kind.to_string(),
                        selection_spec: sel_spec,
                        external_ref: note_ref,
                    });
                }
            }
            _ => {
                return Err(NoteError::invalid(
                    "invalid_source",
                    "Source kind must be file, note_original, or note_typed",
                ));
            }
        }
    }

    let mut prepared_sources = Vec::with_capacity(pending_sources.len());
    let mut uploaded_by_sha: std::collections::HashMap<String, (String, String, i64, String)> =
        std::collections::HashMap::new();
    for item in pending_sources {
        match item {
            PendingPreparedSource::Ready(v) => prepared_sources.push(v),
            PendingPreparedSource::UploadNoteText {
                filename,
                bytes,
                sha256,
                selection_kind,
                selection_spec,
                external_ref,
            } => {
                let (stored_filename, mime_type, size_bytes, fsm_ref) = if let Some(cached) =
                    uploaded_by_sha.get(&sha256)
                {
                    cached.clone()
                } else {
                    let size_bytes = bytes.len() as i64;
                    let temp = store.files.staging_file().map_err(NoteError::from)?;
                    tokio::fs::write(&temp, &bytes)
                        .await
                        .map_err(|_| NoteError::from(Error::unavailable()))?;
                    let file_id = core::id();
                    let album = format!("notes-cert-{file_id}");
                    let uploaded = store
                        .files
                        .upload_path(&temp, &filename, "text/plain", &album, size_bytes)
                        .await
                        .map_err(NoteError::from)?;
                    let now = core::now();
                    if let Err(err) = sqlx::query(
                            "INSERT INTO files(file_id,owner_id,filename,content_type,size_bytes,sha256,purpose,status,preview,created_at,updated_at,fsm_reference) VALUES($1,$2,$3,'text/plain',$4,$5,'content_job','ready',0,$6,$6,$7)",
                        )
                        .bind(&file_id)
                        .bind(owner)
                        .bind(&filename)
                        .bind(size_bytes)
                        .bind(&sha256)
                        .bind(now)
                        .bind(&uploaded.key)
                        .execute(&store.db)
                        .await
                        {
                            let _ = sqlx::query(
                                "INSERT INTO file_cleanup(fsm_reference,attempts,created_at) VALUES($1,0,$2) ON CONFLICT(fsm_reference) DO NOTHING",
                            )
                            .bind(&uploaded.key)
                            .bind(now)
                            .execute(&store.db)
                            .await;
                            return Err(err.into());
                        }
                    let entry = (filename, "text/plain".to_string(), size_bytes, uploaded.key);
                    uploaded_by_sha.insert(sha256.clone(), entry.clone());
                    entry
                };
                prepared_sources.push(json!({
                    "filename": stored_filename,
                    "mime_type": mime_type,
                    "size_bytes": size_bytes,
                    "sha256": sha256,
                    "fsm_ref": fsm_ref,
                    "selection_kind": selection_kind,
                    "selection_spec": selection_spec,
                    "external_ref": external_ref,
                }));
            }
        }
    }

    let mut payload = json!({
        "title": title,
        "description": input.description.as_deref().unwrap_or(""),
        "sources": prepared_sources,
    });
    if let Some(cfg) = validated_config {
        payload["config"] = cfg;
    }
    store
        .certifications
        .create_prepared_study(owner, payload)
        .await
        .map_err(|err| match err.0 {
            axum::http::StatusCode::FORBIDDEN => {
                NoteError::new(StatusCode::FORBIDDEN, "forbidden", err.1)
            }
            axum::http::StatusCode::UNPROCESSABLE_ENTITY | axum::http::StatusCode::BAD_REQUEST => {
                NoteError::invalid("invalid_prepared_study", &err.1)
            }
            _ => NoteError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "certifications_unavailable",
                "Certifications service is unavailable",
            ),
        })
}
