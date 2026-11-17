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
        publication,
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
    })
}

fn clean_text(value: &str, field: &str, max_chars: usize) -> R<String> {
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

const NOTE_COLUMNS: &str = "n.note_id,n.client_note_id,n.main_representation,n.revision,n.created_at,n.updated_at,COALESCE(t.title,'') AS title";

/// The summary of one note row of the owner: metadata, current revisions and
/// whether a publication exists. No document content.
async fn summary(store: &Store, owner: &str, row: &sqlx::any::AnyRow) -> R<Value> {
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
        "created_at": row.get::<i64, _>("created_at"),
        "updated_at": row.get::<i64, _>("updated_at"),
    }))
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

fn page_limit(raw: Option<&str>) -> R<i64> {
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
    let (after_time, after_id) = match &query.cursor {
        None => (None, String::new()),
        Some(c) => {
            let parsed = c
                .split_once('.')
                .and_then(|(t, id)| Some((t.parse::<i64>().ok()?, id)))
                .filter(|(_, id)| token(id));
            match parsed {
                Some((t, id)) => (Some(t), id.to_string()),
                None => {
                    return Err(NoteError::invalid(
                        "invalid_cursor",
                        "The cursor is not valid",
                    ));
                }
            }
        }
    };
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
        Ok(result) => Ok(Created {
            created: true,
            revision: result.revision,
            body: {
                let mut out = save_body(&result);
                out["client_note_id"] = json!(client_note_id);
                out["created"] = json!(true);
                out
            },
        }),
        Err(SaveError::NoteExists { .. }) => existing(store, owner, &client_note_id)
            .await?
            .ok_or_else(|| NoteError::from(Error::unavailable())),
        Err(e) => Err(e.into()),
    }
}

/// Replace one representation completely: `{"representation", "schema_version"?, "document"}`.
pub async fn replace(
    store: &Store,
    owner: &str,
    note_id: &str,
    if_match: i64,
    idempotency_key: Option<&str>,
    raw: &[u8],
) -> R<(Value, i64)> {
    let body = parse_body(raw)?;
    allow(&body, &["representation", "schema_version", "document"], "")?;
    let representation = representation_name(
        body.get("representation")
            .and_then(Value::as_str)
            .unwrap_or(""),
        "representation",
    )?;
    let document = body.get("document").ok_or_else(|| {
        NoteError::invalid("missing_field", "A required field is missing: document")
    })?;
    let request = SaveRequest {
        owner_id: owner.to_string(),
        operation_key: update_key(idempotency_key)?,
        target: Target::Update {
            note_id: note_id.to_string(),
            if_match,
        },
        representations: vec![RepresentationInput {
            representation,
            schema_version: schema_version(&body, "")?,
            content: Content::Document(document.clone()),
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
/// are released. A delete notice goes to the change feed.
pub async fn delete(
    store: &Store,
    owner: &str,
    note_id: &str,
    if_match: i64,
    delete_owned_reports: Option<&str>,
) -> R<()> {
    match delete_owned_reports {
        None | Some("false") => {}
        // Fixed until the Reports owner delete contract exists; never ignored.
        Some("true") => {
            return Err(NoteError::invalid(
                "reports_cascade_unavailable",
                "Deleting owned Reports items with a note is not available yet",
            ));
        }
        Some(_) => {
            return Err(NoteError::invalid(
                "invalid_delete_owned_reports",
                "delete_owned_reports must be true or false",
            ));
        }
    }
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
    Ok(())
}
