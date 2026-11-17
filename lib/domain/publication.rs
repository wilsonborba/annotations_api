//! Publication of a note (`/me/notes/{note_id}/publication`): a fixed public
//! snapshot of saved revisions that the owner chose, separate from private saving.
//!
//! - The owner publishes Original, Typed or both. Each published representation
//!   points at one saved revision of that representation. A working-copy save
//!   never moves these pointers, so readers see only the last published content.
//! - The publication has its own revision (If-Match). It starts at 0 (never
//!   published) and grows with every publish and unpublish, so a stale client
//!   never replaces a newer publication.
//! - Unpublish removes the published pointers and the published title. The working
//!   copy, its revisions and the sync feed stay.
//! - Every public read goes through `public_note`, which applies `public_document`
//!   to every document. Publication never changes Reports, task or quiz visibility:
//!   embedded cards keep only their reference and a restricted marker.
use crate::{
    core,
    dal::Store,
    domain::{
        note_saves::Representation,
        notes::{self, NoteError},
    },
};
use axum::http::StatusCode;
use serde_json::{Map, Value, json};
use sqlx::Row;

type R<T> = std::result::Result<T, NoteError>;

// Public projection -------------------------------------------------------------

/// Container keys of an embedded card: id, kind and the paper geometry (absent in
/// Typed blocks).
const CARD_KEYS: [&str; 6] = ["id", "kind", "x", "y", "width", "height"];

/// The reference fields that a public card keeps, per embedded card kind. Every
/// other field (mode, source, snapshot id, capture time and any later field) is
/// dropped. `None` for kinds that the projection keeps as saved.
fn card_references(kind: &str) -> Option<&'static [&'static str]> {
    match kind {
        "reports_embed" => Some(&["report_id"]),
        "task_card" => Some(&["task_id"]),
        "quiz_card" => Some(&["study_id", "quiz_id"]),
        _ => None,
    }
}

/// The public form of one saved document (Original `items` or Typed `blocks`).
///
/// Reports embeds, task cards and quiz cards keep only their id, kind, geometry,
/// source reference and `"restricted": true`. The reader then loads the source
/// through its owning service, which checks the viewer's access, and shows the
/// restricted placeholder otherwise. No private value, thumbnail, snapshot or
/// private title of a source ever comes from the note.
pub fn public_document(document: &Value) -> Value {
    let mut out = document.clone();
    for key in ["items", "blocks"] {
        if let Some(Value::Array(list)) = out.get_mut(key) {
            for item in list.iter_mut() {
                *item = public_item(item);
            }
        }
    }
    out
}

fn public_item(item: &Value) -> Value {
    let Some(map) = item.as_object() else {
        return item.clone();
    };
    let Some(references) = map
        .get("kind")
        .and_then(Value::as_str)
        .and_then(card_references)
    else {
        return item.clone();
    };
    let mut out = Map::new();
    for key in CARD_KEYS.iter().chain(references) {
        if let Some(value) = map.get(*key) {
            out.insert((*key).to_string(), value.clone());
        }
    }
    out.insert("restricted".into(), Value::Bool(true));
    Value::Object(out)
}

// Stored state ------------------------------------------------------------------

/// One published representation.
struct Published {
    representation: Representation,
    revision_id: String,
    document_id: String,
    schema_version: i64,
    created_at: i64,
    source_revision_id: Option<String>,
    source_created_at: Option<i64>,
}

/// The publication row and its published representations.
struct State {
    revision: i64,
    language: String,
    updated_at: i64,
    title: Option<String>,
    items: Vec<Published>,
}
impl State {
    fn published(&self) -> bool {
        !self.items.is_empty()
    }
    fn get(&self, representation: Representation) -> Option<&Published> {
        self.items
            .iter()
            .find(|p| p.representation == representation)
    }
    /// True only when the published Typed comes from an Original revision older
    /// than the published Original. Unpublished edits never count: only published
    /// revisions and the saved source of the published Typed are compared. When
    /// the two times are equal the order is unknown and no notice is shown.
    fn typed_uses_earlier_revision(&self) -> bool {
        let (Some(original), Some(typed)) = (
            self.get(Representation::Original),
            self.get(Representation::Typed),
        ) else {
            return false;
        };
        match (&typed.source_revision_id, typed.source_created_at) {
            (Some(source), Some(at)) => *source != original.revision_id && at < original.created_at,
            _ => false,
        }
    }
}

async fn state<'e, E>(db: E, note_id: &str) -> R<Option<State>>
where
    E: sqlx::Executor<'e, Database = sqlx::Any> + Copy,
{
    let Some(row) = sqlx::query(
        "SELECT p.revision,p.language_code,p.updated_at,COALESCE(t.title,'') AS title FROM note_publications p LEFT JOIN note_publication_titles t ON t.note_id=p.note_id WHERE p.note_id=$1",
    )
    .bind(note_id)
    .fetch_optional(db)
    .await?
    else {
        return Ok(None);
    };
    let mut items = vec![];
    for r in sqlx::query(
        "SELECT p.representation,p.revision_id,r.document_id,r.schema_version,r.created_at,r.source_revision_id,s.created_at AS source_created_at \
         FROM note_published_representations p JOIN note_revisions r ON r.revision_id=p.revision_id \
         LEFT JOIN note_revisions s ON s.revision_id=r.source_revision_id \
         WHERE p.note_id=$1 ORDER BY p.representation",
    )
    .bind(note_id)
    .fetch_all(db)
    .await?
    {
        let representation = Representation::parse(&r.get::<String, _>("representation"))
            .ok_or_else(|| NoteError::from(core::Error::unavailable()))?;
        items.push(Published {
            representation,
            revision_id: r.get("revision_id"),
            document_id: r.get("document_id"),
            schema_version: r.get("schema_version"),
            created_at: r.get("created_at"),
            source_revision_id: r.try_get("source_revision_id")?,
            source_created_at: r.try_get("source_created_at")?,
        });
    }
    let title: String = row.get("title");
    Ok(Some(State {
        revision: row.get("revision"),
        language: row.get("language_code"),
        updated_at: row.get("updated_at"),
        title: (!title.is_empty()).then_some(title),
        items,
    }))
}

/// The owner view of a publication. Never published: revision 0 and nothing
/// published.
fn owner_json(note_id: &str, state: Option<&State>) -> Value {
    let Some(s) = state else {
        return json!({
            "note_id": note_id,
            "published": false,
            "revision": 0,
            "language": Value::Null,
            "title": Value::Null,
            "representations": [],
            "typed_uses_earlier_revision": false,
            "updated_at": Value::Null,
        });
    };
    let published = s.published();
    json!({
        "note_id": note_id,
        "published": published,
        "revision": s.revision,
        "language": if published { json!(s.language) } else { Value::Null },
        "title": s.title,
        "representations": s.items.iter().map(|p| json!({
            "representation": p.representation.as_str(),
            "revision_id": p.revision_id,
            "schema_version": p.schema_version,
        })).collect::<Vec<_>>(),
        "typed_uses_earlier_revision": s.typed_uses_earlier_revision(),
        "updated_at": s.updated_at,
    })
}

/// The publication block of the owner note detail: `null` while nothing is published.
pub(crate) async fn owner_block(store: &Store, note_id: &str) -> R<Value> {
    Ok(match state(&store.db, note_id).await? {
        Some(s) if s.published() => owner_json(note_id, Some(&s)),
        _ => Value::Null,
    })
}

/// Whether the note has a published representation.
pub(crate) async fn is_published(store: &Store, note_id: &str) -> R<bool> {
    Ok(
        sqlx::query("SELECT note_id FROM note_published_representations WHERE note_id=$1 LIMIT 1")
            .bind(note_id)
            .fetch_optional(&store.db)
            .await?
            .is_some(),
    )
}

async fn owns(store: &Store, owner: &str, note_id: &str) -> R<()> {
    sqlx::query("SELECT note_id FROM notes WHERE note_id=$1 AND owner_id=$2")
        .bind(note_id)
        .bind(owner)
        .fetch_optional(&store.db)
        .await?
        .map(|_| ())
        .ok_or_else(NoteError::not_found)
}

fn conflict(current_revision: i64) -> NoteError {
    NoteError {
        current_revision: Some(current_revision),
        ..NoteError::new(
            StatusCode::CONFLICT,
            "revision_conflict",
            "The publication changed since the given revision",
        )
    }
}

fn not_published() -> NoteError {
    NoteError::new(
        StatusCode::NOT_FOUND,
        "not_published",
        "The note has no published representation",
    )
}

// Owner routes ------------------------------------------------------------------

/// `GET /me/notes/{note_id}/publication`: the published representations and their
/// saved revisions. Returns the body and the publication revision.
pub async fn get(store: &Store, owner: &str, note_id: &str) -> R<(Value, i64)> {
    owns(store, owner, note_id).await?;
    let s = state(&store.db, note_id).await?;
    let revision = s.as_ref().map_or(0, |s| s.revision);
    Ok((owner_json(note_id, s.as_ref()), revision))
}

/// `PUT /me/notes/{note_id}/publication`:
/// `{"language": "en", "representations": {"original"?: revision_id, "typed"?: revision_id}}`.
///
/// Replaces the published set with exactly the named representations. Each id must
/// be a saved revision of this note and representation. `if_match` is the current
/// publication revision (0 when the note was never published).
pub async fn put(
    store: &Store,
    owner: &str,
    note_id: &str,
    if_match: i64,
    raw: &[u8],
) -> R<(Value, i64)> {
    let body = notes::parse_body(raw)?;
    notes::allow(&body, &["language", "representations"], "")?;
    let language = match body.get("language") {
        None => {
            return Err(NoteError::invalid(
                "missing_field",
                "A required field is missing: language",
            ));
        }
        Some(Value::String(s)) if notes::token(s) => s.clone(),
        Some(_) => return Err(notes::wrong_type("language")),
    };
    let reps = body
        .get("representations")
        .and_then(Value::as_object)
        .ok_or_else(|| notes::wrong_type("representations"))?;
    notes::allow(reps, &["original", "typed"], "representations.")?;
    let mut chosen = vec![];
    for (name, value) in reps {
        let representation = notes::representation_name(name, "representations")?;
        let revision_id = value
            .as_str()
            .filter(|s| notes::token(s))
            .ok_or_else(|| notes::wrong_type(&format!("representations.{name}")))?;
        chosen.push((representation, revision_id.to_string()));
    }
    if chosen.is_empty() {
        return Err(NoteError::invalid(
            "representation_required",
            "Publish at least one representation; use DELETE to unpublish",
        ));
    }
    chosen.sort();

    let now = core::now();
    let mut tx = store.db.begin().await?;
    // Lock the owner's note row: a concurrent note delete waits or this fails.
    let locked =
        sqlx::query("UPDATE notes SET updated_at=updated_at WHERE note_id=$1 AND owner_id=$2")
            .bind(note_id)
            .bind(owner)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    if locked != 1 {
        return Err(NoteError::not_found());
    }
    if sqlx::query("SELECT code FROM languages WHERE code=$1")
        .bind(&language)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        return Err(NoteError::invalid(
            "unknown_language",
            "The language is not available",
        ));
    }
    let current: Option<i64> =
        sqlx::query("SELECT revision FROM note_publications WHERE note_id=$1")
            .bind(note_id)
            .fetch_optional(&mut *tx)
            .await?
            .map(|r| r.get("revision"));
    let changed = match current {
        None if if_match == 0 => sqlx::query(
            "INSERT INTO note_publications(note_id,language_code,revision,updated_at) VALUES($1,$2,1,$3) ON CONFLICT DO NOTHING",
        )
        .bind(note_id)
        .bind(&language)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected(),
        Some(revision) if revision == if_match => sqlx::query(
            "UPDATE note_publications SET revision=revision+1,language_code=$1,updated_at=$2 WHERE note_id=$3 AND revision=$4",
        )
        .bind(&language)
        .bind(now)
        .bind(note_id)
        .bind(if_match)
        .execute(&mut *tx)
        .await?
        .rows_affected(),
        _ => 0,
    };
    if changed != 1 {
        let current: i64 = sqlx::query("SELECT revision FROM note_publications WHERE note_id=$1")
            .bind(note_id)
            .fetch_optional(&mut *tx)
            .await?
            .map_or(0, |r| r.get("revision"));
        return Err(conflict(current));
    }
    sqlx::query("DELETE FROM note_published_representations WHERE note_id=$1")
        .bind(note_id)
        .execute(&mut *tx)
        .await?;
    for (representation, revision_id) in &chosen {
        // Only a saved revision of this note and representation can be published.
        let added = sqlx::query(
            "INSERT INTO note_published_representations(note_id,representation,revision_id) SELECT note_id,representation,revision_id FROM note_revisions WHERE revision_id=$1 AND note_id=$2 AND representation=$3",
        )
        .bind(revision_id)
        .bind(note_id)
        .bind(representation.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if added != 1 {
            return Err(NoteError::invalid(
                "unknown_revision",
                format!(
                    "The revision is not a saved revision of this note: representations.{}",
                    representation.as_str()
                ),
            ));
        }
    }
    // The title at publication time; later working-copy titles stay private.
    sqlx::query("DELETE FROM note_publication_titles WHERE note_id=$1")
        .bind(note_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO note_publication_titles(note_id,title) SELECT note_id,title FROM note_titles WHERE note_id=$1")
        .bind(note_id)
        .execute(&mut *tx)
        .await?;
    // The public author identity of the owner. Public reads never show the owner id.
    sqlx::query("INSERT INTO authors(author_id,owner_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(core::id())
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    // Other devices of the owner learn the publication status from the sync feed.
    sqlx::query("INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES($1,$2,'upsert',$3)")
        .bind(owner)
        .bind(note_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    get(store, owner, note_id).await
}

/// `DELETE /me/notes/{note_id}/publication`: unpublish. The working copy, its
/// revisions and its sync history stay. Returns the new publication revision.
pub async fn delete(store: &Store, owner: &str, note_id: &str, if_match: i64) -> R<i64> {
    let now = core::now();
    let mut tx = store.db.begin().await?;
    let locked =
        sqlx::query("UPDATE notes SET updated_at=updated_at WHERE note_id=$1 AND owner_id=$2")
            .bind(note_id)
            .bind(owner)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    if locked != 1 {
        return Err(NoteError::not_found());
    }
    let published =
        sqlx::query("SELECT note_id FROM note_published_representations WHERE note_id=$1 LIMIT 1")
            .bind(note_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
    if !published {
        return Err(not_published());
    }
    let changed = sqlx::query(
        "UPDATE note_publications SET revision=revision+1,updated_at=$1 WHERE note_id=$2 AND revision=$3",
    )
    .bind(now)
    .bind(note_id)
    .bind(if_match)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if changed != 1 {
        let current: i64 = sqlx::query("SELECT revision FROM note_publications WHERE note_id=$1")
            .bind(note_id)
            .fetch_one(&mut *tx)
            .await?
            .get("revision");
        return Err(conflict(current));
    }
    // The publication row stays with its revision; pointers and title go.
    for sql in [
        "DELETE FROM note_published_representations WHERE note_id=$1",
        "DELETE FROM note_publication_titles WHERE note_id=$1",
    ] {
        sqlx::query(sql).bind(note_id).execute(&mut *tx).await?;
    }
    sqlx::query("INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES($1,$2,'upsert',$3)")
        .bind(owner)
        .bind(note_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(if_match + 1)
}

// Public read -------------------------------------------------------------------

/// The public form of a published note: the only function that public reads use.
///
/// `view` selects one published representation; `None` returns every published
/// one. A note without a publication, or a view that is not published, is not
/// found. The answer carries the public author id, never the owner id, and no
/// revision or document id. `default_view` is the main representation when it is
/// published, else the published one, so an unpublished main is never revealed.
pub async fn public_note(store: &Store, note_id: &str, view: Option<Representation>) -> R<Value> {
    let s = state(&store.db, note_id)
        .await?
        .filter(State::published)
        .ok_or_else(NoteError::not_found)?;
    let row = sqlx::query("SELECT n.main_representation,a.author_id FROM notes n LEFT JOIN authors a ON a.owner_id=n.owner_id WHERE n.note_id=$1")
        .bind(note_id)
        .fetch_optional(&store.db)
        .await?
        .ok_or_else(NoteError::not_found)?;
    let main = Representation::parse(&row.get::<String, _>("main_representation"));
    let views: Vec<Representation> = s.items.iter().map(|p| p.representation).collect();
    let default_view = main.filter(|m| views.contains(m)).unwrap_or(views[0]);
    let mut representations = vec![];
    for p in &s.items {
        if view.is_some_and(|v| v != p.representation) {
            continue;
        }
        let stored = store
            .docs
            .get(&p.document_id)
            .await
            .map_err(|_| NoteError::from(core::Error::unavailable()))?;
        representations.push(json!({
            "representation": p.representation.as_str(),
            "schema_version": p.schema_version,
            "document": public_document(&stored["body"]),
        }));
    }
    if representations.is_empty() {
        return Err(NoteError::not_found());
    }
    Ok(json!({
        "note_id": note_id,
        "author_id": row.try_get::<Option<String>, _>("author_id")?,
        "language": s.language,
        "title": s.title,
        "views": views.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
        "default_view": default_view.as_str(),
        "typed_uses_earlier_revision": s.typed_uses_earlier_revision(),
        "published_at": s.updated_at,
        "representations": representations,
    }))
}
