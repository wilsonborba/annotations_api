//! Owner projects (`/me/projects`) and the public project projection.
//!
//! - A project belongs to one owner, the gateway identity. Its id is a global
//!   opaque id; names are never used for identity or in URLs.
//! - Names are unique per owner after trimming and case folding
//!   (`names::normalize`). Different owners may use the same name.
//! - New projects are private. Project visibility never changes note visibility.
//! - A membership links one note and one project of the same owner. Adding is
//!   idempotent. Removing a membership or deleting a project never deletes a note.
//! - Public reads go through `public_project` and `public_note_ids`, which count
//!   and list only notes with a published representation. A public note never
//!   shows its projects (`publication::public_note` has no project field).
use crate::{
    core::{self, NOTE_API_LIMITS},
    dal::Store,
    domain::{
        names,
        notes::{self, NoteError},
    },
};
use axum::http::StatusCode;
use serde_json::{Map, Value, json};
use sqlx::Row;

type R<T> = std::result::Result<T, NoteError>;

/// Largest request body of the project routes.
pub const MAX_BODY_BYTES: usize = 16 * 1024;

/// Project visibility. New projects are private.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Visibility {
    Private,
    Public,
}
impl Visibility {
    fn parse(value: &Value) -> R<Self> {
        match value.as_str() {
            Some("private") => Ok(Self::Private),
            Some("public") => Ok(Self::Public),
            _ => Err(NoteError::invalid(
                "invalid_visibility",
                "visibility must be private or public",
            )),
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Public => "public",
        }
    }
}

fn name_taken() -> NoteError {
    NoteError::new(
        StatusCode::CONFLICT,
        "project_name_taken",
        "You already have a project with this name",
    )
}

fn conflict(current_revision: i64) -> NoteError {
    NoteError {
        current_revision: Some(current_revision),
        ..NoteError::new(
            StatusCode::CONFLICT,
            "revision_conflict",
            "The project changed since the given revision",
        )
    }
}

fn body(raw: &[u8]) -> R<Map<String, Value>> {
    if raw.len() > MAX_BODY_BYTES {
        return Err(NoteError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "The request body is larger than allowed",
        ));
    }
    notes::parse_body(raw)
}

/// A trimmed project name and its normalized form.
fn name(value: &Value) -> R<(String, String)> {
    let text = value.as_str().ok_or_else(|| notes::wrong_type("name"))?;
    let name = notes::clean_text(text, "name", NOTE_API_LIMITS.max_project_name_chars)?;
    let normalized = names::normalize(&name);
    if normalized.is_empty() {
        return Err(NoteError::invalid(
            "empty_name",
            "The project name must not be empty",
        ));
    }
    Ok((name, normalized))
}

fn unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(d) if d.kind() == sqlx::error::ErrorKind::UniqueViolation)
}

/// The public author identity of the owner, needed once something of the owner
/// can be public. Public reads never show the owner id.
async fn ensure_author(tx: &mut sqlx::Transaction<'_, sqlx::Any>, owner: &str) -> R<()> {
    sqlx::query("INSERT INTO authors(author_id,owner_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(core::id())
        .bind(owner)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

const PROJECT_COLUMNS: &str = "p.project_id,p.name,p.visibility,p.revision,p.created_at,p.updated_at,(SELECT COUNT(*) FROM project_notes m WHERE m.project_id=p.project_id) AS note_count";

fn project_json(row: &sqlx::any::AnyRow) -> Value {
    json!({
        "project_id": row.get::<String, _>("project_id"),
        "name": row.get::<String, _>("name"),
        "visibility": row.get::<String, _>("visibility"),
        "revision": row.get::<i64, _>("revision"),
        "note_count": row.get::<i64, _>("note_count"),
        "created_at": row.get::<i64, _>("created_at"),
        "updated_at": row.get::<i64, _>("updated_at"),
    })
}

// Owner routes ------------------------------------------------------------------

/// `GET /me/projects/{project_id}`: the owner's project with its revision and the
/// number of its notes. Another owner's project is not found.
pub async fn get(store: &Store, owner: &str, project_id: &str) -> R<(Value, i64)> {
    let row = sqlx::query(&format!(
        "SELECT {PROJECT_COLUMNS} FROM projects p WHERE p.project_id=$1 AND p.owner_id=$2"
    ))
    .bind(project_id)
    .bind(owner)
    .fetch_optional(&store.db)
    .await?
    .ok_or_else(NoteError::not_found)?;
    let out = project_json(&row);
    let revision = row.get("revision");
    Ok((out, revision))
}

/// `GET /me/projects`: the owner's private and public projects, newest change
/// first, with a keyset cursor.
pub async fn list(
    store: &Store,
    owner: &str,
    cursor: Option<&str>,
    limit: Option<&str>,
) -> R<Value> {
    let limit = notes::page_limit(limit)?;
    let (after_time, after_id) = match cursor {
        None => (None, String::new()),
        Some(c) => match c
            .split_once('.')
            .and_then(|(t, id)| Some((t.parse::<i64>().ok()?, id)))
            .filter(|(_, id)| notes::token(id))
        {
            Some((t, id)) => (Some(t), id.to_string()),
            None => {
                return Err(NoteError::invalid(
                    "invalid_cursor",
                    "The cursor is not valid",
                ));
            }
        },
    };
    let rows = sqlx::query(&format!(
        "SELECT {PROJECT_COLUMNS} FROM projects p WHERE p.owner_id=$1 \
         AND ($2=0 OR p.updated_at<$3 OR (p.updated_at=$3 AND p.project_id<$4)) \
         ORDER BY p.updated_at DESC,p.project_id DESC LIMIT $5"
    ))
    .bind(owner)
    .bind(i64::from(after_time.is_some()))
    .bind(after_time.unwrap_or(0))
    .bind(&after_id)
    .bind(limit + 1)
    .fetch_all(&store.db)
    .await?;
    let more = rows.len() as i64 > limit;
    let mut next = Value::Null;
    let mut items = vec![];
    for row in rows.iter().take(limit as usize) {
        next = json!(format!(
            "{}.{}",
            row.get::<i64, _>("updated_at"),
            row.get::<String, _>("project_id")
        ));
        items.push(project_json(row));
    }
    Ok(json!({
        "items": items,
        "next_cursor": if more { next } else { Value::Null },
    }))
}

/// `POST /me/projects`: `{"name": string, "visibility"?: "private" | "public"}`.
/// New projects are private unless the owner asks for public.
pub async fn create(store: &Store, owner: &str, raw: &[u8]) -> R<(Value, i64)> {
    let body = body(raw)?;
    notes::allow(&body, &["name", "visibility"], "")?;
    let (name, normalized) = name(body.get("name").ok_or_else(|| {
        NoteError::invalid("missing_field", "A required field is missing: name")
    })?)?;
    let visibility = match body.get("visibility") {
        None => Visibility::Private,
        Some(v) => Visibility::parse(v)?,
    };
    let now = core::now();
    let project_id = core::id();
    let mut tx = store.db.begin().await?;
    // Account binding: the gateway account owns the new project.
    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    // The id is new, so a conflict can only be the owner's normalized name.
    let added = sqlx::query(
        "INSERT INTO projects(project_id,owner_id,name,normalized_name,visibility,revision,created_at,updated_at) VALUES($1,$2,$3,$4,$5,1,$6,$6) ON CONFLICT DO NOTHING",
    )
    .bind(&project_id)
    .bind(owner)
    .bind(&name)
    .bind(&normalized)
    .bind(visibility.as_str())
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if added != 1 {
        return Err(name_taken());
    }
    if visibility == Visibility::Public {
        ensure_author(&mut tx, owner).await?;
    }
    tx.commit().await?;
    get(store, owner, &project_id).await
}

/// `PATCH /me/projects/{project_id}`: `{"name"?: string, "visibility"?: ...}` with
/// the project revision in If-Match. Notes keep their own visibility.
pub async fn patch(
    store: &Store,
    owner: &str,
    project_id: &str,
    if_match: i64,
    raw: &[u8],
) -> R<(Value, i64)> {
    let body = body(raw)?;
    notes::allow(&body, &["name", "visibility"], "")?;
    let name = body.get("name").map(name).transpose()?;
    let visibility = body.get("visibility").map(Visibility::parse).transpose()?;
    if name.is_none() && visibility.is_none() {
        return Err(NoteError::invalid(
            "empty_change",
            "Send name, visibility or both",
        ));
    }
    let mut tx = store.db.begin().await?;
    let current: i64 =
        sqlx::query("SELECT revision FROM projects WHERE project_id=$1 AND owner_id=$2")
            .bind(project_id)
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(NoteError::not_found)?
            .get("revision");
    if current != if_match {
        return Err(conflict(current));
    }
    if let Some((_, normalized)) = &name {
        let other = sqlx::query("SELECT project_id FROM projects WHERE owner_id=$1 AND normalized_name=$2 AND project_id<>$3")
            .bind(owner)
            .bind(normalized)
            .bind(project_id)
            .fetch_optional(&mut *tx)
            .await?;
        if other.is_some() {
            return Err(name_taken());
        }
    }
    let changed = sqlx::query(
        "UPDATE projects SET name=COALESCE($1,name),normalized_name=COALESCE($2,normalized_name),visibility=COALESCE($3,visibility),revision=revision+1,updated_at=$4 WHERE project_id=$5 AND owner_id=$6 AND revision=$7",
    )
    .bind(name.as_ref().map(|(n, _)| n.clone()))
    .bind(name.as_ref().map(|(_, n)| n.clone()))
    .bind(visibility.map(|v| v.as_str().to_string()))
    .bind(core::now())
    .bind(project_id)
    .bind(owner)
    .bind(if_match)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        if unique_violation(&e) {
            name_taken()
        } else {
            NoteError::from(e)
        }
    })?
    .rows_affected();
    if changed != 1 {
        let current: i64 =
            sqlx::query("SELECT revision FROM projects WHERE project_id=$1 AND owner_id=$2")
                .bind(project_id)
                .bind(owner)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(NoteError::not_found)?
                .get("revision");
        return Err(conflict(current));
    }
    if visibility == Some(Visibility::Public) {
        ensure_author(&mut tx, owner).await?;
    }
    tx.commit().await?;
    get(store, owner, project_id).await
}

/// `DELETE /me/projects/{project_id}` with the project revision in If-Match.
/// Removes the project and its memberships; every note stays. Member notes get an
/// `upsert` row in the sync feed, because their project list changed.
pub async fn delete(store: &Store, owner: &str, project_id: &str, if_match: i64) -> R<()> {
    let now = core::now();
    let mut tx = store.db.begin().await?;
    // Lock the project row at the expected revision.
    let locked = sqlx::query(
        "UPDATE projects SET updated_at=updated_at WHERE project_id=$1 AND owner_id=$2 AND revision=$3",
    )
    .bind(project_id)
    .bind(owner)
    .bind(if_match)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if locked != 1 {
        let current =
            sqlx::query("SELECT revision FROM projects WHERE project_id=$1 AND owner_id=$2")
                .bind(project_id)
                .bind(owner)
                .fetch_optional(&mut *tx)
                .await?;
        return Err(match current {
            Some(r) => conflict(r.get("revision")),
            None => NoteError::not_found(),
        });
    }
    let members: Vec<String> =
        sqlx::query("SELECT note_id FROM project_notes WHERE project_id=$1 ORDER BY note_id")
            .bind(project_id)
            .fetch_all(&mut *tx)
            .await?
            .iter()
            .map(|r| r.get("note_id"))
            .collect();
    // Memberships first, then the project: no note row is touched.
    sqlx::query("DELETE FROM project_notes WHERE project_id=$1 AND owner_id=$2")
        .bind(project_id)
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM projects WHERE project_id=$1 AND owner_id=$2")
        .bind(project_id)
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    for note_id in &members {
        note_changed(&mut tx, owner, note_id, now).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn note_changed(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    owner: &str,
    note_id: &str,
    now: i64,
) -> R<()> {
    sqlx::query("INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES($1,$2,'upsert',$3)")
        .bind(owner)
        .bind(note_id)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Lock the owner's project and note rows. Either one missing or owned by another
/// account gives not found.
async fn lock_both(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    owner: &str,
    project_id: &str,
    note_id: &str,
) -> R<()> {
    for (sql, id) in [
        (
            "UPDATE projects SET updated_at=updated_at WHERE project_id=$1 AND owner_id=$2",
            project_id,
        ),
        (
            "UPDATE notes SET updated_at=updated_at WHERE note_id=$1 AND owner_id=$2",
            note_id,
        ),
    ] {
        let locked = sqlx::query(sql)
            .bind(id)
            .bind(owner)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if locked != 1 {
            return Err(NoteError::not_found());
        }
    }
    Ok(())
}

/// `PUT /me/projects/{project_id}/notes/{note_id}` with an empty body or `{}`.
/// Adds the membership; a second call changes nothing. Returns whether it was new.
pub async fn add_note(
    store: &Store,
    owner: &str,
    project_id: &str,
    note_id: &str,
    raw: &[u8],
) -> R<bool> {
    if !raw.iter().all(u8::is_ascii_whitespace) {
        notes::allow(&body(raw)?, &[], "")?;
    }
    let mut tx = store.db.begin().await?;
    lock_both(&mut tx, owner, project_id, note_id).await?;
    // Both foreign keys carry the owner, so a membership never crosses owners.
    let added = sqlx::query(
        "INSERT INTO project_notes(project_id,note_id,owner_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
    )
    .bind(project_id)
    .bind(note_id)
    .bind(owner)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        == 1;
    if added {
        note_changed(&mut tx, owner, note_id, core::now()).await?;
    }
    tx.commit().await?;
    Ok(added)
}

/// `DELETE /me/projects/{project_id}/notes/{note_id}`: removes the membership only.
/// The note stays. Removing a membership that does not exist changes nothing.
pub async fn remove_note(store: &Store, owner: &str, project_id: &str, note_id: &str) -> R<()> {
    let mut tx = store.db.begin().await?;
    lock_both(&mut tx, owner, project_id, note_id).await?;
    let removed =
        sqlx::query("DELETE FROM project_notes WHERE project_id=$1 AND note_id=$2 AND owner_id=$3")
            .bind(project_id)
            .bind(note_id)
            .bind(owner)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    if removed == 1 {
        note_changed(&mut tx, owner, note_id, core::now()).await?;
    }
    tx.commit().await?;
    Ok(())
}

// Public projection -------------------------------------------------------------

/// A member note counts in public only when it has a published representation.
const PUBLIC_MEMBER: &str =
    "EXISTS(SELECT 1 FROM note_published_representations r WHERE r.note_id=m.note_id)";

/// The public form of a project: only a public project, only counts of published
/// notes. No owner id, revision or private note detail. A private project is not
/// found.
pub async fn public_project(store: &Store, project_id: &str) -> R<Value> {
    let row = sqlx::query(&format!(
        "SELECT p.project_id,p.name,a.author_id,(SELECT COUNT(*) FROM project_notes m WHERE m.project_id=p.project_id AND {PUBLIC_MEMBER}) AS note_count \
         FROM projects p LEFT JOIN authors a ON a.owner_id=p.owner_id WHERE p.project_id=$1 AND p.visibility='public'"
    ))
    .bind(project_id)
    .fetch_optional(&store.db)
    .await?
    .ok_or_else(NoteError::not_found)?;
    Ok(json!({
        "project_id": row.get::<String, _>("project_id"),
        "author_id": row.try_get::<Option<String>, _>("author_id")?,
        "name": row.get::<String, _>("name"),
        "note_count": row.get::<i64, _>("note_count"),
    }))
}

/// The published notes of a public project, in note id order after `after`, at
/// most `limit`. Private notes and private projects give nothing. Each id is read
/// through `publication::public_note`.
pub async fn public_note_ids(
    store: &Store,
    project_id: &str,
    after: Option<&str>,
    limit: i64,
) -> R<Vec<String>> {
    Ok(sqlx::query(&format!(
        "SELECT m.note_id FROM project_notes m JOIN projects p ON p.project_id=m.project_id \
         WHERE m.project_id=$1 AND p.visibility='public' AND m.note_id>$2 AND {PUBLIC_MEMBER} \
         ORDER BY m.note_id LIMIT $3"
    ))
    .bind(project_id)
    .bind(after.unwrap_or(""))
    .bind(limit)
    .fetch_all(&store.db)
    .await?
    .iter()
    .map(|r| r.get("note_id"))
    .collect())
}

/// The public projects of an author, in project id order after `after`, at most
/// `limit`. Private projects give nothing. Each id is read through `public_project`.
pub async fn public_project_ids(
    store: &Store,
    author_id: &str,
    after: Option<&str>,
    limit: i64,
) -> R<Vec<String>> {
    Ok(sqlx::query(
        "SELECT p.project_id FROM projects p JOIN authors a ON a.owner_id=p.owner_id \
         WHERE a.author_id=$1 AND p.visibility='public' AND p.project_id>$2 \
         ORDER BY p.project_id LIMIT $3",
    )
    .bind(author_id)
    .bind(after.unwrap_or(""))
    .bind(limit)
    .fetch_all(&store.db)
    .await?
    .iter()
    .map(|r| r.get("project_id"))
    .collect())
}
