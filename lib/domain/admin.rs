//! Administrative operations on managed notes, projects and files across owners
//! (`/admin/notes`, `/admin/projects`, `/admin/files`).
//!
//! - The central access level decides who is an administrator; the presentation
//!   layer checks it on every `/admin` route. There is no second role system.
//! - An administrative operation finds the owner of the object and then calls the
//!   owner function of `notes`, `publication`, `projects` or `files` with that
//!   owner. Validation, the save pipeline, the delete order and the shared-file
//!   guards are the same as for the owner.
//! - Only an administrative create names an owner (`owner_id` in the body), and
//!   that owner must already exist.
//! - Administrative access covers the data that this service manages. It never
//!   reaches local files on a user device.
use crate::{
    dal::Store,
    domain::{
        files,
        notes::{self, NoteError},
        projects,
    },
};
use serde_json::{Value, json};
use sqlx::Row;

type R<T> = std::result::Result<T, NoteError>;

fn owner_id(value: &str) -> R<String> {
    if notes::token(value) {
        Ok(value.to_string())
    } else {
        Err(NoteError::invalid(
            "invalid_owner_id",
            "owner_id uses 1 to 128 visible ASCII characters",
        ))
    }
}

/// Split the target owner from an administrative create body. The owner must
/// exist. Returns the owner and the rest of the body for the owner function.
pub async fn target_owner(store: &Store, raw: &[u8]) -> R<(String, Vec<u8>)> {
    let mut body = notes::parse_body(raw)?;
    let owner = match body.remove("owner_id") {
        Some(Value::String(s)) => owner_id(&s)?,
        Some(_) => return Err(notes::wrong_type("owner_id")),
        None => {
            return Err(NoteError::invalid(
                "missing_field",
                "A required field is missing: owner_id",
            ));
        }
    };
    let known = sqlx::query("SELECT id FROM owners WHERE id=$1")
        .bind(&owner)
        .fetch_optional(&store.db)
        .await?;
    if known.is_none() {
        return Err(NoteError::invalid(
            "unknown_owner",
            "The target owner does not exist",
        ));
    }
    let rest = serde_json::to_vec(&Value::Object(body))
        .map_err(|_| NoteError::invalid("invalid_json", "The request body is not valid JSON"))?;
    Ok((owner, rest))
}

async fn owner_by(store: &Store, sql: &str, id: &str) -> R<String> {
    Ok(sqlx::query(sql)
        .bind(id)
        .fetch_optional(&store.db)
        .await?
        .ok_or_else(NoteError::not_found)?
        .get("owner_id"))
}

/// The owner of a note.
pub async fn note_owner(store: &Store, note_id: &str) -> R<String> {
    owner_by(
        store,
        "SELECT owner_id FROM notes WHERE note_id=$1",
        note_id,
    )
    .await
}

/// The owner of a project.
pub async fn project_owner(store: &Store, project_id: &str) -> R<String> {
    owner_by(
        store,
        "SELECT owner_id FROM projects WHERE project_id=$1",
        project_id,
    )
    .await
}

/// The owner of a managed file.
pub async fn file_owner(store: &Store, file_id: &str) -> R<String> {
    files::owner_of(store, file_id)
        .await?
        .ok_or_else(NoteError::not_found)
}

/// An owner answer with the owner id added, for the administrator.
pub fn with_owner(mut body: Value, owner: &str) -> Value {
    body["owner_id"] = json!(owner);
    body
}

/// Query of `GET /admin/notes`. All values are raw query strings.
#[derive(Debug, Default)]
pub struct NoteQuery {
    pub owner_id: Option<String>,
    /// `published` or `unpublished`.
    pub status: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<String>,
}

/// `GET /admin/notes`: notes of every owner (or of `owner_id`), newest change
/// first, with the same summary as the owner list plus `owner_id`.
pub async fn list_notes(store: &Store, query: &NoteQuery) -> R<Value> {
    let limit = notes::page_limit(query.limit.as_deref())?;
    let owner = query.owner_id.as_deref().map(owner_id).transpose()?;
    let status: i64 = match query.status.as_deref() {
        None => 0,
        Some("published") => 1,
        Some("unpublished") => 2,
        Some(_) => {
            return Err(NoteError::invalid(
                "invalid_status",
                "status must be published or unpublished",
            ));
        }
    };
    let (after_time, after_id) = notes::keyset_cursor(query.cursor.as_deref())?;
    let rows = sqlx::query(&format!(
        "SELECT {},n.owner_id FROM notes n LEFT JOIN note_titles t ON t.note_id=n.note_id \
         WHERE ($1='' OR n.owner_id=$1) \
         AND ($2=0 OR ($2=1)=EXISTS(SELECT 1 FROM note_published_representations p WHERE p.note_id=n.note_id)) \
         AND ($3=0 OR n.updated_at<$4 OR (n.updated_at=$4 AND n.note_id<$5)) \
         ORDER BY n.updated_at DESC,n.note_id DESC LIMIT $6",
        notes::NOTE_COLUMNS
    ))
    .bind(owner.as_deref().unwrap_or(""))
    .bind(status)
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
        let owner: String = row.get("owner_id");
        next = json!(format!(
            "{}.{}",
            row.get::<i64, _>("updated_at"),
            row.get::<String, _>("note_id")
        ));
        items.push(with_owner(
            notes::summary(store, &owner, row).await?,
            &owner,
        ));
    }
    Ok(json!({
        "items": items,
        "next_cursor": if more { next } else { Value::Null },
    }))
}

/// `GET /admin/projects`: projects of every owner (or of `owner_id`), newest
/// change first, with the owner project fields plus `owner_id`.
pub async fn list_projects(
    store: &Store,
    owner: Option<&str>,
    cursor: Option<&str>,
    limit: Option<&str>,
) -> R<Value> {
    let limit = notes::page_limit(limit)?;
    let owner = owner.map(owner_id).transpose()?;
    let (after_time, after_id) = notes::keyset_cursor(cursor)?;
    let rows = sqlx::query(&format!(
        "SELECT {},p.owner_id FROM projects p WHERE ($1='' OR p.owner_id=$1) \
         AND ($2=0 OR p.updated_at<$3 OR (p.updated_at=$3 AND p.project_id<$4)) \
         ORDER BY p.updated_at DESC,p.project_id DESC LIMIT $5",
        projects::PROJECT_COLUMNS
    ))
    .bind(owner.as_deref().unwrap_or(""))
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
            row.get::<String, _>("project_id")
        ));
        items.push(with_owner(
            projects::project_json(row),
            &row.get::<String, _>("owner_id"),
        ));
    }
    Ok(json!({
        "items": items,
        "next_cursor": if more { next } else { Value::Null },
    }))
}
