//! Owner task cards backed by the server-side Plane Slim adapter (`/me/tasks`).
//! Each Asodya owner gets an isolated private Plane project container (`owner_plane_containers`).
//! Plane is the source of truth; credentials and host stay on the server.
use crate::{
    core,
    dal::{
        Store,
        plane::{CreateWorkItem, PlaneWorkItem, UpdateWorkItem},
    },
    domain::notes::{NoteError, allow, parse_body},
};
use axum::http::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;

type R<T> = std::result::Result<T, NoteError>;

const MAX_TITLE_CHARS: usize = 255;
const MAX_DESC_CHARS: usize = 10_000;

struct Container {
    project_id: String,
    open_state_id: String,
    completed_state_id: String,
}

impl Container {
    fn state_for(&self, status: &str) -> R<&str> {
        match status {
            "open" => Ok(&self.open_state_id),
            "completed" => Ok(&self.completed_state_id),
            _ => Err(NoteError::invalid(
                "invalid_task_status",
                "status must be open or completed",
            )),
        }
    }
    fn status_of(&self, state_id: &str) -> &'static str {
        if state_id == self.completed_state_id {
            "completed"
        } else {
            "open"
        }
    }
}

fn token(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic())
}

fn check_title(s: &str) -> R<String> {
    let trimmed = s.trim();
    if trimmed.is_empty()
        || trimmed.chars().count() > MAX_TITLE_CHARS
        || trimmed.chars().any(char::is_control)
    {
        return Err(NoteError::invalid(
            "invalid_task_title",
            "title must be 1 to 255 characters without control characters",
        ));
    }
    Ok(trimmed.to_string())
}

fn check_due_date(s: &str) -> R<String> {
    let bytes = s.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
    {
        return Err(NoteError::invalid(
            "invalid_due_date",
            "due_date must use YYYY-MM-DD",
        ));
    }
    let month: u8 = s[5..7].parse().unwrap_or(0);
    let day: u8 = s[8..10].parse().unwrap_or(0);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(NoteError::invalid(
            "invalid_due_date",
            "due_date must use YYYY-MM-DD",
        ));
    }
    Ok(s.to_string())
}

fn escape_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 16);
    out.push_str("<p>");
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\n' => out.push_str("</p><p>"),
            _ => out.push(ch),
        }
    }
    out.push_str("</p>");
    out
}

fn plain_from_html(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
}

fn to_json(container: &Container, item: &PlaneWorkItem) -> Value {
    let desc = plain_from_html(&item.description_html);
    json!({
        "task_id": item.id,
        "title": item.name,
        "description": if desc.is_empty() { Value::Null } else { json!(desc) },
        "due_date": item.target_date,
        "status": container.status_of(&item.state),
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    })
}

async fn ensure_container(store: &Store, owner: &str) -> R<Container> {
    if !store.plane.is_configured() {
        return Err(NoteError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "plane_unavailable",
            "Task integration is not configured",
        ));
    }
    if let Some(row) = sqlx::query(
        "SELECT plane_project_id,open_state_id,completed_state_id FROM owner_plane_containers WHERE owner_id=$1",
    )
    .bind(owner)
    .fetch_optional(&store.db)
    .await?
    {
        return Ok(Container {
            project_id: row.get("plane_project_id"),
            open_state_id: row.get("open_state_id"),
            completed_state_id: row.get("completed_state_id"),
        });
    }

    let hash = format!("{:x}", Sha256::digest(owner.as_bytes()));
    let short_hex = &hash[..6].to_ascii_uppercase();
    let identifier = format!("AN{short_hex}");
    let project_name = format!("Annotations {short_hex}");
    let project_id = store
        .plane
        .create_project(&project_name, &identifier)
        .await?;
    let states = store.plane.list_states(&project_id).await?;
    let open_state_id = states
        .iter()
        .find(|s| s.group == "unstarted")
        .or_else(|| states.iter().find(|s| s.group == "started"))
        .or_else(|| states.iter().find(|s| s.group == "backlog"))
        .map(|s| s.id.clone())
        .ok_or_else(|| {
            NoteError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "plane_unavailable",
                "Plane project has no open workflow state",
            )
        })?;
    let completed_state_id = states
        .iter()
        .find(|s| s.group == "completed")
        .map(|s| s.id.clone())
        .ok_or_else(|| {
            NoteError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "plane_unavailable",
                "Plane project has no completed workflow state",
            )
        })?;

    let mut tx = store.db.begin().await?;
    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO owner_plane_containers(owner_id,plane_project_id,open_state_id,completed_state_id,created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(owner_id) DO NOTHING",
    )
    .bind(owner)
    .bind(&project_id)
    .bind(&open_state_id)
    .bind(&completed_state_id)
    .bind(core::now())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let row = sqlx::query(
        "SELECT plane_project_id,open_state_id,completed_state_id FROM owner_plane_containers WHERE owner_id=$1",
    )
    .bind(owner)
    .fetch_one(&store.db)
    .await?;
    Ok(Container {
        project_id: row.get("plane_project_id"),
        open_state_id: row.get("open_state_id"),
        completed_state_id: row.get("completed_state_id"),
    })
}

/// `GET /me/tasks`: list tasks from the caller's isolated Plane container.
pub async fn list(
    store: &Store,
    owner: &str,
    q: Option<&str>,
    status: Option<&str>,
    cursor: Option<&str>,
    limit: i64,
) -> R<Value> {
    if let Some(st) = status
        && !matches!(st, "open" | "completed")
    {
        return Err(NoteError::invalid(
            "invalid_task_status",
            "status must be open or completed",
        ));
    }
    let container = ensure_container(store, owner).await?;
    let page = store
        .plane
        .list_work_items(&container.project_id, cursor, limit.clamp(1, 100))
        .await?;
    let needle = q
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty());
    let items: Vec<Value> = page
        .results
        .iter()
        .filter(|item| {
            let item_status = container.status_of(&item.state);
            let status_ok = status.is_none_or(|s| s == item_status);
            let q_ok = needle.as_ref().is_none_or(|n| {
                item.name.to_ascii_lowercase().contains(n)
                    || plain_from_html(&item.description_html)
                        .to_ascii_lowercase()
                        .contains(n)
            });
            status_ok && q_ok
        })
        .map(|item| to_json(&container, item))
        .collect();
    Ok(json!({
        "items": items,
        "next_cursor": page.next_cursor,
    }))
}

/// `POST /me/tasks`: create a task in the caller's isolated Plane container.
pub async fn create(store: &Store, owner: &str, raw: &[u8]) -> R<Value> {
    let body = parse_body(raw)?;
    allow(&body, &["title", "description", "due_date", "status"], "")?;
    let title = body
        .get("title")
        .and_then(Value::as_str)
        .ok_or_else(|| NoteError::invalid("invalid_task_title", "title is required"))?;
    let title = check_title(title)?;
    let description_html = match body.get("description") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.chars().count() > MAX_DESC_CHARS {
                return Err(NoteError::invalid(
                    "description_too_long",
                    "description is too long",
                ));
            }
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(escape_html(trimmed))
            }
        }
        Some(_) => {
            return Err(NoteError::invalid(
                "invalid_type",
                "description must be a string",
            ));
        }
    };
    let target_date = match body.get("due_date") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(check_due_date(s)?),
        Some(_) => {
            return Err(NoteError::invalid(
                "invalid_due_date",
                "due_date must use YYYY-MM-DD",
            ));
        }
    };
    let status = match body.get("status") {
        None => "open",
        Some(Value::String(s)) => s.as_str(),
        Some(_) => {
            return Err(NoteError::invalid(
                "invalid_task_status",
                "status must be open or completed",
            ));
        }
    };

    let container = ensure_container(store, owner).await?;
    let state = container.state_for(status)?.to_string();
    let created = store
        .plane
        .create_work_item(
            &container.project_id,
            &CreateWorkItem {
                name: title,
                description_html,
                target_date,
                state,
            },
        )
        .await?;
    Ok(to_json(&container, &created))
}

/// `GET /me/tasks/{task_id}`: read one task from the caller's container.
pub async fn get(store: &Store, owner: &str, task_id: &str) -> R<Value> {
    if !token(task_id) {
        return Err(NoteError::not_found());
    }
    let container = ensure_container(store, owner).await?;
    let item = store
        .plane
        .get_work_item(&container.project_id, task_id)
        .await?;
    Ok(to_json(&container, &item))
}

/// `PATCH /me/tasks/{task_id}`: update fields on a task in the caller's container.
pub async fn update(store: &Store, owner: &str, task_id: &str, raw: &[u8]) -> R<Value> {
    if !token(task_id) {
        return Err(NoteError::not_found());
    }
    let body = parse_body(raw)?;
    allow(&body, &["title", "description", "due_date", "status"], "")?;
    if body.is_empty() {
        return Err(NoteError::invalid(
            "empty_update",
            "Provide at least one field to update",
        ));
    }
    let mut patch = UpdateWorkItem::default();
    if let Some(val) = body.get("title") {
        let s = val
            .as_str()
            .ok_or_else(|| NoteError::invalid("invalid_task_title", "title must be a string"))?;
        patch.name = Some(check_title(s)?);
    }
    if let Some(val) = body.get("description") {
        match val {
            Value::Null => patch.description_html = Some(String::new()),
            Value::String(s) => {
                if s.chars().count() > MAX_DESC_CHARS {
                    return Err(NoteError::invalid(
                        "description_too_long",
                        "description is too long",
                    ));
                }
                patch.description_html = Some(escape_html(s.trim()));
            }
            _ => {
                return Err(NoteError::invalid(
                    "invalid_type",
                    "description must be a string",
                ));
            }
        }
    }
    if let Some(val) = body.get("due_date") {
        match val {
            Value::Null => patch.target_date = Some(None),
            Value::String(s) => patch.target_date = Some(Some(check_due_date(s)?)),
            _ => {
                return Err(NoteError::invalid(
                    "invalid_due_date",
                    "due_date must use YYYY-MM-DD",
                ));
            }
        }
    }
    let container = ensure_container(store, owner).await?;
    if let Some(val) = body.get("status") {
        let s = val.as_str().ok_or_else(|| {
            NoteError::invalid("invalid_task_status", "status must be open or completed")
        })?;
        patch.state = Some(container.state_for(s)?.to_string());
    }
    let updated = store
        .plane
        .update_work_item(&container.project_id, task_id, &patch)
        .await?;
    Ok(to_json(&container, &updated))
}

/// `DELETE /me/tasks/{task_id}`: delete a task from the caller's container.
pub async fn delete(store: &Store, owner: &str, task_id: &str) -> R<()> {
    if !token(task_id) {
        return Err(NoteError::not_found());
    }
    let container = ensure_container(store, owner).await?;
    store
        .plane
        .delete_work_item(&container.project_id, task_id)
        .await?;
    Ok(())
}
