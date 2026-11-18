//! Owner project routes under `/me/projects`. The owner is the gateway identity
//! (`x-uuid`). Business rules live in `domain::projects`.
use super::notes::{Owner, if_match_from, tagged};
use crate::domain::{App, notes::NoteError, projects};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, put},
};
use serde_json::Value;

pub fn routes() -> Router<App> {
    Router::new()
        .route("/me/projects", get(list).post(create))
        .route(
            "/me/projects/{project_id}",
            get(detail).patch(change).delete(remove),
        )
        .route(
            "/me/projects/{project_id}/notes/{note_id}",
            put(add_note).delete(remove_note),
        )
        .layer(DefaultBodyLimit::max(projects::MAX_BODY_BYTES + 1))
}

/// The expected project revision from `If-Match` (`"3"` or `3`).
pub(crate) fn if_match(headers: &HeaderMap) -> Result<i64, NoteError> {
    if_match_from(headers, 1).map_err(|e| match e.code.as_str() {
        "if_match_required" => NoteError::new(
            StatusCode::PRECONDITION_REQUIRED,
            "if_match_required",
            "Send the project revision in the If-Match header",
        ),
        "invalid_if_match" => NoteError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_if_match",
            "If-Match must be one positive project revision",
        ),
        _ => e,
    })
}

#[derive(serde::Deserialize)]
pub struct ListParams {
    cursor: Option<String>,
    limit: Option<String>,
}
async fn list(
    State(app): State<App>,
    Owner(owner): Owner,
    Query(p): Query<ListParams>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        projects::list(&app.store, &owner, p.cursor.as_deref(), p.limit.as_deref()).await?,
    ))
}

async fn create(
    State(app): State<App>,
    Owner(owner): Owner,
    body: Bytes,
) -> Result<Response, NoteError> {
    let (project, revision) = projects::create(&app.store, &owner, &body).await?;
    Ok(tagged(StatusCode::CREATED, project, revision))
}

async fn detail(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(project_id): Path<String>,
) -> Result<Response, NoteError> {
    let (project, revision) = projects::get(&app.store, &owner, &project_id).await?;
    Ok(tagged(StatusCode::OK, project, revision))
}

async fn change(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(project_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = if_match(&headers)?;
    let (project, revision) =
        projects::patch(&app.store, &owner, &project_id, expected, &body).await?;
    Ok(tagged(StatusCode::OK, project, revision))
}

async fn remove(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(project_id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, NoteError> {
    let expected = if_match(&headers)?;
    projects::delete(&app.store, &owner, &project_id, expected).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn add_note(
    State(app): State<App>,
    Owner(owner): Owner,
    Path((project_id, note_id)): Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, NoteError> {
    projects::add_note(&app.store, &owner, &project_id, &note_id, &body).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_note(
    State(app): State<App>,
    Owner(owner): Owner,
    Path((project_id, note_id)): Path<(String, String)>,
) -> Result<StatusCode, NoteError> {
    projects::remove_note(&app.store, &owner, &project_id, &note_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
