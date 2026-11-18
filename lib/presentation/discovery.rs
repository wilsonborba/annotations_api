//! Public discovery routes: no sign-in, published content only. Business rules
//! live in `domain::discovery`, `domain::publication` and `domain::projects`.
use crate::domain::{
    App,
    discovery::{self, ListQuery},
    notes::NoteError,
    projects,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use serde_json::Value;

pub fn routes() -> Router<App> {
    Router::new()
        .route("/notes", get(list))
        .route("/notes/{note_id}", get(note))
        .route("/authors/{author_id}", get(author))
        .route("/authors/{author_id}/projects", get(author_projects))
        .route("/projects/{project_id}", get(project))
}

#[derive(serde::Deserialize)]
pub struct ListParams {
    q: Option<String>,
    author_id: Option<String>,
    language: Option<String>,
    tag: Option<String>,
    project_id: Option<String>,
    cursor: Option<String>,
    limit: Option<String>,
    browse_token: Option<String>,
}
async fn list(
    State(app): State<App>,
    Query(p): Query<ListParams>,
) -> Result<Json<Value>, NoteError> {
    let query = ListQuery {
        q: p.q,
        author_id: p.author_id,
        language: p.language,
        tag: p.tag,
        project_id: p.project_id,
        cursor: p.cursor,
        limit: p.limit,
        browse_token: p.browse_token,
    };
    Ok(Json(discovery::list(&app.store, &query).await?))
}

#[derive(serde::Deserialize)]
pub struct NoteParams {
    view: Option<String>,
    language: Option<String>,
}
async fn note(
    State(app): State<App>,
    Path(note_id): Path<String>,
    Query(p): Query<NoteParams>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        discovery::note(
            &app.store,
            &note_id,
            p.view.as_deref(),
            p.language.as_deref(),
        )
        .await?,
    ))
}

async fn author(
    State(app): State<App>,
    Path(author_id): Path<String>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(discovery::author(&app.store, &author_id).await?))
}

#[derive(serde::Deserialize)]
pub struct PageParams {
    cursor: Option<String>,
    limit: Option<String>,
}
async fn author_projects(
    State(app): State<App>,
    Path(author_id): Path<String>,
    Query(p): Query<PageParams>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        discovery::author_projects(
            &app.store,
            &author_id,
            p.cursor.as_deref(),
            p.limit.as_deref(),
        )
        .await?,
    ))
}

async fn project(
    State(app): State<App>,
    Path(project_id): Path<String>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        projects::public_project(&app.store, &project_id).await?,
    ))
}
