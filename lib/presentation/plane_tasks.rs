//! Owner task card routes (`/me/tasks`) backed by Plane Slim on the LAN.
use crate::{
    domain::{App, notes::NoteError, plane_tasks},
    presentation::notes::Owner,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::Value;

pub fn routes() -> Router<App> {
    Router::new()
        .route("/me/tasks", get(list).post(create))
        .route(
            "/me/tasks/{task_id}",
            get(detail).patch(update).delete(remove),
        )
        .layer(DefaultBodyLimit::max(65_536))
}

#[derive(serde::Deserialize)]
struct ListParams {
    q: Option<String>,
    status: Option<String>,
    cursor: Option<String>,
    limit: Option<i64>,
}

async fn list(
    State(app): State<App>,
    Owner(owner): Owner,
    Query(p): Query<ListParams>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        plane_tasks::list(
            &app.store,
            &owner,
            p.q.as_deref(),
            p.status.as_deref(),
            p.cursor.as_deref(),
            p.limit.unwrap_or(50),
        )
        .await?,
    ))
}

async fn create(
    State(app): State<App>,
    Owner(owner): Owner,
    body: Bytes,
) -> Result<Response, NoteError> {
    let created = plane_tasks::create(&app.store, &owner, &body).await?;
    Ok((StatusCode::CREATED, Json(created)).into_response())
}

async fn detail(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(plane_tasks::get(&app.store, &owner, &task_id).await?))
}

async fn update(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(task_id): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        plane_tasks::update(&app.store, &owner, &task_id, &body).await?,
    ))
}

async fn remove(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(task_id): Path<String>,
) -> Result<StatusCode, NoteError> {
    plane_tasks::delete(&app.store, &owner, &task_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
