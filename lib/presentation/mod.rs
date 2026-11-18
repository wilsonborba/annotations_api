pub mod admin;
pub mod cli;
pub mod content_jobs;
pub mod discovery;
pub mod files;
pub mod logs;
pub mod media;
pub mod models;
pub mod notes;
mod posts;
pub mod projects;
mod rooms;
pub mod transcriptions;
use crate::{
    core::Result,
    domain::{App, schemas},
};
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
pub fn router(app: App) -> Router {
    Router::new()
        .route("/logs/stream", get(logs::stream))
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({"status":"ok"})) }),
        )
        .route("/ready", get(ready))
        .route("/admin/schemas", get(list).post(draft))
        .route(
            "/admin/schemas/{name}/versions/{version}/publish",
            post(publish),
        )
        .merge(posts::routes())
        .merge(discovery::routes())
        .merge(models::routes())
        .merge(notes::routes())
        .merge(files::routes())
        .merge(projects::routes())
        .merge(media::routes())
        .merge(rooms::routes())
        .merge(transcriptions::routes())
        .merge(content_jobs::routes())
        .merge(admin::routes())
        // Every `/admin` route, old and new, needs the administrator access level.
        .layer(axum::middleware::from_fn(admin::guard))
        .layer(axum::middleware::map_response(
            |mut response: axum::response::Response| async move {
                response
                    .headers_mut()
                    .insert("cache-control", "no-store".parse().unwrap());
                response
            },
        ))
        .layer(axum::middleware::from_fn(logs::request_log))
        .with_state(app)
}
async fn ready(State(app): State<App>) -> Result<Json<serde_json::Value>> {
    app.store.ready().await?;
    Ok(Json(serde_json::json!({"status":"ready"})))
}
async fn list(State(app): State<App>) -> Result<Json<serde_json::Value>> {
    Ok(Json(schemas::list(&app.store).await?))
}
async fn draft(
    State(app): State<App>,
    Json(input): Json<schemas::Draft>,
) -> Result<Json<serde_json::Value>> {
    Ok(Json(schemas::draft(&app.store, input).await?))
}
async fn publish(
    State(app): State<App>,
    Path((name, version)): Path<(String, i64)>,
) -> Result<Json<serde_json::Value>> {
    Ok(Json(schemas::publish(&app.store, &name, version).await?))
}
