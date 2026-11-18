//! Signed-in manifest reads. Trusted identity comes only from the gateway header.
use crate::{
    domain::{App, models, notes::NoteError},
    presentation::notes::Owner,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};

pub fn routes() -> Router<App> {
    Router::new()
        .route("/models", get(list))
        .route("/models/{model_version}", get(detail))
}
#[derive(serde::Deserialize)]
struct Filters {
    platform: Option<String>,
    language: Option<String>,
}
async fn list(
    Owner(_): Owner,
    State(app): State<App>,
    Query(filters): Query<Filters>,
) -> Result<Json<models::Listing>, NoteError> {
    Ok(Json(
        models::list(
            &app.store.db,
            filters.platform.as_deref(),
            filters.language.as_deref(),
        )
        .await?,
    ))
}
async fn detail(
    Owner(_): Owner,
    State(app): State<App>,
    Path(version): Path<String>,
) -> Result<Json<models::PublicManifest>, NoteError> {
    Ok(Json(models::detail(&app.store.db, &version).await?))
}
