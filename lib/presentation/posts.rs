use crate::{
    core::Result,
    domain::{App, posts},
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post, put},
};
use serde_json::Value;
pub fn routes() -> Router<App> {
    Router::new()
        .route("/catalog", get(catalog))
        .route("/posts", get(list))
        .route("/posts/{key}", get(detail))
        .route("/admin/catalog", put(catalog_write))
        .route("/admin/posts/{id}", put(write).get(private))
        .route("/admin/posts/{id}/publish", post(publish))
}
pub async fn catalog(State(app): State<App>) -> Result<Json<Value>> {
    Ok(Json(posts::catalog(&app.store).await?))
}
pub async fn catalog_write(State(app): State<App>, Json(body): Json<Value>) -> Result<Json<Value>> {
    Ok(Json(posts::catalog_write(&app.store, body).await?))
}
pub async fn list(State(app): State<App>, Query(q): Query<posts::Query>) -> Result<Json<Value>> {
    Ok(Json(posts::list(&app.store, &q).await?))
}
pub async fn detail(
    State(app): State<App>,
    Path(key): Path<String>,
    Query(q): Query<posts::Query>,
) -> Result<Json<Value>> {
    Ok(Json(
        posts::detail(&app.store, &key, &q.lang, false, &q.channel).await?,
    ))
}
pub async fn private(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(q): Query<posts::Query>,
) -> Result<Json<Value>> {
    Ok(Json(
        posts::detail(&app.store, &id, &q.lang, true, "").await?,
    ))
}
pub async fn write(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<posts::WritePost>,
) -> Result<Json<Value>> {
    Ok(Json(posts::write(&app.store, &id, body).await?))
}
pub async fn publish(State(app): State<App>, Path(id): Path<String>) -> Result<Json<Value>> {
    Ok(Json(posts::publish(&app.store, &id).await?))
}
