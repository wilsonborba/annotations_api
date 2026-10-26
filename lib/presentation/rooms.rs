use crate::{
    core::{Result, now},
    domain::{App, rooms},
};
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use serde_json::Value;
pub fn routes() -> Router<App> {
    Router::new()
        .route("/instant/{code}", get(read).put(create))
        .route("/instant/{code}/items", post(edit))
}
pub async fn read(State(app): State<App>, Path(code): Path<String>) -> Result<Json<Value>> {
    Ok(Json(app.store.rooms.get(&code, now()).await?.public()))
}
pub async fn create(State(app): State<App>, Path(code): Path<String>) -> Result<Json<Value>> {
    Ok(Json(
        rooms::create(&app.store.rooms, &code, now())
            .await?
            .public(),
    ))
}
pub async fn edit(
    State(app): State<App>,
    Path(code): Path<String>,
    Json(input): Json<rooms::Edit>,
) -> Result<Json<Value>> {
    Ok(Json(
        rooms::edit(&app.store.rooms, &code, input, now())
            .await?
            .public(),
    ))
}
