use crate::{core::Result, domain::App};
use axum::{Json, Router, extract::State, routing::get};
pub fn router(app: App) -> Router {
    Router::new()
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({"status":"ok"})) }),
        )
        .route("/ready", get(ready))
        .with_state(app)
}
async fn ready(State(app): State<App>) -> Result<Json<serde_json::Value>> {
    app.store.ready().await?;
    Ok(Json(serde_json::json!({"status":"ready"})))
}
