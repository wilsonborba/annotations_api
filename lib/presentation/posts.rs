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
        .route("/waitlist", post(waitlist))
        .route("/links/preview", get(link_preview))
        .route("/posts", get(list))
        .route("/posts/{key}", get(detail))
        .route("/admin/catalog", put(catalog_write))
        .route("/admin/posts/{id}", put(write).get(private))
        .route("/admin/posts/{id}/publish", post(publish))
}
pub async fn catalog(State(app): State<App>) -> Result<Json<Value>> {
    Ok(Json(
        cached(&app, "pub:catalog", posts::catalog(&app.store)).await?,
    ))
}
pub async fn catalog_write(State(app): State<App>, Json(body): Json<Value>) -> Result<Json<Value>> {
    Ok(Json(posts::catalog_write(&app.store, body).await?))
}
pub async fn list(State(app): State<App>, Query(q): Query<posts::Query>) -> Result<Json<Value>> {
    Ok(Json(
        cached(
            &app,
            &format!("pub:list:{}", serde_json::to_string(&q).unwrap()),
            posts::list(&app.store, &q),
        )
        .await?,
    ))
}
pub async fn detail(
    State(app): State<App>,
    Path(key): Path<String>,
    Query(q): Query<posts::Query>,
) -> Result<Json<Value>> {
    Ok(Json(
        cached(
            &app,
            &format!("pub:detail:{}", serde_json::json!([key, q.lang, q.channel])),
            posts::detail(&app.store, &key, &q.lang, false, &q.channel),
        )
        .await?,
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

async fn cached(
    app: &App,
    key: &str,
    fetch: impl std::future::Future<Output = Result<Value>>,
) -> Result<Value> {
    // SQL owns visibility. Include current metadata in the key even when Redis
    // recovers after a failed invalidation; old public bodies cannot reappear.
    use sqlx::Row;
    use std::hash::{Hash, Hasher};
    let rows = sqlx::query("SELECT p.id,p.status,p.visibility,t.document_id FROM posts p LEFT JOIN post_translations t ON p.id=t.post_id ORDER BY p.id,t.language_code").fetch_all(&app.store.db).await?;
    let mut fingerprint = std::collections::hash_map::DefaultHasher::new();
    for row in rows {
        for column in ["id", "status", "visibility", "document_id"] {
            row.try_get::<String, _>(column)
                .unwrap_or_default()
                .hash(&mut fingerprint);
        }
    }
    let key = format!("{key}:{}", fingerprint.finish());
    let key = key.as_str();
    let epoch = app.store.cache.epoch().await;
    if let Some(epoch) = epoch
        && let Some(hit) = app.store.cache.get_at(epoch, key).await
        && let Ok(value) = serde_json::from_str(&hit)
    {
        return Ok(value);
    }
    let value = fetch.await?;
    if let Some(epoch) = epoch {
        app.store.cache.put_at(epoch, key, &value.to_string()).await;
    }
    Ok(value)
}

#[derive(serde::Deserialize)]
pub struct LinkQuery {
    pub url: String,
}
pub async fn link_preview(
    State(app): State<App>,
    Query(q): Query<LinkQuery>,
) -> Result<Json<Value>> {
    Ok(Json(
        crate::domain::links::preview(&app.store, &q.url).await?,
    ))
}

pub async fn waitlist(
    State(app): State<App>,
    Json(input): Json<crate::domain::waitlist::Request>,
) -> Result<Json<Value>> {
    Ok(Json(
        crate::domain::waitlist::register(&app.store, input).await?,
    ))
}
