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
        .route("/instant/{code}", get(read).put(create).patch(patch_room))
        .route("/instant/{code}/items", post(add))
        .route(
            "/instant/{code}/items/{id}",
            axum::routing::delete(remove).put(replace).patch(patch_item),
        )
        .layer(axum::extract::DefaultBodyLimit::max(
            crate::core::NOTE_LIMITS.max_document_bytes + 1024,
        ))
        .route("/instant/{code}/files", post(reserve))
        .route(
            "/instant/{code}/files/{id}/content",
            axum::routing::put(upload).layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route("/instant/{code}/files/{id}", get(download))
}
pub async fn read(State(app): State<App>, Path(code): Path<String>) -> Result<Json<Value>> {
    Ok(Json(app.store.rooms.get(&code, now()).await?.public()))
}
impl axum::response::IntoResponse for rooms::Failure {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Error(error) => error.into_response(),
            Self::StaleItem { item, item_revision, revision } => (
                axum::http::StatusCode::CONFLICT,
                Json(serde_json::json!({"error":"item_revision_conflict", "message":"The item changed; review the current item",
                    "current_item":item,"item_revision":item_revision,"revision":revision})),
            ).into_response(),
        }
    }
}
pub async fn create(
    State(app): State<App>,
    Path(code): Path<String>,
    Json(input): Json<rooms::Create>,
) -> Result<Json<Value>> {
    Ok(Json(
        rooms::create(&app.store, &code, input, now())
            .await?
            .public(),
    ))
}
pub async fn add(
    State(app): State<App>,
    Path(code): Path<String>,
    Json(input): Json<rooms::ItemWrite>,
) -> rooms::EditResult<Json<Value>> {
    Ok(Json(
        rooms::add(&app.store, &code, input, now()).await?.public(),
    ))
}
pub async fn replace(
    State(app): State<App>,
    Path((code, id)): Path<(String, String)>,
    Json(input): Json<rooms::ItemWrite>,
) -> rooms::EditResult<Json<Value>> {
    Ok(Json(
        rooms::replace(&app.store, &code, &id, input, now())
            .await?
            .public(),
    ))
}
pub async fn patch_item(
    State(app): State<App>,
    Path((code, id)): Path<(String, String)>,
    Json(input): Json<rooms::ItemPatch>,
) -> rooms::EditResult<Json<Value>> {
    Ok(Json(
        rooms::patch_item(&app.store, &code, &id, input, now())
            .await?
            .public(),
    ))
}
pub async fn patch_room(
    State(app): State<App>,
    Path(code): Path<String>,
    Json(input): Json<rooms::RoomPatch>,
) -> rooms::EditResult<Json<Value>> {
    Ok(Json(
        rooms::patch_room(&app.store, &code, input, now())
            .await?
            .public(),
    ))
}
pub async fn remove(
    State(app): State<App>,
    Path((code, id)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> rooms::EditResult<Json<Value>> {
    let revision = super::notes::if_match(&headers).map_err(|e| {
        rooms::Failure::Error(crate::core::Error(
            e.status,
            format!("{}: {}", e.code, e.message),
        ))
    })?;
    Ok(Json(
        rooms::remove(&app.store, &code, &id, revision, now())
            .await?
            .public(),
    ))
}

fn uploader_ip(peer: std::net::SocketAddr, headers: &axum::http::HeaderMap) -> String {
    if peer.ip().is_loopback() {
        headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<std::net::IpAddr>().ok())
            .unwrap_or(peer.ip())
            .to_string()
    } else {
        peer.ip().to_string()
    }
}
pub async fn reserve(
    State(app): State<App>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    Path(code): Path<String>,
    headers: axum::http::HeaderMap,
    Json(input): Json<crate::domain::transfers::Metadata>,
) -> Result<Json<Value>> {
    let ip = uploader_ip(peer, &headers);
    let key = super::notes::idempotency_key(&headers);
    let record = crate::domain::transfers::begin(&app.store, &code, &ip, input, key, now()).await?;
    Ok(Json(record.public()))
}
pub async fn upload(
    State(app): State<App>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    Path((code, id)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: axum::body::Body,
) -> Result<Json<Value>> {
    let ip = uploader_ip(peer, &headers);
    let record = crate::domain::transfers::upload(
        &app.store,
        &code,
        &id,
        &ip,
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        body.into_data_stream(),
        now(),
    )
    .await?;
    Ok(Json(record.public()))
}
pub async fn download(
    State(app): State<App>,
    Path((code, id)): Path<(String, String)>,
) -> Result<axum::response::Response> {
    let record = crate::domain::transfers::download(&app.store, &code, &id, now()).await?;
    let body = app
        .store
        .files
        .read(
            record
                .file_key
                .as_deref()
                .ok_or_else(crate::core::Error::unavailable)?,
        )
        .await?;
    crate::domain::transfers::download(&app.store, &code, &id, now()).await?;
    let mut response =
        axum::response::Response::new(rooms::download_body(body, record.expires_at)?);
    // Only verified images open in place; every other file is a download.
    let inline = record.preview && record.content_type.starts_with("image/");
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        if inline {
            record
                .content_type
                .parse()
                .map_err(|_| crate::core::Error::bad("Invalid media type"))?
        } else {
            "application/octet-stream".parse().unwrap()
        },
    );
    let safe_name = record
        .filename
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '.' | '_' | '-'))
        .collect::<String>();
    response.headers_mut().insert(
        axum::http::header::CONTENT_DISPOSITION,
        format!(
            "{}; filename=\"{}\"",
            if inline { "inline" } else { "attachment" },
            safe_name
        )
        .parse()
        .map_err(|_| crate::core::Error::bad("Invalid filename"))?,
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        "no-store".parse().unwrap(),
    );
    response.headers_mut().insert(
        "content-security-policy",
        "sandbox; default-src 'none'".parse().unwrap(),
    );
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    Ok(response)
}
