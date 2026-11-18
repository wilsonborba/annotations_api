//! Signed-in manifest reads and account-bound model downloads. Trusted identity comes
//! only from the gateway header.
use crate::{
    domain::{
        App,
        models::{self, DownloadRequest},
        notes::NoteError,
    },
    presentation::{files::stream, notes::Owner},
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};

pub fn routes() -> Router<App> {
    Router::new()
        .route("/models", get(list))
        .route("/models/{model_version}", get(detail))
        .route("/me/model_downloads", post(create_download))
        .route("/me/model_downloads/{download_id}", get(download))
        .route("/me/model_downloads/{download_id}/content", get(content))
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
/// 201 for a new download, 200 when a retry returns the existing one.
async fn create_download(
    Owner(owner): Owner,
    State(app): State<App>,
    body: Bytes,
) -> Result<Response, NoteError> {
    let input = DownloadRequest::parse(&body)?;
    let (download, created) = models::create_download(&app.store.db, &owner, &input).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(download)).into_response())
}
async fn download(
    Owner(owner): Owner,
    State(app): State<App>,
    Path(download_id): Path<String>,
) -> Result<Json<models::Download>, NoteError> {
    Ok(Json(
        models::download(&app.store.db, &owner, &download_id).await?,
    ))
}
/// The shared release artifact bytes, streamed from FSM with HTTP Range support.
async fn content(
    Owner(owner): Owner,
    State(app): State<App>,
    Path(download_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, NoteError> {
    let artifact = models::download_content(&app.store.db, &owner, &download_id).await?;
    let mut response = stream(
        &app,
        &artifact.fsm_reference,
        artifact.size as u64,
        HeaderValue::from_static("application/octet-stream"),
        &headers,
    )
    .await?;
    if response.status() != StatusCode::RANGE_NOT_SATISFIABLE {
        let h = response.headers_mut();
        if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", artifact.sha256)) {
            h.insert(header::ETAG, v);
        }
        h.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment"),
        );
    }
    Ok(response)
}
