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
        .route(
            "/instant/{code}/files",
            post(upload).layer(axum::extract::DefaultBodyLimit::max(
                crate::core::UPLOAD_BYTES as usize + 1024 * 1024,
            )),
        )
        .route("/instant/{code}/files/{id}", get(download))
        .route("/instant/{code}/items/{id}", axum::routing::delete(remove))
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
pub async fn remove(
    State(app): State<App>,
    Path((code, id)): Path<(String, String)>,
) -> Result<Json<Value>> {
    Ok(Json(
        rooms::remove(&app.store, &code, &id, now()).await?.public(),
    ))
}

#[derive(serde::Deserialize)]
pub struct UploadQuery {
    pub size: i64,
}
pub async fn upload(
    State(app): State<App>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    Path(code): Path<String>,
    axum::extract::Query(q): axum::extract::Query<UploadQuery>,
    headers: axum::http::HeaderMap,
    mut multipart: axum::extract::Multipart,
) -> Result<Json<Value>> {
    let ip = if peer.ip().is_loopback() {
        headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<std::net::IpAddr>().ok())
            .unwrap_or(peer.ip())
    } else {
        peer.ip()
    };
    let reservation =
        crate::domain::transfers::begin(&app.store, &code, &ip.to_string(), q.size, now()).await?;
    let result: Result<Value> = async {
        let mut field = multipart
            .next_field()
            .await
            .map_err(|_| crate::core::Error::bad("Invalid upload"))?
            .ok_or_else(|| crate::core::Error::bad("Missing file"))?;
        let filename = field.file_name().unwrap_or("file").to_string();
        let mime = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_string();
        let (temp, size) = super::media::spool(&mut field, q.size).await?;
        if size != q.size {
            return Err(crate::core::Error::bad(
                "Uploaded size differs from declared size",
            ));
        }
        let file = app
            .store
            .files
            .upload_path(&temp, &filename, &mime, &reservation.album, size)
            .await?;
        let result =
            crate::domain::transfers::attach(&app.store, &code, &reservation, &file, now()).await;
        if result.is_err() {
            let _ = app.store.files.delete(&file.key).await;
        }
        result
    }
    .await;
    if result.is_err() {
        // Keep cleanup ownership if remote storage failed after creating bytes.
        if let Ok(files) = app.store.files.album(&reservation.album).await {
            let mut all_deleted = true;
            for file in files {
                if app.store.files.delete(&file.key).await.is_err() {
                    all_deleted = false;
                }
            }
            if all_deleted {
                let _ = app.store.transfers.release(&reservation.id).await;
            }
        }
    }
    Ok(Json(result?))
}
pub async fn download(
    State(app): State<App>,
    Path((code, id)): Path<(String, String)>,
) -> Result<axum::response::Response> {
    let room = app.store.rooms.get(&code, now()).await?;
    let item = room
        .items
        .iter()
        .find(|i| i.id == id && !i.file_key.is_empty())
        .ok_or_else(crate::core::Error::missing)?;
    let mut response = axum::response::Response::new(app.store.files.read(&item.file_key).await?);
    let inline = matches!(
        item.content_type.as_str(),
        "image/png" | "image/jpeg" | "image/webp" | "image/gif" | "image/avif"
    );
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        if inline {
            item.content_type
                .parse()
                .map_err(|_| crate::core::Error::bad("Invalid media type"))?
        } else {
            "application/octet-stream".parse().unwrap()
        },
    );
    let safe_name = item
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
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    Ok(response)
}
