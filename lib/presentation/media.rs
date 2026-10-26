use crate::{
    core::{Error, Result, UPLOAD_BYTES},
    dal::files::File,
    domain::App,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{StatusCode, header},
    response::Response,
    routing::{get, post},
};
use serde_json::{Value, json};
use sqlx::Row;
use tokio::io::AsyncWriteExt;
#[derive(serde::Deserialize)]
pub struct Owner {
    pub owner_id: String,
}
pub fn routes() -> Router<App> {
    Router::new()
        .route(
            "/admin/images",
            post(upload).layer(DefaultBodyLimit::max(UPLOAD_BYTES as usize + 1024 * 1024)),
        )
        .route("/media/{key}", get(public))
        .route("/admin/images/{key}", get(private))
}
pub async fn spool(
    field: &mut axum::extract::multipart::Field<'_>,
    limit: i64,
) -> Result<(tempfile::TempPath, i64)> {
    let temp = tempfile::NamedTempFile::new()
        .map_err(|_| Error::unavailable())?
        .into_temp_path();
    let mut output = tokio::fs::File::create(&temp)
        .await
        .map_err(|_| Error::unavailable())?;
    let mut size = 0;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|_| Error::bad("Invalid upload body"))?
    {
        size += chunk.len() as i64;
        if size > limit {
            return Err(Error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "File exceeds 50 MB".into(),
            ));
        }
        output
            .write_all(&chunk)
            .await
            .map_err(|_| Error::unavailable())?;
    }
    output.flush().await.map_err(|_| Error::unavailable())?;
    Ok((temp, size))
}
pub async fn upload(
    State(app): State<App>,
    Query(owner): Query<Owner>,
    mut multipart: Multipart,
) -> Result<Json<Value>> {
    let mut field = multipart
        .next_field()
        .await
        .map_err(|_| Error::bad("Invalid upload"))?
        .ok_or_else(|| Error::bad("Missing image"))?;
    let filename = field.file_name().unwrap_or("image").to_string();
    let mime = field.content_type().unwrap_or("").to_string();
    if !matches!(
        mime.as_str(),
        "image/png" | "image/jpeg" | "image/webp" | "image/gif" | "image/avif"
    ) {
        return Err(Error::bad("Unsupported image type"));
    }
    let (temp, size) = spool(&mut field, UPLOAD_BYTES).await?;
    let file = app
        .store
        .files
        .upload_path(&temp, &filename, &mime, "article-images", size)
        .await?;
    if let Err(e) = sqlx::query(
        "INSERT INTO media(id,owner_id,filename,content_type,size_bytes) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(&file.key)
    .bind(owner.owner_id)
    .bind(&file.filename)
    .bind(&file.content_type)
    .bind(file.size_bytes)
    .execute(&app.store.db)
    .await
    {
        let _ = app.store.files.delete(&file.key).await;
        return Err(e.into());
    }
    Ok(Json(
        json!({"id":file.key,"url":format!("https://api.asodya.com/apps/annotations/v1/media/{}",file.key)}),
    ))
}
async fn response(app: &App, key: &str, private: bool) -> Result<Response> {
    let r=sqlx::query("SELECT m.* FROM media m WHERE m.id=$1 AND ($2=1 OR EXISTS(SELECT 1 FROM post_media pm JOIN posts p ON p.id=pm.post_id WHERE pm.media_id=m.id AND p.status='published' AND p.visibility='public'))").bind(key).bind(i64::from(private)).fetch_optional(&app.store.db).await?.ok_or_else(Error::missing)?;
    let file = File {
        key: key.into(),
        filename: r.get("filename"),
        content_type: r.get("content_type"),
        size_bytes: r.get("size_bytes"),
    };
    let mut response = Response::new(app.store.files.read(key).await?);
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        file.content_type
            .parse()
            .map_err(|_| Error::unavailable())?,
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "private, no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    Ok(response)
}
pub async fn public(State(app): State<App>, Path(key): Path<String>) -> Result<Response> {
    response(&app, &key, false).await
}
pub async fn private(State(app): State<App>, Path(key): Path<String>) -> Result<Response> {
    response(&app, &key, true).await
}
