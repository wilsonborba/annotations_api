//! Owner managed file routes under `/me/files`. The owner is the gateway identity
//! (`x-uuid`). Business rules live in `domain::files`.
use crate::{
    core::FILE_LIMITS,
    domain::{
        App,
        files::{self, Download, Reservation},
        notes::NoteError,
    },
    presentation::notes::Owner,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};

/// The body limit of an upload: the largest purpose limit.
pub(crate) fn upload_limit() -> usize {
    FILE_LIMITS
        .attachment_bytes
        .max(FILE_LIMITS.transcription_bytes)
        .max(FILE_LIMITS.content_job_bytes) as usize
}

pub fn routes() -> Router<App> {
    let largest = upload_limit();
    Router::new()
        .route("/me/files", post(reserve))
        .route("/me/files/{file_id}", get(detail).delete(release))
        .route(
            "/me/files/{file_id}/content",
            get(content)
                .put(upload)
                .layer(DefaultBodyLimit::max(largest + 1)),
        )
}

async fn reserve(
    State(app): State<App>,
    Owner(owner): Owner,
    body: Bytes,
) -> Result<Response, NoteError> {
    let input = Reservation::parse(&body)?;
    let file = files::reserve(&app.store, &owner, &input).await?;
    Ok((StatusCode::CREATED, Json(file.to_json())).into_response())
}

async fn detail(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(file_id): Path<String>,
) -> Result<Response, NoteError> {
    let file = files::get(&app.store, &owner, &file_id).await?;
    Ok(Json(file.to_json()).into_response())
}

async fn upload(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, NoteError> {
    let declared = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let file = files::upload(
        &app.store,
        &owner,
        &file_id,
        declared,
        body.into_data_stream(),
    )
    .await?;
    Ok(Json(file.to_json()).into_response())
}

async fn release(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(file_id): Path<String>,
) -> Result<StatusCode, NoteError> {
    files::release(&app.store, &owner, &file_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn content(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(file_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, NoteError> {
    let download = files::download(&app.store, &owner, &file_id).await?;
    send(&app, &download, &headers).await
}

/// One satisfiable `Range: bytes=` value as an inclusive pair. `Ok(None)` serves the
/// whole file: no header, another unit or several ranges (allowed by RFC 9110).
fn range(headers: &HeaderMap, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| ())?.trim();
    // Another range unit is ignored (RFC 9110).
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Ok(None);
    }
    let (first, last) = spec.split_once('-').ok_or(())?;
    let (first, last) = (first.trim(), last.trim());
    let pair = if first.is_empty() {
        let suffix: u64 = last.parse().map_err(|_| ())?;
        if suffix == 0 || size == 0 {
            return Err(());
        }
        (size.saturating_sub(suffix), size - 1)
    } else {
        let first: u64 = first.parse().map_err(|_| ())?;
        let last = if last.is_empty() {
            size.saturating_sub(1)
        } else {
            last.parse::<u64>()
                .map_err(|_| ())?
                .min(size.saturating_sub(1))
        };
        if first >= size || last < first {
            return Err(());
        }
        (first, last)
    };
    Ok(Some(pair))
}

/// The file bytes with safe headers: the stored type, `nosniff`, a sandbox policy
/// so no script runs from an upload, and a download disposition for non-images.
pub(crate) async fn send(
    app: &App,
    download: &Download,
    headers: &HeaderMap,
) -> Result<Response, NoteError> {
    let size = download.file.size as u64;
    let range = match range(headers, size) {
        Ok(r) => r,
        Err(()) => {
            let mut response = NoteError::new(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "range_not_satisfiable",
                "The requested range is outside the file",
            )
            .into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("bytes */{size}")) {
                response.headers_mut().insert(header::CONTENT_RANGE, v);
            }
            return Ok(response);
        }
    };
    let body = app
        .store
        .files
        .read_range(download.fsm_reference(), range)
        .await?;
    let mut response = Response::new(body);
    let h = response.headers_mut();
    let content_type = HeaderValue::from_str(&download.file.content_type)
        .unwrap_or(HeaderValue::from_static("application/octet-stream"));
    h.insert(header::CONTENT_TYPE, content_type);
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("sandbox; default-src 'none'"),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", download.file.checksum)) {
        h.insert(header::ETAG, v);
    }
    if !download.inline() {
        let disposition = format!(
            "attachment; filename*=UTF-8''{}",
            url::form_urlencoded::byte_serialize(download.file.filename.as_bytes())
                .collect::<String>()
                .replace('+', "%20")
        );
        if let Ok(v) = HeaderValue::from_str(&disposition) {
            h.insert(header::CONTENT_DISPOSITION, v);
        }
    }
    let length = match range {
        Some((first, last)) => {
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            if let Ok(v) = HeaderValue::from_str(&format!("bytes {first}-{last}/{size}")) {
                response.headers_mut().insert(header::CONTENT_RANGE, v);
            }
            last - first + 1
        }
        None => size,
    };
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(length));
    Ok(response)
}
