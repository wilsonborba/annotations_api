//! Owner working-copy routes under `/me/notes`. The owner is the gateway identity
//! (`x-uuid`); the gateway removes that header from client requests. Business rules
//! live in `domain::notes`.
use crate::domain::{
    App,
    notes::{self, ListQuery, NoteError, View},
    publication,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequestParts, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::Value;

/// The gateway header with the signed-in account id.
pub const IDENTITY_HEADER: &str = "x-uuid";

pub fn routes() -> Router<App> {
    Router::new()
        .route("/me/notes", get(list).post(create))
        .route(
            "/me/notes/{note_id}",
            get(detail).put(replace).patch(change).delete(remove),
        )
        .route(
            "/me/notes/{note_id}/publication",
            get(publication_detail).put(publish).delete(unpublish),
        )
        .layer(DefaultBodyLimit::max(notes::MAX_BODY_BYTES + 1))
}

impl IntoResponse for NoteError {
    fn into_response(self) -> Response {
        (self.status, Json(self.to_json())).into_response()
    }
}

/// The caller's account, from the gateway identity only.
pub struct Owner(pub String);
impl<S: Send + Sync> FromRequestParts<S> for Owner {
    type Rejection = NoteError;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts
            .headers
            .get(IDENTITY_HEADER)
            .and_then(|v| v.to_str().ok())
            .filter(|v| (1..=128).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_graphic()))
            .map(|v| Owner(v.to_string()))
            .ok_or_else(NoteError::identity_required)
    }
}

/// The expected note revision from `If-Match` (`"3"` or `3`).
pub(crate) fn if_match(headers: &HeaderMap) -> Result<i64, NoteError> {
    if_match_from(headers, 1)
}

/// The expected publication revision from `If-Match`; `0` means never published.
pub(crate) fn publication_if_match(headers: &HeaderMap) -> Result<i64, NoteError> {
    if_match_from(headers, 0).map_err(|e| {
        if e.code == "invalid_if_match" {
            NoteError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_if_match",
                "If-Match must be one publication revision (0 or more)",
            )
        } else {
            e
        }
    })
}

pub(crate) fn if_match_from(headers: &HeaderMap, min: i64) -> Result<i64, NoteError> {
    let raw = headers
        .get(header::IF_MATCH)
        .ok_or_else(NoteError::if_match_required)?
        .to_str()
        .map_err(|_| NoteError::invalid_if_match())?
        .trim();
    let raw = raw
        .strip_prefix('"')
        .and_then(|r| r.strip_suffix('"'))
        .unwrap_or(raw);
    raw.parse::<i64>()
        .ok()
        .filter(|n| *n >= min)
        .ok_or_else(NoteError::invalid_if_match)
}

pub(crate) fn idempotency_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("idempotency-key")
        .map(|v| v.to_str().unwrap_or("\u{0}"))
}

/// A JSON answer with the note revision as its ETag.
pub(crate) fn tagged(status: StatusCode, body: Value, revision: i64) -> Response {
    let mut response = (status, Json(body)).into_response();
    if let Ok(value) = HeaderValue::from_str(&format!("\"{revision}\"")) {
        response.headers_mut().insert(header::ETAG, value);
    }
    response
}

#[derive(serde::Deserialize)]
pub struct ListParams {
    q: Option<String>,
    project_id: Option<String>,
    tag: Option<String>,
    cursor: Option<String>,
    limit: Option<String>,
    sync_cursor: Option<String>,
}
async fn list(
    State(app): State<App>,
    Owner(owner): Owner,
    Query(p): Query<ListParams>,
) -> Result<Json<Value>, NoteError> {
    let query = ListQuery {
        q: p.q,
        project_id: p.project_id,
        tag: p.tag,
        cursor: p.cursor,
        limit: p.limit,
        sync_cursor: p.sync_cursor,
    };
    Ok(Json(notes::list(&app.store, &owner, &query).await?))
}

async fn create(
    State(app): State<App>,
    Owner(owner): Owner,
    body: Bytes,
) -> Result<Response, NoteError> {
    let created = notes::create(&app.store, &owner, &body).await?;
    let status = if created.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok(tagged(status, created.body, created.revision))
}

#[derive(serde::Deserialize)]
pub struct DetailParams {
    view: Option<String>,
}
async fn detail(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(note_id): Path<String>,
    Query(p): Query<DetailParams>,
) -> Result<Response, NoteError> {
    let view = View::parse(p.view.as_deref())?;
    let note = notes::get(&app.store, &owner, &note_id, view).await?;
    let revision = note["revision"].as_i64().unwrap_or(0);
    Ok(tagged(StatusCode::OK, note, revision))
}

async fn replace(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(note_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = if_match(&headers)?;
    let (saved, revision) = notes::replace(
        &app.store,
        &owner,
        &note_id,
        expected,
        idempotency_key(&headers),
        &body,
    )
    .await?;
    Ok(tagged(StatusCode::OK, saved, revision))
}

async fn change(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(note_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = if_match(&headers)?;
    let (saved, revision) = notes::patch(
        &app.store,
        &owner,
        &note_id,
        expected,
        idempotency_key(&headers),
        &body,
    )
    .await?;
    Ok(tagged(StatusCode::OK, saved, revision))
}

#[derive(serde::Deserialize)]
pub struct DeleteParams {
    delete_owned_reports: Option<String>,
}
async fn remove(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(note_id): Path<String>,
    Query(p): Query<DeleteParams>,
    headers: HeaderMap,
) -> Result<StatusCode, NoteError> {
    let expected = if_match(&headers)?;
    notes::delete(
        &app.store,
        &owner,
        &note_id,
        expected,
        p.delete_owned_reports.as_deref(),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn publication_detail(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(note_id): Path<String>,
) -> Result<Response, NoteError> {
    let (body, revision) = publication::get(&app.store, &owner, &note_id).await?;
    Ok(tagged(StatusCode::OK, body, revision))
}

async fn publish(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(note_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = publication_if_match(&headers)?;
    let (body, revision) = publication::put(&app.store, &owner, &note_id, expected, &body).await?;
    Ok(tagged(StatusCode::OK, body, revision))
}

async fn unpublish(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(note_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, NoteError> {
    let expected = publication_if_match(&headers)?;
    let revision = publication::delete(&app.store, &owner, &note_id, expected).await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    if let Ok(value) = HeaderValue::from_str(&format!("\"{revision}\"")) {
        response.headers_mut().insert(header::ETAG, value);
    }
    Ok(response)
}
