//! Administrative routes for managed notes, projects and files, and the access
//! check of every `/admin` route.
//!
//! The gateway guards `/admin/*` and sends the central access level in
//! `x-access-level`; it removes that header from client requests. Administrator
//! access is level 1. `guard` checks it again here, so a normal session gets 403 on
//! every admin route. Business rules live in `domain::admin` and the owner modules.
use super::{
    files::{send, upload_limit},
    notes::{idempotency_key, if_match, publication_if_match, tagged},
};
use crate::domain::{
    App,
    admin::{self, NoteQuery, with_owner},
    files::{self, Reservation},
    notes::{self, NoteError, View},
    projects, publication,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::Value;

/// The gateway header with the central access level of the caller.
pub const ACCESS_LEVEL_HEADER: &str = "x-access-level";
/// The central access level of an administrator.
pub const ADMIN_LEVEL: &str = "1";

/// Room for `owner_id` in an administrative create body.
const OWNER_FIELD_BYTES: usize = 1024;

/// Refuse every `/admin` request without the administrator access level.
pub async fn guard(request: Request, next: Next) -> Response {
    let path = request.uri().path();
    if path == "/admin" || path.starts_with("/admin/") {
        let level = request
            .headers()
            .get(ACCESS_LEVEL_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim);
        if level != Some(ADMIN_LEVEL) {
            return NoteError::new(
                StatusCode::FORBIDDEN,
                "admin_required",
                "Administrator access is required",
            )
            .into_response();
        }
    }
    next.run(request).await
}

pub fn routes() -> Router<App> {
    let notes = Router::new()
        .route("/admin/notes", get(note_list).post(note_create))
        .route(
            "/admin/notes/{note_id}",
            get(note_detail)
                .put(note_replace)
                .patch(note_change)
                .delete(note_remove),
        )
        .route(
            "/admin/notes/{note_id}/publication",
            get(publication_detail).put(publish).delete(unpublish),
        )
        .layer(DefaultBodyLimit::max(
            notes::MAX_BODY_BYTES + OWNER_FIELD_BYTES + 1,
        ));
    let projects = Router::new()
        .route("/admin/projects", get(project_list).post(project_create))
        .route(
            "/admin/projects/{project_id}",
            get(project_detail)
                .patch(project_change)
                .delete(project_remove),
        )
        .layer(DefaultBodyLimit::max(
            projects::MAX_BODY_BYTES + OWNER_FIELD_BYTES + 1,
        ));
    let files = Router::new()
        .route("/admin/files", post(file_reserve))
        .route(
            "/admin/files/{file_id}",
            get(file_detail).delete(file_release),
        )
        .route(
            "/admin/files/{file_id}/content",
            get(file_content)
                .put(file_upload)
                .layer(DefaultBodyLimit::max(upload_limit() + 1)),
        );
    notes.merge(projects).merge(files)
}

// Notes -------------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub struct NoteListParams {
    owner_id: Option<String>,
    status: Option<String>,
    cursor: Option<String>,
    limit: Option<String>,
}
async fn note_list(
    State(app): State<App>,
    Query(p): Query<NoteListParams>,
) -> Result<Json<Value>, NoteError> {
    let query = NoteQuery {
        owner_id: p.owner_id,
        status: p.status,
        cursor: p.cursor,
        limit: p.limit,
    };
    Ok(Json(admin::list_notes(&app.store, &query).await?))
}

async fn note_create(State(app): State<App>, body: Bytes) -> Result<Response, NoteError> {
    let (owner, rest) = admin::target_owner(&app.store, &body).await?;
    let created = notes::create(&app.store, &owner, &rest).await?;
    let status = if created.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok(tagged(
        status,
        with_owner(created.body, &owner),
        created.revision,
    ))
}

#[derive(serde::Deserialize)]
pub struct ViewParams {
    view: Option<String>,
}
async fn note_detail(
    State(app): State<App>,
    Path(note_id): Path<String>,
    Query(p): Query<ViewParams>,
) -> Result<Response, NoteError> {
    let view = View::parse(p.view.as_deref())?;
    let owner = admin::note_owner(&app.store, &note_id).await?;
    let note = notes::get(&app.store, &owner, &note_id, view).await?;
    let revision = note["revision"].as_i64().unwrap_or(0);
    Ok(tagged(StatusCode::OK, with_owner(note, &owner), revision))
}

async fn note_replace(
    State(app): State<App>,
    Path(note_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = if_match(&headers)?;
    let owner = admin::note_owner(&app.store, &note_id).await?;
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

async fn note_change(
    State(app): State<App>,
    Path(note_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = if_match(&headers)?;
    let owner = admin::note_owner(&app.store, &note_id).await?;
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
async fn note_remove(
    State(app): State<App>,
    Path(note_id): Path<String>,
    Query(p): Query<DeleteParams>,
    headers: HeaderMap,
) -> Result<StatusCode, NoteError> {
    if p.delete_owned_reports.as_deref() == Some("true") {
        return Err(NoteError::invalid(
            "reports_cascade_unavailable",
            "Admin delete does not cascade owned Reports items",
        ));
    }
    let expected = if_match(&headers)?;
    let owner = admin::note_owner(&app.store, &note_id).await?;
    let _ = notes::delete(
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
    Path(note_id): Path<String>,
) -> Result<Response, NoteError> {
    let owner = admin::note_owner(&app.store, &note_id).await?;
    let (body, revision) = publication::get(&app.store, &owner, &note_id).await?;
    Ok(tagged(StatusCode::OK, body, revision))
}

async fn publish(
    State(app): State<App>,
    Path(note_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = publication_if_match(&headers)?;
    let owner = admin::note_owner(&app.store, &note_id).await?;
    let (body, revision) = publication::put(&app.store, &owner, &note_id, expected, &body).await?;
    Ok(tagged(StatusCode::OK, body, revision))
}

async fn unpublish(
    State(app): State<App>,
    Path(note_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, NoteError> {
    let expected = publication_if_match(&headers)?;
    let owner = admin::note_owner(&app.store, &note_id).await?;
    let revision = publication::delete(&app.store, &owner, &note_id, expected).await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    if let Ok(value) = HeaderValue::from_str(&format!("\"{revision}\"")) {
        response.headers_mut().insert(header::ETAG, value);
    }
    Ok(response)
}

// Projects ----------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub struct ProjectListParams {
    owner_id: Option<String>,
    cursor: Option<String>,
    limit: Option<String>,
}
async fn project_list(
    State(app): State<App>,
    Query(p): Query<ProjectListParams>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        admin::list_projects(
            &app.store,
            p.owner_id.as_deref(),
            p.cursor.as_deref(),
            p.limit.as_deref(),
        )
        .await?,
    ))
}

async fn project_create(State(app): State<App>, body: Bytes) -> Result<Response, NoteError> {
    let (owner, rest) = admin::target_owner(&app.store, &body).await?;
    let (project, revision) = projects::create(&app.store, &owner, &rest).await?;
    Ok(tagged(
        StatusCode::CREATED,
        with_owner(project, &owner),
        revision,
    ))
}

async fn project_detail(
    State(app): State<App>,
    Path(project_id): Path<String>,
) -> Result<Response, NoteError> {
    let owner = admin::project_owner(&app.store, &project_id).await?;
    let (project, revision) = projects::get(&app.store, &owner, &project_id).await?;
    Ok(tagged(
        StatusCode::OK,
        with_owner(project, &owner),
        revision,
    ))
}

async fn project_change(
    State(app): State<App>,
    Path(project_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, NoteError> {
    let expected = super::projects::if_match(&headers)?;
    let owner = admin::project_owner(&app.store, &project_id).await?;
    let (project, revision) =
        projects::patch(&app.store, &owner, &project_id, expected, &body).await?;
    Ok(tagged(
        StatusCode::OK,
        with_owner(project, &owner),
        revision,
    ))
}

async fn project_remove(
    State(app): State<App>,
    Path(project_id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, NoteError> {
    let expected = super::projects::if_match(&headers)?;
    let owner = admin::project_owner(&app.store, &project_id).await?;
    projects::delete(&app.store, &owner, &project_id, expected).await?;
    Ok(StatusCode::NO_CONTENT)
}

// Files -------------------------------------------------------------------------

async fn file_reserve(State(app): State<App>, body: Bytes) -> Result<Response, NoteError> {
    let (owner, rest) = admin::target_owner(&app.store, &body).await?;
    let input = Reservation::parse(&rest)?;
    let file = files::reserve(&app.store, &owner, &input).await?;
    Ok((
        StatusCode::CREATED,
        Json(with_owner(file.to_json(), &owner)),
    )
        .into_response())
}

async fn file_detail(
    State(app): State<App>,
    Path(file_id): Path<String>,
) -> Result<Response, NoteError> {
    let owner = admin::file_owner(&app.store, &file_id).await?;
    let file = files::get(&app.store, &owner, &file_id).await?;
    Ok(Json(with_owner(file.to_json(), &owner)).into_response())
}

async fn file_upload(
    State(app): State<App>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, NoteError> {
    let owner = admin::file_owner(&app.store, &file_id).await?;
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
    Ok(Json(with_owner(file.to_json(), &owner)).into_response())
}

/// Release only this logical file reference. The 409 guard of `files::release`
/// stays: bytes that another reference needs are never removed.
async fn file_release(
    State(app): State<App>,
    Path(file_id): Path<String>,
) -> Result<StatusCode, NoteError> {
    let owner = admin::file_owner(&app.store, &file_id).await?;
    files::release(&app.store, &owner, &file_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn file_content(
    State(app): State<App>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, NoteError> {
    let owner = admin::file_owner(&app.store, &file_id).await?;
    let download = files::download(&app.store, &owner, &file_id).await?;
    send(&app, &download, &headers).await
}
