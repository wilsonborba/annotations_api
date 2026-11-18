//! Durable optional Cortex processing routes (`/me/content_jobs`).
use crate::{
    core::TRANSCRIPTION_LIMITS,
    domain::{App, content_jobs, notes::NoteError},
    presentation::notes::Owner,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{self, KeepAlive, Sse},
    },
    routing::{delete, get, post},
};
use futures_util::stream;
use serde_json::json;
use std::{collections::VecDeque, convert::Infallible};

pub fn routes() -> Router<App> {
    Router::new()
        .route("/me/content_jobs", post(create))
        .route("/me/content_jobs/{job_id}", get(detail))
        .route("/me/content_jobs/{job_id}/events", get(events))
        .route("/me/content_jobs/{job_id}/execution", delete(cancel))
        .layer(DefaultBodyLimit::max(1_048_576))
}

async fn create(
    State(app): State<App>,
    Owner(owner): Owner,
    body: Bytes,
) -> Result<Response, NoteError> {
    let (job, created) = content_jobs::create(&app.store, &owner, &body).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(job)).into_response())
}

async fn detail(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, NoteError> {
    Ok(Json(content_jobs::get(&app.store, &owner, &job_id).await?))
}

#[derive(serde::Deserialize)]
struct EventParams {
    after: Option<String>,
}

struct Cursor {
    app: App,
    owner: String,
    job_id: String,
    after: i64,
    queue: VecDeque<content_jobs::JobEvent>,
    finished: bool,
}

async fn events(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    Query(p): Query<EventParams>,
) -> Result<Response, NoteError> {
    let raw_after = p
        .after
        .as_deref()
        .or_else(|| headers.get("last-event-id").and_then(|v| v.to_str().ok()));
    let after = content_jobs::cursor(raw_after)?;
    content_jobs::check_events(&app.store, &owner, &job_id).await?;
    let start = Cursor {
        app,
        owner,
        job_id,
        after,
        queue: VecDeque::new(),
        finished: false,
    };
    let stream = stream::unfold(start, |mut c| async move {
        let mut first = true;
        loop {
            if let Some(event) = c.queue.pop_front() {
                c.after = event.seq;
                let item = sse::Event::default()
                    .id(event.seq.to_string())
                    .event(event.stage)
                    .data(json!({"seq": event.seq}).to_string());
                return Some((Ok::<_, Infallible>(item), c));
            }
            if c.finished {
                return None;
            }
            if !first {
                tokio::time::sleep(TRANSCRIPTION_LIMITS.event_poll).await;
            }
            first = false;
            match content_jobs::events(&c.app.store, &c.owner, &c.job_id, c.after).await {
                Ok((list, finished)) => {
                    c.queue.extend(list);
                    c.finished = finished;
                }
                Err(_) => return None,
            }
        }
    });
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(TRANSCRIPTION_LIMITS.keep_alive))
        .into_response();
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(response)
}

async fn cancel(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, NoteError> {
    Ok(Json(
        content_jobs::cancel(&app.store, &owner, &job_id).await?,
    ))
}
