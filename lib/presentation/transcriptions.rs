//! Owner transcription run routes under `/me/transcriptions`. The owner is the gateway
//! identity only. Business rules live in `domain::transcriptions`.
use crate::{
    core::TRANSCRIPTION_LIMITS,
    domain::{
        App,
        notes::{self, NoteError},
        training,
        transcriptions::{self, Event},
    },
    presentation::notes::Owner,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderValue, StatusCode},
    response::{
        IntoResponse, Response, Sse,
        sse::{self, KeepAlive},
    },
    routing::{delete, get, post, put},
};
use futures_util::stream;
use serde_json::Value;
use std::{collections::VecDeque, convert::Infallible};

pub fn routes() -> Router<App> {
    Router::new()
        .route("/me/transcriptions", post(create))
        .route("/me/transcriptions/{run_id}", get(detail))
        .route("/me/transcriptions/{run_id}/events", get(events))
        .route("/me/transcriptions/{run_id}/result", put(result))
        .route("/me/transcriptions/{run_id}/execution", delete(cancel))
        .route("/me/transcriptions/{run_id}/feedback", post(feedback))
        .layer(DefaultBodyLimit::max(notes::MAX_BODY_BYTES + 1))
}

fn status(created: bool) -> StatusCode {
    if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    }
}
/// 201 for a new run, 200 when an identical request reuses its live run.
async fn create(
    State(app): State<App>,
    Owner(owner): Owner,
    body: Bytes,
) -> Result<Response, NoteError> {
    let (run, created) = transcriptions::create(&app.store, &owner, &body).await?;
    Ok((status(created), Json(run)).into_response())
}
async fn detail(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        transcriptions::get(&app.store, &owner, &run_id).await?,
    ))
}
/// 201 for the accepted result, 200 when a retry with the same event id returns it.
async fn result(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(run_id): Path<String>,
    body: Bytes,
) -> Result<Response, NoteError> {
    let (run, created) = transcriptions::submit(&app.store, &owner, &run_id, &body).await?;
    Ok((status(created), Json(run)).into_response())
}
/// 201 when at least one event is new, 200 when every event is a retry of a stored one.
async fn feedback(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(run_id): Path<String>,
    body: Bytes,
) -> Result<Response, NoteError> {
    let (events, created) = training::submit(&app.store, &owner, &run_id, &body).await?;
    Ok((status(created), Json(events)).into_response())
}
async fn cancel(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, NoteError> {
    Ok(Json(
        transcriptions::cancel(&app.store, &owner, &run_id).await?,
    ))
}

#[derive(serde::Deserialize)]
struct EventParams {
    after: Option<String>,
}
struct Cursor {
    app: App,
    owner: String,
    run_id: String,
    after: i64,
    queue: VecDeque<Event>,
    finished: bool,
}
/// Server-side stages as Server-Sent Events. The SSE id is the event id; reconnect
/// with `after=<last id>` to resume without duplicates. The stream ends after the last
/// stage of a finished run.
async fn events(
    State(app): State<App>,
    Owner(owner): Owner,
    Path(run_id): Path<String>,
    Query(p): Query<EventParams>,
) -> Result<Response, NoteError> {
    let after = transcriptions::cursor(p.after.as_deref())?;
    transcriptions::check_events(&app.store, &owner, &run_id).await?;
    let start = Cursor {
        app,
        owner,
        run_id,
        after,
        queue: VecDeque::new(),
        finished: false,
    };
    let events = stream::unfold(start, |mut c| async move {
        let mut first = true;
        loop {
            if let Some(event) = c.queue.pop_front() {
                c.after = event.id;
                let item = sse::Event::default()
                    .id(event.id.to_string())
                    .event(event.stage)
                    .data(event.data.to_string());
                return Some((Ok::<_, Infallible>(item), c));
            }
            if c.finished {
                return None;
            }
            if !first {
                tokio::time::sleep(TRANSCRIPTION_LIMITS.event_poll).await;
            }
            first = false;
            match transcriptions::events(&c.app.store, &c.owner, &c.run_id, c.after).await {
                Ok((list, finished)) => {
                    c.queue.extend(list);
                    c.finished = finished;
                }
                // Storage failure: end the stream; the client resumes with its cursor.
                Err(_) => return None,
            }
        }
    });
    let mut response = Sse::new(events)
        .keep_alive(KeepAlive::new().interval(TRANSCRIPTION_LIMITS.keep_alive))
        .into_response();
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(response)
}
