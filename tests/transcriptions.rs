//! Transcription runs over SQLite, the in-memory document store and a cloud runtime double.
use annotations_api::{
    dal::{Store, execution::CloudExecution},
    domain::{App, models, note_saves, publication, schemas, transcriptions},
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use sqlx::Row;
use std::{path::Path, sync::Arc, time::Duration};
use tower::ServiceExt;

const A: &str = "alice-account";
const B: &str = "bob-account";

async fn setup() -> (Store, Router) {
    let store = Store::fixture().await;
    for name in ["note-original", "note-typed"] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/document_schema/families")
            .join(format!("{name}.v1.json"));
        let draft: schemas::Draft =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        schemas::draft(&store, draft).await.unwrap();
        schemas::publish(&store, name, 1).await.unwrap();
    }
    sqlx::raw_sql("INSERT INTO languages(code,label) VALUES('en','English');")
        .execute(&store.db)
        .await
        .unwrap();
    for version in ["v1", "v2"] {
        let manifest: models::Manifest = serde_json::from_value(json!({
            "release_id": version, "code_revision": "rev", "dataset_snapshot_id": "snap",
            "source_checkpoint": "ckpt",
            "artifacts": [{"platform": "linux", "fsm_reference": format!("models/{version}"),
                "size": 10, "sha256": "0".repeat(64)}],
            "languages": ["en"], "capabilities": ["handwriting"], "metrics": {"en": {"cer": 0.1}}
        }))
        .unwrap();
        models::register(&store.db, manifest).await.unwrap();
    }
    models::activate(&store.db, "v1").await.unwrap();
    (store.clone(), router(store))
}
fn router(store: Store) -> Router {
    presentation::router(App { store })
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}
async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    if_match: Option<&str>,
    body: Option<Value>,
) -> Reply {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(owner) = owner {
        request = request.header("x-uuid", owner);
    }
    if let Some(v) = if_match {
        request = request.header("if-match", v);
    }
    let request = match body {
        Some(b) => request
            .header("content-type", "application/json")
            .body(Body::from(b.to_string())),
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    Reply {
        status,
        headers,
        body,
    }
}

async fn download(router: &Router, owner: &str, version: &str) -> String {
    let r = call(
        router,
        "POST",
        "/me/model_downloads",
        Some(owner),
        None,
        Some(json!({"model_version": version, "platform": "linux"})),
    )
    .await;
    assert!(r.status.is_success(), "{}", r.body);
    r.body["download_id"].as_str().unwrap().to_string()
}
fn ink() -> Value {
    json!({"document": {
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [{"id": "s1", "kind": "stroke", "tool": "round", "brush_version": 1,
            "width": 2, "color": "#000000FF", "opacity": 1,
            "samples": {"count": 2,"data": "AQAUFBQUAAo"},
            "nodes": [{"x": 10, "y": 10, "mode": "independent", "corner": false, "sample": 0}]}]
    }})
}
fn original(text: &str) -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [{"id": "t1", "kind": "text", "x": 0, "y": 40, "width": 304, "height": 120,
            "spans": [{"text": text}]}]
    })
}
fn typed(text: &str) -> Value {
    json!({"blocks": [{"id": "k1", "kind": "paragraph", "spans": [{"text": text}]}]})
}
fn local_run(download_id: &str, source: Value) -> Value {
    let mut body = json!({"download_id": download_id, "model_version": "v1", "language": "en",
        "mode": "local", "ink": ink()});
    for (k, v) in source.as_object().unwrap() {
        body[k] = v.clone();
    }
    body
}
fn result(event_id: &str, source: Value, text: &str) -> Value {
    let mut body = json!({"event_id": event_id, "document": typed(text),
        "runtime": {"name": "onnx", "version": "1.20", "elapsed_ms": 812}});
    for (k, v) in source.as_object().unwrap() {
        body[k] = v.clone();
    }
    body
}
async fn create(router: &Router, owner: &str, body: Value) -> Reply {
    call(
        router,
        "POST",
        "/me/transcriptions",
        Some(owner),
        None,
        Some(body),
    )
    .await
}
async fn submit(router: &Router, owner: &str, run_id: &str, body: Value) -> Reply {
    call(
        router,
        "PUT",
        &format!("/me/transcriptions/{run_id}/result"),
        Some(owner),
        None,
        Some(body),
    )
    .await
}
/// A note of A with an Original revision; returns (note id, original revision id).
async fn note(router: &Router, client: &str) -> (String, String) {
    let r = call(
        router,
        "POST",
        "/me/notes",
        Some(A),
        None,
        Some(
            json!({"client_note_id": client, "main_representation": "original",
            "representations": {"original": {"document": original("Ink")},
                                "typed": {"document": typed("Old typed")}}}),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    (
        r.body["note_id"].as_str().unwrap().to_string(),
        r.body["revisions"][0]["revision_id"]
            .as_str()
            .unwrap()
            .to_string(),
    )
}
async fn save(router: &Router, note_id: &str, revision: i64, body: Value) -> Reply {
    call(
        router,
        "PUT",
        &format!("/me/notes/{note_id}"),
        Some(A),
        Some(&revision.to_string()),
        Some(body),
    )
    .await
}

#[tokio::test]
async fn anonymous_calls_get_401_on_every_route() {
    let (_, router) = setup().await;
    for (method, uri) in [
        ("POST", "/me/transcriptions"),
        ("GET", "/me/transcriptions/r1"),
        ("GET", "/me/transcriptions/r1/events"),
        ("PUT", "/me/transcriptions/r1/result"),
        ("DELETE", "/me/transcriptions/r1/execution"),
    ] {
        let body = (method == "POST" || method == "PUT").then(|| json!({}));
        let r = call(&router, method, uri, None, None, body).await;
        assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{method} {uri}");
        assert_eq!(r.body["code"], "identity_required");
    }
}

#[tokio::test]
async fn create_checks_the_download_owner_version_language_and_inputs() {
    let (_, router) = setup().await;
    let bob = download(&router, B, "v1").await;
    let alice = download(&router, A, "v1").await;
    // Another user's download id fails like an unknown one.
    let r = create(&router, A, local_run(&bob, json!({"source_hash": "h1"}))).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.body["code"], "model_download_not_found");
    let mut body = local_run(&alice, json!({"source_hash": "h1"}));
    body["model_version"] = json!("v2");
    let r = create(&router, A, body).await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.body["code"], "model_version_mismatch");
    let mut body = local_run(&alice, json!({"source_hash": "h1"}));
    body["language"] = json!("th");
    assert_eq!(
        create(&router, A, body).await.body["code"],
        "unsupported_language"
    );
    let r = create(
        &router,
        A,
        local_run(
            &alice,
            json!({"source_hash": "h1", "source_revision_id": "x"}),
        ),
    )
    .await;
    assert_eq!(r.body["code"], "invalid_source");
    let mut body = local_run(&alice, json!({"source_hash": "h1"}));
    body.as_object_mut().unwrap().remove("ink");
    assert_eq!(
        create(&router, A, body).await.body["code"],
        "input_required"
    );
    let mut body = local_run(&alice, json!({"source_hash": "h1"}));
    body["owner_id"] = json!(B);
    assert_eq!(create(&router, A, body).await.body["code"], "unknown_field");
    let mut body = local_run(&alice, json!({"source_hash": "h1"}));
    body["ink"]["document"] = original("Not ink");
    assert_eq!(create(&router, A, body).await.body["code"], "invalid_ink");
    // A revision of another account is not a source.
    let (_, revision) = note(&router, "n1").await;
    let bob_run = local_run(&bob, json!({"source_revision_id": revision}));
    let r = create(&router, B, bob_run).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.body["code"], "source_not_found");
}

#[tokio::test]
async fn local_run_is_created_once_and_read_only_by_its_owner() {
    let (_, router) = setup().await;
    let alice = download(&router, A, "v1").await;
    let body = local_run(&alice, json!({"source_hash": "local-hash-1"}));
    let r = create(&router, A, body.clone()).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let run_id = r.body["run_id"].as_str().unwrap().to_string();
    assert_eq!(r.body["status"], "registered");
    assert_eq!(r.body["mode"], "local");
    assert_eq!(r.body["model_version"], "v1");
    assert_eq!(r.body["published"], false);
    assert_eq!(r.body["last_event_id"], 1);
    assert_eq!(r.body["inputs"]["ink_schema_version"], 1);
    // An identical retry reuses the run.
    let again = create(&router, A, body).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.body["run_id"], run_id.as_str());
    let uri = format!("/me/transcriptions/{run_id}");
    assert_eq!(
        call(&router, "GET", &uri, Some(A), None, None).await.status,
        StatusCode::OK
    );
    let r = call(&router, "GET", &uri, Some(B), None, None).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.body["code"], "transcription_not_found");
    for (method, path) in [
        ("DELETE", format!("{uri}/execution")),
        ("GET", format!("{uri}/events")),
    ] {
        let r = call(&router, method, &path, Some(B), None, None).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{method} {path}");
    }
    let r = submit(
        &router,
        B,
        &run_id,
        result("e1", json!({"source_hash": "local-hash-1"}), "x"),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn one_local_result_per_run_and_event_id_retries_return_the_first() {
    let (store, router) = setup().await;
    let alice = download(&router, A, "v1").await;
    let source = json!({"source_hash": "local-hash-2"});
    let run = create(&router, A, local_run(&alice, source.clone())).await;
    let run_id = run.body["run_id"].as_str().unwrap().to_string();
    let r = submit(
        &router,
        A,
        &run_id,
        result("e1", json!({"source_hash": "other"}), "x"),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "source_mismatch");
    let first = submit(
        &router,
        A,
        &run_id,
        result("e1", source.clone(), "Leaf cells"),
    )
    .await;
    assert_eq!(first.status, StatusCode::CREATED, "{}", first.body);
    assert_eq!(first.body["status"], "completed");
    assert_eq!(first.body["published"], false);
    assert_eq!(first.body["result"]["event_id"], "e1");
    assert_eq!(
        first.body["result"]["document"]["blocks"][0]["spans"][0]["text"],
        "Leaf cells"
    );
    assert_eq!(first.body["result"]["runtime"]["elapsed_ms"], 812);
    // A retry with the same event id returns the first result.
    let retry = submit(
        &router,
        A,
        &run_id,
        result("e1", source.clone(), "Leaf cells"),
    )
    .await;
    assert_eq!(retry.status, StatusCode::OK);
    assert_eq!(retry.body, first.body);
    // A second result fails.
    let r = submit(&router, A, &run_id, result("e2", source.clone(), "Other")).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "result_exists");
    let r = submit(&router, A, &run_id, result("e1", source.clone(), "Changed")).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "event_id_reused");
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM transcription_results")
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 1);
    // Reopening reuses the saved draft: an identical create returns the completed run.
    let reopen = create(&router, A, local_run(&alice, source)).await;
    assert_eq!(reopen.status, StatusCode::OK);
    assert_eq!(reopen.body["run_id"], run_id.as_str());
    assert_eq!(reopen.body["status"], "completed");
    assert_eq!(reopen.body["result"], first.body["result"]);
}

struct CloudDouble;
impl CloudExecution for CloudDouble {
    fn supports(&self, model_version: &str) -> bool {
        model_version == "v1"
    }
}

#[tokio::test]
async fn cloud_create_is_unavailable_and_cloud_results_are_server_owned() {
    let (store, router) = setup().await;
    let alice = download(&router, A, "v1").await;
    let mut body = local_run(&alice, json!({"source_hash": "h-cloud"}));
    body["mode"] = json!("cloud");
    let r = create(&router, A, body.clone()).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.body["code"], "cloud_execution_unavailable");
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM transcription_runs")
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
    // With a runtime adapter the run registers, and the caller cannot write its result.
    let mut with_runtime = store.clone();
    with_runtime.execution = Arc::new(CloudDouble);
    let router = self::router(with_runtime);
    let r = create(&router, A, body).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    assert_eq!(r.body["mode"], "cloud");
    let run_id = r.body["run_id"].as_str().unwrap();
    let r = submit(
        &router,
        A,
        run_id,
        result("e1", json!({"source_hash": "h-cloud"}), "x"),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "cloud_result_server_owned");
}

#[tokio::test]
async fn stale_source_result_is_marked_stale_and_never_applied_over_newer_work() {
    let (store, router) = setup().await;
    let alice = download(&router, A, "v1").await;
    let (note_id, o1) = note(&router, "n-stale").await;
    let source = json!({"source_revision_id": o1});
    let run = create(&router, A, local_run(&alice, source.clone())).await;
    assert_eq!(run.status, StatusCode::CREATED, "{}", run.body);
    assert_eq!(run.body["source_stale"], false);
    assert_eq!(run.body["source_note_id"], note_id.as_str());
    let run_id = run.body["run_id"].as_str().unwrap().to_string();
    // Newer Original work arrives before the result.
    let r = save(
        &router,
        &note_id,
        1,
        json!({"representation": "original", "document": original("Newer ink")}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let r = submit(&router, A, &run_id, result("e1", source, "From o1")).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    assert_eq!(r.body["source_stale"], true);
    // Accepting it over the newer Original fails and changes nothing.
    let r = save(
        &router,
        &note_id,
        2,
        json!({"representation": "typed", "document": typed("From o1"),
            "transcription_run_id": run_id}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "transcription_source_stale");
    let note = call(
        &router,
        "GET",
        &format!("/me/notes/{note_id}?view=typed"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(note.body["revision"], 2);
    assert!(note.body.to_string().contains("Old typed"));
    // The source revision stays for the run after the revision cleanup.
    note_saves::prune_revisions(&store, i64::MAX, 100)
        .await
        .unwrap();
    let kept: i64 = sqlx::query("SELECT COUNT(*) FROM note_revisions WHERE revision_id=$1")
        .bind(&o1)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(kept, 1);
}

#[tokio::test]
async fn accepted_typed_revision_records_the_source_and_public_read_shows_earlier_revision() {
    let (store, router) = setup().await;
    let alice = download(&router, A, "v1").await;
    let (note_id, o1) = note(&router, "n-accept").await;
    let source = json!({"source_revision_id": o1});
    let run = create(&router, A, local_run(&alice, source.clone())).await;
    let run_id = run.body["run_id"].as_str().unwrap().to_string();
    // Only a completed run can be accepted.
    let body = json!({"representation": "typed", "document": typed("Reviewed"),
        "transcription_run_id": run_id});
    let r = save(&router, &note_id, 1, body.clone()).await;
    assert_eq!(r.body["code"], "transcription_not_completed");
    let r = save(
        &router,
        &note_id,
        1,
        json!({"representation": "original", "document": original("x"),
            "transcription_run_id": run_id}),
    )
    .await;
    assert_eq!(r.body["code"], "source_on_original");
    let r = submit(&router, A, &run_id, result("e1", source, "Draft")).await;
    assert_eq!(r.status, StatusCode::CREATED);
    let r = save(&router, &note_id, 1, body).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let t2 = r.body["revisions"][0]["revision_id"].as_str().unwrap();
    let recorded: Option<String> =
        sqlx::query("SELECT source_revision_id FROM note_revisions WHERE revision_id=$1")
            .bind(t2)
            .fetch_one(&store.db)
            .await
            .unwrap()
            .get(0);
    assert_eq!(recorded.as_deref(), Some(o1.as_str()));
    // The run stays a private draft: nothing was published.
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM note_publications")
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
    // Publish both, change and republish Original: the public read shows the notice.
    let publish = |if_match: &'static str, reps: Value| {
        let router = router.clone();
        let note_id = note_id.clone();
        async move {
            call(
                &router,
                "PUT",
                &format!("/me/notes/{note_id}/publication"),
                Some(A),
                Some(if_match),
                Some(json!({"language": "en", "representations": reps})),
            )
            .await
        }
    };
    let r = publish("0", json!({"original": o1, "typed": t2})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let public = publication::public_note(&store, &note_id, None)
        .await
        .unwrap();
    assert_eq!(public["typed_uses_earlier_revision"], false);
    // Make the order of creation times explicit; the test runs within one second.
    sqlx::query("UPDATE note_revisions SET created_at=100 WHERE revision_id=$1")
        .bind(&o1)
        .execute(&store.db)
        .await
        .unwrap();
    let r = save(
        &router,
        &note_id,
        2,
        json!({"representation": "original", "document": original("Changed ink")}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    let o2 = r.body["revisions"][0]["revision_id"].as_str().unwrap();
    let r = publish("1", json!({"original": o2, "typed": t2})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let public = publication::public_note(&store, &note_id, None)
        .await
        .unwrap();
    assert_eq!(public["typed_uses_earlier_revision"], true);
}

#[tokio::test]
async fn cancel_stops_unfinished_runs_and_keeps_data_and_completed_drafts() {
    let (store, router) = setup().await;
    let alice = download(&router, A, "v1").await;
    sqlx::query("INSERT INTO files(file_id,owner_id,fsm_reference,sha256,size_bytes,content_type,purpose,status,created_at,updated_at,filename) VALUES('f1',$1,'media/f1',$2,10,'image/png','transcription','ready',1,1,'page.png')")
        .bind(A).bind("a".repeat(64)).execute(&store.db).await.unwrap();
    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(B)
        .execute(&store.db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO files(file_id,owner_id,fsm_reference,sha256,size_bytes,content_type,purpose,status,created_at,updated_at,filename) VALUES('f2',$1,'media/f2',$2,10,'image/png','transcription','ready',1,1,'page.png')")
        .bind(B).bind("b".repeat(64)).execute(&store.db).await.unwrap();
    let mut body = local_run(&alice, json!({"source_hash": "h-files"}));
    body["file_refs"] = json!(["f2"]);
    let r = create(&router, A, body.clone()).await;
    assert_eq!(r.body["code"], "unknown_file");
    body["file_refs"] = json!(["f1"]);
    let r = create(&router, A, body).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    assert_eq!(r.body["inputs"]["file_refs"], json!(["f1"]));
    let run_id = r.body["run_id"].as_str().unwrap().to_string();
    let uri = format!("/me/transcriptions/{run_id}/execution");
    let r = call(&router, "DELETE", &uri, Some(A), None, None).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["status"], "cancelled");
    assert_eq!(r.body["last_event_id"], 2);
    // Training inputs stay: the ink document and the file reference.
    let ink_doc = format!("transcription-ink-{run_id}");
    assert!(store.docs.get(&ink_doc).await.is_ok());
    assert_eq!(r.body["inputs"]["file_refs"], json!(["f1"]));
    let r = call(&router, "DELETE", "/me/files/f1", Some(A), None, None).await;
    assert_ne!(r.status, StatusCode::NO_CONTENT);
    // A cancelled run takes no result; a second cancel changes nothing.
    let r = submit(
        &router,
        A,
        &run_id,
        result("e1", json!({"source_hash": "h-files"}), "x"),
    )
    .await;
    assert_eq!(r.body["code"], "run_cancelled");
    let r = call(&router, "DELETE", &uri, Some(A), None, None).await;
    assert_eq!(r.body["last_event_id"], 2);

    // Cancel of a completed run keeps the draft.
    let source = json!({"source_hash": "h-done"});
    let r = create(&router, A, local_run(&alice, source.clone())).await;
    let done = r.body["run_id"].as_str().unwrap().to_string();
    submit(&router, A, &done, result("e1", source, "Kept")).await;
    let r = call(
        &router,
        "DELETE",
        &format!("/me/transcriptions/{done}/execution"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["status"], "completed");
    assert_eq!(
        r.body["result"]["document"]["blocks"][0]["spans"][0]["text"],
        "Kept"
    );
}

/// The `id:` values of an SSE body.
fn ids(text: &str) -> Vec<i64> {
    text.lines()
        .filter_map(|l| l.strip_prefix("id:"))
        .map(|v| v.trim().parse().unwrap())
        .collect()
}

#[tokio::test]
async fn event_stream_resumes_after_the_cursor_without_duplicates() {
    let (store, router) = setup().await;
    let alice = download(&router, A, "v1").await;
    let source = json!({"source_hash": "h-events"});
    let r = create(&router, A, local_run(&alice, source.clone())).await;
    let run_id = r.body["run_id"].as_str().unwrap().to_string();
    submit(&router, A, &run_id, result("e1", source, "x")).await;
    let uri = format!("/me/transcriptions/{run_id}/events");
    let r = call(&router, "GET", &uri, Some(A), None, None).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.headers["content-type"], "text/event-stream");
    assert_eq!(r.headers["x-accel-buffering"], "no");
    let text = r.body.as_str().unwrap().to_string();
    assert_eq!(ids(&text), vec![1, 2]);
    assert!(text.contains("event: registered") || text.contains("event:registered"));
    assert!(!text.contains('%'));
    let r = call(
        &router,
        "GET",
        &format!("{uri}?after=1"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(ids(r.body.as_str().unwrap()), vec![2]);
    let r = call(
        &router,
        "GET",
        &format!("{uri}?after=2"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(ids(r.body.as_str().unwrap()), Vec::<i64>::new());
    let r = call(
        &router,
        "GET",
        &format!("{uri}?after=-1"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.body["code"], "invalid_cursor");

    // A live stream of an unfinished run delivers a later stage, then ends.
    let r = create(
        &router,
        A,
        local_run(&alice, json!({"source_hash": "h-live"})),
    )
    .await;
    let live = r.body["run_id"].as_str().unwrap().to_string();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/me/transcriptions/{live}/events?after=1"))
                .header("x-uuid", A)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    transcriptions::cancel(&store, A, &live).await.unwrap();
    let mut text = String::new();
    while let Ok(Some(chunk)) = tokio::time::timeout(Duration::from_secs(5), body.next()).await {
        text.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert_eq!(ids(&text), vec![2]);
    assert!(text.contains("cancelled"));
}
