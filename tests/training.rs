//! Correction feedback over SQLite and the in-memory document store, and the training
//! export and removal commands.
use annotations_api::{
    core::TRAINING_LIMITS,
    dal::{Store, documents::Documents},
    domain::{App, models, schemas},
    presentation::{
        self,
        cli::{self, Command, Target},
    },
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sqlx::Row;
use std::path::Path;
use tower::ServiceExt;

const A: &str = "alice-account";
const B: &str = "bob-account";

fn family(name: &str) -> schemas::Draft {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/document_schema/families")
        .join(format!("{name}.v1.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}
async fn setup_with(feedback_schema: bool) -> (Store, Router) {
    let store = Store::fixture().await;
    let mut names = vec!["note-original", "note-typed"];
    if feedback_schema {
        names.push("training-feedback");
    }
    for name in names {
        schemas::draft(&store, family(name)).await.unwrap();
        schemas::publish(&store, name, 1).await.unwrap();
    }
    sqlx::raw_sql("INSERT INTO languages(code,label) VALUES('en','English');")
        .execute(&store.db)
        .await
        .unwrap();
    let manifest: models::Manifest = serde_json::from_value(json!({
        "release_id": "v1", "code_revision": "rev", "dataset_snapshot_id": "snap",
        "source_checkpoint": "ckpt",
        "artifacts": [{"platform": "linux", "fsm_reference": "models/v1",
            "size": 10, "sha256": "0".repeat(64)}],
        "languages": ["en"], "capabilities": ["handwriting"], "metrics": {"en": {"cer": 0.1}}
    }))
    .unwrap();
    models::register(&store.db, manifest).await.unwrap();
    models::activate(&store.db, "v1").await.unwrap();
    let router = presentation::router(App {
        store: store.clone(),
    });
    (store, router)
}
async fn setup() -> (Store, Router) {
    setup_with(true).await
}

struct Reply {
    status: StatusCode,
    body: Value,
}
async fn raw(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    if_match: Option<&str>,
    body: Option<String>,
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
            .body(Body::from(b)),
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    Reply { status, body }
}
async fn call(router: &Router, method: &str, uri: &str, owner: &str, body: Value) -> Reply {
    raw(
        router,
        method,
        uri,
        Some(owner),
        None,
        Some(body.to_string()),
    )
    .await
}

fn ink() -> Value {
    json!({"document": {
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [{"id": "s1", "kind": "stroke", "tool": "round", "brush_version": 1,
            "width": 2, "color": "#000000FF", "opacity": 1,
            "samples": {"x": [10, 20], "y": [10, 20], "t": [0, 5]},
            "nodes": [{"x": 10, "y": 10, "mode": "independent", "corner": false, "sample": 0}]}]
    }})
}
/// A completed local run of `owner` with the given source; returns the run id.
async fn completed_run(router: &Router, owner: &str, source: Value) -> String {
    let r = call(
        router,
        "POST",
        "/me/model_downloads",
        owner,
        json!({"model_version": "v1", "platform": "linux"}),
    )
    .await;
    assert!(r.status.is_success(), "{}", r.body);
    let download = r.body["download_id"].as_str().unwrap().to_string();
    let mut body = json!({"download_id": download, "model_version": "v1", "language": "en",
        "mode": "local", "ink": ink()});
    let mut result = json!({"event_id": "r1", "runtime": {"name": "onnx", "version": "1.20",
        "elapsed_ms": 812}, "document": {"blocks": [{"id": "k1", "kind": "paragraph",
        "spans": [{"text": "helo"}]}]}});
    for (k, v) in source.as_object().unwrap() {
        body[k] = v.clone();
        result[k] = v.clone();
    }
    let r = call(router, "POST", "/me/transcriptions", owner, body).await;
    assert!(r.status.is_success(), "{}", r.body);
    let run_id = r.body["run_id"].as_str().unwrap().to_string();
    let r = call(
        router,
        "PUT",
        &format!("/me/transcriptions/{run_id}/result"),
        owner,
        result,
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    run_id
}
fn event(id: &str, stage: &str, correction: &str) -> Value {
    json!({"event_id": id, "segment_id": "k1", "source_item_ids": ["s1"],
        "strokes": [{"x": [1000, 2000], "y": [1000, 2000], "t": [0, 5], "pressure": [500, 600]}],
        "suggestion": "helo", "correction": correction, "stage": stage,
        "language": "en", "model_version": "v1"})
}
async fn feedback(router: &Router, owner: &str, run_id: &str, events: Vec<Value>) -> Reply {
    call(
        router,
        "POST",
        &format!("/me/transcriptions/{run_id}/feedback"),
        owner,
        json!({"events": events}),
    )
    .await
}
async fn rows(store: &Store) -> Vec<(String, String, String, String)> {
    sqlx::query(
        "SELECT run_id,event_id,stage,status FROM training_feedback ORDER BY run_id,event_id",
    )
    .fetch_all(&store.db)
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    .collect()
}
fn documents(store: &Store) -> usize {
    match &store.docs {
        Documents::Memory(m) => m
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with("training-feedback-"))
            .count(),
        _ => unreachable!(),
    }
}
async fn export(store: &Store, include_flagged: bool) -> Vec<Value> {
    let mut out = Vec::new();
    let summary = cli::training(store, Command::TrainingExport { include_flagged }, &mut out)
        .await
        .unwrap();
    let lines: Vec<Value> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        summary,
        format!("Exported training examples: {}", lines.len())
    );
    lines
}
async fn remove(store: &Store, target: Target) -> String {
    cli::training(store, Command::TrainingRemove(target), &mut Vec::new())
        .await
        .unwrap()
}

#[tokio::test]
async fn anonymous_feedback_gets_401() {
    let (_, router) = setup().await;
    let r = raw(
        &router,
        "POST",
        "/me/transcriptions/r1/feedback",
        None,
        None,
        Some(json!({"events": [event("e1", "accepted", "hello")]}).to_string()),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert_eq!(r.body["code"], "identity_required");
}

#[tokio::test]
async fn feedback_for_another_users_run_fails_and_stores_nothing() {
    let (store, router) = setup().await;
    let run = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    let r = feedback(&router, B, &run, vec![event("e1", "accepted", "hello")]).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.body["code"], "transcription_not_found");
    let r = feedback(
        &router,
        A,
        "missing-run",
        vec![event("e1", "accepted", "hello")],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(rows(&store).await.is_empty());
    assert_eq!(documents(&store), 0);
}

#[tokio::test]
async fn malformed_oversize_and_non_finite_payloads_are_rejected() {
    let (store, router) = setup().await;
    let run = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    let uri = format!("/me/transcriptions/{run}/feedback");
    let send = |text: String| raw(&router, "POST", &uri, Some(A), None, Some(text));

    // Request level.
    let r = send("{".into()).await;
    assert_eq!(
        (r.status, r.body["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid_json"))
    );
    let text = r#"{"events": [{"event_id": "e1", "segment_id": "k1", "strokes": [{"x": [1e999], "y": [1], "t": [0]}], "suggestion": "a", "correction": "b", "stage": "accepted", "language": "en", "model_version": "v1"}]}"#;
    let r = send(text.into()).await;
    assert_eq!(r.body["code"], "non_finite_number");
    let big = "x".repeat(TRAINING_LIMITS.max_request_bytes);
    let r = send(json!({"events": [event("e1", "accepted", &big)]}).to_string()).await;
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(r.body["code"], "request_too_large");
    let r = send(json!({"events": [], "owner_id": B}).to_string()).await;
    assert_eq!(r.body["code"], "unknown_field");
    let r = send(json!({"events": []}).to_string()).await;
    assert_eq!(r.body["code"], "empty_array");
    let many: Vec<Value> = (0..=TRAINING_LIMITS.max_events)
        .map(|i| event(&format!("e{i}"), "accepted", "hello"))
        .collect();
    let r = send(json!({"events": many}).to_string()).await;
    assert_eq!(r.body["code"], "too_many_events");
    let r = send(
        json!({"events": [event("e1", "accepted", "a"), event("e1", "accepted", "b")]}).to_string(),
    )
    .await;
    assert_eq!(r.body["code"], "duplicate_event_id");

    // Event level: (change, expected code).
    let long = "a".repeat(TRAINING_LIMITS.max_text_chars + 1);
    let cases: Vec<(Value, &str)> = vec![
        (json!({"correction": null}), "missing_field"),
        (json!({"correction": 5}), "invalid_type"),
        (json!({"correction": long}), "text_too_long"),
        (json!({"correction": "a\u{0007}b"}), "invalid_text"),
        (json!({"stage": "final"}), "invalid_variant"),
        (json!({"event_id": "has space"}), "invalid_reference"),
        (json!({"user_id": B}), "unknown_field"),
        (json!({"language": "th"}), "language_mismatch"),
        (json!({"model_version": "v9"}), "model_version_mismatch"),
        (
            json!({"source_item_ids": ["not-in-ink"]}),
            "unknown_source_item",
        ),
        (
            json!({"strokes": [{"x": [1.5], "y": [1], "t": [0]}]}),
            "invalid_type",
        ),
        (
            json!({"strokes": [{"x": [-1], "y": [1], "t": [0]}]}),
            "out_of_range",
        ),
        (
            json!({"strokes": [{"x": [TRAINING_LIMITS.max_coordinate + 1], "y": [1], "t": [0]}]}),
            "out_of_range",
        ),
        (
            json!({"strokes": [{"x": [1, 2], "y": [1], "t": [0, 1]}]}),
            "invalid_count",
        ),
        (
            json!({"strokes": [{"x": [1, 2], "y": [1, 2], "t": [0, 1], "pressure": [5, 1001]}]}),
            "out_of_range",
        ),
        (
            json!({"strokes": [{"x": [1, 2], "y": [1, 2], "t": [3, 1]}]}),
            "invalid_sample_order",
        ),
        (
            json!({"strokes": [{"x": [1], "y": [1], "t": [TRAINING_LIMITS.max_sample_ms + 1]}]}),
            "invalid_sample_order",
        ),
        (
            json!({"strokes": [{"x": [], "y": [], "t": []}]}),
            "empty_array",
        ),
        (
            json!({"strokes": [{"x": [1], "y": [1], "t": [0], "z": [1]}]}),
            "unknown_field",
        ),
        (
            json!({"strokes": vec![json!({"x": [1], "y": [1], "t": [0]}); TRAINING_LIMITS.max_strokes + 1]}),
            "too_many_strokes",
        ),
        (
            json!({"strokes": [{"x": vec![1; TRAINING_LIMITS.max_samples_per_stroke + 1], "y": [1], "t": [0]}]}),
            "too_many_samples",
        ),
    ];
    for (change, code) in cases {
        let mut e = event("e1", "accepted", "hello");
        for (k, v) in change.as_object().unwrap() {
            e[k] = v.clone();
        }
        let r = feedback(&router, A, &run, vec![e]).await;
        assert_eq!(
            r.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{change}: {}",
            r.body
        );
        assert_eq!(r.body["code"], code, "{change}: {}", r.body);
        assert!(
            r.body["error"].as_str().unwrap().contains("events[0]"),
            "{}",
            r.body
        );
    }
    // A late time inside the order also fails on its bound.
    let mut e = event("e1", "accepted", "hello");
    e["strokes"] = json!([{"x": [1, 2], "y": [1, 2], "t": [0, TRAINING_LIMITS.max_sample_ms + 1]}]);
    let r = feedback(&router, A, &run, vec![e]).await;
    assert_eq!(r.body["code"], "out_of_range");
    // An unknown schema version is rejected.
    let r = call(
        &router,
        "POST",
        &uri,
        A,
        json!({"schema_version": 2, "events": [event("e1", "accepted", "hello")]}),
    )
    .await;
    assert_eq!(r.body["code"], "schema_not_published");
    assert!(rows(&store).await.is_empty());
    assert_eq!(documents(&store), 0);
}

#[tokio::test]
async fn feedback_needs_a_published_schema_and_a_completed_run() {
    let (store, router) = setup_with(false).await;
    let run = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    let r = feedback(&router, A, &run, vec![event("e1", "accepted", "hello")]).await;
    assert_eq!(r.body["code"], "schema_not_published");
    // A feedback schema version without a validator cannot be drafted.
    let mut draft = family("training-feedback");
    draft.version = 2;
    assert!(schemas::draft(&store, draft).await.is_err());

    let (_, router) = setup().await;
    let r = call(
        &router,
        "POST",
        "/me/model_downloads",
        A,
        json!({"model_version": "v1", "platform": "linux"}),
    )
    .await;
    let download = r.body["download_id"].as_str().unwrap();
    let r = call(
        &router,
        "POST",
        "/me/transcriptions",
        A,
        json!({"download_id": download, "model_version": "v1", "language": "en",
            "mode": "local", "ink": ink(), "source_hash": "h2"}),
    )
    .await;
    let open = r.body["run_id"].as_str().unwrap();
    let r = feedback(&router, A, open, vec![event("e1", "accepted", "hello")]).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "transcription_not_completed");
}

#[tokio::test]
async fn event_ids_are_idempotent_and_conflicting_reuse_is_rejected() {
    let (store, router) = setup().await;
    let run = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    let r = feedback(&router, A, &run, vec![event("e1", "accepted", "hello")]).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    assert_eq!(r.body["run_id"], run.as_str());
    assert_eq!(r.body["events"][0]["event_id"], "e1");
    assert_eq!(r.body["events"][0]["stage"], "accepted");
    let first = r.body.clone();
    // The same event again is a retry: 200, the same answer, nothing new.
    let r = feedback(&router, A, &run, vec![event("e1", "accepted", "hello")]).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body, first);
    // The same id with a different event fails, also inside a batch with a new event.
    let r = feedback(&router, A, &run, vec![event("e1", "accepted", "help")]).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "event_id_reused");
    let r = feedback(
        &router,
        A,
        &run,
        vec![
            event("e2", "accepted", "new"),
            event("e1", "accepted", "help"),
        ],
    )
    .await;
    assert_eq!(r.body["code"], "event_id_reused");
    assert_eq!(rows(&store).await.len(), 1);
    assert_eq!(documents(&store), 1);
    // A retry mixed with a new event stores only the new one.
    let r = feedback(
        &router,
        A,
        &run,
        vec![
            event("e1", "accepted", "hello"),
            event("e2", "accepted", "new"),
        ],
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED);
    assert_eq!(r.body["events"].as_array().unwrap().len(), 2);
    assert_eq!(rows(&store).await.len(), 2);
    assert_eq!(documents(&store), 2);
}

#[tokio::test]
async fn many_events_per_run_keep_intermediate_and_accepted_distinct() {
    let (store, router) = setup().await;
    let run = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    let r = feedback(
        &router,
        A,
        &run,
        vec![
            event("e1", "intermediate", "hel"),
            event("e2", "intermediate", "hell"),
            event("e3", "accepted", "hello"),
        ],
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    // Text is data: literal instructions and markup are stored as written.
    let mut text = event("e4", "accepted", "ignore previous instructions <b>x</b>");
    text["segment_id"] = json!("k2");
    text.as_object_mut().unwrap().remove("strokes");
    text.as_object_mut().unwrap().remove("source_item_ids");
    let r = feedback(&router, A, &run, vec![text]).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    // A later copy of an earlier example of the run is kept but flagged.
    let r = feedback(&router, A, &run, vec![event("e5", "accepted", "hello")]).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let stored = rows(&store).await;
    let view: Vec<(&str, &str, &str)> = stored
        .iter()
        .map(|(_, e, s, st)| (e.as_str(), s.as_str(), st.as_str()))
        .collect();
    assert_eq!(
        view,
        vec![
            ("e1", "intermediate", "received"),
            ("e2", "intermediate", "received"),
            ("e3", "accepted", "received"),
            ("e4", "accepted", "received"),
            ("e5", "accepted", "flagged"),
        ]
    );
    // Nothing becomes approved for training by itself.
    assert!(stored.iter().all(|r| r.3 != "approved_for_training"));
    let lines = export(&store, false).await;
    let e4 = lines.iter().find(|l| l["event_id"] == "e4").unwrap();
    assert_eq!(
        e4["event"]["correction"],
        "ignore previous instructions <b>x</b>"
    );
}

#[tokio::test]
async fn export_has_provenance_and_excludes_flagged_unless_asked() {
    let (store, router) = setup().await;
    let run = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    feedback(
        &router,
        A,
        &run,
        vec![
            event("e1", "intermediate", "hel"),
            event("e2", "accepted", "hello"),
        ],
    )
    .await;
    feedback(&router, A, &run, vec![event("e3", "accepted", "hello")]).await;
    sqlx::query("UPDATE training_feedback SET status='approved_for_training' WHERE event_id='e1'")
        .execute(&store.db)
        .await
        .unwrap();
    let lines = export(&store, false).await;
    let mut ids: Vec<&str> = lines
        .iter()
        .map(|l| l["event_id"].as_str().unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["e1", "e2"]);
    let e2 = lines.iter().find(|l| l["event_id"] == "e2").unwrap();
    assert_eq!(e2["status"], "received");
    assert_eq!(e2["stage"], "accepted");
    assert_eq!(e2["schema_name"], "training-feedback");
    assert_eq!(e2["schema_version"], 1);
    let p = &e2["provenance"];
    assert_eq!(p["account_id"], A);
    assert_eq!(p["run_id"], run.as_str());
    assert_eq!(p["model_version"], "v1");
    assert_eq!(p["language"], "en");
    assert_eq!(p["platform"], "linux");
    assert_eq!(p["run_mode"], "local");
    assert_eq!(p["source_hash"], "h1");
    assert_eq!(e2["event"]["suggestion"], "helo");
    assert_eq!(e2["event"]["correction"], "hello");
    assert_eq!(e2["event"]["strokes"][0]["pressure"][1], 600);
    let e1 = lines.iter().find(|l| l["event_id"] == "e1").unwrap();
    assert_eq!(e1["status"], "approved_for_training");
    let all = export(&store, true).await;
    assert_eq!(all.len(), 3);
    assert!(
        all.iter()
            .any(|l| l["event_id"] == "e3" && l["status"] == "flagged")
    );
}

#[tokio::test]
async fn note_deletion_keeps_examples_and_remove_by_note_deletes_them() {
    let (store, router) = setup().await;
    let r = call(
        &router,
        "POST",
        "/me/notes",
        A,
        json!({"client_note_id": "n1", "main_representation": "original",
            "representations": {"original": {"document": {
                "page": {"width": 1000, "background": {"kind": "blank"}}, "items": []}}}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let note_id = r.body["note_id"].as_str().unwrap().to_string();
    let revision = r.body["revisions"][0]["revision_id"]
        .as_str()
        .unwrap()
        .to_string();
    let run = completed_run(&router, A, json!({"source_revision_id": revision})).await;
    let r = feedback(&router, A, &run, vec![event("e1", "accepted", "hello")]).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let other = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    feedback(&router, A, &other, vec![event("e1", "accepted", "hello")]).await;

    let r = raw(
        &router,
        "DELETE",
        &format!("/me/notes/{note_id}"),
        Some(A),
        Some("1"),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT, "{}", r.body);
    assert_eq!(rows(&store).await.len(), 2);
    assert_eq!(documents(&store), 2);
    let lines = export(&store, false).await;
    let kept = lines
        .iter()
        .find(|l| l["provenance"]["run_id"] == run.as_str())
        .unwrap();
    assert_eq!(kept["provenance"]["source_note_id"], note_id.as_str());
    assert!(kept["provenance"]["source_revision_id"].is_null());

    assert_eq!(
        remove(&store, Target::Note(note_id)).await,
        "Removed training examples: 1"
    );
    let left = rows(&store).await;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].0, other);
    assert_eq!(documents(&store), 1);
}

#[tokio::test]
async fn remove_deletes_the_selected_examples_and_only_them() {
    let (store, router) = setup().await;
    let a1 = completed_run(&router, A, json!({"source_hash": "h1"})).await;
    let a2 = completed_run(&router, A, json!({"source_hash": "h2"})).await;
    let b1 = completed_run(&router, B, json!({"source_hash": "h1"})).await;
    for run in [&a1, &a2] {
        feedback(
            &router,
            A,
            run,
            vec![
                event("e1", "intermediate", "hel"),
                event("e2", "accepted", "hello"),
            ],
        )
        .await;
    }
    feedback(&router, B, &b1, vec![event("e1", "accepted", "hello")]).await;
    assert_eq!(rows(&store).await.len(), 5);

    assert_eq!(
        remove(&store, Target::Run(a1.clone())).await,
        "Removed training examples: 2"
    );
    let left = rows(&store).await;
    assert_eq!(left.len(), 3);
    assert!(left.iter().all(|r| r.0 != a1));
    assert_eq!(documents(&store), 3);
    // Run data and the run itself stay.
    let r = raw(
        &router,
        "GET",
        &format!("/me/transcriptions/{a1}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["status"], "completed");

    assert_eq!(
        remove(&store, Target::Account(A.into())).await,
        "Removed training examples: 2"
    );
    let left = rows(&store).await;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].0, b1);
    assert_eq!(documents(&store), 1);
    assert_eq!(
        remove(&store, Target::Account("nobody".into())).await,
        "Removed training examples: 0"
    );
    assert_eq!(rows(&store).await.len(), 1);
}

#[test]
fn training_commands_parse_strictly() {
    let args = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert!(matches!(
        cli::parse(&args(&["training", "export"])).unwrap(),
        Some(Command::TrainingExport {
            include_flagged: false
        })
    ));
    assert!(matches!(
        cli::parse(&args(&["training", "export", "--include-flagged"])).unwrap(),
        Some(Command::TrainingExport {
            include_flagged: true
        })
    ));
    assert!(matches!(
        cli::parse(&args(&["training", "remove", "--run", "r1"])).unwrap(),
        Some(Command::TrainingRemove(Target::Run(id))) if id == "r1"
    ));
    assert!(matches!(
        cli::parse(&args(&["training", "remove", "--note", "n1"])).unwrap(),
        Some(Command::TrainingRemove(Target::Note(_)))
    ));
    assert!(matches!(
        cli::parse(&args(&["training", "remove", "--account", "a1"])).unwrap(),
        Some(Command::TrainingRemove(Target::Account(_)))
    ));
    for bad in [
        &["training"][..],
        &["training", "remove"],
        &["training", "remove", "--run"],
        &["training", "remove", "--run", "r1", "--note", "n1"],
        &["training", "export", "--all"],
    ] {
        assert!(cli::parse(&args(bad)).is_err(), "{bad:?}");
    }
    assert!(
        cli::parse(&args(&["training", "export"]))
            .unwrap()
            .unwrap()
            .training()
    );
}
