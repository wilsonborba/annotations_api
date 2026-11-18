//! Durable optional Cortex processing jobs (`/me/content_jobs`) with a Cortex double.
use annotations_api::{
    core::Result as CoreResult,
    dal::{
        Store,
        cortex::{BoxFuture, CortexClient, CortexOutput, OutputStatus},
    },
    domain::App,
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tower::ServiceExt;

const A: &str = "user-a";
const B: &str = "user-b";

struct MockCortex {
    uploads: AtomicUsize,
    starts: AtomicUsize,
    deletes: AtomicUsize,
    allow_finish: Notify,
    fail_delete: AtomicBool,
    draft_was_saved_before_delete: AtomicBool,
    store_ref: std::sync::Mutex<Option<Store>>,
}

impl MockCortex {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            uploads: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            allow_finish: Notify::new(),
            fail_delete: AtomicBool::new(false),
            draft_was_saved_before_delete: AtomicBool::new(false),
            store_ref: std::sync::Mutex::new(None),
        })
    }
}

impl CortexClient for MockCortex {
    fn is_configured(&self) -> bool {
        true
    }
    fn upload_file<'a>(
        &'a self,
        _owner: &'a str,
        _filename: &'a str,
        _content_type: &'a str,
        _bytes: Vec<u8>,
    ) -> BoxFuture<'a, CoreResult<String>> {
        Box::pin(async move {
            let n = self.uploads.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(format!("cx-file-{n}"))
        })
    }
    fn start_chat<'a>(
        &'a self,
        _owner: &'a str,
        _text: &'a str,
        _file_ids: Vec<String>,
    ) -> BoxFuture<'a, CoreResult<(String, String)>> {
        Box::pin(async move {
            let n = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((format!("cx-sess-{n}"), format!("cx-out-{n}")))
        })
    }
    fn get_output<'a>(
        &'a self,
        _owner: &'a str,
        _session_id: &'a str,
        _output_id: &'a str,
    ) -> BoxFuture<'a, CoreResult<CortexOutput>> {
        Box::pin(async move {
            self.allow_finish.notified().await;
            Ok(CortexOutput {
                status: OutputStatus::Completed,
                text: "Extracted study summary draft".into(),
                error_code: None,
                error_message: None,
            })
        })
    }
    fn cancel_output<'a>(
        &'a self,
        _owner: &'a str,
        _session_id: &'a str,
        _output_id: &'a str,
    ) -> BoxFuture<'a, CoreResult<CortexOutput>> {
        Box::pin(async move {
            Ok(CortexOutput {
                status: OutputStatus::Cancelled,
                text: "Partial study output".into(),
                error_code: None,
                error_message: None,
            })
        })
    }
    fn delete_chat<'a>(
        &'a self,
        _owner: &'a str,
        session_id: &'a str,
    ) -> BoxFuture<'a, CoreResult<()>> {
        Box::pin(async move {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            let store = self.store_ref.lock().unwrap().clone();
            if let Some(store) = store {
                let saved: i64 = sqlx::query_scalar(
                    "SELECT draft_saved FROM content_jobs WHERE cortex_session_id=$1",
                )
                .bind(session_id)
                .fetch_one(&store.db)
                .await
                .unwrap();
                self.draft_was_saved_before_delete
                    .store(saved == 1, Ordering::SeqCst);
            }
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(annotations_api::core::Error::unavailable());
            }
            Ok(())
        })
    }
}

async fn setup(cortex: Arc<MockCortex>) -> (Store, Router) {
    let mut store = Store::fixture().await;
    store.cortex = cortex.clone();
    *cortex.store_ref.lock().unwrap() = Some(store.clone());
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/document_schema/families/note-original.v1.json");
    let draft: annotations_api::domain::schemas::Draft =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    annotations_api::domain::schemas::draft(&store, draft)
        .await
        .unwrap();
    annotations_api::domain::schemas::publish(&store, "note-original", 1)
        .await
        .unwrap();
    let router = presentation::router(App {
        store: store.clone(),
    });
    (store, router)
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(o) = owner {
        req = req.header("x-uuid", o);
    }
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    let resp = router
        .clone()
        .oneshot(
            req.body(Body::from(body.map(|v| v.to_string()).unwrap_or_default()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn upload_ready_text(router: &Router, owner: &str, text: &[u8]) -> String {
    let sha = format!("{:x}", Sha256::digest(text));
    let (status, res) = call(
        router,
        "POST",
        "/me/files",
        Some(owner),
        Some(json!({
            "filename": "notes.txt",
            "content_type": "text/plain",
            "size": text.len(),
            "checksum": sha,
            "purpose": "content_job"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{res}");
    let fid = res["file_id"].as_str().unwrap().to_string();
    let req = Request::builder()
        .method("PUT")
        .uri(format!("/me/files/{fid}/content"))
        .header("x-uuid", owner)
        .header("content-type", "text/plain")
        .body(Body::from(text.to_vec()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    fid
}

#[tokio::test]
async fn default_disabled_returns_503() {
    let store = Store::fixture().await;
    let router = presentation::router(App { store });
    let (status, body) = call(
        &router,
        "POST",
        "/me/content_jobs",
        Some(A),
        Some(json!({"client_job_id": "j1", "kind": "extraction", "text": "hello"})),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "cortex_unavailable");
}

#[tokio::test]
async fn refresh_returns_same_job_without_duplicate_cortex_call_and_saves_draft_before_cleanup() {
    let cortex = MockCortex::new();
    let (_store, router) = setup(cortex.clone()).await;
    let fid = upload_ready_text(&router, A, b"Study material").await;

    let payload = json!({
        "client_job_id": "job-refresh-1",
        "kind": "study_notes",
        "text": "Summarize this",
        "file_ids": [fid]
    });

    let (s1, j1) = call(
        &router,
        "POST",
        "/me/content_jobs",
        Some(A),
        Some(payload.clone()),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "{j1}");
    let job_id = j1["job_id"].as_str().unwrap().to_string();

    // Wait briefly for upload + start_chat to run on background task.
    for _ in 0..40 {
        let (_, cur) = call(
            &router,
            "GET",
            &format!("/me/content_jobs/{job_id}"),
            Some(A),
            None,
        )
        .await;
        if cur["status"] == "running" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Refresh / retry with same client_job_id: returns 200 and same job_id without a second Cortex call.
    let (s2, j2) = call(&router, "POST", "/me/content_jobs", Some(A), Some(payload)).await;
    assert_eq!(s2, StatusCode::OK, "{j2}");
    assert_eq!(j2["job_id"], job_id.as_str());
    assert_eq!(cortex.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(cortex.starts.load(Ordering::SeqCst), 1);

    // Other user cannot read the job.
    let (other_s, _) = call(
        &router,
        "GET",
        &format!("/me/content_jobs/{job_id}"),
        Some(B),
        None,
    )
    .await;
    assert_eq!(other_s, StatusCode::NOT_FOUND);

    // Let Cortex finish; verify draft was saved BEFORE delete_chat was called.
    cortex.allow_finish.notify_waiters();
    for _ in 0..40 {
        let (_, cur) = call(
            &router,
            "GET",
            &format!("/me/content_jobs/{job_id}"),
            Some(A),
            None,
        )
        .await;
        if cur["status"] == "completed" {
            assert_eq!(cur["draft_saved"], true);
            assert_eq!(cur["draft_text"], "Extracted study summary draft");
            assert_eq!(cur["cortex_cleaned"], true);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(cortex.draft_was_saved_before_delete.load(Ordering::SeqCst));
    assert_eq!(cortex.deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_preserves_partial_draft_and_leaves_note_unchanged() {
    let cortex = MockCortex::new();
    let (_store, router) = setup(cortex.clone()).await;

    let (s, note) = call(
        &router,
        "POST",
        "/me/notes",
        Some(A),
        Some(json!({
            "client_note_id": "n-keep",
            "main_representation": "original",
            "representations": {
                "original": {"document": {"page": {"width": 1000, "background": {"kind": "blank"}}, "items": []}}
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{note}");
    let note_id = note["note_id"].as_str().unwrap().to_string();

    let (s, job) = call(
        &router,
        "POST",
        "/me/content_jobs",
        Some(A),
        Some(json!({
            "client_job_id": "job-cancel-1",
            "kind": "extraction",
            "text": "Extract",
            "source_note_id": note_id
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{job}");
    let job_id = job["job_id"].as_str().unwrap().to_string();

    for _ in 0..40 {
        let (_, cur) = call(
            &router,
            "GET",
            &format!("/me/content_jobs/{job_id}"),
            Some(A),
            None,
        )
        .await;
        if cur["status"] == "running" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let (cs, cancelled) = call(
        &router,
        "DELETE",
        &format!("/me/content_jobs/{job_id}/execution"),
        Some(A),
        None,
    )
    .await;
    assert_eq!(cs, StatusCode::OK, "{cancelled}");
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["draft_saved"], true);
    assert_eq!(cancelled["draft_text"], "Partial study output");
    assert_eq!(cancelled["cortex_cleaned"], true);

    // Note revision is untouched.
    let (_, note_after) = call(
        &router,
        "GET",
        &format!("/me/notes/{note_id}"),
        Some(A),
        None,
    )
    .await;
    assert_eq!(note_after["revision"], 1);
}
