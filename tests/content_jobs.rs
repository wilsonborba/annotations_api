//! Durable optional Cortex processing jobs (`/me/content_jobs`) with a Cortex double.
use annotations_api::{
    core::Result as CoreResult,
    dal::{
        Store,
        cortex::{BoxFuture, CortexClient, CortexOutput, OutputStatus, StartedChat},
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
    start_entered: AtomicBool,
    release_start: Notify,
    gate_start: AtomicBool,
    return_immediate: AtomicBool,
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
            start_entered: AtomicBool::new(false),
            release_start: Notify::new(),
            gate_start: AtomicBool::new(false),
            return_immediate: AtomicBool::new(false),
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
    ) -> BoxFuture<'a, CoreResult<StartedChat>> {
        Box::pin(async move {
            let n = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            if self.gate_start.load(Ordering::SeqCst) {
                self.start_entered.store(true, Ordering::SeqCst);
                self.release_start.notified().await;
            }
            Ok(StartedChat {
                session_id: format!("cx-sess-{n}"),
                output_id: format!("cx-out-{n}"),
                immediate_output: self.return_immediate.load(Ordering::SeqCst).then(|| {
                    CortexOutput {
                        status: OutputStatus::Completed,
                        text: "Synchronous result after cancellation".into(),
                        error_code: None,
                        error_message: None,
                    }
                }),
            })
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
                let saved: Option<i64> = sqlx::query_scalar(
                    "SELECT draft_saved FROM content_jobs WHERE cortex_session_id=$1",
                )
                .bind(session_id)
                .fetch_optional(&store.db)
                .await
                .unwrap();
                self.draft_was_saved_before_delete
                    .store(saved == Some(1), Ordering::SeqCst);
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
    for name in ["note-original", "content-job-draft"] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tests/fixtures/document_schema/families/{name}.v1.json"
        ));
        let draft: annotations_api::domain::schemas::Draft =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        annotations_api::domain::schemas::draft(&store, draft)
            .await
            .unwrap();
        annotations_api::domain::schemas::publish(&store, name, 1)
            .await
            .unwrap();
    }
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
    let (store, router) = setup(cortex.clone()).await;
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

    // A foreign account also cannot consume the event stream or cancel this job.
    for (method, path) in [
        ("GET", format!("/me/content_jobs/{job_id}/events")),
        ("DELETE", format!("/me/content_jobs/{job_id}/execution")),
    ] {
        let (status, _) = call(&router, method, &path, Some(B), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}");
    }

    // Let Cortex finish; verify CouchDB draft was saved BEFORE delete_chat was called.
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
            assert_eq!(cur["draft_schema_version"], 1);
            assert_eq!(cur["draft"]["format"], "markdown");
            assert_eq!(cur["draft"]["text"], "Extracted study summary draft");
            assert_eq!(cur["cortex_cleaned"], true);
            let doc_id = cur["draft_document_id"].as_str().unwrap();
            let doc = store.docs.get(doc_id).await.unwrap();
            assert_eq!(doc["schema_name"], "content-job-draft");
            assert_eq!(doc["body"]["text"], "Extracted study summary draft");
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
    assert_eq!(cancelled["draft"]["text"], "Partial study output");
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

#[tokio::test]
async fn cancellation_during_synchronous_start_cleans_remote_without_orphan_or_late_events() {
    let cortex = MockCortex::new();
    cortex.gate_start.store(true, Ordering::SeqCst);
    cortex.return_immediate.store(true, Ordering::SeqCst);
    let (store, router) = setup(cortex.clone()).await;

    let (status, started) = call(
        &router,
        "POST",
        "/me/content_jobs",
        Some(A),
        Some(json!({
            "client_job_id": "job-cancel-sync-start",
            "kind": "extraction",
            "text": "Run synchronously"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let job_id = started["job_id"].as_str().unwrap().to_string();

    for _ in 0..100 {
        if cortex.start_entered.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(cortex.start_entered.load(Ordering::SeqCst));

    let (cancel_status, cancelled) = call(
        &router,
        "DELETE",
        &format!("/me/content_jobs/{job_id}/execution"),
        Some(A),
        None,
    )
    .await;
    assert_eq!(cancel_status, StatusCode::OK, "{cancelled}");
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["draft_saved"], false);
    assert_eq!(cancelled["cortex_session_id"], Value::Null);
    assert_eq!(cancelled["cortex_cleaned"], false);

    cortex.release_start.notify_one();
    for _ in 0..100 {
        let (_, current) = call(
            &router,
            "GET",
            &format!("/me/content_jobs/{job_id}"),
            Some(A),
            None,
        )
        .await;
        if current["cortex_cleaned"] == true {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let (_, final_job) = call(
        &router,
        "GET",
        &format!("/me/content_jobs/{job_id}"),
        Some(A),
        None,
    )
    .await;
    assert_eq!(final_job["status"], "cancelled");
    assert_eq!(final_job["draft_saved"], false);
    assert_eq!(final_job["cortex_session_id"], "cx-sess-1");
    assert_eq!(final_job["cortex_output_id"], "cx-out-1");
    assert_eq!(final_job["cortex_cleaned"], true);
    let stages: Vec<String> =
        sqlx::query_scalar("SELECT stage FROM content_job_events WHERE job_id=$1 ORDER BY seq")
            .bind(&job_id)
            .fetch_all(&store.db)
            .await
            .unwrap();
    assert_eq!(stages, ["queued", "cancelled"]);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/me/content_jobs/{job_id}/events?after=0"))
                .header("x-uuid", A)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let event_body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let event_body = String::from_utf8(event_body.to_vec()).unwrap();
    assert!(event_body.contains("event: cancelled"));
    assert!(!event_body.contains("event: running"));
    assert!(!event_body.contains("event: completed"));
    assert!(
        store
            .docs
            .get(&format!("content-job-draft-{job_id}"))
            .await
            .is_err()
    );
    assert_eq!(cortex.deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn server_restart_resumes_started_job_without_second_cortex_call_and_fails_unstarted_job() {
    let cortex = MockCortex::new();
    let (store, router) = setup(cortex.clone()).await;

    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(A)
        .execute(&store.db)
        .await
        .unwrap();

    // Job 1: left running with Cortex session/output ids from before a server restart.
    sqlx::query(
        "INSERT INTO content_jobs(job_id,owner_id,client_job_id,kind,request_hash,prompt_text,status,cortex_session_id,cortex_output_id,event_cursor,created_at,updated_at) VALUES('job-resumable',$1,'c-res','study_notes',$2,'hi','running','cx-sess-restart','cx-out-restart',2,100,100)",
    )
    .bind(A)
    .bind("a".repeat(64))
    .execute(&store.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO content_job_events(job_id,seq,stage,created_at) VALUES('job-resumable',1,'queued',100),('job-resumable',2,'running',100)",
    )
    .execute(&store.db)
    .await
    .unwrap();

    // Job 2: left queued without Cortex ids (interrupted before start_chat).
    sqlx::query(
        "INSERT INTO content_jobs(job_id,owner_id,client_job_id,kind,request_hash,prompt_text,status,event_cursor,created_at,updated_at) VALUES('job-unstarted',$1,'c-unst','extraction',$2,'hi','queued',1,100,100)",
    )
    .bind(A)
    .bind("b".repeat(64))
    .execute(&store.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO content_job_events(job_id,seq,stage,created_at) VALUES('job-unstarted',1,'queued',100)",
    )
    .execute(&store.db)
    .await
    .unwrap();

    let handled = annotations_api::domain::content_jobs::resume_unfinished(&store)
        .await
        .unwrap();
    assert_eq!(handled, 2);

    // Unstarted job truthfully fails with server_restarted.
    let (_, failed) = call(
        &router,
        "GET",
        "/me/content_jobs/job-unstarted",
        Some(A),
        None,
    )
    .await;
    assert_eq!(failed["status"], "failed");
    assert_eq!(failed["error"]["code"], "server_restarted");

    // Started job resumes polling and finishes with zero duplicate upload/start calls.
    cortex.allow_finish.notify_waiters();
    for _ in 0..40 {
        let (_, cur) = call(
            &router,
            "GET",
            "/me/content_jobs/job-resumable",
            Some(A),
            None,
        )
        .await;
        if cur["status"] == "completed" {
            assert_eq!(cur["draft_saved"], true);
            assert_eq!(cur["draft"]["text"], "Extracted study summary draft");
            assert_eq!(cur["cortex_cleaned"], true);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(cortex.uploads.load(Ordering::SeqCst), 0);
    assert_eq!(cortex.starts.load(Ordering::SeqCst), 0);
    assert_eq!(cortex.deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_cortex_temporary_sync_201_result_persists_even_when_history_returns_404() {
    use annotations_api::dal::cortex::HttpCortex;
    use axum::{
        Json,
        extract::Path,
        http::HeaderMap,
        routing::{delete, get, post},
    };

    let history_hits = Arc::new(AtomicUsize::new(0));
    let history_hits_srv = history_hits.clone();
    let delete_hits = Arc::new(AtomicUsize::new(0));
    let delete_hits_srv = delete_hits.clone();

    let fake_cortex = Router::new()
        .route(
            "/files",
            post(|| async {
                (
                    StatusCode::CREATED,
                    Json(json!({"file_id": "cx-temp-file-1"})),
                )
            }),
        )
        .route(
            "/chats",
            post(
                |headers: HeaderMap, Json(payload): Json<Value>| async move {
                    assert_eq!(headers.get("x-uuid").unwrap(), A);
                    assert_eq!(payload["temporary"], true);
                    assert_eq!(payload["use_memory"], false);
                    (
                        StatusCode::CREATED,
                        Json(json!({
                            "session_id": "tmp-sess-1",
                            "output_id": "tmp-out-1",
                            "status": "done",
                            "text": "Immediate temporary summary"
                        })),
                    )
                },
            ),
        )
        .route(
            "/chats/{sid}/outputs/{oid}",
            get(move |Path((_sid, _oid)): Path<(String, String)>| {
                let hits = history_hits_srv.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    // Temporary outputs intentionally have no history/reconnect route.
                    (
                        StatusCode::NOT_FOUND,
                        Json(json!({"error": "temporary_output_not_persisted"})),
                    )
                }
            }),
        )
        .route(
            "/chats/{sid}",
            delete(move |Path(_sid): Path<String>| {
                let dels = delete_hits_srv.clone();
                async move {
                    dels.fetch_add(1, Ordering::SeqCst);
                    StatusCode::NO_CONTENT
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, fake_cortex).await.unwrap();
    });

    let mock = MockCortex::new();
    let (mut store, _) = setup(mock).await;
    store.cortex = HttpCortex::client(format!("http://{addr}"), reqwest::Client::new());
    let router = presentation::router(App {
        store: store.clone(),
    });

    let fid = upload_ready_text(&router, A, b"Temp source text").await;

    // Start job via public Notes route.
    let (st, started) = call(
        &router,
        "POST",
        "/me/content_jobs",
        Some(A),
        Some(json!({
            "client_job_id": "job-tmp-1",
            "kind": "study_notes",
            "text": "Summarize this",
            "file_ids": [fid]
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{started}");
    let job_id = started["job_id"].as_str().unwrap();

    let mut final_job = json!({});
    for _ in 0..40 {
        let (st_g, cur) = call(
            &router,
            "GET",
            &format!("/me/content_jobs/{job_id}"),
            Some(A),
            None,
        )
        .await;
        assert_eq!(st_g, StatusCode::OK);
        if matches!(cur["status"].as_str(), Some("completed" | "failed")) {
            final_job = cur;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    assert_eq!(final_job["status"], "completed", "{final_job}");
    assert_eq!(final_job["draft_saved"], true);
    assert_eq!(final_job["draft"]["text"], "Immediate temporary summary");
    assert_eq!(history_hits.load(Ordering::SeqCst), 0);
    assert_eq!(delete_hits.load(Ordering::SeqCst), 1);
}
