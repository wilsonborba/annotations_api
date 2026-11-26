//! Owner working-copy routes (`/me/notes`) through the HTTP router. SQLite stands in
//! for PostgreSQL and the in-memory document store for CouchDB.
use annotations_api::{
    dal::Store,
    domain::{App, schemas},
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use serde_json::{Value, json};
use sqlx::Row;
use std::path::Path;
use tower::ServiceExt;

const A: &str = "user-a";

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
    sqlx::query("INSERT INTO languages(code,label) VALUES('en','English')")
        .execute(&store.db)
        .await
        .unwrap();
    let router = presentation::router(App {
        store: store.clone(),
    });
    (store, router)
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
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> Reply {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(owner) = owner {
        request = request.header("x-uuid", owner);
    }
    for (k, v) in headers {
        request = request.header(*k, *v);
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
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    Reply {
        status,
        headers,
        body,
    }
}

fn original(x: i64) -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id": "t1", "kind": "text", "x": x, "y": 40, "width": 304, "height": 120,
             "spans": [{"text": "Photosynthesis"}]}
        ]
    })
}
async fn create(router: &Router, owner: &str, body: Value) -> Reply {
    call(router, "POST", "/me/notes", Some(owner), &[], Some(body)).await
}
async fn create_original(router: &Router, owner: &str, client: &str, document: Value) -> String {
    let r = create(
        router,
        owner,
        json!({"client_note_id": client, "representations": {"original": {"document": document}}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    assert_eq!(r.headers["etag"], "\"1\"");
    r.body["note_id"].as_str().unwrap().to_string()
}
async fn count(store: &Store, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get::<i64, _>(0)
}
use annotations_api::dal::reports::{BoxFuture, CreatedReportMetadata, MockReports, ReportsClient};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

async fn peer_note(router: &Router, client: &str) -> String {
    create_original(router, A, client, original(0)).await
}

#[tokio::test]
async fn peer_old_predictable_preseed_cannot_be_promoted_and_receipt_is_opaque() {
    let (mut store, _) = setup().await;
    let reports = MockReports::new();
    store.reports = reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });
    let note = peer_note(&router, "peer-preseed-note").await;
    let op = "peer-preseed-op";
    let old_key = format!("notes:{note}:{op}");
    let item = json!({"title":"Synthetic chart","template_id":"bar","template_version":1});
    let preexisting = reports
        .create_owned_item(A, "peer-project", &old_key, item.clone())
        .await
        .unwrap();
    let created = call(
        &router,
        "POST",
        &format!("/me/notes/{note}/reports"),
        Some(A),
        &[],
        Some(json!({"mode":"create","operation_key":op,"project":"peer-project","item":item})),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    assert_ne!(
        created.body["report_item_id"], preexisting.id,
        "must not promote the caller-preseeded old predictable item"
    );
    let receipt=sqlx::query("SELECT idempotency_key, project, request_digest FROM note_report_create_receipts WHERE note_id=$1 AND owner_id=$2 AND operation_key=$3").bind(&note).bind(A).bind(op).fetch_one(&store.db).await.unwrap();
    let key: String = receipt.get("idempotency_key");
    assert_ne!(key, old_key);
    assert_ne!(key, op);
    assert!(uuid::Uuid::parse_str(&key).is_ok());
    assert_eq!(receipt.get::<String, _>("project"), "peer-project");
    assert_eq!(receipt.get::<String, _>("request_digest").len(), 64);
    let promoted=sqlx::query("SELECT COUNT(*) FROM note_report_references WHERE report_item_id=$1 AND relation_kind='created_for_note'").bind(&preexisting.id).fetch_one(&store.db).await.unwrap().get::<i64,_>(0);
    assert_eq!(promoted, 0);
}

#[tokio::test]
async fn peer_project_change_is_409_before_any_second_reports_call() {
    let (mut store, _) = setup().await;
    let reports = MockReports::new();
    store.reports = reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });
    let note = peer_note(&router, "peer-project-note").await;
    let item = json!({"title":"Synthetic chart","template_id":"bar","template_version":1});
    let first=call(&router,"POST",&format!("/me/notes/{note}/reports"),Some(A),&[],Some(json!({"mode":"create","operation_key":"peer-project-op","project":"project-a","item":item}))).await;
    assert_eq!(first.status, StatusCode::CREATED);
    let before = reports.create_calls_count();
    let changed=call(&router,"POST",&format!("/me/notes/{note}/reports"),Some(A),&[],Some(json!({"mode":"create","operation_key":"peer-project-op","project":"project-b","item":item}))).await;
    assert_eq!(changed.status, StatusCode::CONFLICT);
    assert_eq!(changed.body["code"], "operation_key_conflict");
    assert_eq!(reports.create_calls_count(), before);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_report_references").await,
        1
    );
}

struct GatedReports {
    inner: Arc<MockReports>,
    entered: Notify,
    release: Notify,
    gate_once: AtomicBool,
}
impl ReportsClient for GatedReports {
    fn owned_item_metadata<'a>(
        &'a self,
        owner: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, annotations_api::core::Result<CreatedReportMetadata>> {
        self.inner.owned_item_metadata(owner, id)
    }
    fn create_owned_item<'a>(
        &'a self,
        owner: &'a str,
        project: &'a str,
        key: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, annotations_api::core::Result<CreatedReportMetadata>> {
        Box::pin(async move {
            let meta = self
                .inner
                .create_owned_item(owner, project, key, payload)
                .await?;
            if self.gate_once.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(meta)
        })
    }
    fn verify_owned_item<'a>(
        &'a self,
        owner: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, annotations_api::core::Result<()>> {
        self.inner.verify_owned_item(owner, id)
    }
    fn delete_owned_item<'a>(
        &'a self,
        owner: &'a str,
        id: &'a str,
        confirm: bool,
    ) -> BoxFuture<'a, annotations_api::core::Result<()>> {
        self.inner.delete_owned_item(owner, id, confirm)
    }
}
async fn assert_create_delete_race(cascade: bool) {
    let (mut store, _) = setup().await;
    let reports = Arc::new(GatedReports {
        inner: MockReports::new(),
        entered: Notify::new(),
        release: Notify::new(),
        gate_once: AtomicBool::new(true),
    });
    store.reports = reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });
    let note = peer_note(&router, "peer-race-note").await;
    let create_router = router.clone();
    let create_uri = format!("/me/notes/{note}/reports");
    let creation = tokio::spawn(async move {
        call(&create_router,"POST",&create_uri,Some(A),&[],Some(json!({"mode":"create","operation_key":"peer-race-op","project":"peer-project","item":{"title":"Synthetic race chart","template_id":"bar","template_version":1}}))).await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reports.entered.notified(),
    )
    .await
    .unwrap();
    assert_eq!(
        reports.inner.create_calls_count(),
        1,
        "remote creation must have happened before note deletion"
    );
    let delete_router = router.clone();
    let delete_uri = format!("/me/notes/{note}?delete_owned_reports={cascade}");
    let mut deletion = tokio::spawn(async move {
        call(
            &delete_router,
            "DELETE",
            &delete_uri,
            Some(A),
            &[("if-match", "1")],
            None,
        )
        .await
    });
    let early_delete = tokio::time::timeout(std::time::Duration::from_millis(100), &mut deletion)
        .await
        .ok()
        .map(Result::unwrap);
    let deleted_while_create_paused = early_delete.is_some();
    reports.release.notify_one();
    let created = tokio::time::timeout(std::time::Duration::from_secs(2), creation)
        .await
        .unwrap()
        .unwrap();
    let deleted = match early_delete {
        Some(reply) => reply,
        None => tokio::time::timeout(std::time::Duration::from_secs(2), deletion)
            .await
            .unwrap()
            .unwrap(),
    };
    if cascade {
        assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
        assert_eq!(deleted.body["note_deleted"], true);
    } else {
        assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    }
    let refs = count(&store, "SELECT COUNT(*) FROM note_report_references").await;
    let cleanup = count(&store, "SELECT COUNT(*) FROM note_report_cleanup").await;
    let notes = count(&store, "SELECT COUNT(*) FROM notes").await;
    eprintln!(
        "ACTUAL_CREATE_DELETE_RACE create_status={} notes={} refs={} cleanup={} downstream_deletes={}",
        created.status,
        notes,
        refs,
        cleanup,
        reports.inner.delete_calls().len()
    );
    assert_eq!(notes, 0);
    assert_eq!(
        refs, 0,
        "a deleted note must not receive a ready created_for_note reference after remote create returns"
    );
    if deleted_while_create_paused {
        assert_ne!(
            created.status,
            StatusCode::CREATED,
            "must not report ready creation after note deletion already completed"
        );
    }
    if cascade {
        assert!(cleanup > 0 || !reports.inner.delete_calls().is_empty());
        assert!(
            reports
                .inner
                .verify_owned_item(A, "rep-created-1")
                .await
                .is_err()
        );
    } else {
        assert_eq!(cleanup, 0);
        assert!(reports.inner.delete_calls().is_empty());
        reports
            .inner
            .verify_owned_item(A, "rep-created-1")
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn peer_report_create_racing_note_delete_never_leaves_ready_or_orphan_reference() {
    assert_create_delete_race(true).await;
}
#[tokio::test]
async fn keep_reports_racing_create_leaves_no_note_reference_and_never_deletes_report() {
    assert_create_delete_race(false).await;
}

fn report_body(operation: Option<&str>) -> Value {
    let mut body = json!({"mode":"create","project":"peer-project","item":{"title":"Synthetic chart","template_id":"bar","template_version":1}});
    if let Some(key) = operation {
        body["operation_key"] = key.into();
    }
    body
}

#[tokio::test]
async fn keyless_lost_reply_reuses_pending_receipt_and_cascade_waits_for_recovery() {
    let (mut store, _) = setup().await;
    let reports = MockReports::new();
    store.reports = reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });
    let note = peer_note(&router, "keyless-recovery").await;
    let uri = format!("/me/notes/{note}/reports");
    reports.set_drop_next_create_response(true);
    let lost = call(&router, "POST", &uri, Some(A), &[], Some(report_body(None))).await;
    assert_eq!(lost.status, StatusCode::SERVICE_UNAVAILABLE);
    let blocked = call(
        &router,
        "DELETE",
        &format!("/me/notes/{note}?delete_owned_reports=true"),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(blocked.status, StatusCode::CONFLICT);
    assert_eq!(blocked.body["code"], "report_creation_pending");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 1);
    let recovered = call(&router, "POST", &uri, Some(A), &[], Some(report_body(None))).await;
    assert_eq!(recovered.status, StatusCode::CREATED, "{}", recovered.body);
    assert_eq!(recovered.body["report_item_id"], "rep-created-1");
    assert_eq!(reports.create_calls_count(), 2);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_report_create_receipts").await,
        1
    );
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM note_report_create_receipts WHERE status='recorded'"
        )
        .await,
        1
    );
    let distinct = call(&router, "POST", &uri, Some(A), &[], Some(report_body(None))).await;
    assert_eq!(distinct.status, StatusCode::CREATED);
    assert_ne!(
        distinct.body["report_item_id"], recovered.body["report_item_id"],
        "two completed keyless intents remain distinct"
    );
}

#[tokio::test]
async fn unknown_create_can_still_delete_note_with_existing_keep_reports_choice() {
    let (mut store, _) = setup().await;
    let reports = MockReports::new();
    store.reports = reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });
    let note = peer_note(&router, "unknown-keep").await;
    reports.set_drop_next_create_response(true);
    let lost = call(
        &router,
        "POST",
        &format!("/me/notes/{note}/reports"),
        Some(A),
        &[],
        Some(report_body(Some("unknown-keep-op"))),
    )
    .await;
    assert_eq!(lost.status, StatusCode::SERVICE_UNAVAILABLE);
    let deleted = call(
        &router,
        "DELETE",
        &format!("/me/notes/{note}?delete_owned_reports=false"),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 0);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_report_references").await,
        0
    );
    assert!(reports.delete_calls().is_empty());
    reports.verify_owned_item(A, "rep-created-1").await.unwrap();
}

#[tokio::test]
async fn recorded_retry_never_recreates_item_after_reports_deleted_its_idempotency_row() {
    let (mut store, _) = setup().await;
    let reports = MockReports::new();
    store.reports = reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });
    let note = peer_note(&router, "recorded-deleted").await;
    let uri = format!("/me/notes/{note}/reports");
    let body = report_body(Some("recorded-op"));
    let first = call(&router, "POST", &uri, Some(A), &[], Some(body.clone())).await;
    assert_eq!(first.status, StatusCode::CREATED);
    let id = first.body["report_item_id"].as_str().unwrap();
    let replay = call(&router, "POST", &uri, Some(A), &[], Some(body.clone())).await;
    assert_eq!(replay.status, StatusCode::CREATED);
    assert_eq!(
        reports.create_calls_count(),
        1,
        "recorded retry reads metadata instead of POST"
    );
    reports.delete_owned_item(A, id, false).await.unwrap();
    let missing = call(&router, "POST", &uri, Some(A), &[], Some(body)).await;
    assert_ne!(missing.status, StatusCode::CREATED);
    assert_eq!(reports.create_calls_count(), 1);
    assert!(reports.verify_owned_item(A, id).await.is_err());
}

struct ValidationReports {
    inner: Arc<MockReports>,
    reject: AtomicBool,
    calls: std::sync::atomic::AtomicUsize,
}
impl ReportsClient for ValidationReports {
    fn create_owned_item<'a>(
        &'a self,
        owner: &'a str,
        project: &'a str,
        key: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, annotations_api::core::Result<CreatedReportMetadata>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.reject.load(Ordering::SeqCst) {
                return Err(annotations_api::core::Error::bad("invalid_report_item"));
            }
            self.inner
                .create_owned_item(owner, project, key, payload)
                .await
        })
    }
    fn owned_item_metadata<'a>(
        &'a self,
        owner: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, annotations_api::core::Result<CreatedReportMetadata>> {
        self.inner.owned_item_metadata(owner, id)
    }
    fn verify_owned_item<'a>(
        &'a self,
        owner: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, annotations_api::core::Result<()>> {
        self.inner.verify_owned_item(owner, id)
    }
    fn delete_owned_item<'a>(
        &'a self,
        owner: &'a str,
        id: &'a str,
        confirm: bool,
    ) -> BoxFuture<'a, annotations_api::core::Result<()>> {
        self.inner.delete_owned_item(owner, id, confirm)
    }
}
#[tokio::test]
async fn rejected_receipt_preserves_binding_allows_matching_retry_and_does_not_block_delete() {
    let (mut store, _) = setup().await;
    let reports = Arc::new(ValidationReports {
        inner: MockReports::new(),
        reject: AtomicBool::new(true),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    store.reports = reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });
    let note = peer_note(&router, "rejected-note").await;
    let uri = format!("/me/notes/{note}/reports");
    let body = report_body(Some("rejected-op"));
    let rejected = call(&router, "POST", &uri, Some(A), &[], Some(body.clone())).await;
    assert_eq!(rejected.status, StatusCode::UNPROCESSABLE_ENTITY);
    let key: String = sqlx::query(
        "SELECT idempotency_key FROM note_report_create_receipts WHERE status='rejected'",
    )
    .fetch_one(&store.db)
    .await
    .unwrap()
    .get(0);
    let mut changed = body.clone();
    changed["item"]["title"] = "Different".into();
    let conflict = call(&router, "POST", &uri, Some(A), &[], Some(changed)).await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(reports.calls.load(Ordering::SeqCst), 1);
    reports.reject.store(false, Ordering::SeqCst);
    let accepted = call(&router, "POST", &uri, Some(A), &[], Some(body)).await;
    assert_eq!(accepted.status, StatusCode::CREATED, "{}", accepted.body);
    let replay_key: String = sqlx::query(
        "SELECT idempotency_key FROM note_report_create_receipts WHERE status='recorded'",
    )
    .fetch_one(&store.db)
    .await
    .unwrap()
    .get(0);
    assert_eq!(key, replay_key);
    reports.reject.store(true, Ordering::SeqCst);
    let second = peer_note(&router, "rejected-delete").await;
    let rejected = call(
        &router,
        "POST",
        &format!("/me/notes/{second}/reports"),
        Some(A),
        &[],
        Some(report_body(Some("rejected-delete-op"))),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::UNPROCESSABLE_ENTITY);
    let deleted = call(
        &router,
        "DELETE",
        &format!("/me/notes/{second}?delete_owned_reports=true"),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
}

#[tokio::test]
async fn actual_http_delete_outcomes_control_cleanup_and_malformed_200_never_means_deleted() {
    use axum::{Json, routing::delete};
    use std::sync::Mutex;
    let outcome = Arc::new(Mutex::new((
        StatusCode::OK,
        json!({"deleted":true,"requires_confirmation":false}),
    )));
    let responder = outcome.clone();
    let fake = Router::new().route(
        "/me/items/{id}",
        delete(move |headers: HeaderMap| {
            let response = responder.lock().unwrap().clone();
            async move {
                assert_eq!(headers["x-uuid"], A);
                (response.0, Json(response.1))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, fake).await.unwrap() });
    let cases = [
        (
            StatusCode::CONFLICT,
            json!({"deleted":false,"requires_confirmation":true,"reference_count":2}),
            Some("reused_confirmation_required"),
        ),
        (
            StatusCode::OK,
            json!({"deleted":false,"requires_confirmation":true}),
            Some("reused_confirmation_required"),
        ),
        (
            StatusCode::OK,
            json!({"unknown":true}),
            Some("reports_unavailable"),
        ),
        (
            StatusCode::OK,
            json!({"deleted":true,"requires_confirmation":false}),
            None,
        ),
        (StatusCode::NOT_FOUND, json!({"error":"missing"}), None),
    ];
    for (index, (status, body, error)) in cases.into_iter().enumerate() {
        let (mut store, _) = setup().await;
        let memory = MockReports::new();
        store.reports = memory;
        let create_router = presentation::router(App {
            store: store.clone(),
        });
        let note = peer_note(&create_router, &format!("http-delete-{index}")).await;
        let created = call(
            &create_router,
            "POST",
            &format!("/me/notes/{note}/reports"),
            Some(A),
            &[],
            Some(report_body(Some("http-delete-op"))),
        )
        .await;
        assert_eq!(created.status, StatusCode::CREATED);
        let item_id = created.body["report_item_id"].as_str().unwrap();
        store.reports = annotations_api::dal::reports::HttpReports::client(
            format!("http://{addr}"),
            reqwest::Client::new(),
        );
        let router = presentation::router(App {
            store: store.clone(),
        });
        *outcome.lock().unwrap() = (status, body);
        let deleted = call(
            &router,
            "DELETE",
            &format!("/me/notes/{note}?delete_owned_reports=true"),
            Some(A),
            &[("if-match", "1")],
            None,
        )
        .await;
        assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
        let state: String =
            sqlx::query("SELECT status FROM note_report_cleanup WHERE report_item_id=$1")
                .bind(item_id)
                .fetch_one(&store.db)
                .await
                .unwrap()
                .get(0);
        if let Some(code) = error {
            assert_eq!(state, "failed");
            assert_eq!(deleted.body["failed_report_deletions"][0]["error"], code);
            assert_eq!(deleted.body["deleted_report_ids"], json!([]));
            *outcome.lock().unwrap() = (
                StatusCode::OK,
                json!({"deleted":true,"requires_confirmation":false}),
            );
            let retried = call(
                &router,
                "POST",
                &format!("/me/notes/{note}/reports/retry-cleanup"),
                Some(A),
                &[],
                None,
            )
            .await;
            assert_eq!(retried.status, StatusCode::OK);
            assert_eq!(retried.body["deleted_report_ids"], json!([item_id]));
        } else {
            assert_eq!(state, "deleted");
            assert_eq!(deleted.body["deleted_report_ids"], json!([item_id]));
        }
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn owned_metadata_get_requires_exact_owner_and_item_identity() {
    use axum::{Json, routing::get};
    use std::sync::Mutex;
    let item = Arc::new(Mutex::new(
        json!({"id":"item-1","owner_id":A,"project":"peer-project"}),
    ));
    let responder = item.clone();
    let fake = Router::new().route(
        "/me/items/{id}",
        get(move |headers: HeaderMap| {
            let body = responder.lock().unwrap().clone();
            async move {
                assert_eq!(headers["x-uuid"], A);
                Json(json!({"item":body}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, fake).await.unwrap() });
    let client = annotations_api::dal::reports::HttpReports::client(
        format!("http://{addr}"),
        reqwest::Client::new(),
    );
    let meta = client.owned_item_metadata(A, "item-1").await.unwrap();
    assert_eq!(meta.id, "item-1");
    assert_eq!(meta.project, "peer-project");
    item.lock().unwrap()["owner_id"] = "another-owner".into();
    assert!(client.owned_item_metadata(A, "item-1").await.is_err());
    item.lock().unwrap()["owner_id"] = A.into();
    item.lock().unwrap()["id"] = "another-item".into();
    assert!(client.owned_item_metadata(A, "item-1").await.is_err());
    server.abort();
    let _ = server.await;
}
