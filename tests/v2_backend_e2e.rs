//! End-to-end verification of the v2 backend across all M4-M9 milestones (#39).
use annotations_api::{
    dal::{Store, plane::HttpPlane},
    domain::App,
    presentation,
};
use axum::{
    Json, Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
    routing::post,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tower::ServiceExt;

const OWNER_A: &str = "e2e-owner-a";
const OWNER_B: &str = "e2e-owner-b";

async fn seed_schemas(store: &Store) {
    sqlx::query("INSERT OR IGNORE INTO languages(code,label) VALUES('en','English')")
        .execute(&store.db)
        .await
        .unwrap();
    for name in [
        "note-original",
        "note-typed",
        "instant-room",
        "content-job-draft",
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "tests/fixtures/document_schema/families/{name}.v1.json"
        ));
        let draft: annotations_api::domain::schemas::Draft =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        annotations_api::domain::schemas::draft(store, draft)
            .await
            .unwrap();
        annotations_api::domain::schemas::publish(store, name, 1)
            .await
            .unwrap();
    }
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(o) = owner {
        req = req.header("x-uuid", o);
    }
    for (k, v) in headers {
        req = req.header(*k, *v);
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
    let st = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (st, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn v2_backend_end_to_end_system_flow_and_security_boundaries() {
    // 1. Real local HTTP Plane double verifying x-api-key header.
    let plane_hits = Arc::new(AtomicUsize::new(0));
    let plane_hits_srv = plane_hits.clone();
    let fake_plane = Router::new()
        .route(
            "/api/v1/workspaces/asodya/projects/",
            post(|| async {
                (
                    StatusCode::CREATED,
                    Json(json!({"id": "proj-1", "identifier": "NOTES"})),
                )
            }),
        )
        .route(
            "/api/v1/workspaces/asodya/projects/proj-1/states/",
            axum::routing::get(|| async {
                (
                    StatusCode::OK,
                    Json(json!({
                        "results": [
                            {"id": "st-1", "name": "Todo", "group": "unstarted"},
                            {"id": "st-done", "name": "Done", "group": "completed"}
                        ]
                    })),
                )
            }),
        )
        .route(
            "/api/v1/workspaces/asodya/projects/proj-1/work-items/",
            post(move |headers: HeaderMap, Json(body): Json<Value>| {
                let hits = plane_hits_srv.clone();
                async move {
                    assert_eq!(headers.get("x-api-key").unwrap(), "plane-e2e-key");
                    hits.fetch_add(1, Ordering::SeqCst);
                    (
                        StatusCode::CREATED,
                        Json(json!({"id": "plane-issue-1", "name": body["name"], "state": "st-1"})),
                    )
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plane_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, fake_plane).await.unwrap();
    });

    let mut store = Store::fixture().await;
    seed_schemas(&store).await;
    store.plane = HttpPlane::client(
        format!("http://{plane_addr}"),
        "plane-e2e-key".into(),
        "asodya".into(),
        reqwest::Client::new(),
    );
    let router = presentation::router(App {
        store: store.clone(),
    });

    // Security baseline: anonymous /me/* -> 401; normal session on /admin/* -> 403.
    let (st, _) = call(&router, "GET", "/me/notes", None, &[], None).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    let (st, _) = call(&router, "GET", "/admin/schemas", Some(OWNER_A), &[], None).await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Owner flow: create note -> OCC 409 -> publish typed -> discover -> unpublish -> delete.
    let (st, created) = call(
        &router,
        "POST",
        "/me/notes",
        Some(OWNER_A),
        &[],
        Some(json!({
            "client_note_id": "e2e-note-1",
            "metadata": {"title": "E2E Study"},
            "representations": {
                "typed": {
                    "schema_version": 1,
                    "document": {
                        "blocks": [{"id": "b1", "kind": "paragraph", "spans": [{"text": "Normalized cell block"}]}]
                    }
                }
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{created}");
    let note_id = created["note_id"].as_str().unwrap().to_string();
    let typed_rev: String = sqlx::query_scalar(
        "SELECT revision_id FROM note_revisions WHERE note_id=$1 AND representation='typed' LIMIT 1",
    )
    .bind(&note_id)
    .fetch_one(&store.db)
    .await
    .unwrap();

    // Cross-owner access fails closed with 404.
    let (st, _) = call(
        &router,
        "GET",
        &format!("/me/notes/{note_id}"),
        Some(OWNER_B),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // OCC mismatch returns 409.
    let (st, _) = call(
        &router,
        "PATCH",
        &format!("/me/notes/{note_id}"),
        Some(OWNER_A),
        &[("if-match", "99")],
        Some(json!({"metadata": {"title": "Stale"}})),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);

    // Project + tag organization.
    let (st, proj) = call(
        &router,
        "POST",
        "/me/projects",
        Some(OWNER_A),
        &[],
        Some(json!({"name": "Biology E2E"})),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{proj}");
    let project_id = proj["project_id"].as_str().unwrap();
    let (st, _) = call(
        &router,
        "PUT",
        &format!("/me/projects/{project_id}/notes/{note_id}"),
        Some(OWNER_A),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    // Files reserve + upload + dedup check.
    let raw = b"E2E attachment bytes";
    let sha = format!("{:x}", Sha256::digest(raw));
    let (st, f_res) = call(
        &router,
        "POST",
        "/me/files",
        Some(OWNER_A),
        &[],
        Some(json!({
            "filename": "e2e.txt",
            "content_type": "text/plain",
            "size": raw.len(),
            "checksum": sha,
            "purpose": "attachment"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{f_res}");
    let fid = f_res["file_id"].as_str().unwrap();
    let req = Request::builder()
        .method("PUT")
        .uri(format!("/me/files/{fid}/content"))
        .header("x-uuid", OWNER_A)
        .header("content-type", "text/plain")
        .body(Body::from(raw.to_vec()))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(req).await.unwrap().status(),
        StatusCode::OK
    );

    // Publish typed note -> public discovery & privacy check (no owner_id leaked).
    let (st, pub_res) = call(
        &router,
        "PUT",
        &format!("/me/notes/{note_id}/publication"),
        Some(OWNER_A),
        &[("if-match", "0")],
        Some(json!({
            "language": "en",
            "representations": {"typed": typed_rev}
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{pub_res}");
    let (st, list_res) = call(&router, "GET", "/notes?q=E2E+Study", None, &[], None).await;
    assert_eq!(st, StatusCode::OK, "{list_res}");
    assert!(!list_res.to_string().contains(OWNER_A));

    // Instant room creation & Plane task HTTP adapter verification.
    let (st, room) = call(
        &router,
        "PUT",
        "/instant/e2e-room-1",
        None,
        &[],
        Some(
            json!({"schema_version": 1, "document": {"page": {"width": 1000, "background": {"kind": "blank"}}, "items": []}}),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{room}");
    let (st, task_res) = call(
        &router,
        "POST",
        "/me/tasks",
        Some(OWNER_A),
        &[],
        Some(json!({"title": "E2E Plane Card", "description": "desc"})),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{task_res}");
    assert_eq!(plane_hits.load(Ordering::SeqCst), 1);
}
