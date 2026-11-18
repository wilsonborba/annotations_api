//! Account-bound model downloads over SQLite and the in-memory FSM double.
use annotations_api::{
    dal::Store,
    domain::{App, models},
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const ALICE: &str = "alice-account";
const BOB: &str = "bob-account";

/// Model bytes in the FSM double, one registered release, and the router.
struct Setup {
    router: Router,
    store: Store,
    bytes: Vec<u8>,
    key: String,
}
fn weights() -> Vec<u8> {
    (0..10_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}
fn manifest(version: &str, key: &str, bytes: &[u8]) -> models::Manifest {
    serde_json::from_value(json!({
        "release_id":version,"code_revision":"rev","dataset_snapshot_id":"snap",
        "source_checkpoint":"ckpt",
        "artifacts":[{"platform":"linux","fsm_reference":key,"size":bytes.len(),
            "sha256":format!("{:x}", Sha256::digest(bytes))}],
        "languages":["en"],"capabilities":["handwriting"],"metrics":{"en":{"cer":0.1}}
    }))
    .unwrap()
}
async fn setup() -> Setup {
    let store = Store::fixture().await;
    let bytes = weights();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("model.bin");
    tokio::fs::write(&path, &bytes).await.unwrap();
    let file = store
        .files
        .upload_path(
            &path,
            "model.bin",
            "application/octet-stream",
            "models",
            bytes.len() as i64,
        )
        .await
        .unwrap();
    models::register(&store.db, manifest("v1", &file.key, &bytes))
        .await
        .unwrap();
    models::activate(&store.db, "v1").await.unwrap();
    Setup {
        router: presentation::router(App {
            store: store.clone(),
        }),
        store,
        bytes,
        key: file.key,
    }
}
struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}
impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}
async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    identity: Option<&str>,
    range: Option<&str>,
    body: Option<&str>,
) -> Reply {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(identity) = identity {
        builder = builder.header("x-uuid", identity);
    }
    if let Some(range) = range {
        builder = builder.header(header::RANGE, range);
    }
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    let res = router
        .clone()
        .oneshot(
            builder
                .body(body.map_or_else(Body::empty, |b| Body::from(b.to_owned())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = axum::body::to_bytes(res.into_body(), 10_000_000)
        .await
        .unwrap()
        .to_vec();
    Reply {
        status,
        headers,
        body,
    }
}
async fn create(router: &Router, identity: &str, body: &str) -> Reply {
    call(
        router,
        "POST",
        "/me/model_downloads",
        Some(identity),
        None,
        Some(body),
    )
    .await
}
async fn content(router: &Router, identity: &str, id: &str, range: Option<&str>) -> Reply {
    call(
        router,
        "GET",
        &format!("/me/model_downloads/{id}/content"),
        Some(identity),
        range,
        None,
    )
    .await
}
const LINUX_V1: &str = r#"{"model_version":"v1","platform":"linux"}"#;

#[tokio::test]
async fn anonymous_requests_need_identity() {
    let s = setup().await;
    let id = create(&s.router, ALICE, LINUX_V1).await.json()["download_id"]
        .as_str()
        .unwrap()
        .to_owned();
    for (method, uri, body) in [
        ("POST", "/me/model_downloads".to_owned(), Some(LINUX_V1)),
        ("GET", format!("/me/model_downloads/{id}"), None),
        ("GET", format!("/me/model_downloads/{id}/content"), None),
    ] {
        let reply = call(&s.router, method, &uri, None, None, body).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{uri}");
        assert_eq!(reply.json()["code"], "identity_required");
    }
}

#[tokio::test]
async fn create_is_idempotent_per_account_version_and_platform() {
    let s = setup().await;
    let first = create(&s.router, ALICE, LINUX_V1).await;
    assert_eq!(first.status, StatusCode::CREATED);
    let body = first.json();
    assert_eq!(body["model_version"], "v1");
    assert_eq!(body["platform"], "linux");
    assert_eq!(body["size"], s.bytes.len());
    assert_eq!(body["model_status"], "active");
    let id = body["download_id"].as_str().unwrap();
    // No FSM key in the response.
    assert!(!String::from_utf8_lossy(&first.body).contains(&s.key));
    assert!(body.get("fsm_reference").is_none());

    let retry = create(&s.router, ALICE, LINUX_V1).await;
    assert_eq!(retry.status, StatusCode::OK);
    assert_eq!(retry.json()["download_id"], id);
    let read = call(
        &s.router,
        "GET",
        &format!("/me/model_downloads/{id}"),
        Some(ALICE),
        None,
        None,
    )
    .await;
    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.json(), body);
    assert!(!String::from_utf8_lossy(&read.body).contains(&s.key));

    // Another account gets its own logical id, on the same stored artifact.
    let other = create(&s.router, BOB, LINUX_V1).await;
    assert_eq!(other.status, StatusCode::CREATED);
    assert_ne!(other.json()["download_id"], id);
    assert_eq!(s.store.files.fixture_counts(), Some((1, 1)));
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM model_downloads")
        .fetch_one(&s.store.db)
        .await
        .unwrap();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn create_rejects_bad_bodies_and_unserved_artifacts() {
    let s = setup().await;
    models::register(&s.store.db, manifest("draft-v2", &s.key, &s.bytes))
        .await
        .unwrap();
    for (body, status, code) in [
        ("not json", 422, "invalid_model_download"),
        (r#"{"model_version":"v1"}"#, 422, "invalid_model_download"),
        (
            r#"{"model_version":"v1","platform":"linux","user_id":"bob-account"}"#,
            422,
            "invalid_model_download",
        ),
        (
            r#"{"model_version":"v1","platform":"windows"}"#,
            422,
            "invalid_model_download",
        ),
        (
            r#"{"model_version":"../v1","platform":"linux"}"#,
            422,
            "invalid_model_download",
        ),
        (
            r#"{"model_version":"v1","platform":"ios"}"#,
            404,
            "model_not_found",
        ),
        (
            r#"{"model_version":"missing","platform":"linux"}"#,
            404,
            "model_not_found",
        ),
        (
            r#"{"model_version":"draft-v2","platform":"linux"}"#,
            404,
            "model_not_found",
        ),
    ] {
        let reply = create(&s.router, ALICE, body).await;
        assert_eq!(reply.status.as_u16(), status, "{body}");
        assert_eq!(reply.json()["code"], code, "{body}");
    }
}

#[tokio::test]
async fn another_account_cannot_read_a_download_id() {
    let s = setup().await;
    let id = create(&s.router, ALICE, LINUX_V1).await.json()["download_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let missing = "00000000-0000-4000-8000-000000000000";
    for target in [id.as_str(), missing] {
        let meta = call(
            &s.router,
            "GET",
            &format!("/me/model_downloads/{target}"),
            Some(BOB),
            None,
            None,
        )
        .await;
        let bytes = content(&s.router, BOB, target, None).await;
        // The same answer for a foreign id and a missing one: no existence leak.
        for reply in [meta, bytes] {
            assert_eq!(reply.status, StatusCode::NOT_FOUND);
            assert_eq!(reply.json()["code"], "model_download_not_found");
        }
    }
    let ranged = content(&s.router, BOB, &id, Some("bytes=0-9")).await;
    assert_eq!(ranged.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn range_resume_reassembles_bytes_matching_the_manifest() {
    let s = setup().await;
    let id = create(&s.router, ALICE, LINUX_V1).await.json()["download_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let manifest = call(&s.router, "GET", "/models/v1", Some(ALICE), None, None)
        .await
        .json();
    let expected = manifest["artifacts"][0]["sha256"].as_str().unwrap();
    let size = s.bytes.len();

    let full = content(&s.router, ALICE, &id, None).await;
    assert_eq!(full.status, StatusCode::OK);
    assert_eq!(full.body, s.bytes);
    assert_eq!(format!("{:x}", Sha256::digest(&full.body)), expected);
    assert_eq!(full.headers[header::CONTENT_LENGTH], size.to_string());
    assert_eq!(full.headers[header::ACCEPT_RANGES], "bytes");
    assert_eq!(full.headers[header::ETAG], format!("\"{expected}\""));
    assert!(full.headers.get(header::CONTENT_RANGE).is_none());
    for (_, value) in &full.headers {
        assert!(!value.to_str().unwrap_or_default().contains(&s.key));
    }

    // An interrupted download resumes from the last received byte.
    let head = content(&s.router, ALICE, &id, Some("bytes=0-4095")).await;
    assert_eq!(head.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        head.headers[header::CONTENT_RANGE],
        format!("bytes 0-4095/{size}")
    );
    assert_eq!(head.headers[header::CONTENT_LENGTH], "4096");
    let middle = content(&s.router, ALICE, &id, Some("bytes=4096-7999")).await;
    assert_eq!(middle.status, StatusCode::PARTIAL_CONTENT);
    let tail = content(&s.router, ALICE, &id, Some("bytes=8000-")).await;
    assert_eq!(tail.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        tail.headers[header::CONTENT_RANGE],
        format!("bytes 8000-{}/{size}", size - 1)
    );
    let joined = [head.body, middle.body, tail.body].concat();
    assert_eq!(joined, s.bytes);
    assert_eq!(format!("{:x}", Sha256::digest(&joined)), expected);

    // Suffix ranges and an end past the size are trimmed to the file.
    let suffix = content(&s.router, ALICE, &id, Some("bytes=-16")).await;
    assert_eq!(suffix.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(suffix.body, s.bytes[size - 16..]);
    let past = content(&s.router, ALICE, &id, Some("bytes=9990-99999")).await;
    assert_eq!(past.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(past.body, s.bytes[9990..]);

    // The retry record is reused: resuming never allocates a new id.
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM model_downloads")
        .fetch_one(&s.store.db)
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn malformed_and_unsatisfiable_ranges() {
    let s = setup().await;
    let id = create(&s.router, ALICE, LINUX_V1).await.json()["download_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let size = s.bytes.len();
    for range in [
        format!("bytes={size}-"),
        "bytes=20000-30000".into(),
        "bytes=50-10".into(),
        "bytes=-0".into(),
        "bytes=abc-def".into(),
        "bytes=5".into(),
    ] {
        let reply = content(&s.router, ALICE, &id, Some(&range)).await;
        assert_eq!(reply.status, StatusCode::RANGE_NOT_SATISFIABLE, "{range}");
        assert_eq!(
            reply.headers[header::CONTENT_RANGE],
            format!("bytes */{size}")
        );
        assert_eq!(reply.json()["code"], "range_not_satisfiable");
    }
    // Another unit or several ranges serve the whole artifact (RFC 9110).
    for range in ["items=0-5", "bytes=0-1,5-6"] {
        let reply = content(&s.router, ALICE, &id, Some(range)).await;
        assert_eq!(reply.status, StatusCode::OK, "{range}");
        assert_eq!(reply.body, s.bytes);
    }
}

#[tokio::test]
async fn retired_release_stays_downloadable() {
    let s = setup().await;
    let id = create(&s.router, ALICE, LINUX_V1).await.json()["download_id"]
        .as_str()
        .unwrap()
        .to_owned();
    models::register(&s.store.db, manifest("v2", &s.key, &s.bytes))
        .await
        .unwrap();
    models::activate(&s.store.db, "v2").await.unwrap();
    let meta = call(
        &s.router,
        "GET",
        &format!("/me/model_downloads/{id}"),
        Some(ALICE),
        None,
        None,
    )
    .await;
    assert_eq!(meta.json()["model_status"], "retired");
    let reply = content(&s.router, ALICE, &id, None).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, s.bytes);
    assert_eq!(
        create(&s.router, BOB, LINUX_V1).await.status,
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn withdrawn_release_is_not_listed_and_downloads_fail() {
    let s = setup().await;
    models::register(&s.store.db, manifest("v2", &s.key, &s.bytes))
        .await
        .unwrap();
    models::activate(&s.store.db, "v2").await.unwrap();
    let v2_req = r#"{"model_version":"v2","platform":"linux"}"#;
    let v2_dl = create(&s.router, ALICE, v2_req).await;
    assert_eq!(v2_dl.status, StatusCode::CREATED);
    let v2_id = v2_dl.json()["download_id"].as_str().unwrap().to_owned();

    // Rollback marks v2 as withdrawn; v1 becomes active again.
    assert_eq!(models::rollback(&s.store.db).await.unwrap(), "v1");

    let list = call(&s.router, "GET", "/models", Some(ALICE), None, None).await;
    assert_eq!(list.status, StatusCode::OK);
    let list_body = list.json();
    let versions: Vec<&str> = list_body["models"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["version"].as_str())
        .collect();
    assert_eq!(versions, ["v1"]);

    let detail = call(&s.router, "GET", "/models/v2", Some(ALICE), None, None).await;
    assert_eq!(detail.status, StatusCode::GONE);
    assert_eq!(detail.json()["code"], "model_withdrawn");

    let new_dl = create(&s.router, BOB, v2_req).await;
    assert_eq!(new_dl.status, StatusCode::GONE);
    assert_eq!(new_dl.json()["code"], "model_withdrawn");

    let old_meta = call(
        &s.router,
        "GET",
        &format!("/me/model_downloads/{v2_id}"),
        Some(ALICE),
        None,
        None,
    )
    .await;
    assert_eq!(old_meta.status, StatusCode::GONE);
    assert_eq!(old_meta.json()["code"], "model_withdrawn");

    let old_bytes = content(&s.router, ALICE, &v2_id, None).await;
    assert_eq!(old_bytes.status, StatusCode::GONE);
    assert_eq!(old_bytes.json()["code"], "model_withdrawn");

    // A non-active release withdrawn later via CLI also stops serving.
    models::register(&s.store.db, manifest("v3", &s.key, &s.bytes))
        .await
        .unwrap();
    models::activate(&s.store.db, "v3").await.unwrap();
    // v1 is now retired and downloadable until withdrawn.
    assert_eq!(
        create(&s.router, ALICE, LINUX_V1).await.status,
        StatusCode::CREATED
    );
    models::withdraw(&s.store.db, "v1").await.unwrap();
    let after_withdraw = create(&s.router, BOB, LINUX_V1).await;
    assert_eq!(after_withdraw.status, StatusCode::GONE);
    assert_eq!(after_withdraw.json()["code"], "model_withdrawn");
}
