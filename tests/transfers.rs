#[path = "support/rooms.rs"]
mod fixture;
use annotations_api::{
    core::{UPLOAD_BYTES, now},
    dal::{Store, transfers::Status},
    domain::{rooms, transfers},
};
use futures_util::stream;
use serde_json::{Value, json};
fn body(bytes: &[u8]) -> impl futures_util::Stream<Item = Result<Vec<u8>, std::io::Error>> + Unpin {
    stream::iter(vec![Ok(bytes.to_vec())])
}
async fn reserve(
    store: &Store,
    code: &str,
    ip: &str,
    key: Option<&str>,
) -> annotations_api::dal::transfers::Reservation {
    transfers::begin(store, code, ip, fixture::metadata(b"hello"), key, now())
        .await
        .unwrap()
}
async fn send(
    store: &Store,
    code: &str,
    id: &str,
    ip: &str,
    bytes: &[u8],
) -> annotations_api::core::Result<annotations_api::dal::transfers::Reservation> {
    transfers::upload(store, code, id, ip, Some("text/plain"), body(bytes), now()).await
}
#[tokio::test]
async fn oversized_metadata_rejects_before_bytes_and_quota_is_shared_across_rooms() {
    let store = fixture::store().await;
    let clock = now();
    fixture::create(&store, "first", clock).await;
    fixture::create(&store, "second", clock).await;
    let mut input = fixture::metadata(b"hello");
    input.size = UPLOAD_BYTES + 1;
    assert_eq!(
        transfers::begin(&store, "first", "uploader", input.clone(), None, clock)
            .await
            .unwrap_err()
            .0
            .as_u16(),
        413
    );
    assert_eq!(store.files.fixture_counts(), Some((0, 0)));
    if let annotations_api::dal::transfers::Transfers::Memory(m) = &store.transfers {
        assert!(m.lock().unwrap().is_empty());
    }
    input.size = 30_000_000;
    let (a, b) = tokio::join!(
        transfers::begin(&store, "first", "uploader", input.clone(), None, clock),
        transfers::begin(&store, "second", "uploader", input.clone(), None, clock)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let saved = a.or(b).unwrap();
    assert!(
        transfers::begin(&store, "second", "other-uploader", input, None, clock)
            .await
            .is_ok()
    );
    store.transfers.release(&saved.id).await.unwrap();
    store.transfers.release(&saved.id).await.unwrap();
    let mut input = fixture::metadata(b"hello");
    input.size = UPLOAD_BYTES;
    assert!(
        transfers::begin(&store, "first", "uploader", input, None, clock)
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn metadata_retries_are_atomic_and_bound_to_exact_metadata_room_and_ip() {
    let store = fixture::store().await;
    fixture::create(&store, "retry", now()).await;
    let (a, b) = tokio::join!(
        reserve(&store, "retry", "uploader", Some("same")),
        reserve(&store, "retry", "uploader", Some("same"))
    );
    assert_eq!(a.id, b.id);
    if let annotations_api::dal::transfers::Transfers::Memory(m) = &store.transfers {
        assert_eq!(m.lock().unwrap().len(), 1);
    }
    let mut other = fixture::metadata(b"hello");
    other.filename = "other.txt".into();
    assert_eq!(
        transfers::begin(&store, "retry", "uploader", other, Some("same"), now())
            .await
            .unwrap_err()
            .0
            .as_u16(),
        409
    );
    assert_ne!(
        reserve(&store, "retry", "different-ip", Some("same"))
            .await
            .id,
        a.id
    );
    for key in ["".to_string(), "x".repeat(129), "bad key".to_string()] {
        assert!(
            transfers::begin(
                &store,
                "retry",
                "uploader",
                fixture::metadata(b"hello"),
                Some(&key),
                now()
            )
            .await
            .is_err()
        );
    }
}
#[tokio::test]
async fn failed_size_checksum_and_type_checks_never_publish_bytes_and_retry_succeeds() {
    let store = fixture::store().await;
    fixture::create(&store, "checks", now()).await;
    let record = reserve(&store, "checks", "uploader", None).await;
    assert_eq!(
        transfers::download(&store, "checks", &record.id, now())
            .await
            .unwrap_err()
            .0
            .as_u16(),
        409
    );
    for (bytes, code) in [
        (b"wrong".as_slice(), "checksum_mismatch:"),
        (b"hi".as_slice(), "size_mismatch:"),
        (b"longer".as_slice(), "size_mismatch:"),
    ] {
        assert!(
            send(&store, "checks", &record.id, "uploader", bytes)
                .await
                .unwrap_err()
                .1
                .starts_with(code)
        );
        assert_eq!(
            store
                .transfers
                .get(&record.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            Status::Failed
        );
        assert!(
            store
                .rooms
                .get("checks", now())
                .await
                .unwrap()
                .files
                .is_empty()
        );
        assert_eq!(store.files.fixture_counts(), Some((0, 0)));
    }
    let ready = send(&store, "checks", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    assert_eq!(ready.status, Status::Ready);
    let revision = store.rooms.get("checks", now()).await.unwrap().revision;
    let retry = send(&store, "checks", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    assert_eq!(retry.file_key, ready.file_key);
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
    assert_eq!(
        store.rooms.get("checks", now()).await.unwrap().revision,
        revision
    );
    assert_eq!(
        send(&store, "checks", &record.id, "uploader", b"other")
            .await
            .unwrap_err()
            .0
            .as_u16(),
        409
    );
    let mut input = fixture::metadata(b"hello");
    input.content_type = "image/png".into();
    let image = transfers::begin(&store, "checks", "uploader", input, None, now())
        .await
        .unwrap();
    assert!(
        transfers::upload(
            &store,
            "checks",
            &image.id,
            "uploader",
            Some("image/png"),
            body(b"hello"),
            now()
        )
        .await
        .unwrap_err()
        .1
        .starts_with("content_type_mismatch:")
    );
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
}
#[tokio::test]
async fn concurrent_binary_retry_has_one_claim_one_alias_and_one_registry_entry() {
    let store = fixture::store().await;
    fixture::create(&store, "concurrent", now()).await;
    let record = reserve(&store, "concurrent", "uploader", None).await;
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
    let incoming = Box::pin(stream::once(async move {
        signal.notify_one();
        Ok::<_, std::io::Error>(rx.await.unwrap())
    }));
    let copy = store.clone();
    let id = record.id.clone();
    let first = tokio::spawn(async move {
        transfers::upload(
            &copy,
            "concurrent",
            &id,
            "uploader",
            Some("text/plain"),
            incoming,
            now(),
        )
        .await
    });
    entered.notified().await;
    assert!(
        send(&store, "concurrent", &record.id, "uploader", b"hello")
            .await
            .unwrap_err()
            .1
            .starts_with("upload_in_progress:")
    );
    tx.send(b"hello".to_vec()).unwrap();
    first.await.unwrap().unwrap();
    send(&store, "concurrent", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
    assert_eq!(
        store
            .rooms
            .get("concurrent", now())
            .await
            .unwrap()
            .files
            .len(),
        1
    );
}
#[tokio::test]
async fn room_and_ip_binding_prevent_other_uploads_and_unknown_fields_are_rejected() {
    let store = fixture::store().await;
    fixture::create(&store, "source", now()).await;
    fixture::create(&store, "other", now()).await;
    let record = reserve(&store, "source", "uploader", None).await;
    assert_eq!(
        send(&store, "other", &record.id, "uploader", b"hello")
            .await
            .unwrap_err()
            .0
            .as_u16(),
        404
    );
    assert_eq!(
        send(&store, "source", &record.id, "other-ip", b"hello")
            .await
            .unwrap_err()
            .0
            .as_u16(),
        404
    );
    assert!(
        transfers::upload(
            &store,
            "source",
            &record.id,
            "uploader",
            Some("image/png"),
            body(b"hello"),
            now()
        )
        .await
        .is_err()
    );
    let mut input = serde_json::to_value(fixture::metadata(b"hello")).unwrap();
    input["owner_id"] = json!("untrusted");
    assert!(serde_json::from_value::<transfers::Metadata>(input).is_err());
}
#[tokio::test]
async fn ready_retry_repairs_interrupted_registration_but_never_restores_deleted_files() {
    let store = fixture::store().await;
    fixture::create(&store, "repair", now()).await;
    let record = reserve(&store, "repair", "uploader", Some("key")).await;
    send(&store, "repair", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    let mut room = store.rooms.get("repair", now()).await.unwrap();
    let revision = room.revision;
    room.files.clear();
    room.revision += 1;
    store
        .rooms
        .compare_set(revision, &room, now())
        .await
        .unwrap();
    send(&store, "repair", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    assert_eq!(
        store.rooms.get("repair", now()).await.unwrap().files.len(),
        1
    );
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
    let item = json!({"id":"card","kind":"file_card","x":0,"y":0,"width":100,"height":50,"file_id":record.id});
    rooms::add(
        &store,
        "repair",
        rooms::ItemWrite {
            expected_revision: 0,
            item,
        },
        now(),
    )
    .await
    .unwrap();
    rooms::remove(&store, "repair", "card", 1, now())
        .await
        .unwrap();
    assert_eq!(
        send(&store, "repair", &record.id, "uploader", b"hello")
            .await
            .unwrap_err()
            .0
            .as_u16(),
        404
    );
    assert_eq!(store.files.fixture_counts(), Some((0, 0)));
    assert!(
        store
            .rooms
            .get("repair", now())
            .await
            .unwrap()
            .files
            .is_empty()
    );
}
async fn http(
    router: &axum::Router,
    method: &str,
    path: &str,
    content_type: &str,
    bytes: Vec<u8>,
    key: Option<&str>,
) -> (u16, Value) {
    use tower::ServiceExt;
    let mut req = axum::http::Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", content_type);
    if let Some(key) = key {
        req = req.header("idempotency-key", key);
    }
    let mut req = req.body(axum::body::Body::from(bytes)).unwrap();
    req.extensions_mut().insert(axum::extract::ConnectInfo(
        "127.0.0.1:54321".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes))),
    )
}
#[tokio::test]
async fn json_then_binary_routes_return_truthful_status_and_safe_download() {
    let store = fixture::store().await;
    fixture::create(&store, "http", now()).await;
    let router = annotations_api::presentation::router(annotations_api::domain::App {
        store: store.clone(),
    });
    let metadata = serde_json::to_vec(&fixture::metadata(b"hello")).unwrap();
    let (status, reserved) = http(
        &router,
        "POST",
        "/instant/http/files",
        "application/json",
        metadata.clone(),
        Some("key"),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(reserved["status"], "reserved");
    assert!(reserved["url"].is_null());
    let (status, retry) = http(
        &router,
        "POST",
        "/instant/http/files",
        "application/json",
        metadata,
        Some("key"),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(reserved["file_id"], retry["file_id"]);
    let path = reserved["upload_url"].as_str().unwrap();
    let (status, ready) = http(&router, "PUT", path, "text/plain", b"hello".to_vec(), None).await;
    assert_eq!(status, 200);
    assert_eq!(ready["status"], "ready");
    assert_eq!(
        http(&router, "PUT", path, "text/plain", b"hello".to_vec(), None)
            .await
            .0,
        200
    );
    use tower::ServiceExt;
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri(ready["url"].as_str().unwrap())
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert!(response.headers().contains_key("content-security-policy"));
    assert_eq!(
        &axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap()[..],
        b"hello"
    );
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
}
#[tokio::test]
async fn staging_uses_configured_directory_and_cleans_after_failures_and_success() {
    let mut store = fixture::store().await;
    fixture::create(&store, "staging", now()).await;
    let directory = tempfile::tempdir().unwrap();
    store.files = store.files.clone().with_staging_dir(directory.path());
    let record = reserve(&store, "staging", "uploader", None).await;
    assert!(
        send(&store, "staging", &record.id, "uploader", b"wrong")
            .await
            .is_err()
    );
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    send(&store, "staging", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn ready_retry_racing_with_card_deletion_cannot_restore_a_reference() {
    let store = fixture::store().await;
    fixture::create(&store, "deleted", now()).await;
    let record = reserve(&store, "deleted", "uploader", Some("key")).await;
    send(&store, "deleted", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    let card = json!({"id":"card","kind":"file_card","x":0,"y":0,"width":100,"height":50,"file_id":record.id});
    rooms::add(
        &store,
        "deleted",
        rooms::ItemWrite {
            expected_revision: 0,
            item: card,
        },
        now(),
    )
    .await
    .unwrap();
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
    let incoming = Box::pin(stream::once(async move {
        signal.notify_one();
        Ok::<_, std::io::Error>(rx.await.unwrap())
    }));
    let copy = store.clone();
    let id = record.id.clone();
    let retry = tokio::spawn(async move {
        transfers::upload(
            &copy,
            "deleted",
            &id,
            "uploader",
            Some("text/plain"),
            incoming,
            now(),
        )
        .await
    });
    entered.notified().await;
    rooms::remove(&store, "deleted", "card", 1, now())
        .await
        .unwrap();
    tx.send(b"hello".to_vec()).unwrap();
    assert_eq!(retry.await.unwrap().unwrap_err().0.as_u16(), 404);
    assert_eq!(
        transfers::begin(
            &store,
            "deleted",
            "uploader",
            fixture::metadata(b"hello"),
            Some("key"),
            now()
        )
        .await
        .unwrap_err()
        .0
        .as_u16(),
        404
    );
    assert!(
        store
            .rooms
            .get("deleted", now())
            .await
            .unwrap()
            .files
            .is_empty()
    );
    assert_eq!(store.files.fixture_counts(), Some((0, 0)));
}
#[tokio::test]
async fn all_expired_states_release_allowance_and_cleanup_preserves_deduplicated_note_file() {
    use annotations_api::domain::{files, tasks};
    let store = fixture::store().await;
    let clock = now();
    fixture::create(&store, "expiry", clock).await;
    let pending = reserve(&store, "expiry", "uploader", None).await;
    let failed = reserve(&store, "expiry", "uploader", None).await;
    assert!(
        send(&store, "expiry", &failed.id, "uploader", b"wrong")
            .await
            .is_err()
    );
    let active = reserve(&store, "expiry", "uploader", None).await;
    let mut claimed = active.clone();
    claimed.status = Status::Uploading;
    claimed.attempt = "attempt".into();
    store
        .transfers
        .compare_set(&active, &claimed, clock)
        .await
        .unwrap();
    let ready = reserve(&store, "expiry", "uploader", None).await;
    send(&store, "expiry", &ready.id, "uploader", b"hello")
        .await
        .unwrap();
    let mut metadata = serde_json::to_value(fixture::metadata(b"hello")).unwrap();
    metadata["purpose"] = json!("attachment");
    let metadata = files::Reservation::parse(&serde_json::to_vec(&metadata).unwrap()).unwrap();
    let permanent = files::reserve(&store, "note-owner", &metadata)
        .await
        .unwrap();
    files::upload(
        &store,
        "note-owner",
        &permanent.file_id,
        Some("text/plain"),
        body(b"hello"),
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO file_references(file_id,reference_kind,reference_id) VALUES($1,'note_revision','lasting-note-revision')")
        .bind(&permanent.file_id).execute(&store.db).await.unwrap();
    assert_eq!(store.files.fixture_counts(), Some((2, 1)));
    assert_eq!(tasks::cleanup(&store, clock + 900).await.unwrap(), 2);
    assert!(store.transfers.get(&pending.id).await.unwrap().is_none());
    assert!(store.transfers.get(&failed.id).await.unwrap().is_some());
    assert!(store.transfers.get(&active.id).await.unwrap().is_some());
    // Expired upload ownership does not consume the active allowance while cleanup waits.
    fixture::create(&store, "next", clock + 900).await;
    let mut maximum = fixture::metadata(b"hello");
    maximum.size = UPLOAD_BYTES;
    let next = transfers::begin(&store, "next", "uploader", maximum, None, clock + 900)
        .await
        .unwrap();
    assert_eq!(tasks::cleanup(&store, clock + 930).await.unwrap(), 2);
    assert!(
        transfers::begin(
            &store,
            "next",
            "uploader",
            fixture::metadata(b"x"),
            None,
            clock + 930
        )
        .await
        .is_err()
    );
    assert!(store.transfers.get(&next.id).await.unwrap().is_some());
    let download = files::download(&store, "note-owner", &permanent.file_id)
        .await
        .unwrap();
    assert!(store.files.read(download.fsm_reference()).await.is_ok());
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
    assert_eq!(
        files::release(&store, "note-owner", &permanent.file_id)
            .await
            .unwrap_err()
            .code,
        "file_in_use"
    );
}

async fn disposable_redis() -> (Store, redis::Client) {
    use std::os::unix::fs::FileTypeExt;
    let socket = std::env::var("ANNOTATIONS_ROOM_TEST_SOCKET")
        .expect("Set the disposable Redis test socket");
    let socket = std::path::Path::new(&socket);
    assert_eq!(socket.file_name().unwrap(), "annotations-room-test.sock");
    assert!(std::fs::metadata(socket).unwrap().file_type().is_socket());
    let client = redis::Client::open(format!("redis+unix://{}", socket.display())).unwrap();
    let mut store = fixture::store().await;
    store.rooms = annotations_api::dal::rooms::Rooms::Redis(client.clone());
    store.transfers = annotations_api::dal::transfers::Transfers::Redis(client.clone());
    (store, client)
}
#[tokio::test]
#[ignore = "Requires an isolated disposable Redis Unix socket"]
async fn redis_quota_idempotency_and_upload_claim_are_atomic() {
    let (store, client) = disposable_redis().await;
    let clock = now();
    let first = format!("quota-{}", annotations_api::core::id());
    let second = format!("quota-{}", annotations_api::core::id());
    let ip = annotations_api::core::id();
    fixture::create(&store, &first, clock).await;
    fixture::create(&store, &second, clock).await;
    let mut metadata = fixture::metadata(b"hello");
    metadata.size = 30_000_000;
    let (a, b) = tokio::join!(
        transfers::begin(&store, &first, &ip, metadata.clone(), Some("key"), clock),
        transfers::begin(&store, &second, &ip, metadata.clone(), Some("key"), clock)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let winner = a.or(b).unwrap();
    let retry = transfers::begin(&store, &winner.code, &ip, metadata, Some("key"), clock)
        .await
        .unwrap();
    assert_eq!(retry.id, winner.id);
    let mut claim = winner.clone();
    claim.status = Status::Uploading;
    claim.attempt = "first".into();
    let mut other = winner.clone();
    other.status = Status::Uploading;
    other.attempt = "second".into();
    let (a, b) = tokio::join!(
        store.transfers.compare_set(&winner, &claim, clock),
        store.transfers.compare_set(&winner, &other, clock)
    );
    assert_ne!(a.unwrap(), b.unwrap());
    let current = store.transfers.get(&winner.id).await.unwrap().unwrap();
    let mut failed = current.clone();
    failed.status = Status::Failed;
    assert!(
        store
            .transfers
            .compare_set(&current, &failed, clock)
            .await
            .unwrap()
    );
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let used: i64 = redis::cmd("GET")
        .arg(format!("annotations:quota:{ip}"))
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(used, 30_000_000);
    store.transfers.release(&winner.id).await.unwrap();
    store.transfers.release(&winner.id).await.unwrap();
    let mut full = fixture::metadata(b"hello");
    full.size = UPLOAD_BYTES;
    assert!(
        transfers::begin(&store, &first, &ip, full, None, clock)
            .await
            .is_ok()
    );
    // Exercise claim, verified finalization and ready retry through the real scripts.
    let binary_ip = annotations_api::core::id();
    let binary = transfers::begin(
        &store,
        &first,
        &binary_ip,
        fixture::metadata(b"hello"),
        Some("binary"),
        clock,
    )
    .await
    .unwrap();
    let completed = send(&store, &first, &binary.id, &binary_ip, b"hello")
        .await
        .unwrap();
    let retried = send(&store, &first, &binary.id, &binary_ip, b"hello")
        .await
        .unwrap();
    assert_eq!(completed.status, Status::Ready);
    assert_eq!(completed.file_key, retried.file_key);
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
    assert_eq!(store.rooms.get(&first, clock).await.unwrap().files.len(), 1);
}
#[tokio::test]
#[ignore = "Requires an isolated disposable Redis Unix socket"]
async fn redis_expired_quota_prunes_before_cleanup_and_late_release_preserves_new_charge() {
    let (store, client) = disposable_redis().await;
    let clock = now();
    let ip = annotations_api::core::id();
    let old = format!("old-{}", annotations_api::core::id());
    let new = format!("new-{}", annotations_api::core::id());
    fixture::create(&store, &old, clock).await;
    let mut metadata = fixture::metadata(b"hello");
    metadata.size = UPLOAD_BYTES;
    let expired = transfers::begin(&store, &old, &ip, metadata.clone(), None, clock)
        .await
        .unwrap();
    fixture::create(&store, &new, clock + 900).await;
    let active = transfers::begin(&store, &new, &ip, metadata, None, clock + 900)
        .await
        .unwrap();
    assert_ne!(active.id, expired.id);
    store.transfers.release(&expired.id).await.unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let used: i64 = redis::cmd("GET")
        .arg(format!("annotations:quota:{ip}"))
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(used, UPLOAD_BYTES);
    assert!(
        transfers::begin(
            &store,
            &new,
            &ip,
            fixture::metadata(b"x"),
            None,
            clock + 900
        )
        .await
        .unwrap_err()
        .1
        .starts_with("ip_quota_exceeded:")
    );
    assert!(store.transfers.get(&active.id).await.unwrap().is_some());
    let mut candidate = expired.clone();
    candidate.status = Status::Uploading;
    assert!(
        store
            .transfers
            .compare_set(&expired, &candidate, clock + 900)
            .await
            .unwrap_err()
            .1
            .starts_with("room_expired:")
    );
    // The Redis claim boundary also checks server time when the caller is stale.
    let late_code = format!("late-{}", annotations_api::core::id());
    fixture::create(&store, &late_code, clock - 898).await;
    let late = transfers::begin(
        &store,
        &late_code,
        "late-uploader",
        fixture::metadata(b"hello"),
        None,
        clock,
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    let mut claim = late.clone();
    claim.status = Status::Uploading;
    assert!(
        store
            .transfers
            .compare_set(&late, &claim, clock)
            .await
            .unwrap_err()
            .1
            .starts_with("room_expired:")
    );
}

#[tokio::test]
async fn retry_reuses_verified_alias_after_lost_storage_response() {
    let store = fixture::store().await;
    fixture::create(&store, "uncertain", now()).await;
    let record = reserve(&store, "uncertain", "uploader", None).await;
    let path = store.files.staging_file().unwrap();
    tokio::fs::write(&path, b"hello").await.unwrap();
    let alias = store
        .files
        .upload_path(
            &path,
            &record.filename,
            &record.content_type,
            &record.album,
            record.bytes,
        )
        .await
        .unwrap();
    let mut failed = record.clone();
    failed.status = Status::Failed;
    store
        .transfers
        .compare_set(&record, &failed, now())
        .await
        .unwrap();
    assert!(
        store
            .rooms
            .get("uncertain", now())
            .await
            .unwrap()
            .files
            .is_empty()
    );
    let ready = send(&store, "uncertain", &record.id, "uploader", b"hello")
        .await
        .unwrap();
    assert_eq!(ready.file_key.as_deref(), Some(alias.key.as_str()));
    assert_eq!(store.files.fixture_counts(), Some((1, 1)));
}
