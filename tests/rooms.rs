#[path = "support/rooms.rs"]
mod fixture;
use annotations_api::{
    core::NOTE_LIMITS,
    dal::{Store, rooms::Rooms},
    domain::{
        editor_rules,
        note_documents::Family,
        rooms::{self, Create, Failure, ItemPatch, ItemWrite, RoomPatch},
    },
};
use serde_json::{Value, json};

fn text(id: &str, y: i64) -> Value {
    json!({"id":id,"kind":"text","x":0,"y":y,"width":100,"height":50,"spans":[{"text":"hello"}]})
}
fn document(items: Vec<Value>) -> Value {
    json!({"page":{"width":1000,"background":{"kind":"blank"}},"items":items})
}
async fn populated(store: &Store, code: &str, now: i64) -> rooms::Room {
    rooms::create(
        store,
        code,
        Create {
            schema_version: 1,
            document: document(vec![text("a", 0), text("b", 100)]),
        },
        now,
    )
    .await
    .unwrap()
}
fn patch(expected_revision: i64, value: &str) -> ItemPatch {
    ItemPatch {
        expected_revision,
        changes: json!({"spans":[{"text":value}]}),
    }
}
fn err_text(error: Failure) -> String {
    match error {
        Failure::Error(e) => e.1,
        other => panic!("unexpected {other:?}"),
    }
}
#[tokio::test]
async fn existing_and_expired_codes_never_overwrite_or_restart() {
    let store = fixture::store().await;
    let first = fixture::create(&store, "fixed", 100).await;
    for now in [200, 1000, 1001] {
        let error = rooms::create(
            &store,
            "fixed",
            Create {
                schema_version: 1,
                document: document(vec![]),
            },
            now,
        )
        .await
        .unwrap_err();
        assert_eq!(error.0.as_u16(), if now < 1000 { 409 } else { 410 });
    }
    assert_eq!(first.expires_at, 1000);
    assert_eq!(
        store.rooms.get("fixed", 1001).await.unwrap_err().0.as_u16(),
        410
    );
    assert_eq!(
        store
            .rooms
            .get("unknown", 1001)
            .await
            .unwrap_err()
            .0
            .as_u16(),
        404
    );
}
#[tokio::test]
async fn independent_item_writes_both_succeed_and_retain_both_changes() {
    let store = fixture::store().await;
    populated(&store, "parallel", 100).await;
    let (a, b) = tokio::join!(
        rooms::patch_item(&store, "parallel", "a", patch(1, "first"), 101),
        rooms::patch_item(&store, "parallel", "b", patch(1, "second"), 101)
    );
    assert!(a.is_ok(), "{a:?}");
    assert!(b.is_ok(), "{b:?}");
    let room = store.rooms.get("parallel", 101).await.unwrap();
    assert_eq!(room.revision, 2);
    assert_eq!(room.item_revisions["a"], 2);
    assert_eq!(room.item_revisions["b"], 2);
    assert_eq!(room.document["items"][0]["spans"][0]["text"], "first");
    assert_eq!(room.document["items"][1]["spans"][0]["text"], "second");
}
#[tokio::test]
async fn same_item_one_wins_and_loser_receives_current_item() {
    let store = fixture::store().await;
    populated(&store, "same", 100).await;
    let (a, b) = tokio::join!(
        rooms::patch_item(&store, "same", "a", patch(1, "first"), 101),
        rooms::patch_item(&store, "same", "a", patch(1, "second"), 101)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let room = store.rooms.get("same", 101).await.unwrap();
    match a.err().or(b.err()).unwrap() {
        Failure::StaleItem {
            item,
            item_revision,
            revision,
        } => {
            assert_eq!(item, room.document["items"][0]);
            assert_eq!(item_revision, 2);
            assert_eq!(revision, 1);
        }
        e => panic!("{e:?}"),
    }
}
#[tokio::test]
async fn edit_before_expiry_keeps_deadline_and_expired_writes_fail() {
    let store = fixture::store().await;
    populated(&store, "expiry", 100).await;
    let room = rooms::patch_item(&store, "expiry", "a", patch(1, "last"), 999)
        .await
        .unwrap();
    assert_eq!(room.expires_at, 1000);
    let error = rooms::patch_item(&store, "expiry", "a", patch(2, "late"), 1000)
        .await
        .unwrap_err();
    assert!(err_text(error).starts_with("room_expired:"));
    assert!(rooms::remove(&store, "expiry", "a", 2, 1000).await.is_err());
}
#[tokio::test]
async fn independent_moves_cannot_merge_into_overlapping_blocks() {
    let store = fixture::store().await;
    populated(&store, "collision", 100).await;
    let make = || ItemPatch {
        expected_revision: 1,
        changes: json!({"y":200}),
    };
    let (a, b) = tokio::join!(
        rooms::patch_item(&store, "collision", "a", make(), 101),
        rooms::patch_item(&store, "collision", "b", make(), 101)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(err_text(a.err().or(b.err()).unwrap()).starts_with("block_overlap:"));
    let room = store.rooms.get("collision", 101).await.unwrap();
    editor_rules::check_edit(Family::Instant, 1, &room.document, None, &NOTE_LIMITS).unwrap();
}
#[tokio::test]
async fn structured_overlap_rejects_but_free_shapes_overlap() {
    let store = fixture::store().await;
    populated(&store, "ink", 100).await;
    let mut overlap = text("overlap", 0);
    overlap["x"] = json!(5);
    assert!(
        rooms::add(
            &store,
            "ink",
            ItemWrite {
                expected_revision: 0,
                item: overlap
            },
            101
        )
        .await
        .is_err()
    );
    let shape = json!({"id":"shape","kind":"shape","shape":"rectangle","x":0,"y":0,"width":100,"height":50,
        "color":"#000000FF","line_width":2,"opacity":1});
    let room = rooms::add(
        &store,
        "ink",
        ItemWrite {
            expected_revision: 0,
            item: shape,
        },
        101,
    )
    .await
    .unwrap();
    assert_eq!(room.document["items"].as_array().unwrap().len(), 3);
    let stroke = json!({"id":"ink-stroke","kind":"stroke","tool":"round","brush_version":1,
        "width":2,"color":"#000000FF","opacity":1,
        "samples":{"x":[1000,2000],"y":[1000,2000],"t":[0,5]},
        "nodes":[{"x":1000,"y":1000,"mode":"independent","corner":false,"sample":0}]});
    let room = rooms::add(
        &store,
        "ink",
        ItemWrite {
            expected_revision: 0,
            item: stroke,
        },
        101,
    )
    .await
    .unwrap();
    assert_eq!(room.document["items"].as_array().unwrap().len(), 4);
}
#[tokio::test]
async fn room_batches_are_atomic_validate_once_and_reject_stale_room_revisions() {
    let store = fixture::store().await;
    populated(&store, "batch", 100).await;
    // Each intermediate state overlaps; the final swap is valid.
    let op = json!({"op":"batch","steps":[{"op":"update","id":"a","changes":{"y":100}},
        {"op":"update","id":"b","changes":{"y":0}},
        {"op":"page","page":{"width":1000,"background":{"kind":"dots","spacing":20,"visibility":0.5}}}]});
    let room = rooms::patch_room(
        &store,
        "batch",
        RoomPatch {
            expected_revision: 0,
            operation: op.clone(),
        },
        101,
    )
    .await
    .unwrap();
    assert_eq!(room.revision, 1);
    assert_eq!(room.item_revisions["a"], 2);
    assert_eq!(room.item_revisions["b"], 2);
    assert!(
        err_text(
            rooms::patch_room(
                &store,
                "batch",
                RoomPatch {
                    expected_revision: 0,
                    operation: op
                },
                101
            )
            .await
            .unwrap_err()
        )
        .starts_with("room_revision_conflict:")
    );
    let bad = json!({"op":"batch","steps":[{"op":"delete","id":"a"},{"op":"update","id":"b","changes":{"x":999}}]});
    assert!(
        rooms::patch_room(
            &store,
            "batch",
            RoomPatch {
                expected_revision: 1,
                operation: bad
            },
            101
        )
        .await
        .is_err()
    );
    assert_eq!(
        store.rooms.get("batch", 101).await.unwrap().document,
        room.document
    );
}
#[tokio::test]
async fn deletion_has_no_undo_and_deleted_id_cannot_be_reused() {
    let store = fixture::store().await;
    populated(&store, "delete", 100).await;
    let room = rooms::remove(&store, "delete", "a", 1, 101).await.unwrap();
    assert_eq!(room.item_revisions["a"], 2);
    match rooms::patch_item(&store, "delete", "a", patch(1, "stale"), 101)
        .await
        .unwrap_err()
    {
        Failure::StaleItem {
            item,
            item_revision,
            ..
        } => {
            assert!(item.is_null());
            assert_eq!(item_revision, 2);
        }
        e => panic!("{e:?}"),
    }
    assert!(
        rooms::add(
            &store,
            "delete",
            ItemWrite {
                expected_revision: 0,
                item: text("a", 0)
            },
            101
        )
        .await
        .is_err()
    );
    assert!(
        rooms::patch_room(
            &store,
            "delete",
            RoomPatch {
                expected_revision: 1,
                operation: json!({"op":"undo"})
            },
            101
        )
        .await
        .is_err()
    );
    let stored = serde_json::to_string(&store.rooms.get("delete", 101).await.unwrap()).unwrap();
    assert!(!stored.contains("history"));
}
#[tokio::test]
async fn schema_publication_allowed_kinds_and_room_file_scope_are_enforced() {
    let bare = Store::fixture().await;
    assert!(
        rooms::create(
            &bare,
            "schema",
            Create {
                schema_version: 1,
                document: document(vec![])
            },
            100
        )
        .await
        .unwrap_err()
        .1
        .starts_with("schema_not_published:")
    );
    let store = fixture::store().await;
    for kind in ["task_card", "quiz_card", "csv_table", "pdf_preview"] {
        let mut item = text("bad", 0);
        item["kind"] = json!(kind);
        assert!(
            rooms::create(
                &store,
                "kinds",
                Create {
                    schema_version: 1,
                    document: document(vec![item])
                },
                100
            )
            .await
            .unwrap_err()
            .1
            .starts_with("kind_not_allowed:")
        );
    }
    let mut report = text("bad", 0);
    report["kind"] = json!("reports_embed");
    report["source"] = json!("upload");
    assert!(
        rooms::create(
            &store,
            "report",
            Create {
                schema_version: 1,
                document: document(vec![report])
            },
            100
        )
        .await
        .is_err()
    );
    let image = json!({"id":"image","kind":"image","x":0,"y":0,"width":100,"height":100,"file_id":"other-room-file"});
    assert!(
        rooms::create(
            &store,
            "file",
            Create {
                schema_version: 1,
                document: document(vec![image])
            },
            100
        )
        .await
        .unwrap_err()
        .1
        .starts_with("missing_file:")
    );
    assert!(
        rooms::create(
            &store,
            "version",
            Create {
                schema_version: 2,
                document: document(vec![])
            },
            100
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn store_rejects_expiry_extension_and_old_snapshot() {
    let store = fixture::store().await;
    let mut room = populated(&store, "store", 100).await;
    room.expires_at += 1;
    room.revision += 1;
    assert!(store.rooms.compare_set(0, &room, 101).await.is_err());
    room.expires_at -= 1;
    assert!(store.rooms.compare_set(0, &room, 101).await.unwrap());
    assert!(!store.rooms.compare_set(0, &room, 101).await.unwrap());
    assert!(
        store
            .rooms
            .compare_set(1, &room, 1000)
            .await
            .unwrap_err()
            .1
            .starts_with("room_expired:")
    );
}

async fn request(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    revision: Option<i64>,
) -> (u16, Value) {
    use tower::ServiceExt;
    let mut request = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(revision) = revision {
        request = request.header("if-match", format!("\"{revision}\""));
    }
    let response = router
        .clone()
        .oneshot(
            request
                .body(body.map_or_else(axum::body::Body::empty, |v| {
                    axum::body::Body::from(v.to_string())
                }))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 10_000_000)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
async fn anonymous_http_crud_returns_revisions_and_current_item_on_conflict() {
    let store = fixture::store().await;
    let router = annotations_api::presentation::router(annotations_api::domain::App { store });
    let (status, created) = request(
        &router,
        "PUT",
        "/instant/http",
        Some(json!({"schema_version":1,"document":document(vec![])})),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        created["expires_at"].as_i64().unwrap() - created["created_at"].as_i64().unwrap(),
        900
    );
    let (status, _) = request(
        &router,
        "POST",
        "/instant/http/items",
        Some(json!({"expected_revision":0,"item":text("a",0)})),
        None,
    )
    .await;
    assert_eq!(status, 200);
    let mut replacement = text("a", 0);
    replacement["spans"] = json!([{"text":"replacement"}]);
    let (status, _) = request(
        &router,
        "PUT",
        "/instant/http/items/a",
        Some(json!({"expected_revision":1,"item":replacement})),
        None,
    )
    .await;
    assert_eq!(status, 200);
    let (status, error) = request(
        &router,
        "PATCH",
        "/instant/http/items/a",
        Some(json!({"expected_revision":1,"changes":{"y":20}})),
        None,
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(error["error"], "item_revision_conflict");
    assert_eq!(error["item_revision"], 2);
    assert_eq!(error["current_item"]["spans"][0]["text"], "replacement");
    let (status, _) = request(
        &router,
        "PATCH",
        "/instant/http/items/a",
        Some(json!({"expected_revision":2,"changes":{"y":20}})),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        request(&router, "DELETE", "/instant/http/items/a", None, None)
            .await
            .0,
        428
    );
    assert_eq!(
        request(&router, "DELETE", "/instant/http/items/a", None, Some(3))
            .await
            .0,
        200
    );
    let (status, room) = request(&router, "GET", "/instant/http", None, None).await;
    assert_eq!(status, 200);
    assert!(room["document"]["items"].as_array().unwrap().is_empty());
    assert_eq!(room["item_revisions"]["a"], 4);
    assert_eq!(room["expires_at"], created["expires_at"]);
    assert!(room.get("owner").is_none());
    assert!(room.get("history").is_none());
    let (status,_)=request(&router,"PATCH","/instant/http",Some(json!({"expected_revision":4,
        "operation":{"op":"page","page":{"width":1000,"background":{"kind":"blank"}}},"expires_at":9999999999_i64})),None).await;
    assert_eq!(status, 422);
}
#[tokio::test]
async fn groups_require_atomic_room_operations() {
    let store = fixture::store().await;
    populated(&store, "groups", 100).await;
    let group = json!({"id":"g","kind":"group","members":["a","b"]});
    assert!(
        err_text(
            rooms::add(
                &store,
                "groups",
                ItemWrite {
                    expected_revision: 0,
                    item: group.clone()
                },
                101
            )
            .await
            .unwrap_err()
        )
        .starts_with("room_operation_required:")
    );
    let room = rooms::patch_room(
        &store,
        "groups",
        RoomPatch {
            expected_revision: 0,
            operation: json!({"op":"add","item":group}),
        },
        101,
    )
    .await
    .unwrap();
    assert_eq!(room.item_revisions["g"], 1);
    assert!(
        err_text(
            rooms::patch_item(&store, "groups", "a", patch(1, "group member"), 101)
                .await
                .unwrap_err()
        )
        .starts_with("room_operation_required:")
    );
    let room = rooms::patch_room(
        &store,
        "groups",
        RoomPatch {
            expected_revision: 1,
            operation: json!({"op":"batch","steps":[
        {"op":"delete","id":"g"},{"op":"delete","id":"a"},{"op":"delete","id":"b"}]}),
        },
        101,
    )
    .await
    .unwrap();
    assert!(room.document["items"].as_array().unwrap().is_empty());
}

/// Only a caller-created disposable Unix socket is accepted, never a service URL
/// or the application's Redis configuration. Run explicitly with --ignored.
async fn disposable_redis() -> (Store, redis::Client) {
    use std::os::unix::fs::FileTypeExt;
    let path = std::env::var("ANNOTATIONS_ROOM_TEST_SOCKET")
        .expect("Set the disposable Redis test socket");
    let path = std::path::Path::new(&path);
    assert_eq!(path.file_name().unwrap(), "annotations-room-test.sock");
    assert!(std::fs::metadata(path).unwrap().file_type().is_socket());
    let client = redis::Client::open(format!("redis+unix://{}", path.display())).unwrap();
    let mut store = fixture::store().await;
    store.rooms = Rooms::Redis(client.clone());
    (store, client)
}
#[tokio::test]
#[ignore = "Requires an isolated disposable Redis Unix socket"]
async fn redis_atomic_concurrency_and_absolute_expiry() {
    let (store, client) = disposable_redis().await;
    let now = annotations_api::core::now();
    let code = format!("test-{}", annotations_api::core::id());
    let initial = populated(&store, &code, now).await;
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let key = format!("annotations:room:{code}");
    let expiry: i64 = redis::cmd("EXPIRETIME")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(expiry, initial.expires_at);
    let (a, b) = tokio::join!(
        rooms::patch_item(&store, &code, "a", patch(1, "one"), now),
        rooms::patch_item(&store, &code, "b", patch(1, "two"), now)
    );
    assert!(a.is_ok(), "{a:?}");
    assert!(b.is_ok(), "{b:?}");
    let (a, b) = tokio::join!(
        rooms::patch_item(&store, &code, "a", patch(2, "three"), now),
        rooms::patch_item(&store, &code, "a", patch(2, "four"), now)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    match a.err().or(b.err()).unwrap() {
        Failure::StaleItem {
            item,
            item_revision,
            ..
        } => {
            assert!(!item.is_null());
            assert_eq!(item_revision, 3);
        }
        e => panic!("{e:?}"),
    }
    let room = store.rooms.get(&code, now).await.unwrap();
    assert_eq!(room.revision, 3);
    assert_eq!(room.item_revisions["b"], 2);
    let current_expiry: i64 = redis::cmd("EXPIRETIME")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(current_expiry, expiry);
    let mut candidate = room.clone();
    candidate.revision += 1;
    candidate.expires_at += 900;
    assert!(
        store
            .rooms
            .compare_set(room.revision, &candidate, now)
            .await
            .is_err()
    );
    assert!(
        rooms::create(
            &store,
            &code,
            Create {
                schema_version: 1,
                document: document(vec![])
            },
            now
        )
        .await
        .is_err()
    );
}
#[tokio::test]
#[ignore = "Requires an isolated disposable Redis Unix socket"]
async fn redis_expiry_commit_boundary_discards_content_and_keeps_only_tombstone() {
    let (store, client) = disposable_redis().await;
    let now = annotations_api::core::now();
    let code = format!("expire-{}", annotations_api::core::id());
    let initial = populated(&store, &code, now - 898).await;
    let room = rooms::patch_item(&store, &code, "a", patch(1, "near expiry"), now)
        .await
        .unwrap();
    assert_eq!(room.expires_at, initial.expires_at);
    let mut candidate = room.clone();
    candidate.revision += 1;
    // Wait for the disposable key to expire, then attempt a write with an old time.
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    assert!(
        store
            .rooms
            .compare_set(room.revision, &candidate, now)
            .await
            .unwrap_err()
            .1
            .starts_with("room_expired:")
    );
    assert_eq!(
        store.rooms.get(&code, now).await.unwrap_err().0.as_u16(),
        410
    );
    assert_eq!(
        rooms::create(
            &store,
            &code,
            Create {
                schema_version: 1,
                document: document(vec![])
            },
            annotations_api::core::now()
        )
        .await
        .unwrap_err()
        .0
        .as_u16(),
        410
    );
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let content: Option<String> = redis::cmd("GET")
        .arg(format!("annotations:room:{code}"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(content.is_none());
    let tombstone: String = redis::cmd("GET")
        .arg(format!("annotations:room-used:{code}"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(tombstone, room.expires_at.to_string());
    let ttl: i64 = redis::cmd("TTL")
        .arg(format!("annotations:room-used:{code}"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(ttl, -1);
}

#[tokio::test]
async fn download_stream_stops_at_original_expiry() {
    let stream =
        futures_util::stream::pending::<std::result::Result<axum::body::Bytes, std::io::Error>>();
    let body = rooms::download_body(
        axum::body::Body::from_stream(stream),
        annotations_api::core::now() + 1,
    )
    .unwrap();
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        axum::body::to_bytes(body, 100),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(bytes.is_empty());
    assert!(
        rooms::download_body(axum::body::Body::empty(), annotations_api::core::now() - 1).is_err()
    );
}
#[tokio::test]
async fn slow_upload_stops_at_expiry_and_keeps_cleanup_ownership() {
    use tower::ServiceExt;
    let store = fixture::store().await;
    fixture::create(&store, "slow-upload", annotations_api::core::now() - 898).await;
    let record = annotations_api::domain::transfers::begin(
        &store,
        "slow-upload",
        "127.0.0.1",
        fixture::metadata(b"hello"),
        None,
        annotations_api::core::now(),
    )
    .await
    .unwrap();
    let router = annotations_api::presentation::router(annotations_api::domain::App {
        store: store.clone(),
    });
    let incoming =
        futures_util::stream::pending::<std::result::Result<axum::body::Bytes, std::io::Error>>();
    let mut request = axum::http::Request::builder()
        .method("PUT")
        .uri(format!("/instant/slow-upload/files/{}/content", record.id))
        .header("content-type", "text/plain")
        .body(axum::body::Body::from_stream(incoming))
        .unwrap();
    request.extensions_mut().insert(axum::extract::ConnectInfo(
        "127.0.0.1:54321".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let response = tokio::time::timeout(std::time::Duration::from_secs(3), router.oneshot(request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 410);
    assert!(store.transfers.get(&record.id).await.unwrap().is_some());
    assert_eq!(
        annotations_api::domain::tasks::cleanup(&store, record.expires_at + 30)
            .await
            .unwrap(),
        1
    );
}
