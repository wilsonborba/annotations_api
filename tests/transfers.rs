#[path = "support/rooms.rs"]
mod fixture;
use annotations_api::domain::transfers;
#[tokio::test]
async fn simultaneous_reservations_cannot_exceed_the_uploader_ip_allowance() {
    let store = fixture::store().await;
    fixture::create(&store, "first", 100).await;
    fixture::create(&store, "second", 100).await;
    let (a, b) = tokio::join!(
        transfers::begin(&store, "first", "uploader", 30_000_000, 101),
        transfers::begin(&store, "second", "uploader", 30_000_000, 101)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let saved = a.or(b).unwrap();
    assert!(
        transfers::begin(&store, "first", "uploader", 51_000_000, 101)
            .await
            .is_err()
    );
    assert!(
        transfers::begin(&store, "second", "other-uploader", 30_000_000, 101)
            .await
            .is_ok()
    );
    assert_eq!(
        store.transfers.get(&saved.id).await.unwrap().unwrap().ip,
        "uploader"
    );
    store.transfers.release(&saved.id).await.unwrap();
    store.transfers.release(&saved.id).await.unwrap();
    assert!(
        transfers::begin(&store, "first", "uploader", 50_000_000, 102)
            .await
            .is_ok()
    );
    assert!(store.rooms.get("first", 1000).await.is_err());
    assert!(!store.transfers.due(1000).await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_size_check_rolls_back_quota_and_valid_upload_streams_back() {
    use axum::{
        body::{Body, to_bytes},
        extract::ConnectInfo,
        http::Request,
    };
    use tower::ServiceExt;
    let store = fixture::store().await;
    fixture::create(&store, "upload", annotations_api::core::now()).await;
    let router = annotations_api::presentation::router(annotations_api::domain::App {
        store: store.clone(),
    });
    let multipart = "--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"note.pdf\"\r\nContent-Type: application/pdf\r\n\r\nhello\r\n--b--\r\n";
    let upload = |size| {
        let mut r = Request::builder()
            .method("POST")
            .uri(format!("/instant/upload/files?size={size}"))
            .header("content-type", "multipart/form-data; boundary=b")
            .body(Body::from(multipart))
            .unwrap();
        r.extensions_mut().insert(ConnectInfo(
            "127.0.0.1:54321".parse::<std::net::SocketAddr>().unwrap(),
        ));
        r
    };
    assert_eq!(
        router.clone().oneshot(upload(10)).await.unwrap().status(),
        422
    );
    if let annotations_api::dal::transfers::Transfers::Memory(m) = &store.transfers {
        assert!(m.lock().unwrap().is_empty());
    }
    let response = router.clone().oneshot(upload(5)).await.unwrap();
    assert_eq!(response.status(), 200);
    let room: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 10000).await.unwrap()).unwrap();
    let url = room["files"][0]["url"].as_str().unwrap();
    let response = router
        .oneshot(Request::builder().uri(url).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(
        &to_bytes(response.into_body(), 10000).await.unwrap()[..],
        b"hello"
    );
}
#[tokio::test]
async fn removing_items_deletes_file_bytes_and_releases_quota_without_extending_expiry() {
    use axum::{
        body::{Body, to_bytes},
        extract::ConnectInfo,
        http::Request,
    };
    use tower::ServiceExt;
    let store = fixture::store().await;
    let created = fixture::create(&store, "mistake", annotations_api::core::now()).await;
    let router = annotations_api::presentation::router(annotations_api::domain::App {
        store: store.clone(),
    });
    let json = |response: axum::response::Response| async move {
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), 10000).await.unwrap(),
        )
        .unwrap()
    };
    let multipart = "--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"wrong.pdf\"\r\nContent-Type: application/pdf\r\n\r\nhello\r\n--b--\r\n";
    let mut upload = Request::builder()
        .method("POST")
        .uri("/instant/mistake/files?size=5")
        .header("content-type", "multipart/form-data; boundary=b")
        .body(Body::from(multipart))
        .unwrap();
    upload.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:54321".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let room = json(router.clone().oneshot(upload).await.unwrap()).await;
    let file_id = room["files"][0]["id"].as_str().unwrap().to_string();
    let url = room["files"][0]["url"].as_str().unwrap().to_string();
    // Uploads register files; the editor explicitly positions the card.
    let card = serde_json::json!({"id":"card", "kind":"file_card", "x":0,"y":0,"width":200,"height":80,"file_id":file_id});
    let text = serde_json::json!({"id":"text", "kind":"text", "x":0,"y":100,"width":200,"height":80,"spans":[{"text":"typo"}]});
    for item in [card, text] {
        let request = Request::builder()
            .method("POST")
            .uri("/instant/mistake/items")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"expected_revision":0,"item":item}).to_string(),
            ))
            .unwrap();
        assert_eq!(router.clone().oneshot(request).await.unwrap().status(), 200);
    }
    let text_id = "text";
    let delete = |id: &str| {
        Request::builder()
            .method("DELETE")
            .uri(format!("/instant/mistake/items/{id}"))
            .header("if-match", "1")
            .body(Body::empty())
            .unwrap()
    };
    let response = router.clone().oneshot(delete("card")).await.unwrap();
    assert_eq!(response.status(), 200);
    let room = json(response).await;
    assert!(room["files"].as_array().unwrap().is_empty());
    assert_eq!(room["document"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(room["expires_at"], created.expires_at);
    if let annotations_api::dal::transfers::Transfers::Memory(m) = &store.transfers {
        assert!(m.lock().unwrap().is_empty(), "quota reservation released");
    }
    let response = router
        .clone()
        .oneshot(Request::builder().uri(&url).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let room = json(router.clone().oneshot(delete(text_id)).await.unwrap()).await;
    assert!(room["document"]["items"].as_array().unwrap().is_empty());
    assert_eq!(
        router.oneshot(delete("unknown")).await.unwrap().status(),
        409
    );
}

#[tokio::test]
async fn room_files_cannot_cross_rooms_and_last_card_releases_the_file() {
    use annotations_api::domain::rooms;
    use serde_json::json;
    let store = fixture::store().await;
    fixture::create(&store, "source", 100).await;
    fixture::create(&store, "other", 100).await;
    let reservation = transfers::begin(&store, "source", "uploader", 5, 101)
        .await
        .unwrap();
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), b"hello").unwrap();
    let file = store
        .files
        .upload_path(temp.path(), "file.txt", "text/plain", &reservation.album, 5)
        .await
        .unwrap();
    transfers::attach(&store, "source", &reservation, &file, 102)
        .await
        .unwrap();
    let card = |id: &str, y: i64| rooms::ItemWrite {
        expected_revision: 0,
        item: json!({"id":id,"kind":"file_card",
        "x":0,"y":y,"width":100,"height":50,"file_id":reservation.id}),
    };
    assert!(
        rooms::add(&store, "other", card("forbidden", 0), 103)
            .await
            .is_err()
    );
    rooms::add(&store, "source", card("first", 0), 103)
        .await
        .unwrap();
    rooms::add(&store, "source", card("second", 100), 103)
        .await
        .unwrap();
    rooms::remove(&store, "source", "first", 1, 104)
        .await
        .unwrap();
    assert!(store.files.read(&file.key).await.is_ok());
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_some()
    );
    rooms::remove(&store, "source", "second", 1, 105)
        .await
        .unwrap();
    assert!(store.files.read(&file.key).await.is_err());
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        transfers::begin(&store, "source", "uploader", 5, 1000)
            .await
            .err()
            .unwrap()
            .1
            .starts_with("room_expired:")
    );
    let router = annotations_api::presentation::router(annotations_api::domain::App { store });
    use tower::ServiceExt;
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/instant/source/files/{}", reservation.id))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 410);
}
