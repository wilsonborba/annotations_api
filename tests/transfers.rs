use annotations_api::{
    dal::Store,
    domain::{rooms, transfers},
};
#[tokio::test]
async fn simultaneous_reservations_cannot_exceed_the_uploader_ip_allowance() {
    let store = Store::fixture().await;
    rooms::create(&store.rooms, "first", 100).await.unwrap();
    rooms::create(&store.rooms, "second", 100).await.unwrap();
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
    let store = Store::fixture().await;
    rooms::create(&store.rooms, "upload", annotations_api::core::now())
        .await
        .unwrap();
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
    let url = room["items"][0]["url"].as_str().unwrap();
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
    let store = Store::fixture().await;
    let created = rooms::create(&store.rooms, "mistake", annotations_api::core::now())
        .await
        .unwrap();
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
    let file_id = room["items"][0]["id"].as_str().unwrap().to_string();
    let url = room["items"][0]["url"].as_str().unwrap().to_string();
    let text = Request::builder()
        .method("POST")
        .uri("/instant/mistake/items")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"kind":"text","text":"typo"}"#))
        .unwrap();
    let room = json(router.clone().oneshot(text).await.unwrap()).await;
    let text_id = room["items"][1]["id"].as_str().unwrap().to_string();
    let delete = |id: &str| {
        Request::builder()
            .method("DELETE")
            .uri(format!("/instant/mistake/items/{id}"))
            .body(Body::empty())
            .unwrap()
    };
    let response = router.clone().oneshot(delete(&file_id)).await.unwrap();
    assert_eq!(response.status(), 200);
    let room = json(response).await;
    assert_eq!(room["items"].as_array().unwrap().len(), 1);
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
    let room = json(router.clone().oneshot(delete(&text_id)).await.unwrap()).await;
    assert!(room["items"].as_array().unwrap().is_empty());
    assert_eq!(
        router.oneshot(delete("unknown")).await.unwrap().status(),
        404
    );
}
