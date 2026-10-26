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
