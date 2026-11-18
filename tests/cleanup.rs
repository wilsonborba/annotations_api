#[path = "support/rooms.rs"]
mod fixture;
use annotations_api::domain::{tasks, transfers};
#[tokio::test]
async fn expiry_keeps_cleanup_references_and_preserves_permanent_media() {
    let store = fixture::store().await;
    let clock = annotations_api::core::now();
    fixture::create(&store, "transfer", clock).await;
    let reservation = transfers::begin(
        &store,
        "transfer",
        "ip",
        fixture::metadata(b"hello"),
        None,
        clock,
    )
    .await
    .unwrap();
    let ready = transfers::upload(
        &store,
        "transfer",
        &reservation.id,
        "ip",
        Some("text/plain"),
        futures_util::stream::iter(vec![Ok::<_, std::io::Error>(b"hello".to_vec())]),
        clock,
    )
    .await
    .unwrap();
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), b"hello").unwrap();
    let permanent = store
        .files
        .upload_path(
            temp.path(),
            "permanent.png",
            "image/png",
            "article-images",
            5,
        )
        .await
        .unwrap();
    assert!(store.rooms.get("transfer", clock + 900).await.is_err());
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(tasks::cleanup(&store, clock + 900).await.unwrap(), 1);
    assert!(
        store
            .files
            .read(ready.file_key.as_deref().unwrap())
            .await
            .is_err()
    );
    assert!(store.files.read(&permanent.key).await.is_ok());
    assert_eq!(tasks::cleanup(&store, clock + 900).await.unwrap(), 0);
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cleanup_retries_after_file_storage_recovers() {
    let mut store = fixture::store().await;
    let clock = annotations_api::core::now();
    fixture::create(&store, "retry", clock).await;
    let reservation = transfers::begin(
        &store,
        "retry",
        "ip",
        fixture::metadata(b"hello"),
        None,
        clock,
    )
    .await
    .unwrap();
    let original = store.files.clone();
    let mut settings = annotations_api::core::Settings::load();
    settings.fsm_url = "http://127.0.0.1:1".into();
    settings.fsm_key = "fixture".into();
    store.files = annotations_api::dal::files::Files::new(settings, reqwest::Client::new());
    assert_eq!(tasks::cleanup(&store, clock + 900).await.unwrap(), 0);
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_some()
    );
    store.files = original;
    assert_eq!(tasks::cleanup(&store, clock + 900).await.unwrap(), 1);
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_none()
    );
}
