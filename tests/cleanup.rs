use annotations_api::{
    dal::Store,
    domain::{rooms, tasks, transfers},
};
#[tokio::test]
async fn expiry_keeps_cleanup_references_and_preserves_permanent_media() {
    let store = Store::fixture().await;
    rooms::create(&store.rooms, "transfer", 100).await.unwrap();
    let reservation = transfers::begin(&store, "transfer", "ip", 5, 101)
        .await
        .unwrap();
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), b"hello").unwrap();
    let file = store
        .files
        .upload_path(temp.path(), "file.txt", "text/plain", &reservation.album, 5)
        .await
        .unwrap();
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
    transfers::attach(&store, "transfer", &reservation, &file, 102)
        .await
        .unwrap();
    assert!(store.rooms.get("transfer", 1000).await.is_err());
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(tasks::cleanup(&store, 1000).await.unwrap(), 1);
    assert!(store.files.read(&file.key).await.is_err());
    assert!(store.files.read(&permanent.key).await.is_ok());
    assert_eq!(tasks::cleanup(&store, 1000).await.unwrap(), 0);
    assert!(
        store
            .transfers
            .get(&reservation.id)
            .await
            .unwrap()
            .is_none()
    );
}
