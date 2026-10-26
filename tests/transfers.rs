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
