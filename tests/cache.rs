use annotations_api::dal::cache::Cache;
#[tokio::test]
async fn invalidation_cannot_republish_a_stale_read_under_new_epoch() {
    let cache = Cache::fixture();
    let old = cache.epoch().await.unwrap();
    cache.put_at(old, "pub:en:post", "English").await;
    assert!(cache.get_at(old, "pub:pt-BR:post").await.is_none());
    cache.invalidate().await;
    cache
        .put_at(old, "pub:en:post", "late old public response")
        .await;
    let new = cache.epoch().await.unwrap();
    assert_ne!(old, new);
    assert!(cache.get_at(new, "pub:en:post").await.is_none());
    let absent = Cache::new(None);
    assert!(absent.epoch().await.is_none());
}
