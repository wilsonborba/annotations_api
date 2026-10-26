use annotations_api::domain::links;
#[test]
fn provider_cards_fall_back_without_remote_metadata() {
    assert_eq!(
        links::metadata("https://drive.google.com/file/d/example/view").unwrap()["provider"],
        "google-drive"
    );
    assert_eq!(
        links::metadata("https://discord.gg/example").unwrap()["provider"],
        "discord"
    );
    assert_eq!(
        links::metadata("https://example.com").unwrap()["thumbnail"],
        ""
    );
    assert!(links::metadata("javascript:alert(1)").is_err());
}
#[tokio::test]
async fn previews_do_not_fetch_private_hosts() {
    let store = annotations_api::dal::Store::fixture().await;
    assert!(
        links::preview(&store, "http://127.0.0.1:8001")
            .await
            .is_err()
    );
}
