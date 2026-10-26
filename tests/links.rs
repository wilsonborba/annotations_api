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

#[test]
fn youtube_embed_is_derived_only_for_a_valid_video_identifier() {
    use annotations_api::domain::links::metadata;
    assert_eq!(
        metadata("https://youtu.be/dQw4w9WgXcQ").unwrap()["embed_url"],
        "https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ"
    );
    assert!(
        metadata("https://youtube.com/watch?v=bad")
            .unwrap()
            .get("embed_url")
            .is_none()
    );
}
