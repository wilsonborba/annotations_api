use annotations_api::{
    dal::rooms::Rooms,
    domain::rooms::{self, Edit},
};
#[tokio::test]
async fn room_code_reuses_live_state_and_does_not_extend_expiry() {
    let db = Rooms::fixture();
    let first = rooms::create(&db, "my-transfer", 100).await.unwrap();
    let updated = rooms::edit(
        &db,
        "my-transfer",
        Edit {
            id: None,
            revision: None,
            kind: "text".into(),
            text: "hello".into(),
        },
        200,
    )
    .await
    .unwrap();
    let reopened = rooms::create(&db, "my-transfer", 300).await.unwrap();
    assert_eq!(reopened.items.len(), 1);
    assert_eq!(first.expires_at, reopened.expires_at);
    assert!(
        rooms::edit(
            &db,
            "my-transfer",
            Edit {
                id: Some(updated.items[0].id.clone()),
                revision: Some(0),
                kind: "text".into(),
                text: "stale edit".into()
            },
            400
        )
        .await
        .is_err()
    );
    assert!(db.get("my-transfer", 1000).await.is_err());
    assert_eq!(
        reopened.public()["items"][0]["file_key"],
        serde_json::Value::Null
    );
}
