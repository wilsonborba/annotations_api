use annotations_api::{
    dal::Store,
    domain::waitlist::{Request, register},
};
#[tokio::test]
async fn waitlist_deduplicates_without_creating_an_author() {
    let s = Store::fixture().await;
    assert_eq!(
        register(
            &s,
            Request {
                email: "Reader@Example.com".into(),
                message: "Interested".into()
            }
        )
        .await
        .unwrap()["status"],
        "registered"
    );
    assert_eq!(
        register(
            &s,
            Request {
                email: "reader@example.com".into(),
                message: "Again".into()
            }
        )
        .await
        .unwrap()["status"],
        "already_registered"
    );
    assert!(
        register(
            &s,
            Request {
                email: "not-email".into(),
                message: "hello".into()
            }
        )
        .await
        .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM owners")
        .fetch_one(&s.db)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
