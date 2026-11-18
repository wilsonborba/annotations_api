use annotations_api::{
    core::Result as CoreResult,
    dal::{
        Store,
        waitlist::{BoxFuture, WaitlistClient},
    },
    domain::waitlist::{Request, register},
};
use std::sync::{Arc, Mutex};

struct MockWaitlist {
    emails: Mutex<Vec<(String, Option<String>)>>,
}

impl MockWaitlist {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            emails: Mutex::new(Vec::new()),
        })
    }
}

impl WaitlistClient for MockWaitlist {
    fn is_configured(&self) -> bool {
        true
    }
    fn join<'a>(
        &'a self,
        email: &'a str,
        forwarded_for: Option<&'a str>,
    ) -> BoxFuture<'a, CoreResult<()>> {
        Box::pin(async move {
            self.emails
                .lock()
                .unwrap()
                .push((email.to_string(), forwarded_for.map(str::to_string)));
            Ok(())
        })
    }
}

#[tokio::test]
async fn waitlist_forwards_to_central_gateway_without_local_table_or_owner() {
    let mock = MockWaitlist::new();
    let mut s = Store::fixture().await;
    s.waitlist = mock.clone();

    let table_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='waitlist'",
    )
    .fetch_one(&s.db)
    .await
    .unwrap();
    assert_eq!(table_count, 0, "local waitlist table must be dropped");

    assert_eq!(
        register(
            &s,
            Request {
                email: "Reader@Example.com".into(),
                message: "Interested".into()
            },
            Some("203.0.113.10"),
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
            },
            None,
        )
        .await
        .unwrap()["status"],
        "registered"
    );
    assert!(
        register(
            &s,
            Request {
                email: "not-email".into(),
                message: "hello".into()
            },
            None,
        )
        .await
        .is_err()
    );
    {
        let recorded = mock.emails.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert_eq!(
            recorded[0],
            ("reader@example.com".into(), Some("203.0.113.10".into()))
        );
        assert_eq!(recorded[1], ("reader@example.com".into(), None));
    }

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM owners")
        .fetch_one(&s.db)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
