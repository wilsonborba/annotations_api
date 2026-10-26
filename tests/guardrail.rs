use annotations_api::{
    dal::Store,
    domain::schemas::{self, Draft},
};
use serde_json::json;
#[tokio::test]
async fn only_registered_published_documents_pass() {
    let store = Store::fixture().await;
    let body = json!({"blocks":[{"type":"paragraph","text":"A plain default article."}]});
    assert!(
        schemas::validate(&store, "article-body", None, &body)
            .await
            .is_err()
    );
    let input = json!({"name":"article-body","version":1,"fields":[{"path":"blocks","field_type":"array","required":true},{"path":"blocks[].type","field_type":"string","required":true}]});
    schemas::draft(
        &store,
        serde_json::from_value::<Draft>(input.clone()).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        schemas::validate(&store, "article-body", Some(1), &body)
            .await
            .is_err()
    );
    schemas::publish(&store, "article-body", 1).await.unwrap();
    assert_eq!(
        schemas::validate(&store, "article-body", None, &body)
            .await
            .unwrap(),
        1
    );
    assert!(
        schemas::draft(&store, serde_json::from_value(input).unwrap())
            .await
            .is_err()
    );
    assert!(
        schemas::validate(&store, "article-body", None, &json!({"blocks":"wrong"}))
            .await
            .is_err()
    );
    assert!(
        schemas::validate(
            &store,
            "article-body",
            None,
            &json!({"blocks":[{"type":"script","text":"bad"}]})
        )
        .await
        .is_err()
    );
    assert!(
        schemas::validate(
            &store,
            "article-body",
            None,
            &json!({"blocks":[{"type":"paragraph","text":"a"}],"unknown":true})
        )
        .await
        .is_err()
    );
    if let annotations_api::dal::documents::Documents::Memory(m) = &store.docs {
        assert!(m.lock().unwrap().is_empty());
    }
}
