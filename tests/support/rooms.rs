use annotations_api::{
    dal::Store,
    domain::{
        rooms::{self, Create, Room},
        schemas,
    },
};
use serde_json::json;
pub async fn store() -> Store {
    let store = Store::fixture().await;
    let draft = serde_json::from_str(include_str!(
        "../fixtures/document_schema/families/instant-room.v1.json"
    ))
    .unwrap();
    schemas::draft(&store, draft).await.unwrap();
    schemas::publish(&store, "instant-room", 1).await.unwrap();
    store
}
pub async fn create(store: &Store, code: &str, now: i64) -> Room {
    rooms::create(
        store,
        code,
        Create {
            schema_version: 1,
            document: json!({"page":{"width":1000,"background":{"kind":"blank"}},"items":[]}),
        },
        now,
    )
    .await
    .unwrap()
}
