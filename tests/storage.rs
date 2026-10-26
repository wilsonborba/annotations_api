use annotations_api::dal::Store;
#[tokio::test]
async fn isolated_schema_has_relations_and_no_json_columns() {
    let store = Store::fixture().await;
    store.ready().await.unwrap();
    let e=sqlx::query("INSERT INTO posts(id,owner_id,slug,visibility,status,created_at,updated_at) VALUES('p','missing','p','public','draft',0,0)").execute(&store.db).await;
    assert!(e.is_err(), "foreign keys must reject unknown owners");
    let migration = include_str!("../migrations/001_schema.sql").to_lowercase();
    assert!(!migration.contains("json"));
    assert!(!migration.contains("insert into"));
}
