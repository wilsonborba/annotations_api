use annotations_api::{
    dal::Store,
    domain::{posts, schemas},
};
use serde_json::json;
async fn prepared() -> Store {
    let s = Store::fixture().await;
    posts::catalog_write(&s,json!({"owners":[{"id":"wilson","display_name":"Wilson"}],"languages":[{"code":"en","label":"English"},{"code":"pt-BR","label":"Português"}],"channels":[{"id":"wilson","name":"Wilson"},{"id":"asodya","name":"Asodya"}],"topics":[{"id":"rag","name":"RAG"}]})).await.unwrap();
    schemas::draft(&s,serde_json::from_value(json!({"name":"article-body","version":1,"fields":[{"path":"blocks","field_type":"array","required":true}]})).unwrap()).await.unwrap();
    schemas::publish(&s, "article-body", 1).await.unwrap();
    s
}
fn post(visibility: &str) -> posts::WritePost {
    serde_json::from_value(json!({"owner_id":"wilson","slug":"retrieval-study","visibility":visibility,"channels":["wilson","asodya"],"topics":["rag"],"translations":[{"language":"en","title":"Retrieval study","body":{"blocks":[{"type":"paragraph","text":"English body"}]}},{"language":"pt-BR","title":"Estudo de recuperação","body":{"blocks":[{"type":"paragraph","text":"Corpo português"}]}}]})).unwrap()
}
#[tokio::test]
async fn translations_and_private_visibility_are_backend_owned() {
    let s = prepared().await;
    posts::write(&s, "study", post("public")).await.unwrap();
    assert!(posts::detail(&s, "study", "en", false, "").await.is_err());
    posts::publish(&s, "study").await.unwrap();
    let en = posts::detail(&s, "retrieval-study", "en", false, "wilson")
        .await
        .unwrap();
    assert_eq!(en["available_languages"], json!(["en", "pt-BR"]));
    assert_eq!(en["channels"], json!(["asodya", "wilson"]));
    let pt = posts::detail(&s, "study", "pt-BR", false, "")
        .await
        .unwrap();
    assert_eq!(pt["body"]["blocks"][0]["text"], "Corpo português");
    assert!(posts::detail(&s, "study", "th", false, "").await.is_err());
    posts::write(&s, "study", post("private")).await.unwrap();
    assert!(posts::detail(&s, "study", "en", false, "").await.is_err());
    assert_eq!(
        posts::list(&s, &posts::Query::default()).await.unwrap()["items"],
        json!([])
    );
    assert!(posts::detail(&s, "study", "en", true, "").await.is_ok());
}
#[tokio::test]
async fn invalid_translation_does_not_write_any_documents() {
    let s = prepared().await;
    let mut p = post("public");
    p.translations[1].body = json!({"blocks":"bad"});
    assert!(posts::write(&s, "bad", p).await.is_err());
    if let annotations_api::dal::documents::Documents::Memory(m) = &s.docs {
        assert!(m.lock().unwrap().is_empty());
    }
}
