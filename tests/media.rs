use annotations_api::{dal::Store,domain::{App,posts}};
use axum::{body::{Body,to_bytes},http::Request};
use tower::ServiceExt;
use serde_json::{Value,json};
#[tokio::test]
async fn uploaded_image_is_not_public_until_a_public_post_references_it(){
 let store=Store::fixture().await;
 posts::catalog_write(&store,json!({"owners":[{"id":"wilson","display_name":"Wilson"}]})).await.unwrap();
 let router=annotations_api::presentation::router(App{store:store.clone()});
 let body="--test-boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"image.png\"\r\nContent-Type: image/png\r\n\r\nimage-data\r\n--test-boundary--\r\n";
 let response=router.oneshot(Request::builder().method("POST").uri("/admin/images?owner_id=wilson").header("content-type","multipart/form-data; boundary=test-boundary").body(Body::from(body)).unwrap()).await.unwrap();
 assert_eq!(response.status(),200);
 let value:Value=serde_json::from_slice(&to_bytes(response.into_body(),10000).await.unwrap()).unwrap();let key=value["id"].as_str().unwrap();
 assert!(annotations_api::presentation::media::public(axum::extract::State(App{store:store.clone()}),axum::extract::Path(key.to_string())).await.is_err());
 assert!(annotations_api::presentation::media::private(axum::extract::State(App{store:store.clone()}),axum::extract::Path(key.to_string())).await.is_ok());
}
