//! Isolated localhost fixture; never reads .env or production storage.
use annotations_api::{dal::Store, domain::App, presentation};
use axum::{
    extract::Request,
    http::{HeaderValue, Method},
    middleware::{self, Next},
    response::Response,
};
async fn cors(request: Request, next: Next) -> Response {
    let origin = request.headers().get("origin").cloned();
    let preflight = request.method() == Method::OPTIONS;
    let mut response = if preflight {
        Response::new(axum::body::Body::empty())
    } else {
        next.run(request).await
    };
    if let Some(origin) = origin
        && origin
            .to_str()
            .is_ok_and(|v| v.starts_with("http://127.0.0.1:") || v.starts_with("http://localhost:"))
    {
        response
            .headers_mut()
            .insert("access-control-allow-origin", origin);
        response.headers_mut().insert(
            "access-control-allow-headers",
            HeaderValue::from_static("content-type"),
        );
        response.headers_mut().insert(
            "access-control-allow-methods",
            HeaderValue::from_static("GET,POST,PUT,OPTIONS"),
        );
    }
    response
}
#[tokio::main]
async fn main() {
    assert!(
        cfg!(debug_assertions),
        "Fixture must never run as a release service"
    );
    let app = presentation::router(App {
        store: Store::fixture().await,
    })
    .layer(middleware::from_fn(cors));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:18111")
        .await
        .unwrap();
    println!("Isolated in-memory/SQLite fixture at http://127.0.0.1:18111");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    .unwrap();
}
