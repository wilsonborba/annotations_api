//! Note capacity (annotations_api#19): a 30-page dense handwritten note.
use annotations_api::{
    core::NOTE_LIMITS,
    dal::{
        Store,
        documents::{Documents, gzip},
    },
    domain::{
        App,
        note_documents::{self, Family},
        schemas, stroke_samples,
    },
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::path::Path;
use tower::ServiceExt;

/// One stroke like the editor capture: 84 samples at 240 Hz, 5 fitted nodes.
fn stroke(i: usize) -> Value {
    let samples = 84;
    let x0 = 40.0 + (i % 40) as f64 * 23.0;
    let y0 = 60.0 + ((i / 40) % 20) as f64 * 36.0 + (i / 800) as f64 * 760.0;
    let q = |v: f64| (v * 100.0).round() as i64;
    let a = |k: usize| k as f64 / (samples - 1) as f64;
    let xs: Vec<i64> = (0..samples)
        .map(|k| q(x0 + a(k) * 18.0 + (a(k) * 12.6).sin() * 3.0))
        .collect();
    let ys: Vec<i64> = (0..samples)
        .map(|k| q(y0 + (a(k) * 12.6).cos() * 4.0))
        .collect();
    let ts: Vec<usize> = (0..samples).map(|k| k * 1000 / 240).collect();
    let ps: Vec<i64> = (0..samples).map(|k| 350 + (k * 5) as i64).collect();
    let tilts: Vec<i64> = (0..samples).map(|k| 600 + k as i64).collect();
    let nodes: Vec<Value> = [0, 20, 41, 62, 83]
        .iter()
        .map(|&j| json!({"x":xs[j],"y":ys[j],"in":{"x":xs[j] - 150,"y":ys[j]},"out":{"x":xs[j] + 150,"y":ys[j]},"mode":"smooth","corner":false,"sample":j}))
        .collect();
    json!({"id":format!("s{i}"),"kind":"stroke","tool":"round","brush_version":1,"width":3,"color":"#000000FF","opacity":1,
        "samples":stroke_samples::encode(&stroke_samples::Columns{x:xs,y:ys,t:ts.iter().map(|t|*t as i64).collect(),pressure:Some(ps),tilt:Some(tilts)}),"nodes":nodes})
}

fn peak_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

#[test]
#[ignore = "measurement, run with --ignored"]
fn measure_a_30_page_note() {
    let pages = 30;
    let items: Vec<Value> = (0..pages * 800).map(stroke).collect();
    let raw = serde_json::to_vec(
        &json!({"page":{"width":1000,"background":{"kind":"blank"}},"items":items}),
    )
    .unwrap();
    drop(items);
    let before = peak_kb();
    let start = std::time::Instant::now();
    let doc: Value = serde_json::from_slice(&raw).unwrap();
    let parse = start.elapsed();
    let limits = NOTE_LIMITS;
    let start = std::time::Instant::now();
    let checked = note_documents::check_document(Family::Original, 1, &doc, &limits);
    let check = start.elapsed();
    println!(
        "30 pages: {} bytes JSON; parse {parse:?}, schema check {check:?} ({}); peak memory {} MB (before parse {} MB)",
        raw.len(),
        checked.is_ok(),
        peak_kb() / 1024,
        before / 1024
    );
}

async fn setup() -> (Store, Router) {
    let store = Store::fixture().await;
    for name in ["note-original", "note-typed"] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/document_schema/families")
            .join(format!("{name}.v1.json"));
        let draft: schemas::Draft =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        schemas::draft(&store, draft).await.unwrap();
        schemas::publish(&store, name, 1).await.unwrap();
    }
    sqlx::query("INSERT INTO languages(code,label) VALUES('en','English')")
        .execute(&store.db)
        .await
        .unwrap();
    let router = presentation::router(App {
        store: store.clone(),
    });
    (store, router)
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-uuid", "user-a")
        .header("content-type", "application/json");
    for (k, v) in headers {
        request = request.header(*k, *v);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

/// annotations_api#19: a 30-page dense note saves (gzip request body), is
/// stored as one CouchDB document with a gzip body attachment, takes a small
/// change as an operation and publishes.
#[tokio::test]
async fn a_thirty_page_note_saves_syncs_by_operation_and_publishes() {
    let (store, router) = setup().await;
    let items: Vec<Value> = (0..30 * 800).map(stroke).collect();
    let document = json!({"page":{"width":1000,"background":{"kind":"blank"}},"items":items});
    let body = serde_json::to_vec(&json!({
        "client_note_id": "big",
        "representations": {"original": {"document": document}},
    }))
    .unwrap();
    let packed = gzip(&body);
    assert!(
        packed.len() * 3 < body.len(),
        "gzip makes the upload smaller"
    );
    let (status, created) = send(
        &router,
        "POST",
        "/me/notes",
        &[("content-encoding", "gzip")],
        packed,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let note = created["note_id"].as_str().unwrap().to_string();
    // One CouchDB document per revision, its body a gzip attachment.
    let Documents::Memory(m) = &store.docs else {
        panic!("memory double expected")
    };
    let stored: Vec<Value> = m.lock().unwrap().values().cloned().collect();
    assert_eq!(stored.len(), 1);
    assert!(stored[0].get("body").is_none());
    let data = stored[0]["_attachments"]["body.json.gz"]["data"]
        .as_str()
        .unwrap();
    assert!(data.len() < body.len() / 2);
    // A later change goes as one small operation, not the whole note.
    let operation =
        serde_json::to_vec(&json!({"original": {"op": "add", "item": stroke(30 * 800)}})).unwrap();
    assert!(operation.len() < 4096);
    let (status, changed) = send(
        &router,
        "PATCH",
        &format!("/me/notes/{note}"),
        &[("if-match", "1")],
        operation,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{changed}");
    assert_eq!(changed["revision"], 2);
    let (status, detail) = send(&router, "GET", &format!("/me/notes/{note}"), &[], vec![]).await;
    assert_eq!(status, StatusCode::OK);
    let revision = detail["representations"][0]["revision_id"]
        .as_str()
        .unwrap()
        .to_string();
    let reply =
        serde_json::to_vec(&json!({"language": "en", "representations": {"original": revision}}))
            .unwrap();
    let (status, published) = send(
        &router,
        "PUT",
        &format!("/me/notes/{note}/publication"),
        &[("if-match", "0")],
        reply,
    )
    .await;
    assert!(status.is_success(), "{status} {published}");
}

#[tokio::test]
async fn a_broken_gzip_body_is_a_clear_error() {
    let (_, router) = setup().await;
    let (status, body) = send(
        &router,
        "POST",
        "/me/notes",
        &[("content-encoding", "gzip")],
        b"not gzip".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["code"], "invalid_encoding");
}
