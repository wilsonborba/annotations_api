use annotations_api::{
    core::{NOTE_LIMITS, NoteLimits},
    dal::Store,
    domain::{
        App,
        note_documents::{
            self, DocumentError, Family, FileScope, INSTANT_KINDS, ORIGINAL_KINDS, TYPED_KINDS,
        },
        schemas,
    },
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashSet},
    path::{Path, PathBuf},
};
use tower::ServiceExt;

const OWNER: &str = "fixture-owner";
const FAMILIES: [&str; 3] = ["note-original", "note-typed", "instant-room"];

fn dir(sub: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/document_schema")
        .join(sub)
}
fn read_dir(sub: &str) -> Vec<(String, Value)> {
    let mut out: Vec<(String, Value)> = std::fs::read_dir(dir(sub))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .map(|p| {
            let text = std::fs::read_to_string(&p).unwrap();
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                serde_json::from_str(&text).unwrap(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Register and publish the three families through the existing admin routes.
async fn published() -> Store {
    let store = Store::fixture().await;
    let router = annotations_api::presentation::router(App {
        store: store.clone(),
    });
    for name in FAMILIES {
        let text =
            std::fs::read_to_string(dir("families").join(format!("{name}.v1.json"))).unwrap();
        let draft: Value = serde_json::from_str(&text).unwrap();
        let (status, _) = call(&router, "POST", "/admin/schemas", Some(&draft)).await;
        assert_eq!(status, StatusCode::OK, "draft {name}");
        let (status, _) = call(
            &router,
            "POST",
            &format!("/admin/schemas/{name}/versions/1/publish"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "publish {name}");
    }
    sqlx::raw_sql(
        "INSERT INTO owners(id,display_name) VALUES('fixture-owner','Fixture'),('other-owner','Other');",
    )
    .execute(&store.db)
    .await
    .unwrap();
    store
}
async fn add_file(store: &Store, id: &str, owner: &str, status: &str) {
    sqlx::query("INSERT INTO files(file_id,owner_id,fsm_reference,sha256,size_bytes,content_type,purpose,status,created_at,updated_at) VALUES($1,$2,'fsm-ref',$3,10,'application/octet-stream','attachment',$4,1,1) ON CONFLICT(file_id) DO NOTHING")
        .bind(id)
        .bind(owner)
        .bind("0".repeat(64))
        .bind(status)
        .execute(&store.db)
        .await
        .unwrap();
}
fn limits(fixture: &Value) -> NoteLimits {
    let mut l = NOTE_LIMITS;
    if let Some(o) = fixture["limits"].as_object() {
        for (k, v) in o {
            let v = v.as_u64().unwrap() as usize;
            match k.as_str() {
                "max_items" => l.max_items = v,
                "max_samples_per_stroke" => l.max_samples_per_stroke = v,
                "max_nodes_per_stroke" => l.max_nodes_per_stroke = v,
                "max_document_bytes" => l.max_document_bytes = v,
                _ => panic!("unknown limit {k}"),
            }
        }
    }
    l
}
/// The fixture verdict: parse (raw text only), pure item check, then registry and files.
async fn verdict(store: &Store, fixture: &Value) -> Result<i64, DocumentError> {
    let limits = limits(fixture);
    let name = fixture["family"].as_str().unwrap();
    let version = fixture["schema_version"].as_i64().unwrap();
    let document = match fixture["document_text"].as_str() {
        Some(text) => note_documents::parse_document(text.as_bytes(), &limits)?,
        None => fixture["document"].clone(),
    };
    let family = Family::parse(name).unwrap();
    note_documents::check_document(family, version, &document, &limits)?;
    let files: HashSet<String> = fixture["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    let scope = if family == Family::Instant {
        FileScope::Room(&files)
    } else {
        for f in &files {
            add_file(store, f, OWNER, "ready").await;
        }
        FileScope::Owner(OWNER)
    };
    note_documents::validate_document(store, name, Some(version), scope, &document, &limits).await
}

#[tokio::test]
async fn families_are_published_through_the_admin_routes() {
    let store = published().await;
    let router = annotations_api::presentation::router(App {
        store: store.clone(),
    });
    let (_, list) = call(&router, "GET", "/admin/schemas", None).await;
    for name in FAMILIES {
        assert!(
            list.as_array().unwrap().iter().any(|s| s["name"] == name
                && s["version"] == 1
                && s["status"] == "published"
                && s["is_default"] == true),
            "{name} is published"
        );
    }
    // A registry entry alone does not teach the server a new version.
    let draft = json!({"name":"note-original","version":2,"fields":[{"path":"items","field_type":"array","required":true}]});
    let (status, _) = call(&router, "POST", "/admin/schemas", Some(&draft)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    // The MVP article validator does not accept note documents.
    let body = json!({"page":{"width":100,"background":{"kind":"blank"}},"items":[]});
    assert!(
        schemas::validate(&store, "note-original", None, &body)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn every_fixture_gives_its_expected_verdict() {
    let store = published().await;
    let mut failures = vec![];
    for sub in ["valid", "invalid"] {
        for (file, fixture) in read_dir(sub) {
            let got = verdict(&store, &fixture).await;
            let expect = &fixture["expect"];
            let ok = match (&got, expect["valid"].as_bool().unwrap()) {
                (Ok(_), true) => true,
                (Err(e), false) => {
                    e.code == expect["code"].as_str().unwrap()
                        && e.path == expect["path"].as_str().unwrap()
                }
                _ => false,
            };
            if !ok {
                failures.push(format!("{sub}/{file}: got {got:?}, expected {expect}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    if let annotations_api::dal::documents::Documents::Memory(m) = &store.docs {
        assert!(
            m.lock().unwrap().is_empty(),
            "validation writes no document"
        );
    }
}

#[test]
fn fixtures_cover_every_kind_and_every_rule() {
    let mut kinds: BTreeSet<(String, String)> = BTreeSet::new();
    for (_, f) in read_dir("valid") {
        let family = f["family"].as_str().unwrap().to_string();
        let doc = &f["document"];
        let list = doc["items"]
            .as_array()
            .or(doc["blocks"].as_array())
            .unwrap();
        for item in list {
            kinds.insert((family.clone(), item["kind"].as_str().unwrap().to_string()));
            if item["kind"] == "shape" {
                kinds.insert((
                    family.clone(),
                    format!("shape:{}", item["shape"].as_str().unwrap()),
                ));
            }
        }
    }
    let mut missing = vec![];
    for (family, list) in [
        ("note-original", &ORIGINAL_KINDS[..]),
        ("note-typed", &TYPED_KINDS[..]),
        ("instant-room", &INSTANT_KINDS[..]),
    ] {
        for k in list {
            if !kinds.contains(&(family.to_string(), k.to_string())) {
                missing.push(format!("{family}:{k}"));
            }
        }
    }
    for s in ["circle", "rectangle", "triangle", "path"] {
        if !kinds.contains(&("note-original".into(), format!("shape:{s}"))) {
            missing.push(format!("shape:{s}"));
        }
    }
    let codes: BTreeSet<String> = read_dir("invalid")
        .into_iter()
        .map(|(_, f)| f["expect"]["code"].as_str().unwrap().to_string())
        .collect();
    // schema_not_published and storage_unavailable depend on server state and are
    // tested in Rust below; every other code has a fixture.
    for code in [
        "invalid_json",
        "non_finite_number",
        "document_too_large",
        "too_many_items",
        "too_many_samples",
        "too_many_nodes",
        "unsupported_schema_version",
        "invalid_type",
        "missing_field",
        "unknown_field",
        "unknown_kind",
        "kind_not_allowed",
        "invalid_variant",
        "invalid_id",
        "invalid_reference",
        "duplicate_id",
        "out_of_bounds",
        "out_of_range",
        "invalid_size",
        "invalid_color",
        "invalid_url",
        "markup_not_allowed",
        "invalid_text",
        "empty_array",
        "invalid_count",
        "invalid_sample_order",
        "missing_reference",
        "missing_file",
        "group_cycle",
        "multiple_groups",
    ] {
        if !codes.contains(code) {
            missing.push(format!("code:{code}"));
        }
    }
    assert!(missing.is_empty(), "missing fixtures: {missing:?}");
}

#[tokio::test]
async fn files_must_be_ready_files_of_the_owner_and_schema_must_be_published() {
    let store = published().await;
    let body = json!({"page":{"width":500,"background":{"kind":"blank"}},"items":[{"id":"i1","kind":"image","x":0,"y":0,"width":10,"height":10,"file_id":"f1"}]});
    add_file(&store, "f1", "other-owner", "ready").await;
    let run = |owner: &'static str| {
        let store = store.clone();
        let body = body.clone();
        async move {
            note_documents::validate_document(
                &store,
                "note-original",
                None,
                FileScope::Owner(owner),
                &body,
                &NOTE_LIMITS,
            )
            .await
        }
    };
    assert_eq!(run(OWNER).await.unwrap_err().code, "missing_file");
    assert_eq!(run("other-owner").await, Ok(1));
    add_file(&store, "f2", OWNER, "uploading").await;
    let pending = json!({"page":{"width":500,"background":{"kind":"blank"}},"items":[{"id":"i1","kind":"file_card","x":0,"y":0,"width":10,"height":10,"file_id":"f2"}]});
    assert_eq!(
        note_documents::validate_document(
            &store,
            "note-original",
            None,
            FileScope::Owner(OWNER),
            &pending,
            &NOTE_LIMITS
        )
        .await
        .unwrap_err()
        .code,
        "missing_file"
    );
    let fresh = Store::fixture().await;
    assert_eq!(
        note_documents::validate_document(
            &fresh,
            "note-typed",
            None,
            FileScope::Owner(OWNER),
            &json!({"blocks":[]}),
            &NOTE_LIMITS
        )
        .await
        .unwrap_err()
        .code,
        "schema_not_published"
    );
    let e: annotations_api::core::Error = DocumentError::new("group_cycle", "items[2]").into();
    assert_eq!(e.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        e.1,
        "group_cycle: Groups must not contain themselves at items[2]"
    );
}

/// Measurement used to set `NOTE_LIMITS`. Run with `--nocapture` to see the numbers.
/// Assumptions: a page 1000 units wide, handwriting sampled at 240 Hz (the highest
/// common stylus rate), 0.35 s per stroke, one editable node per 10 samples,
/// column-wise integer samples (1/100 page units), pressure and tilt present.
#[test]
fn realistic_notes_fit_the_limits() {
    fn stroke(i: usize, samples: usize) -> Value {
        // Integer columns: x and y in 1/100 page units, t in ms, pressure 0 to 1000,
        // tilt in milliradians.
        let x0 = 40.0 + (i % 40) as f64 * 23.0;
        let y0 = 60.0 + (i / 40) as f64 * 36.0;
        let q = |v: f64| (v * 100.0).round() as i64;
        let a = |k: usize| k as f64 / samples as f64;
        let xs: Vec<i64> = (0..samples).map(|k| q(x0 + a(k) * 18.37)).collect();
        let ys: Vec<i64> = (0..samples)
            .map(|k| q(y0 + (a(k) * std::f64::consts::TAU).sin() * 9.13))
            .collect();
        let ts: Vec<usize> = (0..samples).map(|k| k * 1000 / 240).collect();
        let ps: Vec<i64> = (0..samples)
            .map(|k| (350.0 + a(k) * 410.0).round() as i64)
            .collect();
        let tilts: Vec<i64> = (0..samples)
            .map(|k| (610.0 + a(k) * 130.0).round() as i64)
            .collect();
        let n: Vec<Value> = (0..samples.div_ceil(10))
            .map(|k| {
                let j = (k * 10).min(samples - 1);
                json!({"x":xs[j],"y":ys[j],"in":{"x":xs[j] - 211,"y":ys[j]},"out":{"x":xs[j] + 211,"y":ys[j]},"mode":"smooth","corner":false,"sample":j})
            })
            .collect();
        json!({"id":format!("s{i}"),"kind":"stroke","tool":"round","brush_version":1,"width":2.5,"color":"#000000FF","opacity":1,"samples":{"x":xs,"y":ys,"t":ts,"pressure":ps,"tilt":tilts},"nodes":n})
    }
    let samples = (240.0_f64 * 0.35).round() as usize;
    let one = serde_json::to_vec(&stroke(0, samples)).unwrap().len();
    let parts = stroke(0, samples);
    let sample_bytes = serde_json::to_vec(&parts["samples"]).unwrap().len();
    let node_bytes = serde_json::to_vec(&parts["nodes"]).unwrap().len();
    let one_120 = serde_json::to_vec(&stroke(0, samples / 2)).unwrap().len();
    let dense_page = 800; // about 250 words with 3 strokes per word
    let page = |strokes: usize| json!({"page":{"width":1000,"background":{"kind":"ruled","spacing":32,"visibility":0.4}},"items":(0..strokes).map(|i|stroke(i, samples)).collect::<Vec<_>>()});
    let page_bytes = serde_json::to_vec(&page(dense_page)).unwrap().len();
    let long = stroke(0, 7_200); // one 30 s continuous stroke at 240 Hz
    let long_bytes = serde_json::to_vec(&long).unwrap().len();
    let fit = NOTE_LIMITS.max_document_bytes / one;
    println!(
        "samples per stroke {samples}; bytes per stroke {one} (samples {sample_bytes}, nodes {node_bytes}; at 120 Hz {one_120}); dense page {dense_page} strokes = {page_bytes} bytes; 30 s stroke = {long_bytes} bytes; strokes that fit the byte limit {fit}"
    );
    let ok = note_documents::check_document(Family::Original, 1, &page(dense_page), &NOTE_LIMITS);
    assert!(ok.is_ok(), "{ok:?}");
    assert!(page_bytes < NOTE_LIMITS.max_document_bytes);
    let long_doc = json!({"page":{"width":1000,"background":{"kind":"blank"}},"items":[long]});
    let ok = note_documents::check_document(Family::Original, 1, &long_doc, &NOTE_LIMITS);
    assert!(ok.is_ok(), "{ok:?}");
    assert!(fit <= NOTE_LIMITS.max_items);
}
