//! Owner managed files (`/me/files`) and published media (`/media/{file_id}`).
//! SQLite stands in for PostgreSQL, the in-memory document store for CouchDB and
//! the FSM double for FSM: it keeps one blob per SHA-256 and counts its items.
use annotations_api::{
    core::{FILE_LIMITS, FILE_SETTINGS, FileSettings, Settings},
    dal::{Store, files::Files},
    domain::{
        App,
        files::{self, Cleaned, Purpose},
        note_saves::{
            self, Begin, Content, Representation, RepresentationInput, SaveRequest, Target,
        },
        schemas,
    },
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::path::Path;
use tower::ServiceExt;

const A: &str = "user-a";
const B: &str = "user-b";

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

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    bytes: Vec<u8>,
}
impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.bytes).unwrap_or(Value::Null)
    }
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> Reply {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(owner) = owner {
        request = request.header("x-uuid", owner);
    }
    for (k, v) in headers {
        request = request.header(*k, *v);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    Reply {
        status,
        headers,
        bytes,
    }
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// A small PNG: the signature, then filler bytes that make every test file distinct.
fn png(fill: u8, len: usize) -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
    bytes.extend((0..len).map(|i| fill.wrapping_add(i as u8)));
    bytes
}

async fn reserve(
    router: &Router,
    owner: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
    purpose: &str,
) -> Reply {
    call(
        router,
        "POST",
        "/me/files",
        Some(owner),
        &[("content-type", "application/json")],
        json!({"filename": filename, "content_type": content_type, "size": bytes.len(),
               "checksum": sha(bytes), "purpose": purpose})
        .to_string()
        .into_bytes(),
    )
    .await
}

async fn put(
    router: &Router,
    owner: &str,
    file_id: &str,
    content_type: &str,
    bytes: Vec<u8>,
) -> Reply {
    call(
        router,
        "PUT",
        &format!("/me/files/{file_id}/content"),
        Some(owner),
        &[("content-type", content_type)],
        bytes,
    )
    .await
}

/// Reserve and upload; returns the file id of the ready file.
async fn ready_file(router: &Router, owner: &str, content_type: &str, bytes: &[u8]) -> String {
    let r = reserve(
        router,
        owner,
        "picture.png",
        content_type,
        bytes,
        "attachment",
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = put(router, owner, &id, content_type, bytes.to_vec()).await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    assert_eq!(r.json()["status"], "ready");
    id
}

async fn status_of(store: &Store, file_id: &str) -> String {
    sqlx::query("SELECT status FROM files WHERE file_id=$1")
        .bind(file_id)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0)
}

fn fsm(store: &Store) -> (usize, usize) {
    store.files.fixture_counts().unwrap()
}

fn with_image(file_id: &str) -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id": "i1", "kind": "image", "x": 0, "y": 0, "width": 200, "height": 100,
             "file_id": file_id}
        ]
    })
}
fn plain() -> Value {
    json!({"page": {"width": 1000, "background": {"kind": "blank"}}, "items": []})
}
fn typed(text: &str) -> Value {
    json!({"blocks": [{"id": "k1", "kind": "paragraph", "spans": [{"text": text}]}]})
}

/// Create a note of `owner` with an Original that shows the file and a Typed text.
/// Returns (note id, original revision id).
async fn note_with(router: &Router, owner: &str, client: &str, file_id: &str) -> (String, String) {
    let r = call(
        router,
        "POST",
        "/me/notes",
        Some(owner),
        &[("content-type", "application/json")],
        json!({"client_note_id": client, "main_representation": "original",
               "representations": {"original": {"document": with_image(file_id)},
                                   "typed": {"document": typed("Cells")}}})
        .to_string()
        .into_bytes(),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    let body = r.json();
    (
        body["note_id"].as_str().unwrap().to_string(),
        body["revisions"][0]["revision_id"]
            .as_str()
            .unwrap()
            .to_string(),
    )
}

async fn publish_original(
    router: &Router,
    owner: &str,
    note_id: &str,
    revision: &str,
    if_match: &str,
) -> Reply {
    call(
        router,
        "PUT",
        &format!("/me/notes/{note_id}/publication"),
        Some(owner),
        &[("content-type", "application/json"), ("if-match", if_match)],
        json!({"language": "en", "representations": {"original": revision}})
            .to_string()
            .into_bytes(),
    )
    .await
}

/// An FSM adapter that cannot reach any server: every call fails.
fn broken_fsm() -> Files {
    let mut settings = Settings::load();
    settings.fsm_url = "http://127.0.0.1:1".into();
    settings.fsm_key = "fixture".into();
    Files::new(settings, reqwest::Client::new())
}

// Checklist: size or checksum mismatch never becomes ready; the type comes from the bytes.

#[tokio::test]
async fn size_checksum_or_type_mismatch_never_becomes_ready() {
    let (store, router) = setup().await;
    let bytes = png(1, 200);
    let r = reserve(&router, A, "leaf.png", "image/png", &bytes, "attachment").await;
    assert_eq!(r.status, StatusCode::CREATED);
    let body = r.json();
    assert_eq!(body["status"], "reserved");
    assert_eq!(body["filename"], "leaf.png");
    let id = body["file_id"].as_str().unwrap().to_string();

    // Same size, other bytes: the checksum differs.
    let r = put(&router, A, &id, "image/png", png(2, 200)).await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.json()["code"], "checksum_mismatch");
    assert_eq!(status_of(&store, &id).await, "failed");
    // Larger and smaller bodies.
    let r = put(&router, A, &id, "image/png", png(1, 201)).await;
    assert_eq!(r.json()["code"], "size_mismatch");
    let r = put(&router, A, &id, "image/png", png(1, 199)).await;
    assert_eq!(r.json()["code"], "size_mismatch");
    // The declared Content-Type must be the reserved one.
    let r = put(&router, A, &id, "image/jpeg", bytes.clone()).await;
    assert_eq!(r.json()["code"], "content_type_mismatch");
    assert_eq!(status_of(&store, &id).await, "failed");
    assert_eq!(fsm(&store), (0, 0), "nothing reached FSM");
    let content = call(
        &router,
        "GET",
        &format!("/me/files/{id}/content"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(content.status, StatusCode::CONFLICT);
    assert_eq!(content.json()["code"], "file_not_ready");

    // The right bytes after failures: ready, one FSM item.
    let r = put(&router, A, &id, "image/png", bytes.clone()).await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    assert_eq!(r.json()["status"], "ready");
    assert_eq!(fsm(&store), (1, 1));
    // The same bytes again are safe; other bytes never replace a ready file.
    let r = put(&router, A, &id, "image/png", bytes.clone()).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(fsm(&store), (1, 1));
    let r = put(&router, A, &id, "image/png", png(3, 200)).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["code"], "file_immutable");
    let r = put(&router, A, &id, "image/png", png(3, 500)).await;
    assert_eq!(r.json()["code"], "file_immutable");
    let content = call(
        &router,
        "GET",
        &format!("/me/files/{id}/content"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(content.bytes, bytes);
}

#[tokio::test]
async fn real_file_type_is_checked_from_the_bytes() {
    let (store, router) = setup().await;
    // Transcription and content_job still refuse mismatched bytes and unlisted types.
    let script = b"<html><script>alert(1)</script></html>".to_vec();
    let r = reserve(&router, A, "x.png", "image/png", &script, "transcription").await;
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = put(&router, A, &id, "image/png", script.clone()).await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.json()["code"], "content_type_mismatch");
    assert_eq!(status_of(&store, &id).await, "failed");
    let image = png(9, 40);
    let r = reserve(
        &router,
        A,
        "x.pdf",
        "application/pdf",
        &image,
        "content_job",
    )
    .await;
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = put(&router, A, &id, "application/pdf", image).await;
    assert_eq!(r.json()["code"], "content_type_mismatch");
    let binary = vec![b'a', 0xff, 0xfe, b'b'];
    let r = reserve(&router, A, "x.txt", "text/plain", &binary, "content_job").await;
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = put(&router, A, &id, "text/plain", binary).await;
    assert_eq!(r.json()["code"], "content_type_mismatch");
    assert_eq!(fsm(&store), (0, 0));
    // Scriptable types are never accepted for transcription or content_job.
    for kind in ["text/html", "image/svg+xml", "application/javascript"] {
        let r = reserve(&router, A, "x", kind, b"<svg/>", "transcription").await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(r.json()["code"], "unsupported_content_type");
    }
    // Byte signatures of the preview and non-attachment types.
    assert!(files::matches_type("image/jpeg", b"\xff\xd8\xff\xe0"));
    assert!(files::matches_type("image/gif", b"GIF89a.."));
    assert!(files::matches_type("image/webp", b"RIFF\0\0\0\0WEBPVP8 "));
    assert!(files::matches_type("image/avif", b"\0\0\0\x1cftypavif"));
    assert!(!files::matches_type("video/mp4", b"\0\0\0\x1cftypavif"));
    assert!(files::matches_type("video/mp4", b"\0\0\0\x18ftypmp42"));
    assert!(files::matches_type("audio/wav", b"RIFF\0\0\0\0WAVEfmt "));
    assert!(!files::matches_type("image/webp", b"RIFF\0\0\0\0WAVEfmt "));
    assert!(files::matches_type("audio/mpeg", b"ID3\x04"));
    assert!(files::matches_type("audio/ogg", b"OggS\0"));
    assert!(files::matches_type("video/webm", b"\x1a\x45\xdf\xa3"));
    assert!(files::matches_type("application/pdf", b"%PDF-1.7"));
    assert!(!files::matches_type("application/pdf", b"<html>"));
    assert!(!files::matches_type("image/png", b"\x89PNG"));
    // Every preview type and every non-attachment type has a byte check.
    for kind in files::PREVIEW_TYPES {
        assert!(
            !files::matches_type(kind, b"") || kind.starts_with("text/"),
            "{kind}"
        );
    }
    for purpose in [Purpose::Transcription, Purpose::ContentJob] {
        for kind in purpose.content_types().unwrap() {
            assert!(
                !files::matches_type(kind, b"") || kind.starts_with("text/"),
                "{kind}"
            );
        }
    }
    // Valid UTF-8 text works when a character is split across two chunks.
    let text = "Célula e núcleo\n".repeat(10).into_bytes();
    let r = reserve(&router, A, "cells.txt", "text/plain", &text, "attachment").await;
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    let split = text.iter().position(|b| *b >= 0x80).unwrap() + 1;
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
        vec![Ok(text[..split].to_vec()), Ok(text[split..].to_vec())];
    let file = files::upload(
        &store,
        A,
        &id,
        Some("text/plain; charset=utf-8"),
        futures_util::stream::iter(chunks),
    )
    .await
    .unwrap();
    assert_eq!(file.status, "ready");
}

#[tokio::test]
async fn reserve_checks_fields_purpose_limits_and_identity() {
    let (_store, router) = setup().await;
    let bytes = png(1, 10);
    let post = |body: Value, owner: Option<&'static str>| {
        let router = router.clone();
        async move {
            call(
                &router,
                "POST",
                "/me/files",
                owner,
                &[],
                body.to_string().into_bytes(),
            )
            .await
        }
    };
    let good = json!({"filename": "a.png", "content_type": "image/png", "size": bytes.len(),
                      "checksum": sha(&bytes), "purpose": "attachment"});
    let r = post(good.clone(), None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert_eq!(r.json()["code"], "identity_required");
    let cases = [
        ("purpose", json!("avatar"), "invalid_purpose"),
        ("filename", json!(""), "invalid_filename"),
        ("filename", json!("../a.png"), "invalid_filename"),
        ("filename", json!("a\nb.png"), "invalid_filename"),
        ("filename", json!("a".repeat(256)), "invalid_filename"),
        (
            "content_type",
            json!("not-a-media-type"),
            "unsupported_content_type",
        ),
        ("size", json!(0), "invalid_size"),
        (
            "size",
            json!(FILE_LIMITS.attachment_bytes + 1),
            "file_too_large",
        ),
        ("checksum", json!("abc"), "invalid_checksum"),
        ("owner_id", json!(B), "unknown_field"),
    ];
    for (key, value, code) in cases {
        let mut body = good.clone();
        body[key] = value;
        let r = post(body, Some(A)).await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{key}");
        assert_eq!(r.json()["code"], code, "{key}");
    }
    // Limits and types are per purpose.
    let mut body = good.clone();
    body["purpose"] = json!("transcription");
    body["size"] = json!(FILE_LIMITS.transcription_bytes + 1);
    assert_eq!(
        post(body.clone(), Some(A)).await.json()["code"],
        "file_too_large"
    );
    body["size"] = json!(FILE_LIMITS.transcription_bytes);
    assert_eq!(
        post(body.clone(), Some(A)).await.status,
        StatusCode::CREATED
    );
    body["content_type"] = json!("text/plain");
    assert_eq!(
        post(body, Some(A)).await.json()["code"],
        "unsupported_content_type"
    );
    let mut body = good.clone();
    body["purpose"] = json!("content_job");
    body["content_type"] = json!("video/mp4");
    body["size"] = json!(FILE_LIMITS.content_job_bytes);
    assert_eq!(post(body, Some(A)).await.status, StatusCode::CREATED);
    // The answer never carries an FSM key or the owner.
    let r = post(good, Some(A)).await;
    let mut keys: Vec<String> = r.json().as_object().unwrap().keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "checksum",
            "content_type",
            "created_at",
            "file_id",
            "filename",
            "preview",
            "purpose",
            "size",
            "status",
            "updated_at"
        ]
    );
}

// Checklist (#27 redo): any file type uploads as a file card; unverified types get no preview
// and are served as attachment with nosniff.

#[tokio::test]
async fn any_file_type_uploads_as_file_card_and_unverified_gets_no_preview() {
    let (_store, router) = setup().await;
    let docx_bytes = b"PK\x03\x04docx-body".to_vec();
    let zip_bytes = b"PK\x03\x04zip-body".to_vec();
    let docx_mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

    for (name, mime, bytes) in [
        ("report.docx", docx_mime, &docx_bytes),
        ("archive.zip", "application/zip", &zip_bytes),
    ] {
        let r = reserve(&router, A, name, mime, bytes, "attachment").await;
        assert_eq!(r.status, StatusCode::CREATED, "{name}: {:?}", r.json());
        let id = r.json()["file_id"].as_str().unwrap().to_string();
        let r = put(&router, A, &id, mime, bytes.clone()).await;
        assert_eq!(r.status, StatusCode::OK, "{name}: {:?}", r.json());
        assert_eq!(r.json()["status"], "ready");
        assert_eq!(r.json()["preview"], false);

        // Attach the file as a file_card in a note.
        let note_doc = json!({
            "page": {"width": 1000, "background": {"kind": "blank"}},
            "items": [
                {"id": "f1", "kind": "file_card", "x": 0, "y": 0, "width": 320, "height": 80,
                 "file_id": id}
            ]
        });
        let create = call(
            &router,
            "POST",
            "/me/notes",
            Some(A),
            &[("content-type", "application/json")],
            json!({
                "client_note_id": format!("card-{name}"),
                "main_representation": "original",
                "representations": {
                    "original": {"document": note_doc},
                    "typed": {"document": typed("Card")}
                }
            })
            .to_string()
            .into_bytes(),
        )
        .await;
        assert_eq!(create.status, StatusCode::CREATED, "{:?}", create.json());

        let dl = call(
            &router,
            "GET",
            &format!("/me/files/{id}/content"),
            Some(A),
            &[],
            vec![],
        )
        .await;
        assert_eq!(dl.status, StatusCode::OK);
        assert_eq!(dl.bytes, *bytes);
        assert_eq!(
            dl.headers.get("content-type").unwrap().to_str().unwrap(),
            "application/octet-stream"
        );
        assert!(
            dl.headers
                .get("content-disposition")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("attachment;")
        );
        assert_eq!(
            dl.headers
                .get("x-content-type-options")
                .unwrap()
                .to_str()
                .unwrap(),
            "nosniff"
        );
    }

    // A file whose content does not match a preview type gets no preview and is served as attachment.
    let fake_png = b"<html><script>alert(1)</script></html>".to_vec();
    let r = reserve(&router, A, "fake.png", "image/png", &fake_png, "attachment").await;
    assert_eq!(r.status, StatusCode::CREATED);
    let fake_id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = put(&router, A, &fake_id, "image/png", fake_png.clone()).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["status"], "ready");
    assert_eq!(r.json()["preview"], false);
    let dl = call(
        &router,
        "GET",
        &format!("/me/files/{fake_id}/content"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(dl.status, StatusCode::OK);
    assert_eq!(
        dl.headers.get("content-type").unwrap().to_str().unwrap(),
        "application/octet-stream"
    );
    assert!(
        dl.headers
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("attachment;")
    );
    assert_eq!(
        dl.headers
            .get("x-content-type-options")
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );

    // Verified preview types (PNG, PDF) still get preview=true.
    let pdf = b"%PDF-1.7\n%ok".to_vec();
    let r = reserve(&router, A, "doc.pdf", "application/pdf", &pdf, "attachment").await;
    let pdf_id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = put(&router, A, &pdf_id, "application/pdf", pdf).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["preview"], true);
}

// Checklist: two owners upload the same bytes: one FSM blob, two references, no cross access.

#[tokio::test]
async fn two_owners_same_bytes_share_one_blob_without_cross_access() {
    let (store, router) = setup().await;
    let bytes = png(7, 300);
    let a = ready_file(&router, A, "image/png", &bytes).await;
    let b = ready_file(&router, B, "image/png", &bytes).await;
    assert_ne!(a, b);
    assert_eq!(fsm(&store), (2, 1), "two FSM items, one blob");
    assert_eq!(store.files.fixture_refcount(&sha(&bytes)), Some(2));
    let refs: Vec<String> = sqlx::query("SELECT fsm_reference FROM files ORDER BY file_id")
        .fetch_all(&store.db)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(refs.len(), 2);
    assert_ne!(refs[0], refs[1], "each file is its own logical reference");
    // The same checksum never opens the other owner's file.
    for (method, uri) in [
        ("GET", format!("/me/files/{a}")),
        ("GET", format!("/me/files/{a}/content")),
        ("DELETE", format!("/me/files/{a}")),
    ] {
        let r = call(&router, method, &uri, Some(B), &[], vec![]).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{method} {uri}");
    }
    let r = put(&router, B, &a, "image/png", bytes.clone()).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // A note of B cannot use A's file.
    let r = call(
        &router,
        "POST",
        "/me/notes",
        Some(B),
        &[],
        json!({"client_note_id": "x", "representations": {"original": {"document": with_image(&a)}}})
            .to_string()
            .into_bytes(),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    // Each owner reads only its own file.
    let r = call(
        &router,
        "GET",
        &format!("/me/files/{b}/content"),
        Some(B),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.bytes, bytes);
}

// Checklist: delete with a live reference returns 409; last reference removal cleans FSM.

#[tokio::test]
async fn delete_with_live_reference_conflicts_and_last_release_cleans_fsm() {
    let (store, router) = setup().await;
    let bytes = png(5, 120);
    let a = ready_file(&router, A, "image/png", &bytes).await;
    let b = ready_file(&router, B, "image/png", &bytes).await;
    let (note, _) = note_with(&router, A, "n1", &a).await;
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{a}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["code"], "file_in_use");
    assert_eq!(fsm(&store), (2, 1));
    // Removing the note releases its references; the owner's file stays.
    let r = call(
        &router,
        "DELETE",
        &format!("/me/notes/{note}"),
        Some(A),
        &[("if-match", "1")],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(status_of(&store, &a).await, "ready");
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{a}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(fsm(&store), (1, 1), "B's item keeps the shared blob");
    assert_eq!(store.files.fixture_refcount(&sha(&bytes)), Some(1));
    let r = call(
        &router,
        "GET",
        &format!("/me/files/{b}/content"),
        Some(B),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.bytes, bytes);
    let r = call(
        &router,
        "GET",
        &format!("/me/files/{a}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // The last reference goes: FSM has no item and no blob.
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{b}"),
        Some(B),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(fsm(&store), (0, 0));
    let queued: i64 = sqlx::query("SELECT COUNT(*) FROM file_cleanup")
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(queued, 0);
    // A reserved file without bytes is released at once.
    let r = reserve(&router, A, "a.png", "image/png", &bytes, "attachment").await;
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{id}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn publication_and_pending_save_protect_the_file() {
    let (store, router) = setup().await;
    let a = ready_file(&router, A, "image/png", &png(4, 64)).await;
    // A published revision keeps the file after the working copy drops it.
    let (note, original) = note_with(&router, A, "n1", &a).await;
    let r = publish_original(&router, A, &note, &original, "0").await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    let r = call(
        &router,
        "PUT",
        &format!("/me/notes/{note}"),
        Some(A),
        &[("if-match", "1")],
        json!({"representation": "original", "document": plain()})
            .to_string()
            .into_bytes(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{a}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.json()["code"], "file_in_use");

    // A running save of a new note holds its file until it commits or fails.
    let c = ready_file(&router, A, "image/png", &png(6, 64)).await;
    let request = SaveRequest {
        owner_id: A.into(),
        operation_key: "op-1".into(),
        target: Target::Create {
            client_note_id: "n2".into(),
            main_representation: Representation::Original,
        },
        representations: vec![RepresentationInput {
            representation: Representation::Original,
            schema_version: None,
            content: Content::Document(with_image(&c)),
            source_revision_id: None,
        }],
        metadata: None,
    };
    let Begin::Run(pending) = note_saves::Pending::begin(&store, &request).await.unwrap() else {
        panic!("a new save runs")
    };
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{c}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["code"], "file_in_use");
    // The save stops before its commit: the hold is released.
    assert_eq!(pending.abandon(&store).await.unwrap(), None);
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{c}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    // A save that names a released file fails and holds nothing.
    let again = SaveRequest {
        operation_key: "op-2".into(),
        ..request
    };
    assert!(note_saves::save(&store, &again).await.is_err());
    let held: i64 = sqlx::query(
        "SELECT COUNT(*) FROM file_references WHERE reference_kind='pending_operation'",
    )
    .fetch_one(&store.db)
    .await
    .unwrap()
    .get(0);
    assert_eq!(held, 0);
}

#[tokio::test]
async fn fsm_cleanup_retries_a_bounded_number_of_times() {
    let (mut store, router) = setup().await;
    let bytes = png(8, 50);
    let id = ready_file(&router, A, "image/png", &bytes).await;
    let working = store.files.clone();
    store.files = broken_fsm();
    // The release commits although FSM is down; the item waits in the queue.
    files::release(&store, A, &id).await.unwrap();
    assert_eq!(working.fixture_counts(), Some((1, 1)));
    let settings = FileSettings {
        max_attempts: 3,
        ..FILE_SETTINGS
    };
    // The release made attempt 1. Not again before the retry wait.
    let mut now = annotations_api::core::now();
    assert_eq!(
        files::cleanup(&store, now, &settings).await.unwrap(),
        Cleaned::default()
    );
    for attempt in 2..=3 {
        now += settings.retry_seconds;
        let run = files::cleanup(&store, now, &settings).await.unwrap();
        assert_eq!(
            run,
            Cleaned {
                deleted: 0,
                waiting: 1,
                reset: 0
            },
            "attempt {attempt}"
        );
        assert_eq!(
            files::cleanup(&store, now, &settings).await.unwrap(),
            Cleaned::default()
        );
    }
    now += settings.retry_seconds;
    // The bound is reached: the item waits for an operator, nothing else runs.
    assert_eq!(
        files::cleanup(&store, now, &settings).await.unwrap(),
        Cleaned::default()
    );
    let attempts: i64 = sqlx::query("SELECT attempts FROM file_cleanup")
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(attempts, 3);
    // With the default bound and a working FSM the next run deletes it.
    store.files = working;
    let run = files::cleanup(&store, now, &FILE_SETTINGS).await.unwrap();
    assert_eq!(run.deleted, 1);
    assert_eq!(fsm(&store), (0, 0));
}

#[tokio::test]
async fn stopped_upload_is_reset_and_its_fsm_item_removed() {
    let (store, router) = setup().await;
    let bytes = png(3, 30);
    let r = reserve(&router, A, "a.png", "image/png", &bytes, "attachment").await;
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    // A crash after the FSM upload and before `ready`.
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), &bytes).unwrap();
    store
        .files
        .upload_path(
            temp.path(),
            "a.png",
            "image/png",
            &format!("file-{id}"),
            bytes.len() as i64,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE files SET status='uploading',updated_at=100 WHERE file_id=$1")
        .bind(&id)
        .execute(&store.db)
        .await
        .unwrap();
    let r = put(&router, A, &id, "image/png", bytes.clone()).await;
    assert_eq!(r.json()["code"], "upload_in_progress");
    let r = call(
        &router,
        "DELETE",
        &format!("/me/files/{id}"),
        Some(A),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.json()["code"], "upload_in_progress");
    // Inside the grace period nothing changes.
    let run = files::cleanup(&store, 100 + FILE_SETTINGS.grace_seconds, &FILE_SETTINGS)
        .await
        .unwrap();
    assert_eq!(run, Cleaned::default());
    let run = files::cleanup(&store, 101 + FILE_SETTINGS.grace_seconds, &FILE_SETTINGS)
        .await
        .unwrap();
    assert_eq!(
        run,
        Cleaned {
            deleted: 1,
            waiting: 0,
            reset: 1
        }
    );
    assert_eq!(status_of(&store, &id).await, "failed");
    assert_eq!(fsm(&store), (0, 0));
    // The owner uploads again.
    let r = put(&router, A, &id, "image/png", bytes).await;
    assert_eq!(r.json()["status"], "ready");
    assert_eq!(fsm(&store), (1, 1));
}

// Checklist: public media for an unpublished note returns 404.

#[tokio::test]
async fn public_media_needs_a_published_reference() {
    let (store, router) = setup().await;
    let bytes = png(2, 90);
    let a = ready_file(&router, A, "image/png", &bytes).await;
    let private = ready_file(&router, A, "image/png", &png(12, 90)).await;
    let (note, original) = note_with(&router, A, "n1", &a).await;
    let media = |file: &str, query: &str| format!("/media/{file}?{query}");
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={note}")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "unpublished note");
    let r = publish_original(&router, A, &note, &original, "0").await;
    assert_eq!(r.status, StatusCode::OK);
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={note}")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.bytes, bytes);
    assert_eq!(r.headers["content-type"], "image/png");
    assert_eq!(r.headers["x-content-type-options"], "nosniff");
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={note}&view=original")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    // A view that is not published, a private file, another note, a bad view.
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={note}&view=typed")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = call(
        &router,
        "GET",
        &media(&private, &format!("note_id={note}")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let b = ready_file(&router, B, "image/png", &bytes).await;
    let (other, other_original) = note_with(&router, B, "n1", &b).await;
    publish_original(&router, B, &other, &other_original, "0").await;
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={other}")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(
        r.status,
        StatusCode::NOT_FOUND,
        "the same bytes in another note open nothing"
    );
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={note}&view=both")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.json()["code"], "invalid_view");
    // A working-copy save never changes what is public.
    let r = call(
        &router,
        "PUT",
        &format!("/me/notes/{note}"),
        Some(A),
        &[("if-match", "1")],
        json!({"representation": "original", "document": with_image(&private)})
            .to_string()
            .into_bytes(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    let r = call(
        &router,
        "GET",
        &media(&private, &format!("note_id={note}")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={note}")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    // Unpublish: not found again.
    let r = call(
        &router,
        "DELETE",
        &format!("/me/notes/{note}/publication"),
        Some(A),
        &[("if-match", "1")],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    let r = call(
        &router,
        "GET",
        &media(&a, &format!("note_id={note}")),
        None,
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let _ = store;
}

// Checklist: Range download works.

#[tokio::test]
async fn range_download_works() {
    let (_store, router) = setup().await;
    let mut bytes = b"%PDF-1.7\n".to_vec();
    bytes.extend((0..991u32).map(|i| (i % 251) as u8));
    let r = reserve(
        &router,
        A,
        "Notes \"1\".pdf",
        "application/pdf",
        &bytes,
        "attachment",
    )
    .await;
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    put(&router, A, &id, "application/pdf", bytes.clone()).await;
    let uri = format!("/me/files/{id}/content");
    let get = |range: Option<&'static str>| {
        let router = router.clone();
        let uri = uri.clone();
        async move {
            let headers: Vec<(&str, &str)> = range.map(|r| ("range", r)).into_iter().collect();
            call(&router, "GET", &uri, Some(A), &headers, vec![]).await
        }
    };
    let full = get(None).await;
    assert_eq!(full.status, StatusCode::OK);
    assert_eq!(full.bytes, bytes);
    assert_eq!(full.headers["accept-ranges"], "bytes");
    assert_eq!(full.headers["content-length"], "1000");
    assert_eq!(
        full.headers["etag"],
        format!("\"{}\"", sha(&bytes)).as_str()
    );
    assert_eq!(
        full.headers["content-disposition"],
        "attachment; filename*=UTF-8''Notes%20%221%22.pdf"
    );
    assert_eq!(
        full.headers["content-security-policy"],
        "sandbox; default-src 'none'"
    );
    let part = get(Some("bytes=10-19")).await;
    assert_eq!(part.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(part.headers["content-range"], "bytes 10-19/1000");
    assert_eq!(part.headers["content-length"], "10");
    assert_eq!(part.bytes, bytes[10..20]);
    let tail = get(Some("bytes=-5")).await;
    assert_eq!(tail.headers["content-range"], "bytes 995-999/1000");
    assert_eq!(tail.bytes, bytes[995..]);
    let open = get(Some("bytes=990-")).await;
    assert_eq!(open.bytes, bytes[990..]);
    let past = get(Some("bytes=900-5000")).await;
    assert_eq!(past.headers["content-range"], "bytes 900-999/1000");
    let bad = get(Some("bytes=1000-")).await;
    assert_eq!(bad.status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(bad.headers["content-range"], "bytes */1000");
    assert_eq!(bad.json()["code"], "range_not_satisfiable");
    assert_eq!(
        get(Some("bytes=abc")).await.status,
        StatusCode::RANGE_NOT_SATISFIABLE
    );
    // Another unit is ignored: the whole file.
    assert_eq!(get(Some("lines=1-2")).await.status, StatusCode::OK);
    let several = get(Some("bytes=0-1,5-6")).await;
    assert_eq!(several.status, StatusCode::OK);
    assert_eq!(several.bytes, bytes);
}

/// The HTTP adapter cuts a full FSM answer to the range when FSM ignores `Range`.
#[tokio::test]
async fn fsm_adapter_cuts_a_full_answer_to_the_range() {
    use axum::routing::get;
    let bytes: Vec<u8> = (0..=255u8).cycle().take(3000).collect();
    let served = bytes.clone();
    let app = Router::new().route(
        "/annotations/media/{key}",
        get(move || {
            let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
                served.chunks(700).map(|c| Ok(c.to_vec())).collect();
            async move { Body::from_stream(futures_util::stream::iter(chunks)) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut settings = Settings::load();
    settings.fsm_url = format!("http://{address}");
    settings.fsm_key = "fixture".into();
    let fsm = Files::new(settings, reqwest::Client::new());
    for (first, last) in [(0u64, 9u64), (650, 1500), (2999, 2999), (700, 1399)] {
        let body = fsm.read_range("k", Some((first, last))).await.unwrap();
        let got = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        assert_eq!(got.as_ref(), &bytes[first as usize..=last as usize]);
    }
    let body = fsm.read_range("k", None).await.unwrap();
    assert_eq!(
        axum::body::to_bytes(body, usize::MAX).await.unwrap().len(),
        3000
    );
}

#[tokio::test]
async fn upload_stages_in_the_configured_folder_and_leaves_it_empty() {
    let (mut store, router) = setup().await;
    let staging = tempfile::tempdir().unwrap();
    store.files = store.files.clone().with_staging_dir(staging.path());
    let entries = |dir: &Path| std::fs::read_dir(dir).unwrap().count();
    let bytes = png(11, 4000);
    for good in [false, true] {
        let r = reserve(&router, A, "a.png", "image/png", &bytes, "attachment").await;
        let id = r.json()["file_id"].as_str().unwrap().to_string();
        let sent = if good { bytes.clone() } else { png(12, 4000) };
        // The second chunk is read only after the first was staged.
        let dir = staging.path().to_path_buf();
        let chunks = vec![sent[..100].to_vec(), sent[100..].to_vec()];
        let stream =
            futures_util::stream::iter(chunks.into_iter().enumerate()).map(move |(i, c)| {
                if i == 1 {
                    assert_eq!(
                        entries(&dir),
                        1,
                        "the upload stages in the configured folder"
                    );
                }
                Ok::<_, std::io::Error>(c)
            });
        let result = files::upload(&store, A, &id, Some("image/png"), Box::pin(stream)).await;
        assert_eq!(result.is_ok(), good);
        assert_eq!(entries(staging.path()), 0, "nothing stays after the upload");
    }
}
