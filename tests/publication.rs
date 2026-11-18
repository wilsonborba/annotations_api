//! Note publication (`/me/notes/{note_id}/publication`) and the public projection.
//! SQLite stands in for PostgreSQL and the in-memory document store for CouchDB.
use annotations_api::{
    dal::{Store, documents::Documents},
    domain::{
        App,
        note_saves::{self, Representation},
        publication, schemas,
    },
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use serde_json::{Value, json};
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
    sqlx::raw_sql(
        "INSERT INTO languages(code,label) VALUES('en','English');
         INSERT INTO languages(code,label) VALUES('pt-BR','Portuguese');",
    )
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
    body: Value,
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    if_match: Option<&str>,
    body: Option<Value>,
) -> Reply {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(owner) = owner {
        request = request.header("x-uuid", owner);
    }
    if let Some(v) = if_match {
        request = request.header("if-match", v);
    }
    let request = match body {
        Some(b) => request
            .header("content-type", "application/json")
            .body(Body::from(b.to_string())),
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    Reply {
        status,
        headers,
        body,
    }
}

fn original(text: &str) -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id": "t1", "kind": "text", "x": 0, "y": 40, "width": 304, "height": 120,
             "spans": [{"text": text}]}
        ]
    })
}
fn typed(text: &str) -> Value {
    json!({"blocks": [{"id": "k1", "kind": "paragraph", "spans": [{"text": text}]}]})
}

/// A note of A with both representations; returns (note id, original rev, typed rev).
async fn note_with_both(router: &Router, client: &str) -> (String, String, String) {
    let r = call(
        router,
        "POST",
        "/me/notes",
        Some(A),
        None,
        Some(json!({
            "client_note_id": client,
            "main_representation": "original",
            "metadata": {"title": "Leaf cells"},
            "representations": {
                "original": {"document": original("Handwritten lesson")},
                "typed": {"document": typed("Typed lesson")}
            }
        })),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let revisions = r.body["revisions"].as_array().unwrap();
    (
        r.body["note_id"].as_str().unwrap().to_string(),
        revisions[0]["revision_id"].as_str().unwrap().to_string(),
        revisions[1]["revision_id"].as_str().unwrap().to_string(),
    )
}

async fn save(
    router: &Router,
    note_id: &str,
    revision: i64,
    representation: &str,
    doc: Value,
) -> String {
    let r = call(
        router,
        "PUT",
        &format!("/me/notes/{note_id}"),
        Some(A),
        Some(&revision.to_string()),
        Some(json!({"representation": representation, "document": doc})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    r.body["revisions"][0]["revision_id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn publish(router: &Router, note_id: &str, if_match: &str, reps: Value) -> Reply {
    call(
        router,
        "PUT",
        &format!("/me/notes/{note_id}/publication"),
        Some(A),
        Some(if_match),
        Some(json!({"language": "en", "representations": reps})),
    )
    .await
}

async fn count(store: &Store, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

async fn public(store: &Store, note_id: &str) -> Value {
    publication::public_note(store, note_id, None)
        .await
        .unwrap()
}

#[tokio::test]
async fn original_only_typed_only_and_both_work_and_both_can_switch_to_one() {
    let (store, router) = setup().await;
    let (note, o1, t1) = note_with_both(&router, "n1").await;
    let uri = format!("/me/notes/{note}/publication");

    let never = call(&router, "GET", &uri, Some(A), None, None).await;
    assert_eq!(never.status, StatusCode::OK);
    assert_eq!(never.headers["etag"], "\"0\"");
    assert_eq!(never.body["published"], false);
    assert_eq!(never.body["revision"], 0);
    assert!(publication::public_note(&store, &note, None).await.is_err());

    // Original only.
    let r = publish(&router, &note, "0", json!({"original": o1})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.headers["etag"], "\"1\"");
    assert_eq!(r.body["revision"], 1);
    assert_eq!(r.body["language"], "en");
    assert_eq!(r.body["title"], "Leaf cells");
    assert_eq!(
        r.body["representations"],
        json!([{"representation": "original", "revision_id": o1, "schema_version": 1}])
    );
    let p = public(&store, &note).await;
    assert_eq!(p["views"], json!(["original"]));
    assert_eq!(p["default_view"], "original");
    let text = p.to_string();
    assert!(text.contains("Handwritten lesson"));
    assert!(!text.contains("Typed lesson"));

    // Typed only: the original is not exposed and the main view is not revealed.
    let r = publish(&router, &note, "\"1\"", json!({"typed": t1})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.body["revision"], 2);
    let p = public(&store, &note).await;
    assert_eq!(p["views"], json!(["typed"]));
    assert_eq!(p["default_view"], "typed");
    let text = p.to_string();
    assert!(text.contains("Typed lesson"));
    assert!(!text.contains("Handwritten lesson"));
    assert!(!text.contains("original"), "{text}");
    assert!(
        publication::public_note(&store, &note, Some(Representation::Original))
            .await
            .is_err()
    );

    // Both.
    let r = publish(&router, &note, "2", json!({"original": o1, "typed": t1})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let p = public(&store, &note).await;
    assert_eq!(p["views"], json!(["original", "typed"]));
    assert_eq!(p["default_view"], "original");
    assert_eq!(p["representations"].as_array().unwrap().len(), 2);
    let one = publication::public_note(&store, &note, Some(Representation::Typed))
        .await
        .unwrap();
    assert_eq!(one["representations"].as_array().unwrap().len(), 1);
    assert_eq!(one["views"], json!(["original", "typed"]));

    // Both to one.
    let r = publish(&router, &note, "3", json!({"typed": t1})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(public(&store, &note).await["views"], json!(["typed"]));
    assert_eq!(
        count(
            &store,
            &format!("SELECT COUNT(*) FROM note_published_representations WHERE note_id='{note}'")
        )
        .await,
        1
    );

    // A stale publication revision changes nothing.
    let stale = publish(&router, &note, "3", json!({"original": o1})).await;
    assert_eq!(stale.status, StatusCode::CONFLICT);
    assert_eq!(stale.body["code"], "revision_conflict");
    assert_eq!(stale.body["current_revision"], 4);
    let first = publish(&router, &note, "0", json!({"original": o1})).await;
    assert_eq!(first.status, StatusCode::CONFLICT);
    assert_eq!(public(&store, &note).await["views"], json!(["typed"]));
}

#[tokio::test]
async fn working_copy_saves_never_change_the_publication() {
    let (store, router) = setup().await;
    let (note, o1, t1) = note_with_both(&router, "n1").await;
    let r = publish(&router, &note, "0", json!({"original": o1, "typed": t1})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let before = public(&store, &note).await;

    // Unpublished edits of both representations and of the title.
    let o2 = save(
        &router,
        &note,
        1,
        "original",
        original("UNPUBLISHED ink edit"),
    )
    .await;
    save(&router, &note, 2, "typed", typed("UNPUBLISHED typed edit")).await;
    let meta = call(
        &router,
        "PATCH",
        &format!("/me/notes/{note}"),
        Some(A),
        Some("3"),
        Some(json!({"metadata": {"title": "UNPUBLISHED title"}})),
    )
    .await;
    assert_eq!(meta.status, StatusCode::OK, "{}", meta.body);

    let after = public(&store, &note).await;
    assert_eq!(after, before);
    assert!(!after.to_string().contains("UNPUBLISHED"));
    assert_eq!(after["title"], "Leaf cells");
    let owner = call(
        &router,
        "GET",
        &format!("/me/notes/{note}/publication"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(owner.body["revision"], 1);
    assert_eq!(owner.body["representations"][0]["revision_id"], o1.as_str());
    assert_eq!(owner.body["typed_uses_earlier_revision"], false);

    // The working copy has the new content; its detail shows the publication block.
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{note}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(detail.body["published"], true);
    assert_eq!(detail.body["publication"]["revision"], 1);
    assert_eq!(
        detail.body["representations"][0]["revision_id"],
        o2.as_str()
    );

    // The published, no longer current revisions survive cleanup with their documents.
    let removed = note_saves::prune_revisions(&store, i64::MAX, 100)
        .await
        .unwrap();
    assert_eq!(removed, 0);
    assert_eq!(public(&store, &note).await, before);
}

#[tokio::test]
async fn publish_checks_owner_revisions_and_request_fields() {
    let (store, router) = setup().await;
    let (note, o1, t1) = note_with_both(&router, "n1").await;
    let uri = format!("/me/notes/{note}/publication");
    let body = json!({"language": "en", "representations": {"original": o1}});

    let anonymous = call(&router, "PUT", &uri, None, Some("0"), Some(body.clone())).await;
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED);
    let missing = call(&router, "PUT", &uri, Some(A), None, Some(body.clone())).await;
    assert_eq!(missing.status, StatusCode::PRECONDITION_REQUIRED);
    let bad = call(&router, "PUT", &uri, Some(A), Some("x"), Some(body.clone())).await;
    assert_eq!(bad.body["code"], "invalid_if_match");
    for method in ["GET", "PUT", "DELETE"] {
        let other = call(
            &router,
            method,
            &uri,
            Some(B),
            Some("0"),
            Some(body.clone()),
        )
        .await;
        assert_eq!(other.status, StatusCode::NOT_FOUND, "{method}");
        assert_eq!(other.body["code"], "not_found");
    }
    let cases = [
        (
            json!({"language": "en", "representations": {"typed": o1}}),
            "unknown_revision",
        ),
        (
            json!({"language": "en", "representations": {"original": "no-such-rev"}}),
            "unknown_revision",
        ),
        (
            json!({"language": "en", "representations": {}}),
            "representation_required",
        ),
        (
            json!({"language": "xx", "representations": {"original": o1}}),
            "unknown_language",
        ),
        (
            json!({"representations": {"original": o1}}),
            "missing_field",
        ),
        (
            json!({"language": "en", "representations": {"original": o1}, "owner_id": B}),
            "unknown_field",
        ),
        (
            json!({"language": "en", "representations": {"draft": t1}}),
            "unknown_field",
        ),
        (
            json!({"language": "en", "representations": {"original": 5}}),
            "invalid_type",
        ),
    ];
    for (body, code) in cases {
        let r = call(&router, "PUT", &uri, Some(A), Some("0"), Some(body.clone())).await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(r.body["code"], code, "{body}");
    }
    // A revision of another note of the same owner is refused too.
    let (other_note, other_o, _) = note_with_both(&router, "n2").await;
    let r = publish(&router, &note, "0", json!({"original": other_o})).await;
    assert_eq!(r.body["code"], "unknown_revision");
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_publications").await,
        0
    );
    assert!(
        publication::public_note(&store, &other_note, None)
            .await
            .is_err()
    );

    // Publication copies no file and adds no file reference.
    let files = count(&store, "SELECT COUNT(*) FROM files").await;
    let refs = count(&store, "SELECT COUNT(*) FROM file_references").await;
    let r = publish(&router, &note, "0", json!({"original": o1})).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM files").await, files);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM file_references").await,
        refs
    );
    // The public author id is not the owner id.
    let p = public(&store, &note).await;
    assert!(p["author_id"].is_string());
    assert_ne!(p["author_id"], A);
}

#[tokio::test]
async fn stale_flag_appears_only_when_published_typed_uses_an_older_published_original() {
    let (store, router) = setup().await;
    let (note, o1, t1) = note_with_both(&router, "n1").await;
    let db = &store.db;
    let set_source = |typed: String, source: String| async move {
        sqlx::query("UPDATE note_revisions SET source_revision_id=$1 WHERE revision_id=$2")
            .bind(source)
            .bind(typed)
            .execute(db)
            .await
            .unwrap();
    };
    let set_time = |revision: String, at: i64| async move {
        sqlx::query("UPDATE note_revisions SET created_at=$1 WHERE revision_id=$2")
            .bind(at)
            .bind(revision)
            .execute(db)
            .await
            .unwrap();
    };
    let flag = |r: Reply| r.body["typed_uses_earlier_revision"].clone();
    // The typed text is a transcription of o1.
    set_source(t1.clone(), o1.clone()).await;
    set_time(o1.clone(), 100).await;
    set_time(t1.clone(), 110).await;

    let r = publish(&router, &note, "0", json!({"original": o1, "typed": t1})).await;
    assert_eq!(flag(r), false);
    assert_eq!(
        public(&store, &note).await["typed_uses_earlier_revision"],
        false
    );

    // An unpublished Original edit alone never shows the notice.
    let o2 = save(&router, &note, 1, "original", original("Changed ink")).await;
    set_time(o2.clone(), 200).await;
    assert_eq!(
        public(&store, &note).await["typed_uses_earlier_revision"],
        false
    );
    let owner = call(
        &router,
        "GET",
        &format!("/me/notes/{note}/publication"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(flag(owner), false);

    // Publishing the changed Original before updating Typed shows the notice.
    let r = publish(&router, &note, "1", json!({"original": o2, "typed": t1})).await;
    assert_eq!(flag(r), true);
    assert_eq!(
        public(&store, &note).await["typed_uses_earlier_revision"],
        true
    );

    // Typed alone, Original alone: no notice.
    let r = publish(&router, &note, "2", json!({"typed": t1})).await;
    assert_eq!(flag(r), false);
    let r = publish(&router, &note, "3", json!({"original": o2})).await;
    assert_eq!(flag(r), false);

    // A Typed from a newer, unpublished Original: no notice and no leak.
    let o3 = save(&router, &note, 2, "original", original("Newest ink")).await;
    let t2 = save(&router, &note, 3, "typed", typed("Newest typed")).await;
    set_time(o3.clone(), 300).await;
    set_source(t2.clone(), o3.clone()).await;
    let r = publish(&router, &note, "4", json!({"original": o2, "typed": t2})).await;
    assert_eq!(flag(r), false);
    let p = public(&store, &note).await;
    assert_eq!(p["typed_uses_earlier_revision"], false);
    assert!(!p.to_string().contains("Newest ink"));

    // t1 comes from o1, older than o3: notice. Without a recorded source: no notice.
    let r = publish(&router, &note, "5", json!({"original": o3, "typed": t1})).await;
    assert_eq!(flag(r), true);
    sqlx::query("UPDATE note_revisions SET source_revision_id=NULL WHERE revision_id=$1")
        .bind(&t1)
        .execute(db)
        .await
        .unwrap();
    assert_eq!(
        public(&store, &note).await["typed_uses_earlier_revision"],
        false
    );
}

fn embeds_original() -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id": "t1", "kind": "text", "x": 0, "y": 0, "width": 304, "height": 50,
             "spans": [{"text": "Public lesson"}]},
            {"id": "r1", "kind": "reports_embed", "x": 0, "y": 104, "width": 400, "height": 304,
             "report_id": "rep-private-1", "mode": "snapshot", "source": "upload",
             "snapshot_id": "snap-private-9", "captured_at": 1_700_000_123},
            {"id": "r2", "kind": "reports_embed", "x": 496, "y": 104, "width": 400, "height": 304,
             "report_id": "rep-live-2", "mode": "live", "source": "link"},
            {"id": "c1", "kind": "task_card", "x": 0, "y": 496, "width": 304, "height": 100,
             "task_id": "task-77"},
            {"id": "q1", "kind": "quiz_card", "x": 400, "y": 496, "width": 304, "height": 100,
             "study_id": "study-5", "quiz_id": "quiz-6"}
        ]
    })
}
fn embeds_typed() -> Value {
    json!({"blocks": [
        {"id": "k1", "kind": "paragraph", "spans": [{"text": "Public typed lesson"}]},
        {"id": "k2", "kind": "reports_embed", "report_id": "rep-private-1", "mode": "snapshot",
         "source": "upload", "snapshot_id": "snap-private-8", "captured_at": 1_700_000_456},
        {"id": "k3", "kind": "task_card", "task_id": "task-78"}
    ]})
}

#[tokio::test]
async fn public_read_of_a_note_with_a_private_reports_embed_has_no_private_data() {
    let (store, router) = setup().await;
    let r = call(
        &router,
        "POST",
        "/me/notes",
        Some(A),
        None,
        Some(json!({
            "client_note_id": "embeds",
            "main_representation": "original",
            "metadata": {"title": "Public title", "tags": ["Biology"]},
            "representations": {
                "original": {"document": embeds_original()},
                "typed": {"document": embeds_typed()}
            }
        })),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let note = r.body["note_id"].as_str().unwrap().to_string();
    let revisions: Vec<String> = r.body["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["revision_id"].as_str().unwrap().to_string())
        .collect();
    let p = publish(
        &router,
        &note,
        "0",
        json!({"original": revisions[0], "typed": revisions[1]}),
    )
    .await;
    assert_eq!(p.status, StatusCode::OK, "{}", p.body);
    // A later private title and a private unpublished embed.
    let meta = call(
        &router,
        "PATCH",
        &format!("/me/notes/{note}"),
        Some(A),
        Some("1"),
        Some(json!({"metadata": {"title": "Private working title"}})),
    )
    .await;
    assert_eq!(meta.status, StatusCode::OK, "{}", meta.body);

    let public = publication::public_note(&store, &note, None).await.unwrap();
    let text = public.to_string();
    let document_ids: Vec<String> = sqlx::query("SELECT document_id FROM note_revisions")
        .fetch_all(&store.db)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get("document_id"))
        .collect();
    let mut forbidden = vec![
        "snap-private-9".to_string(),
        "snap-private-8".into(),
        "snapshot".into(),
        "captured_at".into(),
        "1700000123".into(),
        "1700000456".into(),
        "upload".into(),
        "\"mode\"".into(),
        "\"source\"".into(),
        "Private working title".into(),
        A.into(),
        "owner".into(),
        "revision_id".into(),
        "document_id".into(),
    ];
    forbidden.extend(revisions);
    forbidden.extend(document_ids);
    for value in &forbidden {
        assert!(
            !text.contains(value.as_str()),
            "public JSON leaks {value}: {text}"
        );
    }
    // The exact public cards: reference, geometry and the restricted marker only.
    let items = &public["representations"][0]["document"]["items"];
    assert_eq!(items[0], embeds_original()["items"][0]);
    assert_eq!(
        items[1],
        json!({"id": "r1", "kind": "reports_embed", "x": 0, "y": 104, "width": 400, "height": 304,
               "report_id": "rep-private-1", "restricted": true})
    );
    assert_eq!(
        items[2],
        json!({"id": "r2", "kind": "reports_embed", "x": 496, "y": 104, "width": 400, "height": 304,
               "report_id": "rep-live-2", "restricted": true})
    );
    assert_eq!(
        items[3],
        json!({"id": "c1", "kind": "task_card", "x": 0, "y": 496, "width": 304, "height": 100,
               "task_id": "task-77", "restricted": true})
    );
    assert_eq!(
        items[4],
        json!({"id": "q1", "kind": "quiz_card", "x": 400, "y": 496, "width": 304, "height": 100,
               "study_id": "study-5", "quiz_id": "quiz-6", "restricted": true})
    );
    let blocks = &public["representations"][1]["document"]["blocks"];
    assert_eq!(
        blocks[1],
        json!({"id": "k2", "kind": "reports_embed", "report_id": "rep-private-1", "restricted": true})
    );
    assert_eq!(
        blocks[2],
        json!({"id": "k3", "kind": "task_card", "task_id": "task-78", "restricted": true})
    );
    assert_eq!(public["title"], "Public title");
    assert_eq!(public["tags"], json!(["Biology"]));
    // The whole public answer has only the public fields.
    let mut keys: Vec<&String> = public.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "author_id",
            "author_name",
            "can_switch_view",
            "default_view",
            "language",
            "note_id",
            "published_at",
            "representations",
            "study_source",
            "summary",
            "tags",
            "title",
            "typed_uses_earlier_revision",
            "views"
        ]
    );
    // The stored documents keep the owner's full content (read through the
    // store, which unpacks the gzip body).
    let Documents::Memory(m) = &store.docs else {
        panic!("memory double expected")
    };
    let ids: Vec<String> = m.lock().unwrap().keys().cloned().collect();
    let mut found = false;
    for id in ids {
        let stored = store.docs.get(&id).await.unwrap();
        found |= stored.to_string().contains("snap-private-9");
    }
    assert!(found);
}

#[test]
fn public_document_keeps_other_items_unchanged() {
    let doc = embeds_original();
    let out = publication::public_document(&doc);
    assert_eq!(out["page"], doc["page"]);
    assert_eq!(out["items"][0], doc["items"][0]);
    assert_eq!(out["items"].as_array().unwrap().len(), 5);
    assert_eq!(publication::public_document(&typed("x")), typed("x"));
}

#[tokio::test]
async fn unpublish_keeps_the_working_copy_and_the_sync_feed() {
    let (store, router) = setup().await;
    let (note, o1, t1) = note_with_both(&router, "n1").await;
    let uri = format!("/me/notes/{note}/publication");
    let r = publish(&router, &note, "0", json!({"original": o1, "typed": t1})).await;
    assert_eq!(r.status, StatusCode::OK);
    let revisions = count(&store, "SELECT COUNT(*) FROM note_revisions").await;
    let documents = {
        let Documents::Memory(m) = &store.docs else {
            panic!()
        };
        m.lock().unwrap().len()
    };

    let missing = call(&router, "DELETE", &uri, Some(A), None, None).await;
    assert_eq!(missing.status, StatusCode::PRECONDITION_REQUIRED);
    let stale = call(&router, "DELETE", &uri, Some(A), Some("0"), None).await;
    assert_eq!(stale.status, StatusCode::CONFLICT);
    assert_eq!(stale.body["current_revision"], 1);
    let other = call(&router, "DELETE", &uri, Some(B), Some("1"), None).await;
    assert_eq!(other.status, StatusCode::NOT_FOUND);

    let gone = call(&router, "DELETE", &uri, Some(A), Some("\"1\""), None).await;
    assert_eq!(gone.status, StatusCode::NO_CONTENT);
    assert_eq!(gone.headers["etag"], "\"2\"");
    assert!(publication::public_note(&store, &note, None).await.is_err());
    let again = call(&router, "DELETE", &uri, Some(A), Some("2"), None).await;
    assert_eq!(again.status, StatusCode::NOT_FOUND);
    assert_eq!(again.body["code"], "not_published");

    // The working copy, its revisions and documents stay.
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{note}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK);
    assert_eq!(detail.body["revision"], 1);
    assert_eq!(detail.body["published"], false);
    assert_eq!(detail.body["publication"], Value::Null);
    assert_eq!(detail.body["representations"].as_array().unwrap().len(), 2);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        revisions
    );
    {
        let Documents::Memory(m) = &store.docs else {
            panic!()
        };
        assert_eq!(m.lock().unwrap().len(), documents);
    }
    let state = call(&router, "GET", &uri, Some(A), None, None).await;
    assert_eq!(state.headers["etag"], "\"2\"");
    assert_eq!(state.body["published"], false);
    assert_eq!(state.body["representations"], json!([]));
    assert_eq!(state.body["title"], Value::Null);

    // The sync feed keeps the note: no delete notice, only upserts.
    let feed = call(
        &router,
        "GET",
        "/me/notes?sync_cursor=0",
        Some(A),
        None,
        None,
    )
    .await;
    let changes = feed.body["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 3, "create, publish, unpublish");
    assert!(
        changes
            .iter()
            .all(|c| c["change"] == "upsert" && c["note_id"] == note.as_str())
    );
    let listed = call(&router, "GET", "/me/notes", Some(A), None, None).await;
    assert_eq!(listed.body["items"][0]["note_id"], note.as_str());

    // Publish again from the kept revision counter.
    let r = publish(&router, &note, "2", json!({"typed": t1})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.body["revision"], 3);

    // Deleting the whole note still removes its publication.
    let deleted = call(
        &router,
        "DELETE",
        &format!("/me/notes/{note}"),
        Some(A),
        Some("1"),
        None,
    )
    .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    for table in [
        "note_publications",
        "note_published_representations",
        "note_publication_titles",
    ] {
        assert_eq!(
            count(&store, &format!("SELECT COUNT(*) FROM {table}")).await,
            0,
            "{table}"
        );
    }
}

#[tokio::test]
async fn study_source_links_published_typed_post_and_hides_when_source_unpublished_or_deleted() {
    let (store, router) = setup().await;
    let (src_id, src_orig_rev, src_typed_rev) = note_with_both(&router, "typed-src").await;

    let study_payload = |client: &str, source_id: &str| {
        json!({
            "client_note_id": client,
            "main_representation": "original",
            "study_source_note_id": source_id,
            "representations": {
                "original": {"document": original("Study")}
            }
        })
    };

    // Linking to a missing or unpublished note fails with study_source_unavailable.
    for bad in ["missing-note", &src_id] {
        let r = call(
            &router,
            "POST",
            "/me/notes",
            Some(B),
            None,
            Some(study_payload("s-fail", bad)),
        )
        .await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", r.body);
        assert_eq!(r.body["code"], "study_source_unavailable");
    }

    // Publishing only Original is still not a published Typed post.
    let pub_orig = publish(&router, &src_id, "0", json!({"original": src_orig_rev})).await;
    assert_eq!(pub_orig.status, StatusCode::OK, "{}", pub_orig.body);
    let r = call(
        &router,
        "POST",
        "/me/notes",
        Some(B),
        None,
        Some(study_payload("s-orig", &src_id)),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.body["code"], "study_source_unavailable");

    // Publishing Typed allows creating a handwritten study linked to it.
    let pub_typed = publish(&router, &src_id, "1", json!({"typed": src_typed_rev})).await;
    assert_eq!(pub_typed.status, StatusCode::OK, "{}", pub_typed.body);
    let created = call(
        &router,
        "POST",
        "/me/notes",
        Some(B),
        None,
        Some(study_payload("s-ok", &src_id)),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    let study_id = created.body["note_id"].as_str().unwrap().to_string();
    let study_orig_rev = created.body["revisions"][0]["revision_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(created.body["study_source"]["note_id"], src_id.as_str());

    // Working-copy GET returns the study_source snapshot.
    let wc = call(
        &router,
        "GET",
        &format!("/me/notes/{study_id}"),
        Some(B),
        None,
        None,
    )
    .await;
    assert_eq!(wc.status, StatusCode::OK);
    assert_eq!(wc.body["study_source"]["note_id"], src_id.as_str());

    // Publishing the study exposes study_source on the public note.
    let pub_study = call(
        &router,
        "PUT",
        &format!("/me/notes/{study_id}/publication"),
        Some(B),
        Some("0"),
        Some(json!({"language": "en", "representations": {"original": study_orig_rev}})),
    )
    .await;
    assert_eq!(pub_study.status, StatusCode::OK, "{}", pub_study.body);
    let public_view = call(
        &router,
        "GET",
        &format!("/notes/{study_id}"),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(public_view.status, StatusCode::OK);
    assert_eq!(public_view.body["study_source"]["note_id"], src_id.as_str());

    // Unpublishing the source hides the public link without touching the study.
    let unpub = call(
        &router,
        "DELETE",
        &format!("/me/notes/{src_id}/publication"),
        Some(A),
        Some("2"),
        None,
    )
    .await;
    assert_eq!(unpub.status, StatusCode::NO_CONTENT, "{}", unpub.body);
    let public_after_unpub = call(
        &router,
        "GET",
        &format!("/notes/{study_id}"),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(public_after_unpub.status, StatusCode::OK);
    assert!(public_after_unpub.body["study_source"].is_null());
    // Owner working copy still keeps the snapshot.
    let wc_after_unpub = call(
        &router,
        "GET",
        &format!("/me/notes/{study_id}"),
        Some(B),
        None,
        None,
    )
    .await;
    assert_eq!(
        wc_after_unpub.body["study_source"]["note_id"],
        src_id.as_str()
    );

    // Deleting the source note keeps the study note intact and clears source_note_id.
    let del_src = call(
        &router,
        "DELETE",
        &format!("/me/notes/{src_id}"),
        Some(A),
        Some("1"),
        None,
    )
    .await;
    assert_eq!(del_src.status, StatusCode::NO_CONTENT, "{}", del_src.body);
    let wc_after_del = call(
        &router,
        "GET",
        &format!("/me/notes/{study_id}"),
        Some(B),
        None,
        None,
    )
    .await;
    assert_eq!(wc_after_del.status, StatusCode::OK);
    assert!(wc_after_del.body["study_source"]["note_id"].is_null());
    let public_after_del = call(
        &router,
        "GET",
        &format!("/notes/{study_id}"),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(public_after_del.status, StatusCode::OK);
    assert!(public_after_del.body["study_source"].is_null());
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_study_sources").await,
        1
    );
}
