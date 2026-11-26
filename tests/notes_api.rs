//! Owner working-copy routes (`/me/notes`) through the HTTP router. SQLite stands in
//! for PostgreSQL and the in-memory document store for CouchDB.
use annotations_api::{
    dal::{Store, documents::Documents},
    domain::{App, schemas},
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
    body: Value,
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> Reply {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(owner) = owner {
        request = request.header("x-uuid", owner);
    }
    for (k, v) in headers {
        request = request.header(*k, *v);
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

fn original(x: i64) -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id": "t1", "kind": "text", "x": x, "y": 40, "width": 304, "height": 120,
             "spans": [{"text": "Photosynthesis"}]}
        ]
    })
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
fn typed(text: &str) -> Value {
    json!({"blocks": [{"id": "k1", "kind": "paragraph", "spans": [{"text": text}]}]})
}

async fn create(router: &Router, owner: &str, body: Value) -> Reply {
    call(router, "POST", "/me/notes", Some(owner), &[], Some(body)).await
}
async fn create_original(router: &Router, owner: &str, client: &str, document: Value) -> String {
    let r = create(
        router,
        owner,
        json!({"client_note_id": client, "representations": {"original": {"document": document}}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    r.body["note_id"].as_str().unwrap().to_string()
}
async fn count(store: &Store, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get::<i64, _>(0)
}
fn document_count(store: &Store) -> usize {
    let Documents::Memory(m) = &store.docs else {
        panic!("memory double expected")
    };
    m.lock().unwrap().len()
}

#[tokio::test]
async fn create_twice_with_the_same_client_id_gives_one_note() {
    let (store, router) = setup().await;
    let body = json!({
        "client_note_id": "device-1-note-1",
        "metadata": {"title": "Cells", "tags": ["Biology"]},
        "representations": {"typed": {"schema_version": 1, "document": typed("Cells")}}
    });
    let first = create(&router, A, body.clone()).await;
    assert_eq!(first.status, StatusCode::CREATED, "{}", first.body);
    assert_eq!(first.body["created"], true);
    assert_eq!(first.body["revision"], 1);
    assert_eq!(first.headers["etag"], "\"1\"");
    let note_id = first.body["note_id"].as_str().unwrap();

    let again = create(&router, A, body).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.body["created"], false);
    assert_eq!(again.body["note_id"], note_id);

    // Another body with the same client id meets the existing note and changes nothing.
    let other = create(
        &router,
        A,
        json!({"client_note_id": "device-1-note-1", "representations": {"typed": {"document": typed("Other")}}}),
    )
    .await;
    assert_eq!(other.status, StatusCode::OK);
    assert_eq!(other.body["note_id"], note_id);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 1);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        1
    );

    // The same client id is a different note for another account.
    let b = create(
        &router,
        B,
        json!({"client_note_id": "device-1-note-1", "representations": {"typed": {"document": typed("B")}}}),
    )
    .await;
    assert_eq!(b.status, StatusCode::CREATED);
    assert_ne!(b.body["note_id"], note_id);
}

#[tokio::test]
async fn reports_references_round_trip_for_link_and_uploaded_snapshots() {
    let (_store, router) = setup().await;
    let document = json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id":"link-live", "kind":"reports_embed", "x":0, "y":0, "width":400, "height":304,
             "report_id":"report-42", "project_slug":"project-1", "report_slug":"report-42", "dataset_id":"dataset-1",
             "mode":"live", "source":"link"},
            {"id":"upload-snapshot", "kind":"reports_embed", "x":496, "y":0, "width":400, "height":304,
             "report_id":"00000000-0000-4000-8000-000000000001", "project_slug":"project-1", "report_slug":"report-42", "dataset_id":"dataset-1",
             "mode":"snapshot", "source":"upload", "snapshot_id":"snapshot-42", "captured_at":1794700000}
        ]
    });
    let note_id = create_original(&router, A, "reports-round-trip", document.clone()).await;
    let read = call(
        &router,
        "GET",
        &format!("/me/notes/{note_id}"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(read.status, StatusCode::OK, "{}", read.body);
    assert_eq!(read.body["representations"][0]["document"], document);
}

#[tokio::test]
async fn typed_only_create_works_without_an_original_and_binds_the_account() {
    let (store, router) = setup().await;
    assert_eq!(count(&store, "SELECT COUNT(*) FROM owners").await, 0);
    let r = create(
        &router,
        A,
        json!({"client_note_id": "api-1", "representations": {"typed": {"document": typed("Formal text")}}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let note_id = r.body["note_id"].as_str().unwrap();
    let owner: String = sqlx::query("SELECT owner_id FROM notes WHERE note_id=$1")
        .bind(note_id)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(owner, A);

    let note = call(
        &router,
        "GET",
        &format!("/me/notes/{note_id}"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(note.status, StatusCode::OK);
    assert_eq!(note.body["main_representation"], "typed");
    assert_eq!(note.body["title"], Value::Null);
    assert_eq!(note.body["published"], false);
    assert_eq!(note.body["publication"], Value::Null);
    let reps = note.body["representations"].as_array().unwrap();
    assert_eq!(reps.len(), 1);
    assert_eq!(reps[0]["representation"], "typed");
    assert_eq!(reps[0]["document"], typed("Formal text"));
    let original_view = call(
        &router,
        "GET",
        &format!("/me/notes/{note_id}?view=original"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(original_view.body["representations"], json!([]));

    // Two representations need an explicit main representation.
    let both = create(
        &router,
        A,
        json!({"client_note_id": "api-2", "representations": {
            "original": {"document": original(40)}, "typed": {"document": typed("x")}}}),
    )
    .await;
    assert_eq!(both.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(both.body["code"], "main_representation_required");
}

#[tokio::test]
async fn two_devices_patch_with_the_same_revision_one_wins_one_gets_409() {
    let (store, router) = setup().await;
    let note_id = create_original(&router, A, "n1", original(40)).await;
    let uri = format!("/me/notes/{note_id}");
    let device_one = json!({"original": {"op": "update", "id": "t1", "changes": {"x": 104}}});
    let device_two = json!({"original": {"op": "update", "id": "t1", "changes": {"x": 496}}});
    let one = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "\"1\"")],
        Some(device_one),
    )
    .await;
    assert_eq!(one.status, StatusCode::OK, "{}", one.body);
    assert_eq!(one.body["revision"], 2);
    assert_eq!(one.headers["etag"], "\"2\"");
    let two = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "1")],
        Some(device_two),
    )
    .await;
    assert_eq!(two.status, StatusCode::CONFLICT);
    assert_eq!(two.body["code"], "revision_conflict");
    assert_eq!(two.body["current_revision"], 2);
    let note = call(&router, "GET", &uri, Some(A), &[], None).await;
    assert_eq!(note.body["revision"], 2);
    assert_eq!(
        note.body["representations"][0]["document"]["items"][0]["x"],
        104
    );
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        2
    );

    // A stale PUT and a stale DELETE never overwrite either.
    let put = call(
        &router,
        "PUT",
        &uri,
        Some(A),
        &[("if-match", "1")],
        Some(json!({"representation": "original", "document": original(0)})),
    )
    .await;
    assert_eq!(put.status, StatusCode::CONFLICT);
    assert_eq!(put.body["current_revision"], 2);
    let delete = call(&router, "DELETE", &uri, Some(A), &[("if-match", "1")], None).await;
    assert_eq!(delete.status, StatusCode::CONFLICT);
    assert_eq!(delete.body["current_revision"], 2);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 1);
}

#[tokio::test]
async fn another_account_gets_404_and_identity_comes_only_from_the_gateway() {
    let (store, router) = setup().await;
    let note_id = create_original(&router, A, "n1", original(40)).await;
    let uri = format!("/me/notes/{note_id}");
    for (method, body) in [
        ("GET", None),
        (
            "PUT",
            Some(json!({"representation": "original", "document": original(0)})),
        ),
        ("PATCH", Some(json!({"metadata": {"title": "Taken"}}))),
        ("DELETE", None),
    ] {
        let r = call(&router, method, &uri, Some(B), &[("if-match", "1")], body).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{method}");
        assert_eq!(r.body["code"], "not_found");
        assert!(!r.body.to_string().contains(A), "{method} leaks the owner");
    }
    let list = call(&router, "GET", "/me/notes", Some(B), &[], None).await;
    assert_eq!(list.body["items"], json!([]));
    let feed = call(
        &router,
        "GET",
        "/me/notes?sync_cursor=0",
        Some(B),
        &[],
        None,
    )
    .await;
    assert_eq!(feed.body["changes"], json!([]));

    // No gateway identity: no access.
    let anonymous = call(&router, "GET", "/me/notes", None, &[], None).await;
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED);
    assert_eq!(anonymous.body["code"], "identity_required");
    // Owner fields in a body are refused, never used.
    let spoof = create(
        &router,
        B,
        json!({"client_note_id": "n9", "owner_id": A,
               "representations": {"typed": {"document": typed("x")}}}),
    )
    .await;
    assert_eq!(spoof.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(spoof.body["code"], "unknown_field");
    let spoof_patch = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "1")],
        Some(json!({"metadata": {"title": "x", "owner_id": B}})),
    )
    .await;
    assert_eq!(spoof_patch.body["code"], "unknown_field");
    let owner: String = sqlx::query("SELECT owner_id FROM notes WHERE note_id=$1")
        .bind(&note_id)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(owner, A);
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM notes WHERE client_note_id='n9'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn sync_cursor_returns_creates_updates_and_deletes_in_order() {
    let (store, router) = setup().await;
    let first = create_original(&router, A, "n1", original(40)).await;
    let second = create_original(&router, A, "n2", original(40)).await;
    let _other = create_original(&router, B, "n1", original(40)).await;
    let listed = call(&router, "GET", "/me/notes", Some(A), &[], None).await;
    let head = listed.body["sync_cursor"].as_str().unwrap().to_string();
    let patched = call(
        &router,
        "PATCH",
        &format!("/me/notes/{first}"),
        Some(A),
        &[("if-match", "1")],
        Some(json!({"metadata": {"title": "Renamed"}})),
    )
    .await;
    assert_eq!(patched.status, StatusCode::OK, "{}", patched.body);
    let deleted = call(
        &router,
        "DELETE",
        &format!("/me/notes/{second}"),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    assert_eq!(
        count(
            &store,
            &format!("SELECT COUNT(*) FROM notes WHERE note_id='{second}'")
        )
        .await,
        0
    );

    let feed = call(
        &router,
        "GET",
        "/me/notes?sync_cursor=0",
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(feed.status, StatusCode::OK);
    let got: Vec<(String, String)> = feed.body["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["note_id"].as_str().unwrap().to_string(),
                c["change"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (first.clone(), "upsert".to_string()),
            (second.clone(), "upsert".to_string()),
            (first.clone(), "upsert".to_string()),
            (second.clone(), "delete".to_string()),
        ]
    );
    assert_eq!(feed.body["has_more"], false);

    // Since the cursor of the earlier list: only the later change and the deletion.
    let since = call(
        &router,
        "GET",
        &format!("/me/notes?sync_cursor={head}"),
        Some(A),
        &[],
        None,
    )
    .await;
    let kinds: Vec<&str> = since.body["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["change"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, vec!["upsert", "delete"]);

    // Pages of two follow the feed order.
    let page = call(
        &router,
        "GET",
        "/me/notes?sync_cursor=0&limit=2",
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(page.body["changes"].as_array().unwrap().len(), 2);
    assert_eq!(page.body["has_more"], true);
    let next = page.body["next_sync_cursor"].as_str().unwrap();
    let rest = call(
        &router,
        "GET",
        &format!("/me/notes?sync_cursor={next}&limit=2"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(rest.body["changes"][1]["change"], "delete");
    assert_eq!(rest.body["has_more"], false);
    let end = rest.body["next_sync_cursor"].as_str().unwrap();
    let empty = call(
        &router,
        "GET",
        &format!("/me/notes?sync_cursor={end}"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(empty.body["changes"], json!([]));
    assert_eq!(empty.body["next_sync_cursor"], end);

    let bad = call(
        &router,
        "GET",
        "/me/notes?sync_cursor=0&tag=x",
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(bad.body["code"], "sync_cursor_with_filters");
    let bad = call(
        &router,
        "GET",
        "/me/notes?sync_cursor=-1",
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(bad.body["code"], "invalid_sync_cursor");
}

#[tokio::test]
async fn delete_removes_the_working_copy_and_keeps_a_shared_file() {
    let (store, router) = setup().await;
    sqlx::raw_sql(&format!(
        "INSERT INTO owners(id,display_name) VALUES('{A}','');
         INSERT INTO files(file_id,owner_id,fsm_reference,sha256,size_bytes,content_type,purpose,status,created_at,updated_at)
         VALUES('img-1','{A}','fsm-1','{}',10,'image/png','attachment','ready',1,1);
         INSERT INTO projects(project_id,owner_id,name,normalized_name,created_at,updated_at) VALUES('p1','{A}','Biology','biology',1,1);",
        "0".repeat(64)
    ))
    .execute(&store.db)
    .await
    .unwrap();
    let r = create(
        &router,
        A,
        json!({"client_note_id": "n1", "project_ids": ["p1"],
               "metadata": {"title": "Cells", "tags": ["Bio"]},
               "representations": {"original": {"document": with_image("img-1")},
                                   "typed": {"document": typed("Cells")}},
               "main_representation": "original"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let gone = r.body["note_id"].as_str().unwrap().to_string();
    let kept = create_original(&router, A, "n2", with_image("img-1")).await;
    // A publication of the deleted note (the publication routes come later).
    let revision: String = sqlx::query(
        "SELECT revision_id FROM note_current_revisions WHERE note_id=$1 AND representation='typed'",
    )
    .bind(&gone)
    .fetch_one(&store.db)
    .await
    .unwrap()
    .get(0);
    sqlx::raw_sql(&format!(
        "INSERT INTO note_publications(note_id,language_code,revision,updated_at) VALUES('{gone}','en',1,1);
         INSERT INTO note_published_representations(note_id,representation,revision_id) VALUES('{gone}','typed','{revision}');"
    ))
    .execute(&store.db)
    .await
    .unwrap();
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{gone}"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(detail.body["published"], true);
    assert_eq!(
        detail.body["publication"]["representations"][0]["revision_id"],
        revision.as_str()
    );
    assert_eq!(detail.body["project_ids"], json!(["p1"]));
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM file_references WHERE file_id='img-1'"
        )
        .await,
        2
    );
    assert_eq!(document_count(&store), 3);

    let uri = format!("/me/notes/{gone}");
    let invalid_cascade = call(
        &router,
        "DELETE",
        &format!("{uri}?delete_owned_reports=maybe"),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(invalid_cascade.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid_cascade.body["code"], "invalid_delete_owned_reports");
    let missing = call(&router, "DELETE", &uri, Some(A), &[], None).await;
    assert_eq!(missing.status, StatusCode::PRECONDITION_REQUIRED);
    assert_eq!(missing.body["code"], "if_match_required");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 2);

    let deleted = call(
        &router,
        "DELETE",
        &format!("{uri}?delete_owned_reports=false"),
        Some(A),
        &[("if-match", "\"1\"")],
        None,
    )
    .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    assert_eq!(deleted.body, Value::Null);
    for (table, column) in [
        ("note_revisions", "note_id"),
        ("note_current_revisions", "note_id"),
        ("note_publications", "note_id"),
        ("note_published_representations", "note_id"),
        ("project_notes", "note_id"),
        ("note_tags", "note_id"),
        ("note_titles", "note_id"),
        ("note_save_operations", "note_id"),
    ] {
        assert_eq!(
            count(
                &store,
                &format!("SELECT COUNT(*) FROM {table} WHERE {column}='{gone}'")
            )
            .await,
            0,
            "{table}"
        );
    }
    // The shared file and the other note's reference stay; the project stays.
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM files WHERE file_id='img-1'").await,
        1
    );
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM file_references WHERE file_id='img-1'"
        )
        .await,
        1
    );
    assert_eq!(count(&store, "SELECT COUNT(*) FROM projects").await, 1);
    // The deleted note's documents are removed; the queue is empty.
    assert_eq!(document_count(&store), 1);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revision_cleanup").await,
        0
    );
    let still = call(
        &router,
        "GET",
        &format!("/me/notes/{kept}"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(still.status, StatusCode::OK);
    let again = call(&router, "GET", &uri, Some(A), &[], None).await;
    assert_eq!(again.status, StatusCode::NOT_FOUND);

    // A later explicit sync of the same local note creates a new cloud copy.
    let resync = create(
        &router,
        A,
        json!({"client_note_id": "n1", "representations": {"typed": {"document": typed("Cells")}}}),
    )
    .await;
    assert_eq!(resync.status, StatusCode::CREATED, "{}", resync.body);
    assert_ne!(resync.body["note_id"], gone.as_str());
}

#[tokio::test]
async fn list_filters_by_text_project_and_tag_with_cursor_pages() {
    let (store, router) = setup().await;
    sqlx::raw_sql(&format!(
        "INSERT INTO owners(id,display_name) VALUES('{A}','');
         INSERT INTO projects(project_id,owner_id,name,normalized_name,created_at,updated_at) VALUES('p1','{A}','Math','math',1,1);
         INSERT INTO projects(project_id,owner_id,name,normalized_name,created_at,updated_at) VALUES('pb','{A}','Other','other',1,1);"
    ))
    .execute(&store.db)
    .await
    .unwrap();
    let mut ids = vec![];
    for (i, (title, tags, projects)) in [
        ("Algebra 100%", json!(["Math", "exam"]), json!(["p1"])),
        ("Biology", json!(["science"]), json!([])),
        ("Linear algebra", json!([" MATH "]), json!(["p1"])),
    ]
    .into_iter()
    .enumerate()
    {
        let r = create(
            &router,
            A,
            json!({"client_note_id": format!("n{i}"), "project_ids": projects,
                   "metadata": {"title": title, "tags": tags},
                   "representations": {"typed": {"document": typed(title)}}}),
        )
        .await;
        assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
        ids.push(r.body["note_id"].as_str().unwrap().to_string());
    }
    let titles = |r: &Reply| -> Vec<String> {
        r.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["title"].as_str().unwrap().to_string())
            .collect()
    };
    let mut q = titles(&call(&router, "GET", "/me/notes?q=ALGEBRA", Some(A), &[], None).await);
    q.sort();
    assert_eq!(q, vec!["Algebra 100%", "Linear algebra"]);
    // LIKE wildcards in q are literal.
    assert_eq!(
        titles(&call(&router, "GET", "/me/notes?q=100%25", Some(A), &[], None).await),
        vec!["Algebra 100%"]
    );
    assert_eq!(
        titles(&call(&router, "GET", "/me/notes?q=_", Some(A), &[], None).await),
        Vec::<String>::new()
    );
    let mut tag = titles(&call(&router, "GET", "/me/notes?tag=math", Some(A), &[], None).await);
    tag.sort();
    assert_eq!(tag, vec!["Algebra 100%", "Linear algebra"]);
    let project = call(
        &router,
        "GET",
        "/me/notes?project_id=p1&tag=exam",
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(titles(&project), vec!["Algebra 100%"]);
    assert_eq!(project.body["items"][0]["tags"], json!(["exam", "Math"]));
    assert_eq!(project.body["items"][0]["project_ids"], json!(["p1"]));
    assert!(project.body["items"][0].get("document").is_none());
    // Tag names are shared by normalized form: " MATH " joined "Math".
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM tags WHERE normalized_name='math'"
        )
        .await,
        1
    );

    // Pages of one, newest change first, cover every note once.
    let mut seen = vec![];
    let mut uri = "/me/notes?limit=1".to_string();
    loop {
        let page = call(&router, "GET", &uri, Some(A), &[], None).await;
        assert_eq!(page.status, StatusCode::OK, "{}", page.body);
        for item in page.body["items"].as_array().unwrap() {
            seen.push(item["note_id"].as_str().unwrap().to_string());
        }
        match page.body["next_cursor"].as_str() {
            Some(c) => uri = format!("/me/notes?limit=1&cursor={c}"),
            None => break,
        }
    }
    seen.sort();
    ids.sort();
    assert_eq!(seen, ids);

    for (uri, code) in [
        ("/me/notes?limit=0", "invalid_limit"),
        ("/me/notes?limit=101", "invalid_limit"),
        ("/me/notes?cursor=nonsense", "invalid_cursor"),
    ] {
        let r = call(&router, "GET", uri, Some(A), &[], None).await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}");
        assert_eq!(r.body["code"], code, "{uri}");
    }
    // A project of another owner gives no membership.
    let foreign = create(
        &router,
        B,
        json!({"client_note_id": "x", "project_ids": ["p1"],
               "representations": {"typed": {"document": typed("x")}}}),
    )
    .await;
    assert_eq!(foreign.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(foreign.body["code"], "unknown_project");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM project_notes").await, 2);
}

#[tokio::test]
async fn put_and_patch_use_validated_content_and_replay_by_idempotency_key() {
    let (store, router) = setup().await;
    let note_id = create_original(&router, A, "n1", original(40)).await;
    let uri = format!("/me/notes/{note_id}");

    // PUT replaces one representation; adding Typed keeps the main representation.
    let put = call(
        &router,
        "PUT",
        &uri,
        Some(A),
        &[("if-match", "1")],
        Some(json!({"representation": "typed", "schema_version": 1, "document": typed("Draft")})),
    )
    .await;
    assert_eq!(put.status, StatusCode::OK, "{}", put.body);
    assert_eq!(put.body["revision"], 2);
    assert_eq!(put.body["revisions"][0]["representation"], "typed");
    let note = call(&router, "GET", &uri, Some(A), &[], None).await;
    assert_eq!(note.body["main_representation"], "original");
    assert_eq!(note.body["representations"].as_array().unwrap().len(), 2);

    // Invalid content and broken editor rules are refused with stable codes.
    let invalid = call(
        &router,
        "PUT",
        &uri,
        Some(A),
        &[("if-match", "2")],
        Some(json!({"representation": "typed", "document": {"blocks": [{"id": "k1", "kind": "poem"}]}})),
    )
    .await;
    assert_eq!(invalid.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid.body["code"], "unknown_kind");
    let overlap = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "2")],
        Some(
            json!({"original": {"op": "add", "item": {"id": "t2", "kind": "text", "x": 48, "y": 48,
            "width": 96, "height": 100, "spans": [{"text": "x"}]}}}),
        ),
    )
    .await;
    assert_eq!(overlap.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(overlap.body["code"], "block_overlap");
    let narrow = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "2")],
        Some(json!({"original": {"op": "page", "page": {"width": 200, "background": {"kind": "blank"}}}})),
    )
    .await;
    assert_eq!(narrow.body["code"], "out_of_bounds");
    let typed_op = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "2")],
        Some(json!({"typed": {"op": "delete", "id": "k1"}})),
    )
    .await;
    assert_eq!(typed_op.body["code"], "typed_operations_unsupported");
    let empty = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "2")],
        Some(json!({})),
    )
    .await;
    assert_eq!(empty.body["code"], "empty_save");
    let no_match = call(&router, "PATCH", &uri, Some(A), &[], Some(json!({}))).await;
    assert_eq!(no_match.status, StatusCode::PRECONDITION_REQUIRED);
    let bad_match = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "*")],
        Some(json!({})),
    )
    .await;
    assert_eq!(bad_match.body["code"], "invalid_if_match");

    // One PATCH: page settings, a move and metadata, in one revision.
    let change = json!({
        "original": {"op": "batch", "steps": [
            {"op": "page", "page": {"width": 1200, "background": {"kind": "grid", "spacing": 24, "visibility": 0.4}}},
            {"op": "update", "id": "t1", "changes": {"x": 800}}
        ]},
        "metadata": {"title": "  Leaves  ", "tags": ["Plants", "plants"]}
    });
    let key = [("if-match", "2"), ("idempotency-key", "device-1-op-7")];
    let first = call(&router, "PATCH", &uri, Some(A), &key, Some(change.clone())).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    assert_eq!(first.body["revision"], 3);
    // The response was lost; the retry with the same key gets the same answer.
    let retry = call(&router, "PATCH", &uri, Some(A), &key, Some(change)).await;
    assert_eq!(retry.status, StatusCode::OK);
    assert_eq!(retry.body, first.body);
    let reused = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &key,
        Some(json!({"metadata": {"title": "Other"}})),
    )
    .await;
    assert_eq!(reused.body["code"], "idempotency_key_reused");
    let note = call(
        &router,
        "GET",
        &format!("{uri}?view=original"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(note.body["revision"], 3);
    assert_eq!(note.body["title"], "Leaves");
    assert_eq!(note.body["tags"], json!(["Plants"]));
    let doc = &note.body["representations"][0]["document"];
    assert_eq!(doc["page"]["width"], 1200);
    assert_eq!(doc["items"][0]["x"], 800);
    assert_eq!(note.body["representations"].as_array().unwrap().len(), 1);

    // Metadata only: clear the title and the tags.
    let clear = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "3")],
        Some(json!({"metadata": {"title": null, "tags": []}})),
    )
    .await;
    assert_eq!(clear.status, StatusCode::OK, "{}", clear.body);
    assert_eq!(clear.body["revisions"], json!([]));
    let note = call(&router, "GET", &uri, Some(A), &[], None).await;
    assert_eq!(note.body["title"], Value::Null);
    assert_eq!(note.body["tags"], json!([]));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM note_titles").await, 0);
    let long = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        &[("if-match", "4")],
        Some(json!({"metadata": {"title": "x".repeat(201)}})),
    )
    .await;
    assert_eq!(long.body["code"], "text_too_long");
}

#[tokio::test]
async fn request_bodies_are_checked_before_any_write() {
    let (store, router) = setup().await;
    let raw = |body: &'static str| {
        Request::builder()
            .method("POST")
            .uri("/me/notes")
            .header("x-uuid", A)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    };
    for (body, code) in [
        ("{", "invalid_json"),
        ("[]", "invalid_type"),
        (
            r#"{"client_note_id":"n","representations":{"typed":{"document":{"blocks":[{"id":"a","kind":"heading","level":1e999,"spans":[]}]}}}}"#,
            "non_finite_number",
        ),
        (
            r#"{"client_note_id":"","representations":{}}"#,
            "invalid_client_note_id",
        ),
        (
            r#"{"client_note_id":"n","representations":{}}"#,
            "representation_required",
        ),
        (
            r#"{"client_note_id":"n","representations":{"draft":{}}}"#,
            "unknown_field",
        ),
    ] {
        let response = router.clone().oneshot(raw(body)).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{body}"
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["code"], code, "{body}");
    }
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 0);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM pending_operations").await,
        0
    );
    assert_eq!(document_count(&store), 0);
}

#[tokio::test]
async fn notes_prepared_certifications_handoff_uses_owned_files_and_service_key() {
    use annotations_api::dal::certifications::HttpCertifications;
    use axum::{Json, Router, http::HeaderMap, routing::post};
    use std::sync::{Arc, Mutex};

    let captured: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_srv = captured.clone();
    let fake_cert = Router::new().route(
        "/studies/prepared",
        post(move |headers: HeaderMap, Json(body): Json<Value>| {
            let captured = captured_srv.clone();
            async move {
                let uuid = headers
                    .get("x-uuid")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let key = headers
                    .get("x-service-key")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                if key != "cert-svc-secret" {
                    return (StatusCode::FORBIDDEN, Json(json!({"error": "bad key"})));
                }
                captured.lock().unwrap().push((uuid.clone(), key, body.clone()));
                (
                    StatusCode::CREATED,
                    Json(json!({
                        "study": {"id": "study-prep-1", "owner_id": uuid, "source_origin": "annotations"},
                        "sources": body["sources"]
                    })),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, fake_cert).await.unwrap();
    });

    let (mut store, _) = setup().await;
    let router_unconfigured = presentation::router(App {
        store: store.clone(),
    });

    let n1 = create(
        &router_unconfigured,
        A,
        json!({
            "client_note_id": "prep-n1",
            "metadata": {"title": "Study Note"},
            "representations": {"typed": {"schema_version": 1, "document": typed("Study Note")}}
        }),
    )
    .await;
    let n1_id = n1.body["note_id"].as_str().unwrap().to_string();
    let _ = create(
        &router_unconfigured,
        B,
        json!({
            "client_note_id": "prep-nb",
            "metadata": {"title": "Study Note B"},
            "representations": {"typed": {"schema_version": 1, "document": typed("Study Note B")}}
        }),
    )
    .await;

    sqlx::query(
        "INSERT INTO files(file_id,owner_id,filename,content_type,size_bytes,sha256,purpose,status,created_at,updated_at,fsm_reference)
         VALUES('file-a-ready',$1,'slides.pdf','application/pdf',42,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','attachment','ready',1,1,'annotations/a/slides.pdf')",
    )
    .bind(A)
    .execute(&store.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO files(file_id,owner_id,filename,content_type,size_bytes,sha256,purpose,status,created_at,updated_at,fsm_reference)
         VALUES('file-b-ready',$1,'b.pdf','application/pdf',10,'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','attachment','ready',1,1,'annotations/b/b.pdf')",
    )
    .bind(B)
    .execute(&store.db)
    .await
    .unwrap();

    // Unconfigured certifications client fails closed with 503.
    let unconf = call(
        &router_unconfigured,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Biology Prep",
            "sources": [{"file_id": "file-a-ready"}]
        })),
    )
    .await;
    assert_eq!(unconf.status, StatusCode::SERVICE_UNAVAILABLE);

    // Configured HttpCertifications forwards x-uuid + x-service-key + authoritative fsm_reference.
    store.certifications = HttpCertifications::client(
        format!("http://{addr}"),
        "cert-svc-secret".into(),
        reqwest::Client::new(),
    );
    let router = presentation::router(App {
        store: store.clone(),
    });

    // Reject referencing another user's file_id.
    let stolen = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Biology Prep",
            "sources": [{"file_id": "file-b-ready"}]
        })),
    )
    .await;
    assert_eq!(stolen.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(captured.lock().unwrap().is_empty());

    // Succeeds for owned ready file without browser holding CERTIFICATIONS_SERVICE_KEY or raw fsm_ref.
    let ok = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Biology Prep",
            "description": "From note",
            "sources": [{"file_id": "file-a-ready", "selection_kind": "pages", "selection_spec": "1-3"}]
        })),
    )
    .await;
    assert_eq!(ok.status, StatusCode::CREATED, "{:?}", ok.body);
    assert_eq!(ok.body["study"]["id"], "study-prep-1");
    assert_eq!(captured.lock().unwrap().len(), 1);
    assert_eq!(captured.lock().unwrap()[0].0, A);
    assert_eq!(captured.lock().unwrap()[0].1, "cert-svc-secret");
    assert_eq!(
        captured.lock().unwrap()[0].2["sources"][0]["fsm_ref"],
        "annotations/a/slides.pdf"
    );

    // Reject forged authority fields and malformed selections with zero downstream calls.
    let forged = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Forged",
            "sources": [{"kind": "file", "file_id": "file-a-ready", "fsm_ref": "evil/ref"}]
        })),
    )
    .await;
    assert!(
        matches!(
            forged.status,
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
        ),
        "{:?}",
        forged.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    let bad_sel = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Bad Sel",
            "sources": [{"kind": "file", "file_id": "file-a-ready", "selection_kind": "pages", "selection_spec": "5-2"}]
        })),
    )
    .await;
    assert_eq!(
        bad_sel.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        bad_sel.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    let bad_pages_cap = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Over 100 Pages",
            "sources": [{"kind": "file", "file_id": "file-a-ready", "selection_kind": "pages", "selection_spec": "1-101"}]
        })),
    )
    .await;
    assert_eq!(
        bad_pages_cap.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        bad_pages_cap.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    // Reject invalid config language with 422 and zero downstream calls.
    let bad_config_lang = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Bad Lang Config",
            "sources": [{"file_id": "file-a-ready"}],
            "config": {
                "language": "de",
                "count": 5
            }
        })),
    )
    .await;
    assert_eq!(
        bad_config_lang.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        bad_config_lang.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    // Reject invalid config count (< 1 or > 20) with 422 and zero downstream calls.
    let bad_config_count = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Bad Count Config",
            "sources": [{"file_id": "file-a-ready"}],
            "config": {
                "language": "en",
                "count": 25
            }
        })),
    )
    .await;
    assert_eq!(
        bad_config_count.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        bad_config_count.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    // Reject unknown fields in config with 400/422 and zero downstream calls.
    let bad_config_forged = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Forged Config",
            "sources": [{"file_id": "file-a-ready"}],
            "config": {
                "language": "en",
                "count": 5,
                "admin": true
            }
        })),
    )
    .await;
    assert!(
        matches!(
            bad_config_forged.status,
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
        ),
        "{:?}",
        bad_config_forged.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    // Stale expected_revision fails closed with 409 and zero downstream calls.
    let stale_rev = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Stale Rev",
            "expected_revision": 999,
            "sources": [{"kind": "note_original"}]
        })),
    )
    .await;
    assert_eq!(
        stale_rev.status,
        StatusCode::CONFLICT,
        "{:?}",
        stale_rev.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    // Stale/wrong revision_id fails closed with 422 and zero downstream calls.
    let wrong_rev_id = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Wrong RevId",
            "sources": [{"kind": "note_typed", "revision_id": "rev-does-not-exist"}]
        })),
    )
    .await;
    assert_eq!(
        wrong_rev_id.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        wrong_rev_id.body
    );
    assert_eq!(captured.lock().unwrap().len(), 1);

    // Nondefault config (Portuguese, count 15) successfully forwards to downstream Certifications.
    let ok_with_config = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Biology Prep with Config",
            "description": "Portuguese 15 questions",
            "sources": [{"file_id": "file-a-ready"}],
            "config": {
                "language": "pt-BR",
                "count": 15
            }
        })),
    )
    .await;
    assert_eq!(
        ok_with_config.status,
        StatusCode::CREATED,
        "{:?}",
        ok_with_config.body
    );
    assert_eq!(captured.lock().unwrap().len(), 2);
    assert_eq!(captured.lock().unwrap()[1].2["config"]["language"], "pt-BR");
    assert_eq!(captured.lock().unwrap()[1].2["config"]["count"], 15);

    // Create a note with both original and typed representations.
    let typed_doc = json!({
        "blocks": [
            {"id": "h1", "kind": "heading", "level": 1, "spans": [{"text": "Cellular Respiration"}]},
            {"id": "p1", "kind": "paragraph", "spans": [{"text": "ATP synthesis in mitochondria."}]}
        ]
    });
    let create_both = create(
        &router,
        A,
        json!({
            "client_note_id": "n-mixed-cert",
            "main_representation": "original",
            "representations": {
                "original": {"schema_version": 1, "document": original(40)},
                "typed": {"schema_version": 1, "document": typed_doc}
            }
        }),
    )
    .await;
    assert_eq!(
        create_both.status,
        StatusCode::CREATED,
        "{:?}",
        create_both.body
    );
    let mixed_note_id = create_both.body["note_id"].as_str().unwrap().to_string();
    let updated_rev = create_both.body["revision"].as_i64().unwrap();
    let typed_rev_id = create_both.body["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["representation"] == "typed")
        .unwrap()["revision_id"]
        .as_str()
        .unwrap()
        .to_string();

    let files_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE owner_id=$1")
        .bind(A)
        .fetch_one(&store.db)
        .await
        .unwrap();

    // Handoff mixing note_original, note_typed (explicit revision_id), and file materializes note text into FSM once.
    let mixed = call(
        &router,
        "POST",
        &format!("/me/notes/{mixed_note_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Mixed Study",
            "description": "Chosen settings",
            "expected_revision": updated_rev,
            "sources": [
                {"kind": "note_original"},
                {"kind": "note_typed", "revision_id": typed_rev_id, "selection_kind": "passages", "selection_spec": "p1:0-10"},
                {"kind": "file", "file_id": "file-a-ready", "selection_kind": "pages", "selection_spec": "1-3"}
            ]
        })),
    )
    .await;
    assert_eq!(mixed.status, StatusCode::CREATED, "{:?}", mixed.body);
    {
        let calls = captured.lock().unwrap();
        let payload = &calls.last().unwrap().2;
        assert_eq!(payload["title"], "Mixed Study");
        assert_eq!(payload["description"], "Chosen settings");
        assert_eq!(payload["sources"][0]["mime_type"], "text/plain");
        assert_eq!(payload["sources"][0]["selection_kind"], "all");
        assert_eq!(payload["sources"][1]["mime_type"], "text/plain");
        assert_eq!(payload["sources"][1]["selection_kind"], "passages");
        assert_eq!(payload["sources"][1]["selection_spec"], "p1:0-10");
        assert_eq!(payload["sources"][2]["fsm_ref"], "annotations/a/slides.pdf");
        assert_eq!(payload["sources"][2]["selection_kind"], "pages");
        assert_eq!(payload["sources"][2]["selection_spec"], "1-3");
    }
    let files_after_first: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE owner_id=$1")
        .bind(A)
        .fetch_one(&store.db)
        .await
        .unwrap();
    assert_eq!(files_after_first, files_before + 2);

    // Attempting handoff with note_original that has only non-text items (image/strokes) fails with 422 empty_note_source.
    let calls_before_empty = captured.lock().unwrap().len();
    let img_only_note = create(
        &router,
        A,
        json!({
            "client_note_id": "n-img-only",
            "main_representation": "original",
            "representations": {
                "original": {"schema_version": 1, "document": with_image("file-a-ready")}
            }
        }),
    )
    .await;
    assert_eq!(img_only_note.status, StatusCode::CREATED);
    let img_note_id = img_only_note.body["note_id"].as_str().unwrap();

    let empty_extracted = call(
        &router,
        "POST",
        &format!("/me/notes/{img_note_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Empty Extracted Text",
            "sources": [{"kind": "note_original"}]
        })),
    )
    .await;
    assert_eq!(
        empty_extracted.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        empty_extracted.body
    );
    assert_eq!(empty_extracted.body["code"], "empty_note_source");
    assert_eq!(captured.lock().unwrap().len(), calls_before_empty);

    // Repeating the same note_original + note_typed handoff dedups against existing ready files (no new FSM uploads).
    let mixed_repeat = call(
        &router,
        "POST",
        &format!("/me/notes/{mixed_note_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Mixed Study Repeat",
            "expected_revision": updated_rev,
            "sources": [
                {"kind": "note_original"},
                {"kind": "note_typed"}
            ]
        })),
    )
    .await;
    assert_eq!(
        mixed_repeat.status,
        StatusCode::CREATED,
        "{:?}",
        mixed_repeat.body
    );
    let files_after_repeat: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE owner_id=$1")
            .bind(A)
            .fetch_one(&store.db)
            .await
            .unwrap();
    assert_eq!(files_after_repeat, files_after_first);

    // Mixed document with text, ink strokes, images, and group proves only text subset is materialized.
    let mixed_doc = json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id": "t1", "kind": "text", "x": 0, "y": 0, "width": 160, "height": 80,
             "spans": [{"text": "First chapter on cellular respiration."}]},
            {"id": "s1", "kind": "stroke", "tool": "round", "brush_version": 1,
             "width": 2.5, "color": "#000000FF", "opacity": 1,
             "samples": {"count": 2, "data": "AQCAGYDIAYCWAcAlABA"},
             "nodes": [
                 {"x": 1600, "y": 9600, "mode": "smooth", "corner": false, "sample": 0},
                 {"x": 14400, "y": 12000, "mode": "smooth", "corner": false, "sample": 1}
             ]},
            {"id": "t2", "kind": "text", "x": 240, "y": 0, "width": 160, "height": 80,
             "spans": [{"text": "Second chapter on oxidative phosphorylation."}]},
            {"id": "i1", "kind": "image", "x": 480, "y": 0, "width": 160, "height": 80, "file_id": "file-a-ready"},
            {"id": "g1", "kind": "group", "members": ["t1", "s1"]}
        ]
    });
    let mixed_content_note = create(
        &router,
        A,
        json!({
            "client_note_id": "n-mixed-content-subset",
            "main_representation": "original",
            "representations": {
                "original": {"schema_version": 1, "document": mixed_doc}
            }
        }),
    )
    .await;
    assert_eq!(
        mixed_content_note.status,
        StatusCode::CREATED,
        "{:?}",
        mixed_content_note.body
    );
    let mixed_c_id = mixed_content_note.body["note_id"].as_str().unwrap();

    let mixed_c_study = call(
        &router,
        "POST",
        &format!("/me/notes/{mixed_c_id}/certifications/prepared"),
        Some(A),
        &[],
        Some(json!({
            "title": "Cellular Biology Study",
            "description": "Subset text extraction test",
            "sources": [{"kind": "note_original"}],
            "config": {"language": "pt-BR", "count": 15}
        })),
    )
    .await;
    assert_eq!(
        mixed_c_study.status,
        StatusCode::CREATED,
        "{:?}",
        mixed_c_study.body
    );
    let payload = {
        let calls = captured.lock().unwrap();
        calls.last().unwrap().2.clone()
    };
    assert_eq!(payload["title"], "Cellular Biology Study");
    assert_eq!(payload["config"]["language"], "pt-BR");
    assert_eq!(payload["config"]["count"], 15);
    assert_eq!(payload["sources"].as_array().unwrap().len(), 1);
    use sha2::{Digest, Sha256};
    let fsm_ref = payload["sources"][0]["fsm_ref"].as_str().unwrap();
    let expected_text =
        "First chapter on cellular respiration.\nSecond chapter on oxidative phosphorylation.\n";
    let expected_sha = format!("{:x}", Sha256::digest(expected_text.as_bytes()));
    assert_eq!(payload["sources"][0]["sha256"], expected_sha);
    assert!(!fsm_ref.is_empty());

    let file_row =
        sqlx::query("SELECT filename, content_type, sha256 FROM files WHERE fsm_reference=$1")
            .bind(fsm_ref)
            .fetch_one(&store.db)
            .await
            .unwrap();
    let sha: String = file_row.get("sha256");
    assert_eq!(sha, expected_sha);
}

#[tokio::test]
async fn reports_created_for_note_cascade_reuse_warning_and_retry() {
    let (mut store, _) = setup().await;
    let mock_reports = annotations_api::dal::reports::MockReports::new();
    mock_reports.insert_item("rep-linked-preexisting", A);
    mock_reports.insert_item("rep-other-owner", B);
    store.reports = mock_reports.clone();

    let router = presentation::router(App {
        store: store.clone(),
    });

    let n1 = create(
        &router,
        A,
        json!({
            "client_note_id": "cascade-n1",
            "metadata": {"title": "Note One"},
            "representations": {"typed": {"schema_version": 1, "document": typed("Note One")}}
        }),
    )
    .await;
    assert_eq!(n1.status, StatusCode::CREATED, "{}", n1.body);
    let n1_id = n1.body["note_id"].as_str().unwrap().to_string();

    let n2 = create(
        &router,
        A,
        json!({
            "client_note_id": "cascade-n2",
            "metadata": {"title": "Note Two"},
            "representations": {"typed": {"schema_version": 1, "document": typed("Note Two")}}
        }),
    )
    .await;
    assert_eq!(n2.status, StatusCode::CREATED);
    let n2_id = n2.body["note_id"].as_str().unwrap().to_string();

    let n_other = create(
        &router,
        B,
        json!({
            "client_note_id": "cascade-other-b",
            "metadata": {"title": "Note B"},
            "representations": {"typed": {"schema_version": 1, "document": typed("Note B")}}
        }),
    )
    .await;
    assert_eq!(n_other.status, StatusCode::CREATED);
    let n_other_id = n_other.body["note_id"].as_str().unwrap().to_string();

    // Server-verified creation through Notes->Reports path records created_for_note.
    let mut created_ids = Vec::new();
    for title in ["Unreused Ok", "Fail Retry", "Reused By N2"] {
        let r = call(
            &router,
            "POST",
            &format!("/me/notes/{n1_id}/reports"),
            Some(A),
            &[],
            Some(json!({
                "mode": "create",
                "project": "notes-proj",
                "item": {"title": title, "template_id": "t1", "template_version": 1}
            })),
        )
        .await;
        assert_eq!(r.status, StatusCode::CREATED, "{title}: {:?}", r.body);
        assert_eq!(r.body["relation_kind"], "created_for_note");
        assert_eq!(r.body["item"]["project"], "notes-proj");
        assert!(
            !r.body["item"]["report_slug"]
                .as_str()
                .unwrap_or("")
                .is_empty()
        );
        assert!(
            !r.body["item"]["dataset_id"]
                .as_str()
                .unwrap_or("")
                .is_empty()
        );
        created_ids.push(r.body["report_item_id"].as_str().unwrap().to_string());
    }
    let rep_ok = created_ids[0].clone();
    let rep_fail = created_ids[1].clone();
    let rep_reused = created_ids[2].clone();
    mock_reports.set_delete_failure(&rep_fail, true);

    // Register pre-existing item as linked.
    let linked_reg = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/reports"),
        Some(A),
        &[],
        Some(json!({"report_item_id": "rep-linked-preexisting", "relation_kind": "linked"})),
    )
    .await;
    assert_eq!(linked_reg.status, StatusCode::CREATED);

    // Reject client-asserted created_for_note (no pre-existing promotion or guessed IDs!).
    for candidate in [
        "nonexistent-uuid",
        "rep-linked-preexisting",
        "rep-other-owner",
        &rep_ok,
    ] {
        let rej = call(
            &router,
            "POST",
            &format!("/me/notes/{n2_id}/reports"),
            Some(A),
            &[],
            Some(json!({"report_item_id": candidate, "relation_kind": "created_for_note"})),
        )
        .await;
        assert_eq!(rej.status, StatusCode::UNPROCESSABLE_ENTITY, "{candidate}");
        assert_eq!(rej.body["code"], "provenance_required");
    }

    // DB partial unique index forbids two notes holding created_for_note for one report_item_id.
    let dup_creator = sqlx::query(
        "INSERT INTO note_report_references(note_id,owner_id,report_item_id,relation_kind,created_at,updated_at) VALUES($1,$2,$3,'created_for_note',1,1)",
    )
    .bind(&n2_id)
    .bind(A)
    .bind(&rep_ok)
    .execute(&store.db)
    .await;
    assert!(
        dup_creator.is_err(),
        "partial unique index must reject second creator"
    );

    // n2 links rep_reused, and user B also has a link row on rep_reused (must NEVER leak n_other_id to user A!).
    let link_n2 = call(
        &router,
        "POST",
        &format!("/me/notes/{n2_id}/reports"),
        Some(A),
        &[],
        Some(json!({"report_item_id": rep_reused, "relation_kind": "linked"})),
    )
    .await;
    assert_eq!(link_n2.status, StatusCode::CREATED);
    let link_other = call(
        &router,
        "POST",
        &format!("/me/notes/{n_other_id}/reports"),
        Some(B),
        &[],
        Some(json!({"report_item_id": rep_reused, "relation_kind": "linked"})),
    )
    .await;
    assert_eq!(link_other.status, StatusCode::CREATED);

    // Pre-delete preview on n1 warns only with caller A's reused note IDs (never n_other_id).
    let preview = call(
        &router,
        "GET",
        &format!("/me/notes/{n1_id}/reports/delete-preview"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(preview.status, StatusCode::OK);
    let created_items = preview.body["created_items"].as_array().unwrap();
    let reused_entry = created_items
        .iter()
        .find(|e| e["report_item_id"] == rep_reused)
        .unwrap();
    assert_eq!(reused_entry["reused_by_note_ids"], json!([n2_id]));
    assert!(!preview.body.to_string().contains(&n_other_id));

    // Delete n1 with delete_owned_reports=true WITHOUT confirm_reused_report_ids:
    // - rep_ok deleted (confirm=false)
    // - rep_reused SKIPPED with explicit reuse warning (ZERO delete calls on rep_reused!)
    // - rep_fail recorded in failed_report_deletions
    // - rep-linked-preexisting NEVER sent to MockReports::delete_owned_item
    let del = call(
        &router,
        "DELETE",
        &format!("/me/notes/{n1_id}?delete_owned_reports=true"),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(del.status, StatusCode::OK, "{:?}", del.body);
    assert_eq!(del.body["note_deleted"], true);
    assert_eq!(del.body["deleted_report_ids"], json!([rep_ok]));
    assert_eq!(del.body["skipped_linked_count"], 1);
    assert_eq!(del.body["reused_warnings"].as_array().unwrap().len(), 1);
    assert_eq!(del.body["reused_warnings"][0]["report_item_id"], rep_reused);
    assert_eq!(
        del.body["reused_warnings"][0]["reused_by_note_ids"],
        json!([n2_id])
    );
    assert!(!del.body.to_string().contains(&n_other_id));
    assert_eq!(
        del.body["failed_report_deletions"][0]["report_item_id"],
        rep_fail
    );
    assert!(
        !mock_reports
            .delete_calls()
            .iter()
            .any(|(_, id, _)| id == &rep_reused
                || id == "rep-linked-preexisting"
                || id == "rep-other-owner")
    );

    // Retry cleanup after Reports recovers -> deletes rep_fail cleanly.
    mock_reports.set_delete_failure(&rep_fail, false);
    let retry = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/reports/retry-cleanup"),
        Some(A),
        &[],
        None,
    )
    .await;
    assert_eq!(retry.status, StatusCode::OK);
    assert_eq!(retry.body["deleted_report_ids"], json!([rep_fail]));
    assert_eq!(retry.body["failed_report_deletions"], json!([]));

    // Scoped explicit confirmation: create on n3, reuse on n2, delete n3 WITH confirm_reused_report_ids -> deleted with confirm=true!
    let n3 = create(
        &router,
        A,
        json!({
            "client_note_id": "cascade-n3",
            "metadata": {"title": "Note Three"},
            "representations": {"typed": {"schema_version": 1, "document": typed("Note Three")}}
        }),
    )
    .await;
    let n3_id = n3.body["note_id"].as_str().unwrap().to_string();
    let r3 = call(
        &router,
        "POST",
        &format!("/me/notes/{n3_id}/reports"),
        Some(A),
        &[],
        Some(json!({
            "mode": "create",
            "project": "notes-proj",
            "item": {"title": "Confirmed Reuse", "template_id": "t1", "template_version": 1}
        })),
    )
    .await;
    let rep_confirmed = r3.body["report_item_id"].as_str().unwrap().to_string();
    let _ = call(
        &router,
        "POST",
        &format!("/me/notes/{n2_id}/reports"),
        Some(A),
        &[],
        Some(json!({"report_item_id": rep_confirmed, "relation_kind": "linked"})),
    )
    .await;
    let del3 = call(
        &router,
        "DELETE",
        &format!(
            "/me/notes/{n3_id}?delete_owned_reports=true&confirm_reused_report_ids={rep_confirmed}"
        ),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(del3.status, StatusCode::OK);
    assert_eq!(del3.body["deleted_report_ids"], json!([rep_confirmed]));
    assert_eq!(del3.body["reused_warnings"], json!([]));
    assert!(
        mock_reports
            .delete_calls()
            .iter()
            .any(|(o, id, c)| o == A && id == &rep_confirmed && *c)
    );

    // Opaque server-minted receipt idempotency:
    // 1) Reusing operation_key with a different project or payload fails 409 BEFORE calling Reports.
    let op_k = "op-create-1";
    let first_create = call(
        &router,
        "POST",
        &format!("/me/notes/{n2_id}/reports"),
        Some(A),
        &[],
        Some(json!({
            "mode": "create",
            "project": "notes-proj",
            "operation_key": op_k,
            "item": {"title": "Receipt Chart", "template_id": "t1", "template_version": 1}
        })),
    )
    .await;
    assert_eq!(first_create.status, StatusCode::CREATED);
    let first_id = first_create.body["report_item_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Retry with exact same operation_key + payload returns the same item without duplicating.
    let retry_create = call(
        &router,
        "POST",
        &format!("/me/notes/{n2_id}/reports"),
        Some(A),
        &[],
        Some(json!({
            "mode": "create",
            "project": "notes-proj",
            "operation_key": op_k,
            "item": {"title": "Receipt Chart", "template_id": "t1", "template_version": 1}
        })),
    )
    .await;
    assert_eq!(retry_create.status, StatusCode::CREATED);
    assert_eq!(retry_create.body["report_item_id"], first_id);

    // Changed payload or project with same operation_key fails 409 Conflict!
    let changed_payload = call(
        &router,
        "POST",
        &format!("/me/notes/{n2_id}/reports"),
        Some(A),
        &[],
        Some(json!({
            "mode": "create",
            "project": "notes-proj",
            "operation_key": op_k,
            "item": {"title": "Different Chart", "template_id": "t1", "template_version": 1}
        })),
    )
    .await;
    assert_eq!(changed_payload.status, StatusCode::CONFLICT);
    assert_eq!(changed_payload.body["code"], "operation_key_conflict");
}

#[tokio::test]
async fn http_reports_client_enforces_x_uuid_confirm_flag_and_bounded_reads() {
    use annotations_api::dal::reports::HttpReports;
    use axum::{
        Json, Router,
        extract::{Path, Query},
        http::{HeaderMap, StatusCode},
        routing::{get, post},
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(serde::Deserialize)]
    struct DelQ {
        #[serde(default)]
        confirm: bool,
    }

    let items: Arc<Mutex<HashMap<String, (String, bool)>>> = Arc::new(Mutex::new(HashMap::new()));
    let items_post = items.clone();
    let items_get = items.clone();
    let items_del = items.clone();

    let fake = Router::new()
        .route(
            "/me/projects/{project}/items",
            post(move |headers: HeaderMap, Path(project): Path<String>| {
                let items = items_post.clone();
                async move {
                    let owner = headers
                        .get("x-uuid")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    if owner.is_empty() {
                        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauth"})));
                    }
                    let id = "http-rep-1".to_string();
                    let replay = headers
                        .get("idempotency-key")
                        .and_then(|v| v.to_str().ok())
                        .is_some_and(|k| k == "idem-replay");
                    items
                        .lock()
                        .unwrap()
                        .insert(id.clone(), (owner.to_string(), true));
                    let status = if replay {
                        StatusCode::OK
                    } else {
                        StatusCode::CREATED
                    };
                    (
                        status,
                        Json(json!({
                            "item": {
                                "id": id,
                                "project": project,
                                "report_slug": "owner-http-1",
                                "dataset_id": "ds-http-1",
                                "source_app": "annotations",
                                "template_id": "bar",
                                "template_version": 1,
                                "audience": "personal",
                                "control_authority": "owner",
                                "etag": "\"e1\"",
                                "embed_url": "/v1/embed/p1/owner-http-1"
                            }
                        })),
                    )
                }
            }),
        )
        .route(
            "/me/items/{id}",
            get(move |headers: HeaderMap, Path(id): Path<String>| {
                let items = items_get.clone();
                async move {
                    let owner = headers
                        .get("x-uuid")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    match items.lock().unwrap().get(&id) {
                        Some((o, _)) if o == owner => {
                            (StatusCode::OK, Json(json!({"item": {"id": id}})))
                        }
                        Some(_) => (StatusCode::FORBIDDEN, Json(json!({"error": "forbidden"}))),
                        None => (StatusCode::NOT_FOUND, Json(json!({"error": "missing"}))),
                    }
                }
            })
            .delete(
                move |headers: HeaderMap, Path(id): Path<String>, Query(q): Query<DelQ>| {
                    let items = items_del.clone();
                    async move {
                        let owner = headers
                            .get("x-uuid")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        let mut map = items.lock().unwrap();
                        match map.get(&id).cloned() {
                            Some((o, _)) if o != owner => {
                                (StatusCode::FORBIDDEN, Json(json!({"error": "forbidden"})))
                            }
                            Some((_, reused)) if reused && !q.confirm => (
                                StatusCode::CONFLICT,
                                Json(json!({"deleted": false, "requires_confirmation": true})),
                            ),
                            Some(_) => {
                                map.remove(&id);
                                (
                                    StatusCode::OK,
                                    Json(json!({"deleted": true, "requires_confirmation": false})),
                                )
                            }
                            None => (StatusCode::NOT_FOUND, Json(json!({"error": "missing"}))),
                        }
                    }
                },
            ),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, fake).await.unwrap();
    });

    let client = HttpReports::client(format!("http://{addr}"), reqwest::Client::new());
    let created = client
        .create_owned_item(A, "p1", "idem-1", json!({"title": "Item"}))
        .await
        .unwrap();
    assert_eq!(created.id, "http-rep-1");
    assert_eq!(created.report_slug, "owner-http-1");
    assert_eq!(created.dataset_id, "ds-http-1");

    // 200 OK idempotent replay is accepted with full metadata.
    let replayed = client
        .create_owned_item(A, "p1", "idem-replay", json!({"title": "Item"}))
        .await
        .unwrap();
    assert_eq!(replayed.id, "http-rep-1");

    // Wrong x-uuid is rejected with 403.
    let wrong_owner = client.verify_owned_item(B, &created.id).await.unwrap_err();
    assert_eq!(wrong_owner.0, StatusCode::FORBIDDEN);

    // Unconfirmed delete on reused item does NOT append ?confirm=true -> gets 409 CONFLICT.
    let no_confirm = client
        .delete_owned_item(A, &created.id, false)
        .await
        .unwrap_err();
    assert_eq!(no_confirm.0, StatusCode::CONFLICT);

    // Confirmed delete appends ?confirm=true -> succeeds with 200 OK.
    client
        .delete_owned_item(A, &created.id, true)
        .await
        .unwrap();
}

#[tokio::test]
async fn create_report_handles_lost_response_concurrent_duplicates_and_digest_conflict() {
    let (mut store, _) = setup().await;
    let mock_reports = annotations_api::dal::reports::MockReports::new();
    store.reports = mock_reports.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });

    let n1 = create(
        &router,
        A,
        json!({
            "client_note_id": "fault-retry-n1",
            "metadata": {"title": "Fault Retry Note"},
            "representations": {"typed": {"schema_version": 1, "document": typed("Body")}}
        }),
    )
    .await;
    let n1_id = n1.body["note_id"].as_str().unwrap().to_string();

    // 1. Reports creates the item but the response is lost (503).
    mock_reports.set_drop_next_create_response(true);
    let lost = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/reports"),
        Some(A),
        &[],
        Some(json!({
            "mode": "create",
            "operation_key": "op-lost-1",
            "project": "proj-a",
            "item": {"title": "First Title", "template_id": "bar", "template_version": 1}
        })),
    )
    .await;
    assert_eq!(lost.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(mock_reports.create_calls_count(), 1);
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM note_report_references WHERE note_id IS NOT NULL"
        )
        .await,
        0
    );

    // 2. Retrying the same operation_key with a changed digest fails with 409 BEFORE calling Reports.
    let conflict = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/reports"),
        Some(A),
        &[],
        Some(json!({
            "mode": "create",
            "operation_key": "op-lost-1",
            "project": "proj-a",
            "item": {"title": "Different Title", "template_id": "bar", "template_version": 1}
        })),
    )
    .await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.body["code"], "operation_key_conflict");
    assert_eq!(mock_reports.create_calls_count(), 1);

    // 3. Retrying with the same request replays the already-created item via the server-minted receipt.
    let replay = call(
        &router,
        "POST",
        &format!("/me/notes/{n1_id}/reports"),
        Some(A),
        &[],
        Some(json!({
            "mode": "create",
            "operation_key": "op-lost-1",
            "project": "proj-a",
            "item": {"title": "First Title", "template_id": "bar", "template_version": 1}
        })),
    )
    .await;
    assert_eq!(replay.status, StatusCode::CREATED, "{:?}", replay.body);
    assert_eq!(replay.body["report_item_id"], "rep-created-1");
    assert_eq!(mock_reports.create_calls_count(), 2);

    // 4. Concurrent duplicate requests with the same operation_key create only one report item.
    let conc_body = json!({
        "mode": "create",
        "operation_key": "op-conc-1",
        "project": "proj-a",
        "item": {"title": "Concurrent Item", "template_id": "bar", "template_version": 1}
    });
    let reports_uri = format!("/me/notes/{n1_id}/reports");
    let (c1, c2) = tokio::join!(
        call(
            &router,
            "POST",
            &reports_uri,
            Some(A),
            &[],
            Some(conc_body.clone()),
        ),
        call(&router, "POST", &reports_uri, Some(A), &[], Some(conc_body))
    );
    assert_eq!(c1.status, StatusCode::CREATED, "{:?}", c1.body);
    assert_eq!(c2.status, StatusCode::CREATED, "{:?}", c2.body);
    assert_eq!(c1.body["report_item_id"], "rep-created-2");
    assert_eq!(c2.body["report_item_id"], "rep-created-2");
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_report_references").await,
        2
    );
}
