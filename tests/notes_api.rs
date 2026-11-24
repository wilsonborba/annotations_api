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
    let reports = call(
        &router,
        "DELETE",
        &format!("{uri}?delete_owned_reports=true"),
        Some(A),
        &[("if-match", "1")],
        None,
    )
    .await;
    assert_eq!(reports.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(reports.body["code"], "reports_cascade_unavailable");
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
}
