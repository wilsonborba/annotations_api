//! Administrative note, project and file routes (`/admin/notes`, `/admin/projects`,
//! `/admin/files`) and the access check of every `/admin` route.
//! SQLite stands in for PostgreSQL, the in-memory document store for CouchDB and
//! the FSM double for FSM.
use annotations_api::{
    dal::Store,
    domain::{App, schemas},
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::path::Path;
use tower::ServiceExt;

const A: &str = "user-a";
const B: &str = "user-b";
/// The gateway header of an administrator (central access level 1).
const ADMIN: (&str, &str) = ("x-access-level", "1");

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
    // Both accounts exist: they used the owner API before.
    for owner in [A, B] {
        sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'')")
            .bind(owner)
            .execute(&store.db)
            .await
            .unwrap();
    }
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
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> Reply {
    let mut request = Request::builder().method(method).uri(uri);
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

/// An administrator call with a JSON body.
async fn admin(
    router: &Router,
    method: &str,
    uri: &str,
    extra: &[(&str, &str)],
    body: Value,
) -> Reply {
    let mut headers = vec![ADMIN, ("content-type", "application/json")];
    headers.extend_from_slice(extra);
    let body = if body.is_null() {
        vec![]
    } else {
        body.to_string().into_bytes()
    };
    call(router, method, uri, &headers, body).await
}

/// An owner call with a JSON body.
async fn owner(
    router: &Router,
    owner: &str,
    method: &str,
    uri: &str,
    extra: &[(&str, &str)],
    body: Value,
) -> Reply {
    let mut headers = vec![("x-uuid", owner), ("content-type", "application/json")];
    headers.extend_from_slice(extra);
    let body = if body.is_null() {
        vec![]
    } else {
        body.to_string().into_bytes()
    };
    call(router, method, uri, &headers, body).await
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn png(fill: u8, len: usize) -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
    bytes.extend((0..len).map(|i| fill.wrapping_add(i as u8)));
    bytes
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

/// Reserve and upload a file of `who` through the owner routes.
async fn owner_file(router: &Router, who: &str, bytes: &[u8]) -> String {
    let r = owner(
        router,
        who,
        "POST",
        "/me/files",
        &[],
        json!({"filename": "leaf.png", "content_type": "image/png", "size": bytes.len(),
               "checksum": sha(bytes), "purpose": "attachment"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    let r = call(
        router,
        "PUT",
        &format!("/me/files/{id}/content"),
        &[("x-uuid", who), ("content-type", "image/png")],
        bytes.to_vec(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    id
}

async fn count(store: &Store, sql: &str) -> i64 {
    sqlx::query(sql).fetch_one(&store.db).await.unwrap().get(0)
}

// Checklist: a normal session gets 403 on every admin route.

#[tokio::test]
async fn normal_session_gets_403_on_every_admin_route() {
    let (store, router) = setup().await;
    // Real objects, so a missing check could not hide behind a 404.
    let file = owner_file(&router, A, &png(1, 64)).await;
    let r = owner(
        &router,
        A,
        "POST",
        "/me/notes",
        &[],
        json!({"client_note_id": "n1", "representations": {"original": {"document": plain()}}}),
    )
    .await;
    let note = r.json()["note_id"].as_str().unwrap().to_string();
    let r = owner(
        &router,
        A,
        "POST",
        "/me/projects",
        &[],
        json!({"name": "Cells"}),
    )
    .await;
    let project = r.json()["project_id"].as_str().unwrap().to_string();
    let routes: Vec<(&str, String)> = vec![
        ("GET", "/admin/notes".into()),
        ("POST", "/admin/notes".into()),
        ("GET", format!("/admin/notes/{note}")),
        ("PUT", format!("/admin/notes/{note}")),
        ("PATCH", format!("/admin/notes/{note}")),
        ("DELETE", format!("/admin/notes/{note}")),
        ("GET", format!("/admin/notes/{note}/publication")),
        ("PUT", format!("/admin/notes/{note}/publication")),
        ("DELETE", format!("/admin/notes/{note}/publication")),
        ("GET", "/admin/projects".into()),
        ("POST", "/admin/projects".into()),
        ("GET", format!("/admin/projects/{project}")),
        ("PATCH", format!("/admin/projects/{project}")),
        ("DELETE", format!("/admin/projects/{project}")),
        ("POST", "/admin/files".into()),
        ("GET", format!("/admin/files/{file}")),
        ("DELETE", format!("/admin/files/{file}")),
        ("GET", format!("/admin/files/{file}/content")),
        ("PUT", format!("/admin/files/{file}/content")),
        // The existing admin routes keep working and get the same check.
        ("PUT", "/admin/catalog".into()),
        ("GET", "/admin/schemas".into()),
        ("POST", "/admin/schemas".into()),
        (
            "POST",
            "/admin/schemas/note-typed/versions/1/publish".into(),
        ),
        ("GET", "/admin/posts/p1".into()),
        ("PUT", "/admin/posts/p1".into()),
        ("POST", "/admin/posts/p1/publish".into()),
        ("POST", "/admin/images?owner_id=user-a".into()),
        ("GET", "/admin/images/k1".into()),
    ];
    let callers: [(&str, Vec<(&str, &str)>); 4] = [
        (
            "normal session",
            vec![("x-uuid", A), ("x-access-level", "3")],
        ),
        (
            "level 2 session",
            vec![("x-uuid", A), ("x-access-level", "2")],
        ),
        ("identity without a level", vec![("x-uuid", A)]),
        ("no identity", vec![]),
    ];
    for (method, uri) in &routes {
        for (caller, headers) in &callers {
            let mut headers = headers.clone();
            headers.push(("content-type", "application/json"));
            headers.push(("if-match", "1"));
            let r = call(&router, method, uri, &headers, b"{}".to_vec()).await;
            assert_eq!(r.status, StatusCode::FORBIDDEN, "{caller}: {method} {uri}");
            assert_eq!(
                r.json()["code"],
                "admin_required",
                "{caller}: {method} {uri}"
            );
        }
        // The administrator level passes the check (the route answers on its own).
        let r = call(
            &router,
            method,
            uri,
            &[ADMIN, ("content-type", "application/json")],
            vec![],
        )
        .await;
        assert_ne!(r.status, StatusCode::FORBIDDEN, "admin: {method} {uri}");
    }
    // The refused calls changed nothing (the administrator calls without If-Match
    // were refused by the owner rules too).
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM projects").await, 1);
}

// Checklist: admin create for an unknown owner fails.

#[tokio::test]
async fn admin_create_for_an_unknown_owner_fails() {
    let (store, router) = setup().await;
    let bytes = png(2, 64);
    let file = json!({"filename": "a.png", "content_type": "image/png", "size": bytes.len(),
                      "checksum": sha(&bytes), "purpose": "attachment"});
    let note =
        json!({"client_note_id": "n1", "representations": {"original": {"document": plain()}}});
    let project = json!({"name": "Cells"});
    for (uri, body) in [
        ("/admin/notes", &note),
        ("/admin/projects", &project),
        ("/admin/files", &file),
    ] {
        let mut unknown = body.clone();
        unknown["owner_id"] = json!("nobody");
        let r = admin(&router, "POST", uri, &[], unknown).await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}");
        assert_eq!(r.json()["code"], "unknown_owner", "{uri}");
        // The body must name the owner.
        let r = admin(&router, "POST", uri, &[], body.clone()).await;
        assert_eq!(r.json()["code"], "missing_field", "{uri}");
        let mut wrong = body.clone();
        wrong["owner_id"] = json!(7);
        let r = admin(&router, "POST", uri, &[], wrong).await;
        assert_eq!(r.json()["code"], "invalid_type", "{uri}");
        let mut bad = body.clone();
        bad["owner_id"] = json!("two words");
        let r = admin(&router, "POST", uri, &[], bad).await;
        assert_eq!(r.json()["code"], "invalid_owner_id", "{uri}");
    }
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM projects").await, 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM files").await, 0);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM owners").await,
        2,
        "no owner was made"
    );
    // An existing owner works, and the new object belongs to that owner.
    for (uri, body) in [
        ("/admin/notes", &note),
        ("/admin/projects", &project),
        ("/admin/files", &file),
    ] {
        let mut known = body.clone();
        known["owner_id"] = json!(B);
        let r = admin(&router, "POST", uri, &[], known).await;
        assert_eq!(r.status, StatusCode::CREATED, "{uri}: {:?}", r.json());
        assert_eq!(r.json()["owner_id"], B, "{uri}");
    }
    let r = owner(&router, B, "GET", "/me/notes", &[], Value::Null).await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 1);
    let r = owner(&router, B, "GET", "/me/projects", &[], Value::Null).await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 1);
    // Owner routes still refuse a body that names an owner.
    let r = owner(
        &router,
        A,
        "POST",
        "/me/projects",
        &[],
        json!({"name": "Other", "owner_id": B}),
    )
    .await;
    assert_eq!(r.json()["code"], "unknown_field");
}

// Checklist: admin delete keeps shared files that other references need.

#[tokio::test]
async fn admin_delete_keeps_shared_files_that_other_references_need() {
    let (store, router) = setup().await;
    let bytes = png(5, 120);
    let a = owner_file(&router, A, &bytes).await;
    let b = owner_file(&router, B, &bytes).await;
    assert_eq!(fsm(&store), (2, 1), "two FSM items, one blob");
    // A published note of A shows A's file.
    let r = owner(
        &router,
        A,
        "POST",
        "/me/notes",
        &[],
        json!({"client_note_id": "n1", "main_representation": "original",
               "representations": {"original": {"document": with_image(&a)},
                                   "typed": {"document": typed("Cells")}}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    let note = r.json()["note_id"].as_str().unwrap().to_string();
    let original = r.json()["revisions"][0]["revision_id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = admin(
        &router,
        "PUT",
        &format!("/admin/notes/{note}/publication"),
        &[("if-match", "0")],
        json!({"language": "en", "representations": {"original": original}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    // The note and its publication need the file: admin release is refused.
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/files/{a}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["code"], "file_in_use");
    assert_eq!(fsm(&store), (2, 1));
    // Admin note delete: stale If-Match first, then the #22 delete order.
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/notes/{note}"),
        &[("if-match", "9")],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["current_revision"], 1);
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/notes/{note}"),
        &[("if-match", "1")],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT, "{:?}", r.json());
    for table in [
        "notes",
        "note_revisions",
        "note_current_revisions",
        "note_publications",
        "note_published_representations",
    ] {
        assert_eq!(
            count(&store, &format!("SELECT COUNT(*) FROM {table}")).await,
            0,
            "{table}"
        );
    }
    let r = call(
        &router,
        "GET",
        &format!("/media/{a}?note_id={note}"),
        &[],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "no longer public");
    // The note released only its own references; both files and the blob stay.
    assert_eq!(fsm(&store), (2, 1));
    let r = owner(
        &router,
        A,
        "GET",
        &format!("/me/files/{a}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["status"], "ready");
    // The sync feed of A tells other devices about the delete.
    let r = owner(
        &router,
        A,
        "GET",
        "/me/notes?sync_cursor=0",
        &[],
        Value::Null,
    )
    .await;
    let changes = r.json()["changes"].as_array().unwrap().clone();
    assert_eq!(changes.last().unwrap()["change"], "delete");
    // Admin file delete releases A's logical reference; B still needs the blob.
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/files/{a}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(fsm(&store), (1, 1), "B's item keeps the shared blob");
    assert_eq!(store.files.fixture_refcount(&sha(&bytes)), Some(1));
    let r = owner(
        &router,
        B,
        "GET",
        &format!("/me/files/{b}/content"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.bytes, bytes);
    let r = admin(
        &router,
        "GET",
        &format!("/admin/files/{a}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // The last reference goes: FSM has no item and no blob.
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/files/{b}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(fsm(&store), (0, 0));
}

#[tokio::test]
async fn admin_note_routes_use_the_owner_pipeline() {
    let (store, router) = setup().await;
    let r = admin(
        &router,
        "POST",
        "/admin/notes",
        &[],
        json!({"owner_id": A, "client_note_id": "n1",
               "representations": {"original": {"document": plain()}},
               "metadata": {"title": "Cells", "tags": ["Biology"]}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    assert_eq!(r.headers["etag"], "\"1\"");
    let note = r.json()["note_id"].as_str().unwrap().to_string();
    // The same create again is idempotent for that owner.
    let r = admin(
        &router,
        "POST",
        "/admin/notes",
        &[],
        json!({"owner_id": A, "client_note_id": "n1",
               "representations": {"original": {"document": plain()}}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["note_id"], note.as_str());
    // The owner sees the note; another owner does not.
    let r = owner(
        &router,
        A,
        "GET",
        &format!("/me/notes/{note}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["title"], "Cells");
    let r = owner(
        &router,
        B,
        "GET",
        &format!("/me/notes/{note}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // Detail with the owner and the view.
    let r = admin(
        &router,
        "GET",
        &format!("/admin/notes/{note}?view=original"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["owner_id"], A);
    assert_eq!(r.json()["representations"][0]["representation"], "original");
    let r = admin(
        &router,
        "GET",
        &format!("/admin/notes/{note}?view=x"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["code"], "invalid_view");
    let r = admin(&router, "GET", "/admin/notes/missing", &[], Value::Null).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // PUT: the same validation and If-Match rules as the owner.
    let bad = json!({"representation": "typed", "document": {"blocks": [{"id": "k1", "kind": "unknown"}]}});
    let owner_bad = owner(
        &router,
        A,
        "PUT",
        &format!("/me/notes/{note}"),
        &[("if-match", "1")],
        bad.clone(),
    )
    .await;
    let admin_bad = admin(
        &router,
        "PUT",
        &format!("/admin/notes/{note}"),
        &[("if-match", "1")],
        bad,
    )
    .await;
    assert_eq!(admin_bad.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(admin_bad.json()["code"], owner_bad.json()["code"]);
    let r = admin(
        &router,
        "PUT",
        &format!("/admin/notes/{note}"),
        &[],
        json!({}),
    )
    .await;
    assert_eq!(r.status, StatusCode::PRECONDITION_REQUIRED);
    let replace = json!({"representation": "typed", "document": typed("Cells")});
    let r = admin(
        &router,
        "PUT",
        &format!("/admin/notes/{note}"),
        &[("if-match", "1")],
        replace.clone(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    assert_eq!(r.json()["revision"], 2);
    let typed_revision = r.json()["revisions"][0]["revision_id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = admin(
        &router,
        "PUT",
        &format!("/admin/notes/{note}"),
        &[("if-match", "1")],
        replace,
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.json()["current_revision"], 2);
    // PATCH metadata through the pipeline; owner_id is not a field there.
    let r = admin(
        &router,
        "PATCH",
        &format!("/admin/notes/{note}"),
        &[("if-match", "2")],
        json!({"owner_id": B}),
    )
    .await;
    assert_eq!(r.json()["code"], "unknown_field");
    let r = admin(
        &router,
        "PATCH",
        &format!("/admin/notes/{note}"),
        &[("if-match", "2")],
        json!({"metadata": {"title": "Tissues"}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    assert_eq!(r.json()["revision"], 3);
    let r = owner(
        &router,
        A,
        "GET",
        &format!("/me/notes/{note}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["title"], "Tissues");
    // Publication: read, publish, unpublish with the publication revision.
    let r = admin(
        &router,
        "GET",
        &format!("/admin/notes/{note}/publication"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.headers["etag"], "\"0\"");
    let r = admin(
        &router,
        "PUT",
        &format!("/admin/notes/{note}/publication"),
        &[("if-match", "0")],
        json!({"language": "en", "representations": {"typed": typed_revision}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    let r = call(&router, "GET", &format!("/notes/{note}"), &[], vec![]).await;
    assert_eq!(r.status, StatusCode::OK, "public after admin publish");
    // List across owners, by owner and by status.
    let r = owner(
        &router,
        B,
        "POST",
        "/me/notes",
        &[],
        json!({"client_note_id": "b1", "representations": {"original": {"document": plain()}}}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED);
    let r = admin(&router, "GET", "/admin/notes", &[], Value::Null).await;
    let items = r.json()["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 2);
    assert!(items.iter().any(|i| i["owner_id"] == A) && items.iter().any(|i| i["owner_id"] == B));
    let r = admin(
        &router,
        "GET",
        &format!("/admin/notes?owner_id={A}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 1);
    let r = admin(
        &router,
        "GET",
        "/admin/notes?status=published",
        &[],
        Value::Null,
    )
    .await;
    let items = r.json()["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["note_id"], note.as_str());
    let r = admin(
        &router,
        "GET",
        "/admin/notes?status=unpublished",
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["items"][0]["owner_id"], B);
    let r = admin(&router, "GET", "/admin/notes?limit=1", &[], Value::Null).await;
    let cursor = r.json()["next_cursor"].as_str().unwrap().to_string();
    let r = admin(
        &router,
        "GET",
        &format!("/admin/notes?limit=1&cursor={cursor}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 1);
    assert!(r.json()["next_cursor"].is_null());
    for (query, code) in [
        ("status=draft", "invalid_status"),
        ("cursor=x", "invalid_cursor"),
        ("limit=0", "invalid_limit"),
        ("owner_id=a%20b", "invalid_owner_id"),
    ] {
        let r = admin(
            &router,
            "GET",
            &format!("/admin/notes?{query}"),
            &[],
            Value::Null,
        )
        .await;
        assert_eq!(r.json()["code"], code, "{query}");
    }
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/notes/{note}/publication"),
        &[("if-match", "1")],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    let r = call(&router, "GET", &format!("/notes/{note}"), &[], vec![]).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // Delete keeps the owner rules: the Reports cascade is still refused.
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/notes/{note}?delete_owned_reports=true"),
        &[("if-match", "3")],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["code"], "reports_cascade_unavailable");
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/notes/{note}"),
        &[("if-match", "3")],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 1);
}

#[tokio::test]
async fn admin_project_routes_use_the_owner_rules() {
    let (store, router) = setup().await;
    let r = admin(
        &router,
        "POST",
        "/admin/projects",
        &[],
        json!({"owner_id": A, "name": "Cells"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    assert_eq!(r.json()["visibility"], "private");
    let project = r.json()["project_id"].as_str().unwrap().to_string();
    // The same per-owner name rule; another owner may use the name.
    let r = admin(
        &router,
        "POST",
        "/admin/projects",
        &[],
        json!({"owner_id": A, "name": " cells "}),
    )
    .await;
    assert_eq!(r.json()["code"], "project_name_taken");
    let r = admin(
        &router,
        "POST",
        "/admin/projects",
        &[],
        json!({"owner_id": B, "name": "Cells"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED);
    // A member note of A.
    let r = owner(&router, A, "POST", "/me/notes", &[], json!({"client_note_id": "n1", "representations": {"original": {"document": plain()}}, "project_ids": [project]})).await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    // List across owners and by owner.
    let r = admin(&router, "GET", "/admin/projects", &[], Value::Null).await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 2);
    let r = admin(
        &router,
        "GET",
        &format!("/admin/projects?owner_id={A}&limit=1"),
        &[],
        Value::Null,
    )
    .await;
    let items = r.json()["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["owner_id"], A);
    assert_eq!(items[0]["note_count"], 1);
    assert!(r.json()["next_cursor"].is_null());
    let r = admin(&router, "GET", "/admin/projects?limit=1", &[], Value::Null).await;
    let cursor = r.json()["next_cursor"].as_str().unwrap().to_string();
    let r = admin(
        &router,
        "GET",
        &format!("/admin/projects?cursor={cursor}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 1);
    let r = admin(&router, "GET", "/admin/projects?cursor=x", &[], Value::Null).await;
    assert_eq!(r.json()["code"], "invalid_cursor");
    // Detail, patch with If-Match, delete without touching the note.
    let r = admin(
        &router,
        "GET",
        &format!("/admin/projects/{project}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["owner_id"], A);
    assert_eq!(r.headers["etag"], "\"1\"");
    let r = admin(
        &router,
        "PATCH",
        &format!("/admin/projects/{project}"),
        &[],
        json!({"visibility": "public"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::PRECONDITION_REQUIRED);
    let r = admin(
        &router,
        "PATCH",
        &format!("/admin/projects/{project}"),
        &[("if-match", "1")],
        json!({"owner_id": B}),
    )
    .await;
    assert_eq!(r.json()["code"], "unknown_field");
    let r = admin(
        &router,
        "PATCH",
        &format!("/admin/projects/{project}"),
        &[("if-match", "1")],
        json!({"visibility": "public"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    assert_eq!(r.json()["revision"], 2);
    let r = owner(
        &router,
        A,
        "GET",
        &format!("/me/projects/{project}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["visibility"], "public");
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/projects/{project}"),
        &[("if-match", "1")],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/projects/{project}"),
        &[("if-match", "2")],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM notes").await,
        1,
        "the member note stays"
    );
    let r = admin(
        &router,
        "GET",
        &format!("/admin/projects/{project}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_file_routes_use_the_owner_pipeline() {
    let (store, router) = setup().await;
    let bytes = png(8, 300);
    let r = admin(
        &router,
        "POST",
        "/admin/files",
        &[],
        json!({"owner_id": A, "filename": "leaf.png", "content_type": "image/png",
               "size": bytes.len(), "checksum": sha(&bytes), "purpose": "attachment"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CREATED, "{:?}", r.json());
    assert_eq!(r.json()["status"], "reserved");
    assert!(r.json().get("fsm_reference").is_none());
    let id = r.json()["file_id"].as_str().unwrap().to_string();
    // The same reservation checks as the owner route.
    let r = admin(&router, "POST", "/admin/files", &[], json!({"owner_id": A, "filename": "x", "content_type": "image/png", "size": 1, "checksum": "x", "purpose": "attachment"})).await;
    assert_eq!(r.json()["code"], "invalid_checksum");
    // Upload: the same size, checksum and type checks; ready only after them.
    let r = call(
        &router,
        "PUT",
        &format!("/admin/files/{id}/content"),
        &[ADMIN, ("content-type", "image/png")],
        png(9, 300),
    )
    .await;
    assert_eq!(r.json()["code"], "checksum_mismatch");
    assert_eq!(fsm(&store), (0, 0));
    let r = call(
        &router,
        "PUT",
        &format!("/admin/files/{id}/content"),
        &[ADMIN, ("content-type", "image/png")],
        bytes.clone(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{:?}", r.json());
    assert_eq!(r.json()["status"], "ready");
    let r = call(
        &router,
        "PUT",
        &format!("/admin/files/{id}/content"),
        &[ADMIN, ("content-type", "image/png")],
        png(10, 300),
    )
    .await;
    assert_eq!(r.json()["code"], "file_immutable", "no in-place overwrite");
    // Metadata and bytes, with Range and the safe headers.
    let r = admin(
        &router,
        "GET",
        &format!("/admin/files/{id}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["owner_id"], A);
    assert_eq!(r.json()["checksum"], sha(&bytes));
    let r = call(
        &router,
        "GET",
        &format!("/admin/files/{id}/content"),
        &[ADMIN, ("range", "bytes=0-9")],
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(r.bytes, bytes[..10]);
    assert_eq!(r.headers["x-content-type-options"], "nosniff");
    assert_eq!(
        r.headers["content-range"],
        format!("bytes 0-9/{}", bytes.len())
    );
    // The file belongs to A.
    let r = owner(
        &router,
        A,
        "GET",
        &format!("/me/files/{id}/content"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.bytes, bytes);
    let r = owner(
        &router,
        B,
        "GET",
        &format!("/me/files/{id}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // A reserved file has no bytes yet.
    let r = admin(&router, "POST", "/admin/files", &[], json!({"owner_id": B, "filename": "b.png", "content_type": "image/png", "size": 5, "checksum": sha(b"12345"), "purpose": "attachment"})).await;
    let reserved = r.json()["file_id"].as_str().unwrap().to_string();
    let r = admin(
        &router,
        "GET",
        &format!("/admin/files/{reserved}/content"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.json()["code"], "file_not_ready");
    for uri in ["/admin/files/missing", "/admin/files/missing/content"] {
        let r = admin(&router, "GET", uri, &[], Value::Null).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{uri}");
    }
    let r = call(
        &router,
        "PUT",
        "/admin/files/missing/content",
        &[ADMIN, ("content-type", "image/png")],
        bytes.clone(),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = admin(
        &router,
        "DELETE",
        &format!("/admin/files/{id}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(fsm(&store), (0, 0));
}
