//! Owner projects (`/me/projects`), memberships, optional tags and the public
//! project projection. SQLite stands in for PostgreSQL and the in-memory document
//! store for CouchDB.
use annotations_api::{
    dal::Store,
    domain::{App, projects, publication, schemas},
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

async fn count(store: &Store, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

async fn project(router: &Router, owner: &str, body: Value) -> Reply {
    call(
        router,
        "POST",
        "/me/projects",
        Some(owner),
        None,
        Some(body),
    )
    .await
}
async fn project_id(router: &Router, owner: &str, name: &str) -> String {
    let r = project(router, owner, json!({"name": name})).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    r.body["project_id"].as_str().unwrap().to_string()
}

/// A note of `owner` with one Original; returns (note id, revision id).
async fn note(router: &Router, owner: &str, client: &str, metadata: Value) -> (String, String) {
    let mut body = json!({
        "client_note_id": client,
        "representations": {"original": {"document": {
            "page": {"width": 1000, "background": {"kind": "blank"}},
            "items": [{"id": "t1", "kind": "text", "x": 0, "y": 40, "width": 304, "height": 120,
                       "spans": [{"text": "Cells"}]}]
        }}}
    });
    if !metadata.is_null() {
        body["metadata"] = metadata;
    }
    let r = call(router, "POST", "/me/notes", Some(owner), None, Some(body)).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    (
        r.body["note_id"].as_str().unwrap().to_string(),
        r.body["revisions"][0]["revision_id"]
            .as_str()
            .unwrap()
            .to_string(),
    )
}

async fn publish(router: &Router, owner: &str, note_id: &str, revision_id: &str) {
    let r = call(
        router,
        "PUT",
        &format!("/me/notes/{note_id}/publication"),
        Some(owner),
        Some("0"),
        Some(json!({"language": "en", "representations": {"original": revision_id}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
}

async fn member(router: &Router, owner: &str, method: &str, project: &str, note: &str) -> Reply {
    call(
        router,
        method,
        &format!("/me/projects/{project}/notes/{note}"),
        Some(owner),
        None,
        None,
    )
    .await
}

#[tokio::test]
async fn projects_are_private_by_default_and_names_are_unique_per_owner() {
    let (store, router) = setup().await;
    let r = project(&router, A, json!({"name": "  Biology "})).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    assert_eq!(r.headers["etag"], "\"1\"");
    assert_eq!(r.body["name"], "Biology");
    assert_eq!(r.body["visibility"], "private");
    assert_eq!(r.body["revision"], 1);
    assert_eq!(r.body["note_count"], 0);
    let biology = r.body["project_id"].as_str().unwrap().to_string();
    assert_ne!(biology, "Biology");

    // Same name after trim and case fold: refused for the same owner.
    for name in ["biology", " BIOLOGY  ", "Biology"] {
        let r = project(&router, A, json!({"name": name})).await;
        assert_eq!(r.status, StatusCode::CONFLICT, "{name}");
        assert_eq!(r.body["code"], "project_name_taken");
    }
    // Full case folding: "Straße" and "STRASSE" are the same name.
    project_id(&router, A, "Straße").await;
    let r = project(&router, A, json!({"name": "STRASSE"})).await;
    assert_eq!(r.body["code"], "project_name_taken");
    // Another owner may use the same name and gets another id.
    let other = project_id(&router, B, "biology").await;
    assert_ne!(other, biology);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM projects").await, 3);

    // Rename onto another project's name is refused; a case-only rename works.
    let math = project_id(&router, A, "Math").await;
    let uri = format!("/me/projects/{math}");
    let r = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        Some("1"),
        Some(json!({"name": "BIOLOGY"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "project_name_taken");
    let r = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        Some("\"1\""),
        Some(json!({"name": " MATH "})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.body["name"], "MATH");
    assert_eq!(r.body["revision"], 2);
    assert_eq!(r.headers["etag"], "\"2\"");

    // Explicitly public at create.
    let r = project(
        &router,
        A,
        json!({"name": "Shared", "visibility": "public"}),
    )
    .await;
    assert_eq!(r.body["visibility"], "public");

    // Request checks.
    for (body, code) in [
        (json!({"name": "   "}), "empty_name"),
        (json!({}), "missing_field"),
        (json!({"name": 3}), "invalid_type"),
        (
            json!({"name": "X", "visibility": "friends"}),
            "invalid_visibility",
        ),
        (json!({"name": "X", "owner_id": B}), "unknown_field"),
        (json!({"name": "x".repeat(101)}), "text_too_long"),
        (json!({"name": "a\u{0007}b"}), "invalid_text"),
    ] {
        let r = project(&router, A, body).await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(r.body["code"], code);
    }
    let r = call(
        &router,
        "POST",
        "/me/projects",
        None,
        None,
        Some(json!({"name": "X"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert_eq!(r.body["code"], "identity_required");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM projects").await, 5);
}

#[tokio::test]
async fn every_project_route_checks_ownership_and_revisions() {
    let (store, router) = setup().await;
    let p = project_id(&router, A, "Biology").await;
    let uri = format!("/me/projects/{p}");
    let (note_a, _) = note(&router, A, "a1", Value::Null).await;
    let (note_b, _) = note(&router, B, "b1", Value::Null).await;

    // Another account sees nothing and changes nothing.
    assert_eq!(
        call(&router, "GET", &uri, Some(B), None, None).await.status,
        StatusCode::NOT_FOUND
    );
    let r = call(
        &router,
        "PATCH",
        &uri,
        Some(B),
        Some("1"),
        Some(json!({"name": "Mine"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.body["code"], "not_found");
    assert_eq!(
        call(&router, "DELETE", &uri, Some(B), Some("1"), None)
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        member(&router, B, "PUT", &p, &note_a).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        member(&router, B, "PUT", &p, &note_b).await.status,
        StatusCode::NOT_FOUND
    );
    // Membership needs ownership of both: A cannot add B's note.
    assert_eq!(
        member(&router, A, "PUT", &p, &note_b).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        member(&router, A, "PUT", &p, "missing").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        member(&router, B, "DELETE", &p, &note_a).await.status,
        StatusCode::NOT_FOUND
    );
    let list_b = call(&router, "GET", "/me/projects", Some(B), None, None).await;
    assert_eq!(list_b.body["items"], json!([]));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM project_notes").await, 0);

    // If-Match is required and must be current.
    let r = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        None,
        Some(json!({"name": "X"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::PRECONDITION_REQUIRED);
    assert_eq!(r.body["code"], "if_match_required");
    let r = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        Some("x"),
        Some(json!({"name": "X"})),
    )
    .await;
    assert_eq!(r.body["code"], "invalid_if_match");
    let r = call(&router, "PATCH", &uri, Some(A), Some("1"), Some(json!({}))).await;
    assert_eq!(r.body["code"], "empty_change");
    let r = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        Some("1"),
        Some(json!({"visibility": "public"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let r = call(
        &router,
        "PATCH",
        &uri,
        Some(A),
        Some("1"),
        Some(json!({"name": "Old"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["code"], "revision_conflict");
    assert_eq!(r.body["current_revision"], 2);
    let r = call(&router, "DELETE", &uri, Some(A), Some("1"), None).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.body["current_revision"], 2);
    assert_eq!(
        call(&router, "DELETE", &uri, Some(A), None, None)
            .await
            .status,
        StatusCode::PRECONDITION_REQUIRED
    );

    let r = call(&router, "GET", &uri, Some(A), None, None).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.headers["etag"], "\"2\"");
    assert_eq!(r.body["visibility"], "public");
    assert!(r.body.get("owner_id").is_none());
}

#[tokio::test]
async fn memberships_are_idempotent_and_never_delete_notes() {
    let (store, router) = setup().await;
    let p1 = project_id(&router, A, "Biology").await;
    let p2 = project_id(&router, A, "Exams").await;
    let (n1, _) = note(&router, A, "n1", Value::Null).await;
    let (n2, _) = note(&router, A, "n2", Value::Null).await;
    let feed = "SELECT COUNT(*) FROM note_changes";
    let start = count(&store, feed).await;

    // Adding twice keeps one membership and one feed row.
    for _ in 0..2 {
        let r = member(&router, A, "PUT", &p1, &n1).await;
        assert_eq!(r.status, StatusCode::NO_CONTENT, "{}", r.body);
    }
    let r = call(
        &router,
        "PUT",
        &format!("/me/projects/{p1}/notes/{n1}"),
        Some(A),
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    let r = call(
        &router,
        "PUT",
        &format!("/me/projects/{p1}/notes/{n1}"),
        Some(A),
        None,
        Some(json!({"owner_id": A})),
    )
    .await;
    assert_eq!(r.body["code"], "unknown_field");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM project_notes").await, 1);
    assert_eq!(count(&store, feed).await, start + 1);

    // One note in many projects, one project with many notes.
    member(&router, A, "PUT", &p2, &n1).await;
    member(&router, A, "PUT", &p1, &n2).await;
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{n1}"),
        Some(A),
        None,
        None,
    )
    .await;
    let mut ids = vec![p1.clone(), p2.clone()];
    ids.sort();
    assert_eq!(detail.body["project_ids"], json!(ids));
    let p1_detail = call(
        &router,
        "GET",
        &format!("/me/projects/{p1}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(p1_detail.body["note_count"], 2);
    let listed = call(
        &router,
        "GET",
        &format!("/me/notes?project_id={p1}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(listed.body["items"].as_array().unwrap().len(), 2);

    // Removing a membership keeps the note; removing it again changes nothing.
    for _ in 0..2 {
        let r = member(&router, A, "DELETE", &p1, &n2).await;
        assert_eq!(r.status, StatusCode::NO_CONTENT);
    }
    let r = call(
        &router,
        "GET",
        &format!("/me/notes/{n2}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["project_ids"], json!([]));

    // Deleting a project removes its memberships only; notes and the other project stay.
    let before = count(&store, feed).await;
    let r = call(
        &router,
        "DELETE",
        &format!("/me/projects/{p1}"),
        Some(A),
        Some("1"),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT, "{}", r.body);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 2);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM projects").await, 1);
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{n1}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(detail.body["project_ids"], json!([p2]));
    assert_eq!(count(&store, feed).await, before + 1);
    assert_eq!(
        call(
            &router,
            "GET",
            &format!("/me/projects/{p1}"),
            Some(A),
            None,
            None
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );

    // Deleting a note removes its memberships and keeps the project.
    let revision = detail.body["revision"].as_i64().unwrap().to_string();
    let r = call(
        &router,
        "DELETE",
        &format!("/me/notes/{n1}"),
        Some(A),
        Some(&revision),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT, "{}", r.body);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM project_notes").await, 0);
    let r = call(
        &router,
        "GET",
        &format!("/me/projects/{p2}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(r.body["note_count"], 0);
}

#[tokio::test]
async fn project_list_pages_newest_change_first() {
    let (_store, router) = setup().await;
    for name in ["One", "Two", "Three"] {
        project_id(&router, A, name).await;
    }
    project_id(&router, B, "Hidden").await;
    let first = call(&router, "GET", "/me/projects?limit=2", Some(A), None, None).await;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(first.body["items"].as_array().unwrap().len(), 2);
    let cursor = first.body["next_cursor"].as_str().unwrap().to_string();
    let second = call(
        &router,
        "GET",
        &format!("/me/projects?limit=2&cursor={cursor}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(second.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(second.body["next_cursor"], Value::Null);
    let mut names: Vec<String> = first.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second.body["items"].as_array().unwrap())
        .map(|p| p["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["One", "Three", "Two"]);
    let bad = call(&router, "GET", "/me/projects?cursor=x", Some(A), None, None).await;
    assert_eq!(bad.body["code"], "invalid_cursor");
    let bad = call(&router, "GET", "/me/projects?limit=0", Some(A), None, None).await;
    assert_eq!(bad.body["code"], "invalid_limit");
}

#[tokio::test]
async fn public_project_counts_only_public_notes_and_public_notes_hide_private_projects() {
    let (store, router) = setup().await;
    let open = project_id(&router, A, "Open course").await;
    let secret = project_id(&router, A, "Secret plans").await;
    let (public_note, r1) = note(&router, A, "n1", json!({"title": "Public cells"})).await;
    let (private_note, _) = note(&router, A, "n2", json!({"title": "Private diary"})).await;
    let (withdrawn, r3) = note(&router, A, "n3", Value::Null).await;
    publish(&router, A, &public_note, &r1).await;
    publish(&router, A, &withdrawn, &r3).await;
    let r = call(
        &router,
        "DELETE",
        &format!("/me/notes/{withdrawn}/publication"),
        Some(A),
        Some("1"),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    for n in [&public_note, &private_note, &withdrawn] {
        member(&router, A, "PUT", &open, n).await;
    }
    member(&router, A, "PUT", &secret, &public_note).await;

    // A private project is not public at all.
    assert_eq!(
        projects::public_project(&store, &open)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    assert!(
        projects::public_note_ids(&store, &open, None, 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Making the project public never publishes its notes.
    let r = call(
        &router,
        "PATCH",
        &format!("/me/projects/{open}"),
        Some(A),
        Some("1"),
        Some(json!({"visibility": "public"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.body["note_count"], 3);
    assert!(
        publication::public_note(&store, &private_note, None)
            .await
            .is_err()
    );

    let public = projects::public_project(&store, &open).await.unwrap();
    assert_eq!(public["note_count"], 1);
    assert_eq!(public["name"], "Open course");
    let author: String = sqlx::query("SELECT author_id FROM authors WHERE owner_id=$1")
        .bind(A)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get("author_id");
    assert_eq!(public["author_id"], author);
    let text = public.to_string();
    for hidden in [
        A,
        "Private diary",
        private_note.as_str(),
        withdrawn.as_str(),
        "revision",
    ] {
        assert!(!text.contains(hidden), "public project shows {hidden}");
    }
    assert_eq!(
        projects::public_note_ids(&store, &open, None, 10)
            .await
            .unwrap(),
        vec![public_note.clone()]
    );
    assert!(
        projects::public_note_ids(&store, &open, Some(&public_note), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        projects::public_project(&store, &secret)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );

    // The public note shows no project, private or public.
    let note = publication::public_note(&store, &public_note, None)
        .await
        .unwrap();
    let text = note.to_string();
    for hidden in [
        secret.as_str(),
        "Secret plans",
        open.as_str(),
        "Open course",
        A,
    ] {
        assert!(!text.contains(hidden), "public note shows {hidden}");
    }
    assert!(note.get("project_ids").is_none());

    // Unpublish removes the note from the public count at once.
    let r = call(
        &router,
        "DELETE",
        &format!("/me/notes/{public_note}/publication"),
        Some(A),
        Some("1"),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(
        projects::public_project(&store, &open).await.unwrap()["note_count"],
        0
    );
}

#[tokio::test]
async fn tags_are_optional_manual_and_never_automatic() {
    let (store, router) = setup().await;
    // No tags: the note saves, publishes and is found by its title.
    let (plain, rev) = note(&router, A, "plain", json!({"title": "Mitosis summary"})).await;
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{plain}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(detail.body["tags"], json!([]));
    publish(&router, A, &plain, &rev).await;
    let public = publication::public_note(&store, &plain, None)
        .await
        .unwrap();
    assert_eq!(public["title"], "Mitosis summary");
    let found = call(&router, "GET", "/me/notes?q=mitosis", Some(A), None, None).await;
    assert_eq!(found.body["items"][0]["note_id"], plain);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM tags").await, 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM note_tags").await, 0);

    // Many manual tags through note PATCH metadata; duplicates after case fold collapse.
    let r = call(
        &router,
        "PATCH",
        &format!("/me/notes/{plain}"),
        Some(A),
        Some("1"),
        Some(json!({"metadata": {"tags": [" Biology ", "exam", "EXAM"]}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{plain}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(detail.body["tags"], json!(["Biology", "exam"]));
    let tagged = call(&router, "GET", "/me/notes?tag=EXAM", Some(A), None, None).await;
    assert_eq!(tagged.body["items"].as_array().unwrap().len(), 1);

    // An empty list removes every tag; nothing is added in its place.
    let r = call(
        &router,
        "PATCH",
        &format!("/me/notes/{plain}"),
        Some(A),
        Some("2"),
        Some(json!({"metadata": {"tags": []}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let detail = call(
        &router,
        "GET",
        &format!("/me/notes/{plain}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(detail.body["tags"], json!([]));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM note_tags").await, 0);
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM tags WHERE normalized_name='general'"
        )
        .await,
        0
    );
}
