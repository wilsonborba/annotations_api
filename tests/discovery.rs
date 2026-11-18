//! Public discovery: `/notes` search and shuffled browsing, the public reader,
//! public authors and projects, and the `/catalog` review. SQLite stands in for
//! PostgreSQL and the in-memory document store for CouchDB.
use annotations_api::{
    dal::Store,
    domain::{
        App,
        rooms::{self, Create},
        schemas,
    },
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sqlx::Row;
use std::{collections::HashSet, path::Path};
use tower::ServiceExt;

const A: &str = "user-a";
const B: &str = "user-b";

async fn setup() -> (Store, Router) {
    let store = Store::fixture().await;
    for name in ["note-original", "note-typed", "instant-room"] {
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
         INSERT INTO languages(code,label) VALUES('th','Thai');",
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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    Reply { status, body }
}

/// A public GET without identity.
async fn get(router: &Router, uri: &str) -> Reply {
    call(router, "GET", uri, None, None, None).await
}

fn original(text: &str) -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [{"id": "t1", "kind": "text", "x": 0, "y": 40, "width": 304, "height": 120,
                   "spans": [{"text": text}]}]
    })
}
fn typed(text: &str) -> Value {
    json!({"blocks": [{"id": "k1", "kind": "paragraph", "spans": [{"text": text}]}]})
}

/// A note of `owner`; returns (note id, [(representation, revision id)]).
async fn note(
    router: &Router,
    owner: &str,
    client: &str,
    metadata: Value,
    representations: Value,
) -> (String, Vec<(String, String)>) {
    let mut body = json!({
        "client_note_id": client,
        "metadata": metadata,
        "representations": representations,
    });
    if representations.as_object().unwrap().len() == 2 {
        body["main_representation"] = json!("original");
    }
    let r = call(router, "POST", "/me/notes", Some(owner), None, Some(body)).await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.body);
    let revisions = r.body["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["representation"].as_str().unwrap().to_string(),
                v["revision_id"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    (r.body["note_id"].as_str().unwrap().to_string(), revisions)
}

/// A Typed-only note of `owner`; returns (note id, typed revision id).
async fn typed_note(
    router: &Router,
    owner: &str,
    client: &str,
    title: &str,
    text: &str,
) -> (String, String) {
    let (id, revs) = note(
        router,
        owner,
        client,
        json!({"title": title}),
        json!({"typed": {"document": typed(text)}}),
    )
    .await;
    (id, revs[0].1.clone())
}

async fn publish_as(
    router: &Router,
    owner: &str,
    note_id: &str,
    if_match: &str,
    language: &str,
    reps: Value,
) {
    let r = call(
        router,
        "PUT",
        &format!("/me/notes/{note_id}/publication"),
        Some(owner),
        Some(if_match),
        Some(json!({"language": language, "representations": reps})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
}

async fn publish_typed(router: &Router, owner: &str, note_id: &str, revision: &str) {
    publish_as(
        router,
        owner,
        note_id,
        "0",
        "en",
        json!({"typed": revision}),
    )
    .await;
}

async fn unpublish(router: &Router, owner: &str, note_id: &str, if_match: &str) {
    let r = call(
        router,
        "DELETE",
        &format!("/me/notes/{note_id}/publication"),
        Some(owner),
        Some(if_match),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT, "{}", r.body);
}

async fn count(store: &Store, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

async fn author_of(store: &Store, owner: &str) -> String {
    sqlx::query("SELECT author_id FROM authors WHERE owner_id=$1")
        .bind(owner)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get("author_id")
}

fn ids(reply: &Reply) -> Vec<String> {
    reply.body["items"]
        .as_array()
        .unwrap_or_else(|| panic!("{}", reply.body))
        .iter()
        .map(|i| i["note_id"].as_str().unwrap().to_string())
        .collect()
}

/// Every note id of a browse, page by page; panics on a duplicate.
async fn walk(router: &Router, query: &str, token: &str, limit: usize) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    let mut cursor: Option<String> = None;
    loop {
        let mut uri = format!("/notes?limit={limit}&browse_token={token}{query}");
        if let Some(c) = &cursor {
            uri.push_str(&format!("&cursor={c}"));
        }
        let r = get(router, &uri).await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.body);
        assert_eq!(r.body["browse_token"], token);
        for id in ids(&r) {
            assert!(!out.contains(&id), "duplicate {id}");
            out.push(id);
        }
        match r.body["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => return out,
        }
    }
}

/// Five authors with `counts[i]` published notes each; returns the note ids.
async fn five_authors(router: &Router, counts: [usize; 5]) -> Vec<String> {
    let mut out = vec![];
    for (a, n) in counts.iter().enumerate() {
        let owner = format!("author-{a}");
        for i in 0..*n {
            let (id, rev) = typed_note(
                router,
                &owner,
                &format!("c{i}"),
                &format!("Note {a} {i}"),
                "Shared lesson text",
            )
            .await;
            publish_typed(router, &owner, &id, &rev).await;
            out.push(id);
        }
    }
    out
}

#[tokio::test]
async fn same_browse_token_gives_same_order_and_cursor_gives_no_duplicates() {
    let (store, router) = setup().await;
    let all = five_authors(&router, [3, 3, 3, 3, 3]).await;

    let first = get(&router, "/notes?limit=4").await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    let token = first.body["browse_token"].as_str().unwrap().to_string();
    let again = get(&router, &format!("/notes?limit=4&browse_token={token}")).await;
    assert_eq!(ids(&first), ids(&again));
    assert_eq!(first.body["next_cursor"], again.body["next_cursor"]);

    // Pages of 4 cover every published note once, in the same order each time.
    let order = walk(&router, "", &token, 4).await;
    assert_eq!(order.len(), all.len());
    assert_eq!(
        order.iter().collect::<HashSet<_>>(),
        all.iter().collect::<HashSet<_>>()
    );
    assert_eq!(walk(&router, "", &token, 4).await, order);
    assert_eq!(walk(&router, "", &token, 15).await, order);
    assert_eq!(ids(&first), order[..4].to_vec());

    // Another seed gives another order of the same notes.
    let other = walk(&router, "", "4102444800.fedcba9876543210", 5).await;
    let other2 = walk(&router, "", "4102444800.0123456789abcdef", 5).await;
    assert_ne!(other, other2);
    assert_eq!(
        other.iter().collect::<HashSet<_>>(),
        all.iter().collect::<HashSet<_>>()
    );

    // A note published after the browse started joins the next browse, never the
    // running one, so a running browse shows no duplicate.
    let started: i64 = token.split('.').next().unwrap().parse().unwrap();
    let (late, rev) = typed_note(&router, "author-0", "late", "Late", "Late text").await;
    publish_typed(&router, "author-0", &late, &rev).await;
    sqlx::query("UPDATE note_publications SET updated_at=$1 WHERE note_id=$2")
        .bind(started + 5)
        .bind(&late)
        .execute(&store.db)
        .await
        .unwrap();
    assert_eq!(walk(&router, "", &token, 4).await, order);
    let fresh = get(
        &router,
        "/notes?limit=100&browse_token=4102444800.0123456789abcdef",
    )
    .await;
    assert!(ids(&fresh).contains(&late));

    // Errors.
    let r = get(&router, "/notes?cursor=0.x").await;
    assert_eq!(r.body["code"], "browse_token_required");
    let r = get(&router, &format!("/notes?cursor=zz&browse_token={token}")).await;
    assert_eq!(r.body["code"], "invalid_cursor");
    let r = get(&router, "/notes?browse_token=nope").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.body["code"], "invalid_browse_token");
    let r = get(&router, "/notes?limit=0").await;
    assert_eq!(r.body["code"], "invalid_limit");
    let r = get(&router, &format!("/notes?q={}", "a".repeat(201))).await;
    assert_eq!(r.body["code"], "query_too_long");
}

#[tokio::test]
async fn first_page_of_five_authors_shows_every_author_before_repeats() {
    let (store, router) = setup().await;
    five_authors(&router, [6, 3, 1, 2, 4]).await;
    let mut authors = vec![];
    for a in 0..5 {
        authors.push(author_of(&store, &format!("author-{a}")).await);
    }
    for seed in [
        "0123456789abcdef",
        "fedcba9876543210",
        "00000000000000000000000000000001",
        "abababababababab",
        "1234567812345678",
    ] {
        let r = get(
            &router,
            &format!("/notes?limit=10&browse_token=4102444800.{seed}"),
        )
        .await;
        let items = r.body["items"].as_array().unwrap();
        assert_eq!(items.len(), 10);
        let first: HashSet<&str> = items[..5]
            .iter()
            .map(|i| i["author_id"].as_str().unwrap())
            .collect();
        assert_eq!(first.len(), 5, "seed {seed}: {}", r.body);
        assert_eq!(
            first,
            authors.iter().map(String::as_str).collect::<HashSet<_>>()
        );
        // The second round has the four authors with more than one note.
        let second: HashSet<&str> = items[5..9]
            .iter()
            .map(|i| i["author_id"].as_str().unwrap())
            .collect();
        assert_eq!(second.len(), 4, "seed {seed}");
        assert!(!second.contains(authors[2].as_str()));
    }
}

#[tokio::test]
async fn unpublish_removes_the_note_from_search_at_once() {
    let (store, router) = setup().await;
    let (id, rev) = typed_note(&router, A, "n1", "Photosynthesis basics", "Light reactions").await;
    publish_typed(&router, A, &id, &rev).await;
    let token = "4102444800.0123456789abcdef";
    let query = format!("/notes?q=photosynthesis&browse_token={token}");
    assert_eq!(ids(&get(&router, &query).await), vec![id.clone()]);
    assert_eq!(
        get(&router, &format!("/notes/{id}")).await.status,
        StatusCode::OK
    );

    unpublish(&router, A, &id, "1").await;
    assert!(ids(&get(&router, &query).await).is_empty());
    assert!(ids(&get(&router, &format!("/notes?browse_token={token}")).await).is_empty());
    assert!(ids(&get(&router, "/notes?q=light").await).is_empty());
    let r = get(&router, &format!("/notes/{id}")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.body["code"], "not_found");
    for table in [
        "note_search",
        "note_publication_tags",
        "note_publication_titles",
    ] {
        assert_eq!(
            count(&store, &format!("SELECT COUNT(*) FROM {table}")).await,
            0,
            "{table}"
        );
    }

    // Republish rebuilds the index row.
    publish_as(&router, A, &id, "2", "en", json!({"typed": rev})).await;
    assert_eq!(ids(&get(&router, &query).await), vec![id.clone()]);
}

#[tokio::test]
async fn untagged_note_is_found_by_title_summary_typed_text_author_language_and_project() {
    let (store, router) = setup().await;
    let filler = "Leaves capture sunlight in green cells. ".repeat(8);
    let text = format!("{filler}Zygomorphic flowers appear near the end.");
    let (id, revs) = note(
        &router,
        A,
        "plain",
        json!({"title": "Chloroplast atlas"}),
        json!({"typed": {"document": typed(&text)}}),
    )
    .await;
    publish_as(&router, A, &id, "0", "th", json!({"typed": revs[0].1})).await;
    // Other notes: another author, another language, not in the project.
    let (other, rev) = typed_note(&router, B, "other", "Other note", "Leaves and roots").await;
    publish_typed(&router, B, &other, &rev).await;
    let (sibling, rev) = typed_note(&router, A, "sibling", "Sibling", "Leaves again").await;
    publish_typed(&router, A, &sibling, &rev).await;
    let project = call(
        &router,
        "POST",
        "/me/projects",
        Some(A),
        None,
        Some(json!({"name": "Plants", "visibility": "public"})),
    )
    .await
    .body["project_id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = call(
        &router,
        "PUT",
        &format!("/me/projects/{project}/notes/{id}"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM note_tags").await, 0);

    let author = author_of(&store, A).await;
    let one = |r: Reply| ids(&r) == vec![id.clone()];
    // Title, summary, Typed text after the summary, a word prefix, any case.
    assert!(one(get(&router, "/notes?q=chloroplast").await));
    assert!(one(get(&router, "/notes?q=ATLAS%20chloro").await));
    assert!(one(get(&router, "/notes?q=zygomorphic").await));
    let summary_word = get(&router, "/notes?q=sunlight").await;
    assert!(one(summary_word));
    let r = get(&router, &format!("/notes/{id}")).await;
    let summary = r.body["summary"].as_str().unwrap();
    assert!(summary.starts_with("Leaves capture sunlight"));
    assert!(!summary.contains("Zygomorphic"));
    assert!(summary.chars().count() <= 200);
    assert_eq!(r.body["tags"], json!([]));
    // Author, language and public project filters.
    let by_author = get(&router, &format!("/notes?author_id={author}")).await;
    let mut found = ids(&by_author);
    found.sort();
    let mut expected = vec![id.clone(), sibling.clone()];
    expected.sort();
    assert_eq!(found, expected);
    assert!(one(get(&router, "/notes?language=th").await));
    assert!(one(
        get(&router, &format!("/notes?project_id={project}")).await
    ));
    assert!(one(get(
        &router,
        &format!("/notes?q=leaves&author_id={author}&language=th")
    )
    .await));
    // Several words must all match; no match gives an empty list.
    assert!(ids(&get(&router, "/notes?q=chloroplast%20roots").await).is_empty());
    assert!(ids(&get(&router, "/notes?q=%21%21").await).is_empty());
    assert!(ids(&get(&router, "/notes?q=leaves&language=th&tag=x").await).is_empty());
    let r = get(&router, "/notes?author_id=bad%20id").await;
    assert_eq!(r.body["code"], "invalid_filter");
}

#[tokio::test]
async fn public_tags_are_the_tags_saved_at_publication() {
    let (store, router) = setup().await;
    let (id, revs) = note(
        &router,
        A,
        "t",
        json!({"title": "Cells", "tags": ["Biology"]}),
        json!({"typed": {"document": typed("Cell walls")}}),
    )
    .await;
    publish_typed(&router, A, &id, &revs[0].1).await;
    let r = call(
        &router,
        "PATCH",
        &format!("/me/notes/{id}"),
        Some(A),
        Some("1"),
        Some(json!({"metadata": {"tags": ["Secret plan"]}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);

    let public = get(&router, &format!("/notes/{id}")).await;
    assert_eq!(public.body["tags"], json!(["Biology"]));
    assert_eq!(
        ids(&get(&router, "/notes?tag=BIOLOGY").await),
        vec![id.clone()]
    );
    assert_eq!(
        ids(&get(&router, "/notes?q=biology").await),
        vec![id.clone()]
    );
    assert!(ids(&get(&router, "/notes?tag=secret%20plan").await).is_empty());
    assert!(ids(&get(&router, "/notes?q=secret").await).is_empty());
    let owner = call(
        &router,
        "GET",
        &format!("/me/notes/{id}/publication"),
        Some(A),
        None,
        None,
    )
    .await;
    assert_eq!(owner.body["tags"], json!(["Biology"]));

    // Republish makes the current tags public.
    publish_as(&router, A, &id, "1", "en", json!({"typed": revs[0].1})).await;
    let public = get(&router, &format!("/notes/{id}")).await;
    assert_eq!(public.body["tags"], json!(["Secret plan"]));
    assert_eq!(
        ids(&get(&router, "/notes?tag=secret%20plan").await),
        vec![id.clone()]
    );
    assert!(ids(&get(&router, "/notes?tag=biology").await).is_empty());

    unpublish(&router, A, &id, "2").await;
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_publication_tags").await,
        0
    );
    // The working tags stay.
    assert_eq!(count(&store, "SELECT COUNT(*) FROM note_tags").await, 1);
}

#[tokio::test]
async fn reader_opens_the_main_view_and_switches_only_when_both_are_published() {
    let (_store, router) = setup().await;
    let (id, revs) = note(
        &router,
        A,
        "both",
        json!({"title": "Lesson"}),
        json!({
            "original": {"document": original("Ink lesson")},
            "typed": {"document": typed("Typed lesson")}
        }),
    )
    .await;
    let rev = |name: &str| revs.iter().find(|r| r.0 == name).unwrap().1.clone();
    let (o, t) = (rev("original"), rev("typed"));
    // Typed only: the main Original is not published, so Typed opens.
    publish_as(&router, A, &id, "0", "en", json!({"typed": t})).await;
    let r = get(&router, &format!("/notes/{id}")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.body["default_view"], "typed");
    assert_eq!(r.body["views"], json!(["typed"]));
    assert_eq!(r.body["can_switch_view"], false);
    assert_eq!(r.body["representations"].as_array().unwrap().len(), 1);
    assert_eq!(r.body["representations"][0]["representation"], "typed");
    let r = get(&router, &format!("/notes/{id}?view=original")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);

    // Both: the main Original opens and the switch is available.
    publish_as(
        &router,
        A,
        &id,
        "1",
        "en",
        json!({"original": o, "typed": t}),
    )
    .await;
    let r = get(&router, &format!("/notes/{id}")).await;
    assert_eq!(r.body["default_view"], "original");
    assert_eq!(r.body["can_switch_view"], true);
    assert_eq!(r.body["representations"].as_array().unwrap().len(), 1);
    assert_eq!(r.body["representations"][0]["representation"], "original");
    let r = get(&router, &format!("/notes/{id}?view=typed&language=en")).await;
    assert_eq!(r.body["representations"][0]["representation"], "typed");
    let list = get(&router, "/notes").await;
    assert_eq!(list.body["items"][0]["views"], json!(["original", "typed"]));
    assert_eq!(list.body["items"][0]["can_switch_view"], true);
    assert!(list.body["items"][0].get("representations").is_none());

    let r = get(&router, &format!("/notes/{id}?language=th")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = get(&router, &format!("/notes/{id}?view=both")).await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.body["code"], "invalid_view");
    assert_eq!(
        get(&router, "/notes/missing").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn authors_and_projects_show_public_content_only() {
    let (store, router) = setup().await;
    let mut public_projects = vec![];
    for name in ["Alpha", "Beta", "Gamma"] {
        let r = call(
            &router,
            "POST",
            "/me/projects",
            Some(A),
            None,
            Some(json!({"name": name, "visibility": "public"})),
        )
        .await;
        public_projects.push(r.body["project_id"].as_str().unwrap().to_string());
    }
    let private = call(
        &router,
        "POST",
        "/me/projects",
        Some(A),
        None,
        Some(json!({"name": "Hidden"})),
    )
    .await
    .body["project_id"]
        .as_str()
        .unwrap()
        .to_string();
    let author = author_of(&store, A).await;

    // An author with only public projects is public; names come from owners.
    let r = get(&router, &format!("/authors/{author}")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.body, json!({"author_id": author, "name": null}));
    sqlx::query("UPDATE owners SET display_name='Ana' WHERE id=$1")
        .bind(A)
        .execute(&store.db)
        .await
        .unwrap();
    assert_eq!(
        get(&router, &format!("/authors/{author}")).await.body["name"],
        "Ana"
    );

    // Projects page by page, private ones never.
    let mut seen = vec![];
    let mut cursor = String::new();
    loop {
        let r = get(
            &router,
            &format!("/authors/{author}/projects?limit=2{cursor}"),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.body);
        for p in r.body["items"].as_array().unwrap() {
            assert_eq!(p["author_id"], author.as_str());
            seen.push(p["project_id"].as_str().unwrap().to_string());
        }
        match r.body["next_cursor"].as_str() {
            Some(c) => cursor = format!("&cursor={c}"),
            None => break,
        }
    }
    public_projects.sort();
    assert_eq!(seen, public_projects);
    let r = get(&router, &format!("/projects/{}", public_projects[0])).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["note_count"], 0);
    assert_eq!(
        get(&router, &format!("/projects/{private}")).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&router, "/projects/unknown").await.status,
        StatusCode::NOT_FOUND
    );

    // An author with nothing public is not found; B has only a private note.
    note(
        &router,
        B,
        "b",
        json!({"title": "B private"}),
        json!({"typed": {"document": typed("x")}}),
    )
    .await;
    sqlx::query("INSERT INTO authors(author_id,owner_id) VALUES('author-b',$1)")
        .bind(B)
        .execute(&store.db)
        .await
        .unwrap();
    assert_eq!(
        get(&router, "/authors/author-b").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&router, "/authors/author-b/projects").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&router, "/authors/unknown").await.status,
        StatusCode::NOT_FOUND
    );
    let r = get(&router, &format!("/authors/{author}/projects?cursor=a%20b")).await;
    assert_eq!(r.body["code"], "invalid_cursor");
}

#[tokio::test]
async fn catalog_has_no_forced_topics_and_no_general_tag() {
    let (_store, router) = setup().await;
    let r = get(&router, "/catalog").await;
    assert_eq!(r.status, StatusCode::OK);
    let keys: Vec<&String> = r.body.as_object().unwrap().keys().collect();
    assert_eq!(keys, vec!["channels", "languages"]);
    assert!(!r.body.to_string().to_lowercase().contains("general"));
}

#[tokio::test]
async fn public_responses_show_no_private_data() {
    let (store, router) = setup().await;
    // Published note: title and tags change after publication, and a newer Typed
    // draft is saved but not published.
    let (open, revs) = note(
        &router,
        A,
        "open",
        json!({"title": "Open lesson", "tags": ["Botany"]}),
        json!({"typed": {"document": typed("Open text")}}),
    )
    .await;
    publish_typed(&router, A, &open, &revs[0].1).await;
    let r = call(
        &router,
        "PATCH",
        &format!("/me/notes/{open}"),
        Some(A),
        Some("1"),
        Some(json!({"metadata": {"title": "Private retitle", "tags": ["Hidden tag"]}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    let r = call(
        &router,
        "PUT",
        &format!("/me/notes/{open}"),
        Some(A),
        Some("2"),
        Some(json!({"representation": "typed", "document": typed("Unpublished draft words")})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    // A draft that is never published.
    let (draft, _) = note(
        &router,
        A,
        "draft",
        json!({"title": "Secret draft", "tags": ["Draft tag"]}),
        json!({"typed": {"document": typed("Draft body text")}}),
    )
    .await;
    // A note that was published and then unpublished.
    let (gone, revs) = note(
        &router,
        A,
        "gone",
        json!({"title": "Withdrawn essay"}),
        json!({"typed": {"document": typed("Withdrawn words")}}),
    )
    .await;
    publish_typed(&router, A, &gone, &revs[0].1).await;
    unpublish(&router, A, &gone, "1").await;
    // Projects: a public one with every note, a private one with the public note.
    let mut project_ids = vec![];
    for (name, visibility) in [("Open course", "public"), ("Private project", "private")] {
        let r = call(
            &router,
            "POST",
            "/me/projects",
            Some(A),
            None,
            Some(json!({"name": name, "visibility": visibility})),
        )
        .await;
        project_ids.push(r.body["project_id"].as_str().unwrap().to_string());
    }
    let (course, hidden) = (project_ids[0].clone(), project_ids[1].clone());
    for n in [&open, &draft, &gone] {
        call(
            &router,
            "PUT",
            &format!("/me/projects/{course}/notes/{n}"),
            Some(A),
            None,
            None,
        )
        .await;
    }
    call(
        &router,
        "PUT",
        &format!("/me/projects/{hidden}/notes/{open}"),
        Some(A),
        None,
        None,
    )
    .await;
    // An Instant room.
    rooms::create(&store, "room-one", Create { schema_version: 1, document: json!({
        "page":{"width":1000,"background":{"kind":"blank"}},
        "items":[{"id":"private-room-item","kind":"text","x":0,"y":0,"width":200,"height":80,
            "spans":[{"text":"Room secret words"}]}]
    })}, 100).await.unwrap();

    let author = author_of(&store, A).await;
    let mut text = String::new();
    let mut checked = vec![];
    for uri in [
        "/notes".to_string(),
        "/notes?q=open".to_string(),
        "/notes?q=secret".to_string(),
        "/notes?q=draft".to_string(),
        "/notes?q=retitle".to_string(),
        "/notes?q=unpublished".to_string(),
        "/notes?q=withdrawn".to_string(),
        "/notes?q=room".to_string(),
        "/notes?tag=hidden%20tag".to_string(),
        "/notes?tag=draft%20tag".to_string(),
        format!("/notes?project_id={course}"),
        format!("/notes?project_id={hidden}"),
        format!("/notes?author_id={author}"),
        format!("/notes/{open}"),
        format!("/notes/{open}?view=typed"),
        format!("/notes/{draft}"),
        format!("/notes/{gone}"),
        format!("/authors/{author}"),
        format!("/authors/{author}/projects"),
        format!("/projects/{course}"),
        format!("/projects/{hidden}"),
        "/catalog".to_string(),
    ] {
        let r = get(&router, &uri).await;
        checked.push((uri.clone(), r.status, r.body.clone()));
        text.push_str(&r.body.to_string());
    }
    for hidden_value in [
        A,
        "Private retitle",
        "Hidden tag",
        "Unpublished draft",
        "Secret draft",
        "Draft tag",
        "Draft body",
        "Withdrawn",
        "Private project",
        hidden.as_str(),
        draft.as_str(),
        gone.as_str(),
        "Room secret",
        "room-one",
        "revision_id",
        "\"revision\"",
        "client_note_id",
        "project_ids",
        "owner",
    ] {
        assert!(
            !text.contains(hidden_value),
            "public response shows {hidden_value}"
        );
    }

    // Lists hold the published note only; counts hold published notes only.
    let status = |uri: &str| checked.iter().find(|c| c.0 == uri).unwrap();
    for uri in ["/notes", "/notes?q=open"] {
        let (_, code, body) = status(uri);
        assert_eq!(*code, StatusCode::OK);
        assert_eq!(body["items"].as_array().unwrap().len(), 1, "{uri}");
        assert_eq!(body["items"][0]["note_id"], open.as_str());
        assert_eq!(body["items"][0]["title"], "Open lesson");
        assert_eq!(body["items"][0]["tags"], json!(["Botany"]));
        assert_eq!(body["items"][0]["summary"], "Open text");
    }
    for uri in [
        "/notes?q=secret",
        "/notes?q=draft",
        "/notes?q=room",
        "/notes?tag=hidden%20tag",
    ] {
        assert!(
            status(uri).2["items"].as_array().unwrap().is_empty(),
            "{uri}"
        );
    }
    assert_eq!(status(&format!("/projects/{course}")).2["note_count"], 1);
    let projects = &status(&format!("/authors/{author}/projects")).2;
    assert_eq!(projects["items"].as_array().unwrap().len(), 1);
    assert_eq!(projects["items"][0]["note_count"], 1);
    assert_eq!(
        status(&format!("/projects/{hidden}")).1,
        StatusCode::NOT_FOUND
    );
    assert_eq!(status(&format!("/notes/{draft}")).1, StatusCode::NOT_FOUND);
    assert!(
        status(&format!("/notes?project_id={hidden}")).2["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let reader = &status(&format!("/notes/{open}")).2;
    let document = reader["representations"][0]["document"].to_string();
    assert!(document.contains("Open text"));
}

#[tokio::test]
async fn gateway_display_name_updates_and_thai_name_round_trips_without_trusting_client() {
    let (_store, router) = setup().await;
    // Percent-encoded Thai name + '+' (must not be form-decoded to a space).
    let thai_name = "สมชาย+ใจดี";
    let encoded: String = thai_name
        .as_bytes()
        .iter()
        .map(|b| format!("%{b:02X}"))
        .collect();

    // Anonymous request with x-display-name fails with 401.
    let anon = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/me/notes")
                .header("x-display-name", encoded.as_str())
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "client_note_id": "anon",
                        "main_representation": "typed",
                        "representations": {"typed": {"document": typed("x")}}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anon.status(), StatusCode::UNAUTHORIZED);

    // Client-sent author_name / display_name in body is rejected.
    let bad_body = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/me/notes")
                .header("x-uuid", "thai-owner")
                .header("x-display-name", encoded.as_str())
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "client_note_id": "bad-author",
                        "author_name": "Spoofed",
                        "main_representation": "typed",
                        "representations": {"typed": {"document": typed("x")}}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_body.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // Authenticated request stores the decoded Thai name and exposes it on note, author, project.
    let create_proj = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/me/projects")
                .header("x-uuid", "thai-owner")
                .header("x-display-name", encoded.as_str())
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "Thai Study", "visibility": "public"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_proj.status(), StatusCode::CREATED);
    let proj_bytes = axum::body::to_bytes(create_proj.into_body(), usize::MAX)
        .await
        .unwrap();
    let project_id = serde_json::from_slice::<Value>(&proj_bytes).unwrap()["project_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (note_id, rev) = typed_note(&router, "thai-owner", "t1", "Thai Title", "Text").await;
    publish_typed(&router, "thai-owner", &note_id, &rev).await;
    let pub_note = call(
        &router,
        "GET",
        &format!("/notes/{note_id}"),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(pub_note.status, StatusCode::OK);
    assert_eq!(pub_note.body["author_name"], thai_name);
    let author_id = pub_note.body["author_id"].as_str().unwrap().to_string();

    let pub_author = call(
        &router,
        "GET",
        &format!("/authors/{author_id}"),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(pub_author.status, StatusCode::OK);
    assert_eq!(pub_author.body["name"], thai_name);

    let pub_project = call(
        &router,
        "GET",
        &format!("/projects/{project_id}"),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(pub_project.status, StatusCode::OK);
    assert_eq!(pub_project.body["author_name"], thai_name);

    // Updating x-display-name on a later authenticated request updates public responses.
    let updated_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/me/notes")
                .header("x-uuid", "thai-owner")
                .header("x-display-name", "Ada%20Lovelace")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(updated_resp.status(), StatusCode::OK);
    let pub_note2 = call(
        &router,
        "GET",
        &format!("/notes/{note_id}"),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(pub_note2.body["author_name"], "Ada Lovelace");
}
