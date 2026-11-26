use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Access {
    Public,
    Session,
    Admin,
}

fn gateway_public(method: &str, path: &str) -> bool {
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    matches!(
        (method, parts.as_slice()),
        ("GET", ["health" | "ready" | "catalog" | "notes"])
            | ("GET", ["notes" | "media" | "authors" | "projects", _])
            | ("GET", ["authors", _, "projects"])
            | ("GET", ["links", "preview"])
            | ("POST", ["waitlist"])
            | ("GET" | "PUT" | "PATCH", ["instant", _])
            | ("POST", ["instant", _, "items" | "files"])
            | ("PUT" | "PATCH" | "DELETE", ["instant", _, "items", _])
            | ("GET", ["instant", _, "files", _])
            | ("PUT", ["instant", _, "files", _, "content"])
    )
}

fn gateway_session(path: &str) -> bool {
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    matches!(parts.as_slice(), ["models"] | ["models", _] | ["me", _, ..])
}

fn gateway_admin(path: &str) -> bool {
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    if matches!(parts.as_slice(), ["admin", "images" | "posts", ..]) {
        return false;
    }
    path == "/logs/stream" || path.starts_with("/admin/")
}

const V2_ROUTES: &[(&str, &str, Access)] = &[
    // Public routes
    ("GET", "/health", Access::Public),
    ("GET", "/ready", Access::Public),
    ("GET", "/catalog", Access::Public),
    ("POST", "/waitlist", Access::Public),
    ("GET", "/links/preview", Access::Public),
    ("GET", "/notes", Access::Public),
    ("GET", "/notes/{note_id}", Access::Public),
    ("GET", "/authors/{author_id}", Access::Public),
    ("GET", "/authors/{author_id}/projects", Access::Public),
    ("GET", "/projects/{project_id}", Access::Public),
    ("GET", "/media/{key}", Access::Public),
    ("GET", "/instant/{code}", Access::Public),
    ("PUT", "/instant/{code}", Access::Public),
    ("PATCH", "/instant/{code}", Access::Public),
    ("POST", "/instant/{code}/items", Access::Public),
    ("PUT", "/instant/{code}/items/{id}", Access::Public),
    ("PATCH", "/instant/{code}/items/{id}", Access::Public),
    ("DELETE", "/instant/{code}/items/{id}", Access::Public),
    ("POST", "/instant/{code}/files", Access::Public),
    ("PUT", "/instant/{code}/files/{id}/content", Access::Public),
    ("GET", "/instant/{code}/files/{id}", Access::Public),
    // Signed-in session routes
    ("GET", "/models", Access::Session),
    ("GET", "/models/{model_version}", Access::Session),
    ("GET", "/me/notes", Access::Session),
    ("POST", "/me/notes", Access::Session),
    ("GET", "/me/notes/{note_id}", Access::Session),
    ("PUT", "/me/notes/{note_id}", Access::Session),
    ("PATCH", "/me/notes/{note_id}", Access::Session),
    ("DELETE", "/me/notes/{note_id}", Access::Session),
    ("GET", "/me/notes/{note_id}/publication", Access::Session),
    ("PUT", "/me/notes/{note_id}/publication", Access::Session),
    ("DELETE", "/me/notes/{note_id}/publication", Access::Session),
    (
        "POST",
        "/me/notes/{note_id}/certifications/prepared",
        Access::Session,
    ),
    ("POST", "/me/notes/{note_id}/reports", Access::Session),
    (
        "GET",
        "/me/notes/{note_id}/reports/delete-preview",
        Access::Session,
    ),
    (
        "POST",
        "/me/notes/{note_id}/reports/retry-cleanup",
        Access::Session,
    ),
    ("GET", "/me/projects", Access::Session),
    ("POST", "/me/projects", Access::Session),
    ("GET", "/me/projects/{project_id}", Access::Session),
    ("PATCH", "/me/projects/{project_id}", Access::Session),
    ("DELETE", "/me/projects/{project_id}", Access::Session),
    (
        "PUT",
        "/me/projects/{project_id}/notes/{note_id}",
        Access::Session,
    ),
    (
        "DELETE",
        "/me/projects/{project_id}/notes/{note_id}",
        Access::Session,
    ),
    ("POST", "/me/files", Access::Session),
    ("GET", "/me/files/{file_id}", Access::Session),
    ("DELETE", "/me/files/{file_id}", Access::Session),
    ("GET", "/me/files/{file_id}/content", Access::Session),
    ("PUT", "/me/files/{file_id}/content", Access::Session),
    ("POST", "/me/model_downloads", Access::Session),
    ("GET", "/me/model_downloads/{download_id}", Access::Session),
    (
        "GET",
        "/me/model_downloads/{download_id}/content",
        Access::Session,
    ),
    ("POST", "/me/transcriptions", Access::Session),
    ("GET", "/me/transcriptions/{run_id}", Access::Session),
    ("GET", "/me/transcriptions/{run_id}/events", Access::Session),
    ("PUT", "/me/transcriptions/{run_id}/result", Access::Session),
    (
        "POST",
        "/me/transcriptions/{run_id}/feedback",
        Access::Session,
    ),
    (
        "DELETE",
        "/me/transcriptions/{run_id}/execution",
        Access::Session,
    ),
    ("POST", "/me/content_jobs", Access::Session),
    ("GET", "/me/content_jobs/{job_id}", Access::Session),
    ("GET", "/me/content_jobs/{job_id}/events", Access::Session),
    (
        "DELETE",
        "/me/content_jobs/{job_id}/execution",
        Access::Session,
    ),
    ("GET", "/me/tasks", Access::Session),
    ("POST", "/me/tasks", Access::Session),
    ("GET", "/me/tasks/{task_id}", Access::Session),
    ("PATCH", "/me/tasks/{task_id}", Access::Session),
    ("DELETE", "/me/tasks/{task_id}", Access::Session),
    // Admin routes
    ("GET", "/logs/stream", Access::Admin),
    ("PUT", "/admin/catalog", Access::Admin),
    ("GET", "/admin/schemas", Access::Admin),
    ("POST", "/admin/schemas", Access::Admin),
    (
        "POST",
        "/admin/schemas/{name}/versions/{version}/publish",
        Access::Admin,
    ),
    ("GET", "/admin/notes", Access::Admin),
    ("POST", "/admin/notes", Access::Admin),
    ("GET", "/admin/notes/{note_id}", Access::Admin),
    ("PUT", "/admin/notes/{note_id}", Access::Admin),
    ("PATCH", "/admin/notes/{note_id}", Access::Admin),
    ("DELETE", "/admin/notes/{note_id}", Access::Admin),
    ("GET", "/admin/notes/{note_id}/publication", Access::Admin),
    ("PUT", "/admin/notes/{note_id}/publication", Access::Admin),
    (
        "DELETE",
        "/admin/notes/{note_id}/publication",
        Access::Admin,
    ),
    ("GET", "/admin/projects", Access::Admin),
    ("POST", "/admin/projects", Access::Admin),
    ("GET", "/admin/projects/{project_id}", Access::Admin),
    ("PATCH", "/admin/projects/{project_id}", Access::Admin),
    ("DELETE", "/admin/projects/{project_id}", Access::Admin),
    ("POST", "/admin/files", Access::Admin),
    ("GET", "/admin/files/{file_id}", Access::Admin),
    ("DELETE", "/admin/files/{file_id}", Access::Admin),
    ("GET", "/admin/files/{file_id}/content", Access::Admin),
    ("PUT", "/admin/files/{file_id}/content", Access::Admin),
];

const MVP_REMOVED_ROUTES: &[(&str, &str)] = &[
    ("GET", "/posts"),
    ("GET", "/posts/{key}"),
    ("PUT", "/admin/posts/{id}"),
    ("GET", "/admin/posts/{id}"),
    ("POST", "/admin/posts/{id}/publish"),
    ("POST", "/admin/images"),
    ("GET", "/admin/images/{key}"),
];

fn sample_path(pattern: &str) -> String {
    pattern
        .replace("{note_id}", "note-1")
        .replace("{author_id}", "author-1")
        .replace("{project_id}", "proj-1")
        .replace("{file_id}", "file-1")
        .replace("{model_version}", "v1.0.0")
        .replace("{download_id}", "dl-1")
        .replace("{run_id}", "run-1")
        .replace("{job_id}", "job-1")
        .replace("{task_id}", "task-1")
        .replace("{code}", "room-1")
        .replace("{id}", "item-1")
        .replace("{key}", "key-1")
        .replace("{name}", "note-typed")
        .replace("{version}", "1")
}

#[test]
fn every_v2_route_has_exactly_one_access_class_and_no_overlap() {
    let mut seen: HashMap<(&str, &str), Access> = HashMap::new();
    for &(method, pattern, access) in V2_ROUTES {
        if let Some(prev) = seen.insert((method, pattern), access) {
            panic!("duplicate route {method} {pattern} ({prev:?} vs {access:?})");
        }
        let concrete = sample_path(pattern);
        let is_pub = gateway_public(method, &concrete);
        let is_sess = gateway_session(&concrete);
        let is_adm = gateway_admin(&concrete);
        let count = usize::from(is_pub) + usize::from(is_sess) + usize::from(is_adm);
        assert_eq!(
            count, 1,
            "route {method} {concrete} matched {count} classes (pub={is_pub}, sess={is_sess}, adm={is_adm})"
        );
        match access {
            Access::Public => assert!(is_pub, "{method} {concrete} should be public"),
            Access::Session => {
                assert!(!is_pub, "session route {method} {concrete} matched public");
                assert!(is_sess, "{method} {concrete} should be session");
            }
            Access::Admin => {
                assert!(!is_pub, "admin route {method} {concrete} matched public");
                assert!(is_adm, "{method} {concrete} should be admin");
            }
        }
    }
}

#[test]
fn mvp_routes_are_excluded_from_v2_gateway_lists() {
    for &(method, pattern) in MVP_REMOVED_ROUTES {
        let concrete = sample_path(pattern);
        assert!(
            !gateway_public(method, &concrete),
            "MVP route {method} {concrete} must not match public"
        );
        assert!(
            !gateway_session(&concrete),
            "MVP route {method} {concrete} must not match session"
        );
        assert!(
            !gateway_admin(&concrete),
            "MVP route {method} {concrete} must not match admin"
        );
    }
}

#[test]
fn every_presentation_route_path_is_accounted_for() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("lib/presentation");
    let mut mounted_paths = BTreeSet::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let content = fs::read_to_string(path).unwrap();
        for segment in content.split(".route(\"").skip(1) {
            if let Some(route_path) = segment.split('"').next() {
                mounted_paths.insert(route_path.to_string());
            }
        }
    }
    let known_paths: BTreeSet<String> = V2_ROUTES
        .iter()
        .map(|(_, p, _)| (*p).to_string())
        .chain(MVP_REMOVED_ROUTES.iter().map(|(_, p)| (*p).to_string()))
        .collect();
    let missing: Vec<_> = mounted_paths.difference(&known_paths).collect();
    assert!(
        missing.is_empty(),
        "presentation routes missing from V2_ROUTES or MVP_REMOVED_ROUTES: {missing:?}"
    );
}
