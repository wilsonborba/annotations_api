//! Owner task card routes (`/me/tasks`) against a Plane Slim double enforcing containers.
use annotations_api::{
    core::{Error as CoreError, Result as CoreResult},
    dal::{
        Store,
        plane::{
            BoxFuture, CreateWorkItem, PlaneClient, PlaneState, PlaneWorkItem, PlaneWorkItemPage,
            UpdateWorkItem,
        },
    },
    domain::App,
    presentation,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tower::ServiceExt;

const A: &str = "user-a";
const B: &str = "user-b";

struct MockPlane {
    seq: AtomicUsize,
    projects: Mutex<HashMap<String, Vec<PlaneWorkItem>>>,
    identifiers: Mutex<HashSet<String>>,
    fail_update: AtomicBool,
}

impl MockPlane {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seq: AtomicUsize::new(0),
            projects: Mutex::new(HashMap::new()),
            identifiers: Mutex::new(HashSet::new()),
            fail_update: AtomicBool::new(false),
        })
    }
}

impl PlaneClient for MockPlane {
    fn is_configured(&self) -> bool {
        true
    }
    fn create_project<'a>(
        &'a self,
        _name: &'a str,
        identifier: &'a str,
    ) -> BoxFuture<'a, CoreResult<String>> {
        Box::pin(async move {
            let mut ids = self.identifiers.lock().unwrap();
            if ids.contains(identifier) {
                return Err(CoreError(
                    StatusCode::CONFLICT,
                    "plane_identifier_exists: Plane project identifier already exists".into(),
                ));
            }
            ids.insert(identifier.to_string());
            let n = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
            let pid = format!("proj-{n}");
            self.projects.lock().unwrap().insert(pid.clone(), vec![]);
            Ok(pid)
        })
    }
    fn list_states<'a>(
        &'a self,
        project_id: &'a str,
    ) -> BoxFuture<'a, CoreResult<Vec<PlaneState>>> {
        Box::pin(async move {
            Ok(vec![
                PlaneState {
                    id: format!("{project_id}-open"),
                    group: "unstarted".into(),
                },
                PlaneState {
                    id: format!("{project_id}-done"),
                    group: "completed".into(),
                },
            ])
        })
    }
    fn list_work_items<'a>(
        &'a self,
        project_id: &'a str,
        _cursor: Option<&'a str>,
        _limit: i64,
    ) -> BoxFuture<'a, CoreResult<PlaneWorkItemPage>> {
        Box::pin(async move {
            let projects = self.projects.lock().unwrap();
            let results = projects.get(project_id).cloned().unwrap_or_default();
            Ok(PlaneWorkItemPage {
                results,
                next_cursor: None,
            })
        })
    }
    fn create_work_item<'a>(
        &'a self,
        project_id: &'a str,
        input: &'a CreateWorkItem,
    ) -> BoxFuture<'a, CoreResult<PlaneWorkItem>> {
        Box::pin(async move {
            let n = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
            let item = PlaneWorkItem {
                id: format!("wi-{n}"),
                name: input.name.clone(),
                description_html: input.description_html.clone().unwrap_or_default(),
                target_date: input.target_date.clone(),
                state: input.state.clone(),
                created_at: Some("2026-11-18T00:00:00Z".into()),
                updated_at: Some("2026-11-18T00:00:00Z".into()),
            };
            self.projects
                .lock()
                .unwrap()
                .entry(project_id.to_string())
                .or_default()
                .push(item.clone());
            Ok(item)
        })
    }
    fn get_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
    ) -> BoxFuture<'a, CoreResult<PlaneWorkItem>> {
        Box::pin(async move {
            let projects = self.projects.lock().unwrap();
            projects
                .get(project_id)
                .and_then(|items| items.iter().find(|i| i.id == task_id))
                .cloned()
                .ok_or_else(CoreError::missing)
        })
    }
    fn update_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
        input: &'a UpdateWorkItem,
    ) -> BoxFuture<'a, CoreResult<PlaneWorkItem>> {
        Box::pin(async move {
            if self.fail_update.load(Ordering::SeqCst) {
                return Err(CoreError::bad(
                    "plane_rejected: Plane rejected the task request",
                ));
            }
            let mut projects = self.projects.lock().unwrap();
            let item = projects
                .get_mut(project_id)
                .and_then(|items| items.iter_mut().find(|i| i.id == task_id))
                .ok_or_else(CoreError::missing)?;
            if let Some(name) = &input.name {
                item.name = name.clone();
            }
            if let Some(desc) = &input.description_html {
                item.description_html = desc.clone();
            }
            if let Some(date) = &input.target_date {
                item.target_date = date.clone();
            }
            if let Some(state) = &input.state {
                item.state = state.clone();
            }
            Ok(item.clone())
        })
    }
    fn delete_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
    ) -> BoxFuture<'a, CoreResult<()>> {
        Box::pin(async move {
            let mut projects = self.projects.lock().unwrap();
            let items = projects
                .get_mut(project_id)
                .ok_or_else(CoreError::missing)?;
            let before = items.len();
            items.retain(|i| i.id != task_id);
            if items.len() == before {
                return Err(CoreError::missing());
            }
            Ok(())
        })
    }
}

async fn setup(plane: Arc<MockPlane>) -> Router {
    let mut store = Store::fixture().await;
    store.plane = plane;
    presentation::router(App { store })
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    owner: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(o) = owner {
        req = req.header("x-uuid", o);
    }
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    let resp = router
        .clone()
        .oneshot(
            req.body(Body::from(body.map(|v| v.to_string()).unwrap_or_default()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn user_isolation_update_failure_preserves_state_and_no_plane_secrets_leak() {
    let plane = MockPlane::new();
    let router = setup(plane.clone()).await;

    // Anonymous request fails with 401.
    let (s, _) = call(&router, "GET", "/me/tasks", None, None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);

    // User A creates a task.
    let (s, created) = call(
        &router,
        "POST",
        "/me/tasks",
        Some(A),
        Some(json!({
            "title": "Review cell biology",
            "description": "Chapter 3",
            "due_date": "2026-11-30"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let task_id = created["task_id"].as_str().unwrap().to_string();
    assert_eq!(created["status"], "open");

    // User B cannot list or read User A's task.
    let (sb_list, b_list) = call(&router, "GET", "/me/tasks", Some(B), None).await;
    assert_eq!(sb_list, StatusCode::OK);
    assert!(b_list["items"].as_array().unwrap().is_empty());
    let (sb_get, _) = call(
        &router,
        "GET",
        &format!("/me/tasks/{task_id}"),
        Some(B),
        None,
    )
    .await;
    assert_eq!(sb_get, StatusCode::NOT_FOUND);

    // Foreign updates and deletes are denied before the Plane adapter is called.
    let (sb_update, _) = call(
        &router,
        "PATCH",
        &format!("/me/tasks/{task_id}"),
        Some(B),
        Some(json!({"status": "completed"})),
    )
    .await;
    assert_eq!(sb_update, StatusCode::NOT_FOUND);
    let (sb_delete, _) = call(
        &router,
        "DELETE",
        &format!("/me/tasks/{task_id}"),
        Some(B),
        None,
    )
    .await;
    assert_eq!(sb_delete, StatusCode::NOT_FOUND);
    let (_, owner_view) = call(
        &router,
        "GET",
        &format!("/me/tasks/{task_id}"),
        Some(A),
        None,
    )
    .await;
    assert_eq!(owner_view["status"], "open");

    // Update failure leaves the old state and returns the Plane error category.
    plane.fail_update.store(true, Ordering::SeqCst);
    let (sf, err) = call(
        &router,
        "PATCH",
        &format!("/me/tasks/{task_id}"),
        Some(A),
        Some(json!({"status": "completed"})),
    )
    .await;
    assert_eq!(sf, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(err["code"], "plane_rejected");
    let err_str = err.to_string();
    assert!(!err_str.contains("X-Api-Key") && !err_str.contains("http://"));
    let (_, after_fail) = call(
        &router,
        "GET",
        &format!("/me/tasks/{task_id}"),
        Some(A),
        None,
    )
    .await;
    assert_eq!(after_fail["status"], "open");
    assert_eq!(after_fail["title"], "Review cell biology");

    // Successful update + filter + delete.
    plane.fail_update.store(false, Ordering::SeqCst);
    let (su, updated) = call(
        &router,
        "PATCH",
        &format!("/me/tasks/{task_id}"),
        Some(A),
        Some(json!({"status": "completed"})),
    )
    .await;
    assert_eq!(su, StatusCode::OK);
    assert_eq!(updated["status"], "completed");

    let (_, filtered) = call(
        &router,
        "GET",
        "/me/tasks?status=completed&q=biology",
        Some(A),
        None,
    )
    .await;
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);

    let (sd, _) = call(
        &router,
        "DELETE",
        &format!("/me/tasks/{task_id}"),
        Some(A),
        None,
    )
    .await;
    assert_eq!(sd, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn identifier_collision_retries_with_next_hash_slice_and_stores_accepted() {
    use sha2::{Digest, Sha256};
    use sqlx::Row;

    let plane = MockPlane::new();
    let hash = format!("{:x}", Sha256::digest(A.as_bytes()));
    let first = format!("AN{}", &hash[..10].to_ascii_uppercase());
    let expected = format!("AN{}", &hash[1..11].to_ascii_uppercase());
    assert_eq!(first.len(), 12);
    assert_eq!(expected.len(), 12);
    plane.identifiers.lock().unwrap().insert(first);

    let mut store = Store::fixture().await;
    store.plane = plane.clone();
    let router = presentation::router(App {
        store: store.clone(),
    });

    let (status, created) = call(
        &router,
        "POST",
        "/me/tasks",
        Some(A),
        Some(json!({"title": "Retry collided identifier"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["title"], "Retry collided identifier");
    assert!(plane.identifiers.lock().unwrap().contains(&expected));

    let row = sqlx::query("SELECT plane_identifier FROM owner_plane_containers WHERE owner_id=$1")
        .bind(A)
        .fetch_one(&store.db)
        .await
        .unwrap();
    let stored: String = row.get("plane_identifier");
    assert_eq!(stored, expected);
}
