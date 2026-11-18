use annotations_api::{
    dal::Store,
    domain::{App, models},
    presentation::{
        self,
        cli::{self, Command},
    },
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sqlx::Row;
use tower::ServiceExt;

fn fixture(version: &str) -> models::Manifest {
    serde_json::from_value(json!({
        "release_id":version,"code_revision":"private-revision-sentinel",
        "dataset_snapshot_id":"private-dataset-sentinel", "source_checkpoint":"private-checkpoint-sentinel",
        "artifacts":[{"platform":"linux","fsm_reference":"private-fsm-sentinel","size":64,
            "sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],
        "languages":["en","pt-BR","th"],"capabilities":["handwriting"],
        "metrics":{"en":{"cer":0.1},"pt-BR":{"cer":0.12},"th":{"cer":0.15}}
    })).unwrap()
}
async fn pointer(store: &Store) -> (Option<String>, Option<String>) {
    let row = sqlx::query("SELECT current_version,previous_version FROM model_default WHERE id=1")
        .fetch_one(&store.db)
        .await
        .unwrap();
    (row.get("current_version"), row.get("previous_version"))
}
async fn request(router: &Router, uri: &str, identity: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri);
    if let Some(identity) = identity {
        builder = builder.header("x-uuid", identity);
    }
    let res = router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = axum::body::to_bytes(res.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
#[tokio::test]
async fn cli_register_activate_rollback_and_immutable_history() {
    let store = Store::fixture().await;
    // All writes target a fresh, isolated in-memory SQLite fixture, never environment settings.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manifest.json");
    let manifest = include_str!("fixtures/model_manifest.json");
    tokio::fs::write(&path, manifest).await.unwrap();
    let command = cli::parse(&[
        "models".into(),
        "register".into(),
        path.display().to_string(),
    ])
    .unwrap()
    .unwrap();
    assert_eq!(cli::run(&store.db, command).await.unwrap(), "fixture-v1");
    assert_eq!(pointer(&store).await, (None, None));
    assert!(models::detail(&store.db, "fixture-v1").await.is_err());
    assert_eq!(
        cli::run(&store.db, Command::Activate("fixture-v1".into()))
            .await
            .unwrap(),
        "fixture-v1"
    );
    assert!(
        cli::run(&store.db, Command::Rollback)
            .await
            .unwrap_err()
            .to_string()
            .starts_with("model_rollback_unavailable:")
    );
    models::register(&store.db, fixture("v2")).await.unwrap();
    models::activate(&store.db, "v2").await.unwrap();
    assert_eq!(
        pointer(&store).await,
        (Some("v2".into()), Some("fixture-v1".into()))
    );
    models::activate(&store.db, "v2").await.unwrap();
    assert_eq!(
        pointer(&store).await,
        (Some("v2".into()), Some("fixture-v1".into()))
    );
    assert_eq!(
        cli::run(&store.db, Command::Rollback).await.unwrap(),
        "fixture-v1"
    );
    assert_eq!(pointer(&store).await, (Some("fixture-v1".into()), None));
    assert_eq!(
        models::detail(&store.db, "v2").await.unwrap_err().code,
        "model_withdrawn"
    );
    assert!(
        !models::list(&store.db, None, None)
            .await
            .unwrap()
            .models
            .iter()
            .any(|m| m.version == "v2")
    );
    // A second rollback never restores the release that was just rolled back, and a withdrawn
    // release cannot be activated again.
    assert_eq!(
        models::rollback(&store.db).await.unwrap_err().code,
        "model_rollback_unavailable"
    );
    assert_eq!(
        models::activate(&store.db, "v2").await.unwrap_err().code,
        "model_withdrawn"
    );
    assert_eq!(pointer(&store).await, (Some("fixture-v1".into()), None));
    assert_eq!(
        models::register(&store.db, fixture("v2"))
            .await
            .unwrap_err()
            .code,
        "model_release_exists"
    );
    let mut changed = fixture("v2");
    changed.artifacts[0].sha256 = "b".repeat(64);
    assert_eq!(
        models::register(&store.db, changed).await.unwrap_err().code,
        "model_release_exists"
    );
    assert!(models::activate(&store.db, "missing").await.is_err());
    assert_eq!(pointer(&store).await, (Some("fixture-v1".into()), None));
    // Normal activation keeps the replaced release retired; models withdraw withdraws a non-active release.
    models::register(&store.db, fixture("v3")).await.unwrap();
    models::activate(&store.db, "v3").await.unwrap();
    assert_eq!(
        models::detail(&store.db, "fixture-v1")
            .await
            .unwrap()
            .status,
        "retired"
    );
    assert_eq!(
        models::withdraw(&store.db, "v3").await.unwrap_err().code,
        "model_active"
    );
    assert_eq!(
        cli::run(&store.db, Command::Withdraw("fixture-v1".into()))
            .await
            .unwrap(),
        "fixture-v1"
    );
    assert_eq!(pointer(&store).await, (Some("v3".into()), None));
    assert_eq!(
        models::detail(&store.db, "fixture-v1")
            .await
            .unwrap_err()
            .code,
        "model_withdrawn"
    );
    assert!(cli::parse(&["models".into(), "activate".into()]).is_err());
    assert!(cli::parse(&["models".into(), "withdraw".into()]).is_err());
}
#[tokio::test]
async fn signed_in_whitelist_filters_and_no_http_promotion() {
    let store = Store::fixture().await;
    models::register(&store.db, fixture("v1")).await.unwrap();
    models::activate(&store.db, "v1").await.unwrap();
    models::register(&store.db, fixture("draft")).await.unwrap();
    let router = presentation::router(App { store });
    for uri in [
        "/models",
        "/models/v1",
        "/models?platform=linux&language=th",
    ] {
        let (status, body) = request(&router, uri, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "identity_required");
    }
    assert_eq!(
        request(&router, "/models", Some(" ")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&router, "/models?owner_id=user", None).await.0,
        StatusCode::UNAUTHORIZED
    );
    for uri in ["/models?platform=linux&language=pt-BR", "/models/v1"] {
        let (status, body) = request(&router, uri, Some("account-a")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!body.to_string().contains("private-"));
        assert!(!body.to_string().contains("metrics"));
        let manifest = if uri.contains('?') {
            &body["models"][0]
        } else {
            &body
        };
        assert_eq!(manifest.as_object().unwrap().len(), 7);
        assert_eq!(manifest["artifacts"][0].as_object().unwrap().len(), 3);
        assert_eq!(manifest["version"], "v1");
        assert_eq!(manifest["languages"], json!(["en", "pt-BR", "th"]));
        assert_eq!(manifest["capabilities"], json!(["handwriting"]));
    }
    assert_eq!(
        request(&router, "/models/draft", Some("account-a")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&router, "/models/missing", Some("account-a"))
            .await
            .1["code"],
        "model_not_found"
    );
    assert_eq!(
        request(&router, "/models?platform=macos", Some("account-a"))
            .await
            .1["models"],
        json!([])
    );
    assert_eq!(
        request(&router, "/models?language=xx", Some("account-a"))
            .await
            .1["code"],
        "invalid_model_filter"
    );
    for filter in ["web", "windows"] {
        assert_eq!(
            request(
                &router,
                &format!("/models?platform={filter}"),
                Some("account-a")
            )
            .await
            .1["code"],
            "invalid_model_filter"
        );
    }
    for uri in ["/models/register", "/models/v1/activate", "/admin/models"] {
        let res = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("x-uuid", "account-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            res.status(),
            StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED | StatusCode::FORBIDDEN
        ));
    }
}
#[tokio::test]
async fn validation_and_failed_promotion_leave_state_unchanged() {
    let store = Store::fixture().await;
    for mutation in 0..10 {
        let mut m = fixture("invalid");
        match mutation {
            0 => m.artifacts[0].sha256 = "x".repeat(64),
            1 => m.artifacts[0].size = 0,
            2 => m.languages.push("en".into()),
            3 => {
                m.metrics.remove("th");
            }
            4 => {
                m.metrics
                    .get_mut("en")
                    .unwrap()
                    .insert("cer".into(), f64::NAN);
            }
            5 => m.artifacts.push(m.artifacts[0].clone()),
            6 => m.capabilities[0] = "https://private.invalid".into(),
            7 => m.release_id = "../invalid".into(),
            8 => m.artifacts[0].platform = "web".into(),
            _ => m.artifacts[0].platform = "windows".into(),
        }
        assert_eq!(
            models::register(&store.db, m).await.unwrap_err().code,
            "invalid_model_manifest"
        );
    }
    models::register(&store.db, fixture("v1")).await.unwrap();
    models::register(&store.db, fixture("v2")).await.unwrap();
    models::activate(&store.db, "v1").await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_pointer BEFORE UPDATE OF current_version ON model_default BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").execute(&store.db).await.unwrap();
    assert!(models::activate(&store.db, "v2").await.is_err());
    assert_eq!(pointer(&store).await, (Some("v1".into()), None));
    assert_eq!(
        models::detail(&store.db, "v1").await.unwrap().status,
        "active"
    );
    assert!(models::detail(&store.db, "v2").await.is_err());
}
#[tokio::test]
async fn concurrent_promotions_keep_one_active_and_previous_release() {
    let store = Store::fixture().await;
    for version in ["v1", "v2", "v3"] {
        models::register(&store.db, fixture(version)).await.unwrap();
    }
    models::activate(&store.db, "v1").await.unwrap();
    let (a, b) = tokio::join!(
        models::activate(&store.db, "v2"),
        models::activate(&store.db, "v3")
    );
    a.unwrap();
    b.unwrap();
    let (current, previous) = pointer(&store).await;
    assert!(matches!(
        (current.as_deref(), previous.as_deref()),
        (Some("v2"), Some("v3")) | (Some("v3"), Some("v2"))
    ));
    let rows = sqlx::query("SELECT version FROM model_releases WHERE status='active'")
        .fetch_all(&store.db)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>(0), current.unwrap());
    assert_eq!(
        models::rollback(&store.db).await.unwrap(),
        previous.unwrap()
    );
}

#[tokio::test]
async fn binary_cli_uses_only_explicit_isolated_sqlite_and_separate_connections() {
    use annotations_api::dal::SQLITE_MIGRATIONS;
    use sqlx::any::AnyPoolOptions;
    // The process runs outside the repository with an empty environment.
    // Its sole database identity is a new disposable fixture file.
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("registry-test.sqlite").display()
    );
    sqlx::any::install_default_drivers();
    let db = AnyPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    for migration in SQLITE_MIGRATIONS {
        sqlx::raw_sql(migration).execute(&db).await.unwrap();
    }
    let path = dir.path().join("manifest.json");
    tokio::fs::write(&path, include_str!("fixtures/model_manifest.json"))
        .await
        .unwrap();
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_annotations-api"))
            .args(args)
            .env_clear()
            .env("DATABASE_URL", &url)
            .current_dir(dir.path())
            .output()
            .unwrap()
    };
    for args in [
        vec!["models", "register", path.to_str().unwrap()],
        vec!["models", "activate", "fixture-v1"],
    ] {
        let out = run(&args);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    for version in ["v2", "v3"] {
        models::register(&db, fixture(version)).await.unwrap();
    }
    let (a, b) = tokio::join!(models::activate(&db, "v2"), models::activate(&db, "v3"));
    a.unwrap();
    b.unwrap();
    let before = sqlx::query("SELECT current_version,previous_version FROM model_default")
        .fetch_one(&db)
        .await
        .unwrap();
    let previous: String = before.get("previous_version");
    let out = run(&["models", "rollback"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let after: String = sqlx::query("SELECT current_version FROM model_default")
        .fetch_one(&db)
        .await
        .unwrap()
        .get("current_version");
    assert_eq!(after, previous);
    // Withdraw the retired release (whichever of fixture-v1/v2/v3 is not active).
    let retired: String =
        sqlx::query("SELECT version FROM model_releases WHERE status='retired' LIMIT 1")
            .fetch_one(&db)
            .await
            .unwrap()
            .get("version");
    let out = run(&["models", "withdraw", &retired]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(&["models", "register", path.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("model_release_exists"));
    db.close().await;
}

#[tokio::test]
async fn sql_constraints_and_interrupted_registration_preserve_metadata() {
    let store = Store::fixture().await;
    sqlx::raw_sql("CREATE TRIGGER fail_artifact BEFORE INSERT ON model_artifacts BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").execute(&store.db).await.unwrap();
    assert!(models::register(&store.db, fixture("v1")).await.is_err());
    let count: i64 = sqlx::query("SELECT count(*) FROM model_releases")
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    sqlx::raw_sql("DROP TRIGGER fail_artifact")
        .execute(&store.db)
        .await
        .unwrap();
    models::register(&store.db, fixture("v1")).await.unwrap();
    models::register(&store.db, fixture("v2")).await.unwrap();
    models::activate(&store.db, "v1").await.unwrap();
    for sql in [
        "UPDATE model_releases SET status='unknown' WHERE version='v2'",
        "UPDATE model_releases SET status='active' WHERE version='v2'",
        "UPDATE model_default SET current_version='missing' WHERE id=1",
        "INSERT INTO model_default(id) VALUES(2)",
        "UPDATE model_artifacts SET size=0 WHERE version='v1'",
        "UPDATE model_languages SET language='xx' WHERE version='v1'",
    ] {
        assert!(
            sqlx::raw_sql(sql).execute(&store.db).await.is_err(),
            "{sql}"
        );
    }
}

#[tokio::test]
#[ignore = "Requires an explicitly isolated PostgreSQL 16 registry test database"]
async fn postgres_registry_transaction_and_concurrency_proof() {
    let url = std::env::var("ANNOTATIONS_MODELS_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/annotations_api31_test"));
    sqlx::any::install_default_drivers();
    let db = sqlx::any::AnyPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let identity: String = sqlx::query("SELECT CAST(current_database() AS TEXT) AS name")
        .fetch_one(&db)
        .await
        .unwrap()
        .get("name");
    assert_eq!(identity, "annotations_api31_test");
    for version in ["pg-v1", "pg-v2", "pg-v3"] {
        models::register(&db, fixture(version)).await.unwrap();
    }
    models::activate(&db, "pg-v1").await.unwrap();
    assert_eq!(
        models::list(&db, Some("linux"), Some("th"))
            .await
            .unwrap()
            .models
            .len(),
        1
    );
    assert_eq!(
        models::detail(&db, "pg-v1").await.unwrap().languages,
        ["en", "pt-BR", "th"]
    );
    let metric: f64 = sqlx::query(
        "SELECT value FROM model_metrics WHERE version='pg-v1' AND language='th' AND name='cer'",
    )
    .fetch_one(&db)
    .await
    .unwrap()
    .get("value");
    assert_eq!(metric, 0.15);
    for _ in 0..8 {
        let (a, b) = tokio::join!(
            models::activate(&db, "pg-v2"),
            models::activate(&db, "pg-v3")
        );
        a.unwrap();
        b.unwrap();
        let row = sqlx::query("SELECT current_version,previous_version FROM model_default")
            .fetch_one(&db)
            .await
            .unwrap();
        let current: String = row.get("current_version");
        let previous: String = row.get("previous_version");
        assert_ne!(current, previous);
        assert_eq!(models::rollback(&db).await.unwrap(), previous);
        let active: i64 =
            sqlx::query("SELECT count(*) AS n FROM model_releases WHERE status='active'")
                .fetch_one(&db)
                .await
                .unwrap()
                .get("n");
        assert_eq!(active, 1);
    }
    sqlx::raw_sql("CREATE FUNCTION registry_test_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture failure'; END $$; CREATE TRIGGER registry_test_failure BEFORE UPDATE ON model_default FOR EACH ROW WHEN (NEW.current_version IS DISTINCT FROM OLD.current_version) EXECUTE FUNCTION registry_test_failure();")
        .execute(&db).await.unwrap();
    let before = models::list(&db, None, None)
        .await
        .unwrap()
        .default_version
        .unwrap();
    assert!(models::activate(&db, "pg-v1").await.is_err());
    assert_eq!(
        models::list(&db, None, None)
            .await
            .unwrap()
            .default_version
            .unwrap(),
        before
    );
    assert_eq!(models::detail(&db, &before).await.unwrap().status, "active");
    db.close().await;
}
