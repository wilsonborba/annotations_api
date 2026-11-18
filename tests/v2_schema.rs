use annotations_api::{
    dal::{SQLITE_MIGRATIONS, Store},
    domain::names,
};
use sqlx::{AnyPool, Row};

const V2_TABLES: [&str; 30] = [
    "authors",
    "notes",
    "note_revisions",
    "note_current_revisions",
    "note_publications",
    "note_published_representations",
    "note_changes",
    "projects",
    "project_notes",
    "tags",
    "note_tags",
    "files",
    "file_references",
    "pending_operations",
    "note_save_operations",
    "note_save_revisions",
    "note_revision_cleanup",
    "note_titles",
    "note_publication_titles",
    "note_publication_tags",
    "note_search",
    "file_cleanup",
    "model_releases",
    "model_artifacts",
    "model_languages",
    "model_metrics",
    "model_capabilities",
    "model_default",
    "model_downloads",
    "training_feedback",
];

async fn run(db: &AnyPool, sql: &str) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(sql).execute(db).await.map(|_| ())
}

async fn count(db: &AnyPool, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(db)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

async fn seeded() -> Store {
    let store = Store::fixture().await;
    run(
        &store.db,
        "INSERT INTO owners(id,display_name) VALUES('u1','One'),('u2','Two');
         INSERT INTO languages(code,label) VALUES('en','English');
         INSERT INTO schema_versions(name,version,status,is_default) VALUES('note-original',1,'published',1),('note-typed',1,'published',1);",
    )
    .await
    .unwrap();
    store
}

async fn note(db: &AnyPool, id: &str, owner: &str, client: &str) -> Result<(), sqlx::Error> {
    run(db, &format!("INSERT INTO notes(note_id,owner_id,client_note_id,main_representation,created_at,updated_at) VALUES('{id}','{owner}','{client}','original',1,1)")).await
}

async fn revision(
    db: &AnyPool,
    id: &str,
    note: &str,
    kind: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    let schema = if kind == "typed" {
        "note-typed"
    } else {
        "note-original"
    };
    run(db, &format!("INSERT INTO note_revisions(revision_id,note_id,representation,document_id,schema_name,schema_version,content_hash,source_revision_id,created_at) VALUES('{id}','{note}','{kind}','doc-{id}','{schema}',1,'h-{id}',{source},1)")).await
}

async fn project(db: &AnyPool, id: &str, owner: &str, name: &str) -> Result<(), sqlx::Error> {
    let normalized = names::normalize(name);
    run(db, &format!("INSERT INTO projects(project_id,owner_id,name,normalized_name,created_at,updated_at) VALUES('{id}','{owner}','{name}','{normalized}',1,1)")).await
}

#[tokio::test]
async fn migrations_apply_on_clean_database_and_rerun_safely() {
    let store = Store::fixture().await;
    // SQLite has no ADD COLUMN IF NOT EXISTS: the rerun skips the one ADD COLUMN of
    // 013. The PostgreSQL file uses IF NOT EXISTS (checked below and on PostgreSQL).
    for migration in SQLITE_MIGRATIONS {
        let rerun: String = migration
            .lines()
            .filter(|l| !l.starts_with("ALTER TABLE files ADD COLUMN filename"))
            .collect::<Vec<_>>()
            .join("\n");
        run(&store.db, &rerun).await.unwrap();
    }
    assert!(
        include_str!("../migrations/013_v2_managed_files.sql")
            .contains("ALTER TABLE files ADD COLUMN IF NOT EXISTS filename")
    );
    for table in V2_TABLES {
        let sql =
            format!("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'");
        assert_eq!(count(&store.db, &sql).await, 1, "{table} missing");
    }
}

#[test]
fn sqlite_change_feed_equivalent_differs_only_in_sequence_column() {
    let postgres = include_str!("../migrations/005_v2_note_changes.sql");
    let sqlite = include_str!("sqlite/005_v2_note_changes.sql");
    let body = |text: &str, sequence: &str| {
        text.lines()
            .filter(|line| !line.starts_with("--"))
            .map(|line| line.replace(sequence, "SEQUENCE"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        body(
            postgres,
            "sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"
        ),
        body(sqlite, "sequence INTEGER PRIMARY KEY AUTOINCREMENT")
    );
}

#[test]
fn sqlite_discovery_equivalent_differs_only_in_search_vector_type() {
    let postgres = include_str!("../migrations/012_v2_public_discovery.sql");
    let sqlite = include_str!("sqlite/012_v2_public_discovery.sql");
    fn body(text: &str) -> Vec<String> {
        text.lines()
            .filter(|line| !line.starts_with("--") && !line.contains("USING GIN"))
            .map(|line| line.replace("TSVECTOR", "TEXT"))
            .collect()
    }
    assert_eq!(body(postgres), body(sqlite));
    assert!(postgres.contains("search_vector TSVECTOR NOT NULL"));
    assert!(postgres.contains("ON note_search USING GIN(search_vector)"));
    assert!(sqlite.contains("search_vector TEXT NOT NULL"));
}

#[tokio::test]
async fn schema_has_no_json_array_or_body_columns() {
    let store = Store::fixture().await;
    for table in V2_TABLES {
        let rows = sqlx::query(&format!(
            "SELECT name, type FROM pragma_table_info('{table}')"
        ))
        .fetch_all(&store.db)
        .await
        .unwrap();
        assert!(!rows.is_empty(), "{table} has no columns");
        for row in rows {
            let name = row.get::<String, _>(0).to_lowercase();
            let kind = row.get::<String, _>(1).to_lowercase();
            assert!(
                ["text", "bigint", "integer", "double precision"].contains(&kind.as_str()),
                "{table}.{name} uses type {kind}"
            );
            for word in ["json", "body", "points", "content"] {
                assert!(
                    !name.contains(word) || name == "content_hash" || name == "content_type",
                    "{table}.{name}"
                );
            }
        }
    }
    let migrations = [
        include_str!("../migrations/004_v2_notes.sql"),
        include_str!("../migrations/005_v2_note_changes.sql"),
        include_str!("../migrations/006_v2_projects_tags.sql"),
        include_str!("../migrations/007_v2_files_operations.sql"),
        include_str!("sqlite/005_v2_note_changes.sql"),
        include_str!("sqlite/008_v2_save_operations.sql"),
        include_str!("../migrations/009_v2_note_titles.sql"),
        include_str!("../migrations/010_v2_publication_titles.sql"),
        include_str!("../migrations/011_v2_project_listing.sql"),
        include_str!("../migrations/012_v2_public_discovery.sql"),
        include_str!("sqlite/012_v2_public_discovery.sql"),
        include_str!("../migrations/017_v2_training_feedback.sql"),
    ];
    for text in migrations {
        let lower = text.to_lowercase();
        for word in [
            "json",
            "array",
            "[]",
            "insert into",
            "deleted",
            "trash",
            "drop ",
            "alter ",
        ] {
            assert!(!lower.contains(word), "migration contains {word}");
        }
    }
}

#[tokio::test]
async fn client_note_id_is_unique_per_owner_only() {
    let store = seeded().await;
    note(&store.db, "n1", "u1", "c1").await.unwrap();
    assert!(note(&store.db, "n2", "u1", "c1").await.is_err());
    note(&store.db, "n3", "u2", "c1").await.unwrap();
    assert_eq!(
        count(
            &store.db,
            "SELECT COUNT(*) FROM notes WHERE owner_id='u1' AND client_note_id='c1'"
        )
        .await,
        1
    );
    let first =
        sqlx::query("SELECT note_id FROM notes WHERE owner_id='u1' AND client_note_id='c1'")
            .fetch_one(&store.db)
            .await
            .unwrap()
            .get::<String, _>(0);
    assert_eq!(first, "n1");
}

#[tokio::test]
async fn check_constraints_reject_unknown_values() {
    let store = seeded().await;
    let db = &store.db;
    assert!(run(db, "INSERT INTO notes(note_id,owner_id,client_note_id,main_representation,created_at,updated_at) VALUES('x','u1','c','draft',1,1)").await.is_err());
    assert!(run(db, "INSERT INTO notes(note_id,owner_id,client_note_id,main_representation,created_at,updated_at) VALUES('x','missing','c','typed',1,1)").await.is_err());
    note(db, "n1", "u1", "c1").await.unwrap();
    assert_eq!(
        count(db, "SELECT revision FROM notes WHERE note_id='n1'").await,
        1
    );
    assert!(revision(db, "r0", "n1", "summary", "NULL").await.is_err());
    revision(db, "o1", "n1", "original", "NULL").await.unwrap();
    assert!(
        revision(db, "o2", "n1", "original", "'o1'").await.is_err(),
        "only typed revisions record a source"
    );
    revision(db, "t1", "n1", "typed", "'o1'").await.unwrap();
    note(db, "n2", "u1", "c2").await.unwrap();
    assert!(
        revision(db, "t2", "n2", "typed", "'o1'").await.is_err(),
        "source must belong to the same note"
    );
    assert!(run(db, "INSERT INTO note_revisions(revision_id,note_id,representation,document_id,schema_name,schema_version,content_hash,created_at) VALUES('u','n1','original','doc-u','unknown',1,'h',1)").await.is_err());
    assert!(run(db, "INSERT INTO note_current_revisions(note_id,representation,revision_id) VALUES('n1','typed','o1')").await.is_err(), "pointer kind must match revision kind");
    assert!(run(db, "INSERT INTO note_current_revisions(note_id,representation,revision_id) VALUES('n2','original','o1')").await.is_err(), "pointer must use a revision of its note");
    run(db, "INSERT INTO note_current_revisions(note_id,representation,revision_id) VALUES('n1','original','o1'),('n1','typed','t1')").await.unwrap();
    assert!(run(db, "INSERT INTO note_current_revisions(note_id,representation,revision_id) VALUES('n1','original','o1')").await.is_err());
    assert!(run(db, "INSERT INTO note_published_representations(note_id,representation,revision_id) VALUES('n1','original','o1')").await.is_err(), "publication row first");
    run(
        db,
        "INSERT INTO note_publications(note_id,language_code,updated_at) VALUES('n1','en',1)",
    )
    .await
    .unwrap();
    assert!(
        run(
            db,
            "INSERT INTO note_publications(note_id,language_code,updated_at) VALUES('n2','xx',1)"
        )
        .await
        .is_err()
    );
    run(db, "INSERT INTO note_published_representations(note_id,representation,revision_id) VALUES('n1','original','o1')").await.unwrap();
    assert!(
        run(db, "DELETE FROM note_revisions WHERE revision_id='o1'")
            .await
            .is_err(),
        "published and current revisions stay"
    );
    assert!(run(db, "INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES('u1','n1','update',1)").await.is_err());
    assert!(run(db, "INSERT INTO projects(project_id,owner_id,name,normalized_name,visibility,created_at,updated_at) VALUES('p','u1','A','a','shared',1,1)").await.is_err());
    project(db, "p1", "u1", "A").await.unwrap();
    assert_eq!(
        sqlx::query("SELECT visibility FROM projects WHERE project_id='p1'")
            .fetch_one(db)
            .await
            .unwrap()
            .get::<String, _>(0),
        "private"
    );
    let file = |status: &str, reference: &str, hash: &str| {
        format!(
            "INSERT INTO files(file_id,owner_id,fsm_reference,sha256,size_bytes,content_type,purpose,status,created_at,updated_at) VALUES('f-{status}-{hash}','u1',{reference},'{hash}',3,'image/png','attachment','{status}',1,1)"
        )
    };
    let hash = "a".repeat(64);
    assert!(run(db, &file("deleted", "NULL", &hash)).await.is_err());
    assert!(
        run(db, &file("ready", "NULL", &hash)).await.is_err(),
        "ready needs an FSM reference"
    );
    assert!(run(db, &file("reserved", "NULL", "short")).await.is_err());
    run(db, &file("reserved", "NULL", &hash)).await.unwrap();
    run(db, &file("ready", "'fsm-1'", &hash)).await.unwrap();
    run(db, &format!("INSERT INTO file_references(file_id,reference_kind,reference_id) VALUES('f-ready-{hash}','note_revision','o1')")).await.unwrap();
    assert!(
        run(
            db,
            &format!("DELETE FROM files WHERE file_id='f-ready-{hash}'")
        )
        .await
        .is_err(),
        "referenced files stay"
    );
    assert!(
        run(
            db,
            "INSERT INTO authors(author_id,owner_id) VALUES('a1','u1'),('a2','u1')"
        )
        .await
        .is_err(),
        "one author per owner"
    );
    assert!(run(db, "INSERT INTO pending_operations(operation_id,owner_id,state,created_at) VALUES('op1','u1','prepared',1)").await.is_err(), "state value list");
    run(db, "INSERT INTO pending_operations(operation_id,owner_id,state,created_at) VALUES('op1','u1','started',1)").await.unwrap();
    assert!(run(db, "INSERT INTO pending_operations(operation_id,owner_id,state,created_at) VALUES('op1','u1','started',1)").await.is_err());
    for state in ["abandoned", "failed", "started", "done"] {
        run(
            db,
            &format!("UPDATE pending_operations SET state='{state}' WHERE operation_id='op1'"),
        )
        .await
        .unwrap();
    }
    assert!(
        run(
            db,
            "UPDATE pending_operations SET state='committed' WHERE operation_id='op1'"
        )
        .await
        .is_err()
    );
    let hash = "b".repeat(64);
    assert!(run(db, "INSERT INTO note_save_operations(operation_id,note_id,request_hash,updated_at) VALUES('op1','n1','short',1)").await.is_err());
    assert!(run(db, &format!("INSERT INTO note_save_operations(operation_id,note_id,request_hash,updated_at) VALUES('missing','n1','{hash}',1)")).await.is_err(), "detail rows need their operation");
    run(db, &format!("INSERT INTO note_save_operations(operation_id,note_id,request_hash,updated_at) VALUES('op1','n1','{hash}',1)")).await.unwrap();
    assert!(
        run(
            db,
            "UPDATE note_save_operations SET result_revision=0 WHERE operation_id='op1'"
        )
        .await
        .is_err()
    );
    assert!(run(db, "INSERT INTO note_save_revisions(operation_id,representation,revision_id,document_id) VALUES('op1','summary','r1','d1')").await.is_err());
    run(db, "INSERT INTO note_save_revisions(operation_id,representation,revision_id,document_id) VALUES('op1','original','r1','d1')").await.unwrap();
    assert!(run(db, "INSERT INTO note_save_revisions(operation_id,representation,revision_id,document_id) VALUES('op1','typed','r2','d1')").await.is_err(), "one document per revision");
    run(
        db,
        "DELETE FROM pending_operations WHERE operation_id='op1'",
    )
    .await
    .unwrap();
    assert_eq!(
        count(db, "SELECT COUNT(*) FROM note_save_revisions").await,
        0
    );
}

#[test]
fn sqlite_save_operations_equivalent_differs_only_in_state_value_list() {
    let postgres = include_str!("../migrations/008_v2_save_operations.sql");
    let sqlite = include_str!("sqlite/008_v2_save_operations.sql");
    fn body<'a>(text: &'a str, marker: &str) -> Vec<&'a str> {
        text.lines()
            .filter(|line| !line.starts_with("--") && !line.contains(marker))
            .collect()
    }
    assert_eq!(
        body(postgres, "pending_operations_state_values"),
        body(sqlite, "pending_operations_state_")
    );
    let values = "state IN ('started','abandoned','done','failed')";
    assert!(postgres.contains(&format!("CHECK({values})")));
    assert_eq!(
        sqlite
            .matches("NOT IN ('started','abandoned','done','failed')")
            .count(),
        2
    );
    // The only ALTER statements replace the named state CHECK; no data is dropped.
    let alters: Vec<&str> = postgres
        .lines()
        .filter(|l| l.to_lowercase().contains("alter ") || l.to_lowercase().contains("drop "))
        .collect();
    assert_eq!(alters.len(), 2);
    assert!(
        alters
            .iter()
            .all(|l| l.starts_with("ALTER TABLE pending_operations ") && l.contains("CONSTRAINT"))
    );
}

#[tokio::test]
async fn project_names_collide_per_owner_after_normalization() {
    let store = seeded().await;
    assert_eq!(names::normalize("  Math "), "math");
    assert_eq!(names::normalize("Straße"), names::normalize("STRASSE"));
    project(&store.db, "p1", "u1", "Math").await.unwrap();
    assert!(project(&store.db, "p2", "u1", " math ").await.is_err());
    project(&store.db, "p3", "u2", " math ").await.unwrap();
    project(&store.db, "p4", "u2", "Physics").await.unwrap();
}

#[tokio::test]
async fn project_delete_keeps_notes_and_note_delete_removes_links() {
    let store = seeded().await;
    let db = &store.db;
    note(db, "n1", "u1", "c1").await.unwrap();
    note(db, "n2", "u1", "c2").await.unwrap();
    note(db, "n3", "u2", "c3").await.unwrap();
    project(db, "p1", "u1", "One").await.unwrap();
    project(db, "p2", "u1", "Two").await.unwrap();
    run(db, "INSERT INTO project_notes(project_id,note_id,owner_id) VALUES('p1','n1','u1'),('p1','n2','u1'),('p2','n1','u1')").await.unwrap();
    assert!(
        run(
            db,
            "INSERT INTO project_notes(project_id,note_id,owner_id) VALUES('p1','n3','u1')"
        )
        .await
        .is_err(),
        "membership never crosses owners"
    );
    assert!(
        run(
            db,
            "INSERT INTO project_notes(project_id,note_id,owner_id) VALUES('p1','n3','u2')"
        )
        .await
        .is_err(),
        "membership never crosses owners"
    );
    run(db, "DELETE FROM projects WHERE project_id='p2'")
        .await
        .unwrap();
    assert_eq!(
        count(db, "SELECT COUNT(*) FROM notes WHERE owner_id='u1'").await,
        2
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM project_notes WHERE project_id='p2'"
        )
        .await,
        0
    );

    run(
        db,
        "INSERT INTO tags(tag_id,name,normalized_name) VALUES('t1','Math','math')",
    )
    .await
    .unwrap();
    assert!(
        run(
            db,
            "INSERT INTO tags(tag_id,name,normalized_name) VALUES('t2','MATH','math')"
        )
        .await
        .is_err()
    );
    run(
        db,
        "INSERT INTO note_tags(note_id,tag_id) VALUES('n1','t1'),('n2','t1')",
    )
    .await
    .unwrap();
    revision(db, "o1", "n1", "original", "NULL").await.unwrap();
    revision(db, "t1", "n1", "typed", "'o1'").await.unwrap();
    run(db, "INSERT INTO note_current_revisions(note_id,representation,revision_id) VALUES('n1','original','o1'),('n1','typed','t1');
             INSERT INTO note_publications(note_id,language_code,updated_at) VALUES('n1','en',1);
             INSERT INTO note_published_representations(note_id,representation,revision_id) VALUES('n1','original','o1'),('n1','typed','t1');").await.unwrap();

    run(db, "DELETE FROM notes WHERE note_id='n1'")
        .await
        .unwrap();
    for table in [
        "project_notes",
        "note_tags",
        "note_revisions",
        "note_current_revisions",
        "note_publications",
        "note_published_representations",
    ] {
        assert_eq!(
            count(
                db,
                &format!("SELECT COUNT(*) FROM {table} WHERE note_id='n1'")
            )
            .await,
            0,
            "{table}"
        );
    }
    assert_eq!(
        count(db, "SELECT COUNT(*) FROM projects WHERE project_id='p1'").await,
        1
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM project_notes WHERE project_id='p1'"
        )
        .await,
        1
    );
    assert_eq!(
        count(db, "SELECT COUNT(*) FROM note_tags WHERE tag_id='t1'").await,
        1
    );
    assert_eq!(count(db, "SELECT COUNT(*) FROM tags").await, 1);
}

#[tokio::test]
async fn change_feed_grows_and_keeps_delete_notices() {
    let store = seeded().await;
    let db = &store.db;
    note(db, "n1", "u1", "c1").await.unwrap();
    run(db, "INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES('u1','n1','upsert',1)").await.unwrap();
    run(db, "INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES('u2','other','upsert',2)").await.unwrap();
    run(db, "DELETE FROM notes WHERE note_id='n1'")
        .await
        .unwrap();
    run(db, "INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES('u1','n1','delete',3)").await.unwrap();
    let rows = sqlx::query("SELECT sequence, change_kind FROM note_changes WHERE owner_id='u1' AND sequence > 0 ORDER BY sequence")
        .fetch_all(db)
        .await
        .unwrap();
    let feed: Vec<(i64, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(feed.len(), 2);
    assert!(feed[0].0 < feed[1].0);
    assert_eq!(feed[1].1, "delete");
    assert_eq!(count(db, "SELECT COUNT(*) FROM notes").await, 0);
    run(
        db,
        "DELETE FROM note_changes WHERE sequence = (SELECT MAX(sequence) FROM note_changes)",
    )
    .await
    .unwrap();
    run(db, "INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES('u1','n1','delete',4)").await.unwrap();
    assert!(
        count(db, "SELECT MAX(sequence) FROM note_changes").await > feed[1].0,
        "sequence values are never reused"
    );
}

#[test]
fn sqlite_managed_files_equivalent_differs_only_in_value_lists() {
    let postgres = include_str!("../migrations/013_v2_managed_files.sql");
    let sqlite = include_str!("sqlite/013_v2_managed_files.sql");
    fn body(text: &str) -> Vec<&str> {
        text.lines()
            .filter(|line| {
                !line.starts_with("--")
                    && !line.contains("_values")
                    && !line.contains("TRIGGER")
                    && !line.starts_with("ALTER TABLE files ADD COLUMN")
            })
            .collect()
    }
    assert_eq!(body(postgres), body(sqlite));
    let purposes = "('attachment','transcription','content_job')";
    let kinds = "('note_revision','pending_operation','transcription','content_job')";
    assert!(postgres.contains(&format!("CHECK(purpose IN {purposes})")));
    assert!(postgres.contains(&format!("CHECK(reference_kind IN {kinds})")));
    assert_eq!(sqlite.matches(&format!("NOT IN {purposes}")).count(), 2);
    assert_eq!(sqlite.matches(&format!("NOT IN {kinds}")).count(), 2);
    let filename = "CHECK(filename <> '' AND length(filename) <= 255)";
    assert!(postgres.contains(filename) && sqlite.contains(filename));
    // ALTER statements only add the file name column and replace named CHECKs; no data is dropped.
    for line in postgres.lines().filter(|l| {
        let l = l.to_lowercase();
        l.contains("alter ") || l.contains("drop ")
    }) {
        assert!(
            line.starts_with("ALTER TABLE files ADD COLUMN IF NOT EXISTS filename ")
                || (line.starts_with("ALTER TABLE ") && line.contains(" CONSTRAINT ")),
            "{line}"
        );
    }
    for text in [postgres, sqlite] {
        let lower = text.to_lowercase();
        for word in ["json", "array", "[]", "insert into", "trash"] {
            assert!(!lower.contains(word), "migration contains {word}");
        }
    }
}

#[tokio::test]
async fn file_purpose_and_reference_kind_use_value_lists() {
    let store = seeded().await;
    let db = &store.db;
    let hash = "b".repeat(64);
    let file = |id: &str, purpose: &str| {
        format!(
            "INSERT INTO files(file_id,owner_id,sha256,size_bytes,content_type,purpose,status,created_at,updated_at) VALUES('{id}','u1','{hash}',3,'image/png','{purpose}','reserved',1,1)"
        )
    };
    for purpose in ["attachment", "transcription", "content_job"] {
        run(db, &file(purpose, purpose)).await.unwrap();
    }
    assert!(run(db, &file("x", "avatar")).await.is_err());
    assert!(
        run(
            db,
            "UPDATE files SET purpose='avatar' WHERE file_id='attachment'"
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query("SELECT filename FROM files WHERE file_id='attachment'")
            .fetch_one(db)
            .await
            .unwrap()
            .get::<String, _>(0),
        "file"
    );
    assert!(
        run(
            db,
            "UPDATE files SET filename='' WHERE file_id='attachment'"
        )
        .await
        .is_err()
    );
    for kind in [
        "note_revision",
        "pending_operation",
        "transcription",
        "content_job",
    ] {
        run(db, &format!("INSERT INTO file_references(file_id,reference_kind,reference_id) VALUES('attachment','{kind}','r1')")).await.unwrap();
    }
    assert!(run(db, "INSERT INTO file_references(file_id,reference_kind,reference_id) VALUES('attachment','undo','r1')").await.is_err());
    assert!(
        run(
            db,
            "UPDATE file_references SET reference_kind='undo' WHERE reference_kind='content_job'"
        )
        .await
        .is_err()
    );
    assert!(
        run(
            db,
            "INSERT INTO file_cleanup(fsm_reference,created_at) VALUES('',1)"
        )
        .await
        .is_err()
    );
    run(
        db,
        "INSERT INTO file_cleanup(fsm_reference,created_at) VALUES('k1',1)",
    )
    .await
    .unwrap();
    assert!(
        run(
            db,
            "INSERT INTO file_cleanup(fsm_reference,created_at) VALUES('k1',2)"
        )
        .await
        .is_err()
    );
}
