//! Failure-injection tests of the note save pipeline. SQLite stands in for
//! PostgreSQL, the in-memory document store for CouchDB and the in-memory file
//! store for FSM. A crash is simulated by stopping between the public steps.
use annotations_api::{
    core::{self, Error, SAVE_SETTINGS, Settings},
    dal::{Store, documents::Documents},
    domain::{
        note_saves::{
            self, Begin, Content, Pending, Representation, RepresentationInput, SaveError,
            SaveRequest, SaveResult, Target,
        },
        schemas,
        tasks::{self, Reconciled},
    },
};
use axum::http::StatusCode;
use serde_json::{Value, json};
use sqlx::Row;
use std::path::Path;

const OWNER: &str = "u1";

async fn store() -> Store {
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
        "INSERT INTO owners(id,display_name) VALUES('u1','One'),('u2','Two');
         INSERT INTO languages(code,label) VALUES('en','English');
         INSERT INTO files(file_id,owner_id,fsm_reference,sha256,size_bytes,content_type,purpose,status,created_at,updated_at)
         VALUES('img-1','u1','fsm-1','0000000000000000000000000000000000000000000000000000000000000000',10,'image/png','attachment','ready',1,1);",
    )
    .execute(&store.db)
    .await
    .unwrap();
    store
}

fn original(x: i64) -> Value {
    json!({
        "page": {"width": 1000, "background": {"kind": "blank"}},
        "items": [
            {"id": "t1", "kind": "text", "x": x, "y": 40, "width": 304, "height": 120,
             "spans": [{"text": "Photosynthesis"}]},
            {"id": "i1", "kind": "image", "x": 400, "y": 400, "width": 200, "height": 100,
             "file_id": "img-1"}
        ]
    })
}
fn typed(text: &str) -> Value {
    json!({"blocks": [{"id": "k1", "kind": "paragraph", "spans": [{"text": text}]}]})
}
fn input(representation: Representation, content: Content) -> RepresentationInput {
    RepresentationInput {
        representation,
        schema_version: None,
        content,
        source_revision_id: None,
    }
}
fn create(key: &str, client: &str) -> SaveRequest {
    SaveRequest {
        owner_id: OWNER.into(),
        operation_key: key.into(),
        target: Target::Create {
            client_note_id: client.into(),
            main_representation: Representation::Original,
        },
        representations: vec![input(
            Representation::Original,
            Content::Document(original(40)),
        )],
        metadata: None,
    }
}
fn update(key: &str, note_id: &str, if_match: i64, document: Value) -> SaveRequest {
    SaveRequest {
        owner_id: OWNER.into(),
        operation_key: key.into(),
        target: Target::Update {
            note_id: note_id.into(),
            if_match,
        },
        representations: vec![input(Representation::Original, Content::Document(document))],
        metadata: None,
    }
}
async fn begin(store: &Store, request: &SaveRequest) -> Pending {
    match Pending::begin(store, request).await.unwrap() {
        Begin::Run(p) => p,
        Begin::Replay(_) => panic!("expected a new operation"),
    }
}
async fn count(store: &Store, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get::<i64, _>(0)
}
async fn state(store: &Store, operation_id: &str) -> String {
    sqlx::query("SELECT state FROM pending_operations WHERE operation_id=$1")
        .bind(operation_id)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0)
}
fn has_document(store: &Store, id: &str) -> bool {
    let Documents::Memory(m) = &store.docs else {
        panic!("memory double expected")
    };
    m.lock().unwrap().contains_key(id)
}
fn document_count(store: &Store) -> usize {
    let Documents::Memory(m) = &store.docs else {
        panic!("memory double expected")
    };
    m.lock().unwrap().len()
}
/// A CouchDB double that is down: nothing listens on the discard port.
fn couch_down() -> Documents {
    Documents::remote(
        Settings {
            database_url: String::new(),
            couch_url: "http://127.0.0.1:9".into(),
            couch_database: "test".into(),
            couch_user: String::new(),
            couch_password: String::new(),
            redis_url: String::new(),
            fsm_url: String::new(),
            fsm_key: String::new(),
            upload_staging_dir: String::new(),
            host: String::new(),
            port: 0,
            development: false,
        },
        reqwest::Client::new(),
    )
}
/// The task as it runs after the grace period.
async fn reconcile_late(store: &Store) -> Reconciled {
    tasks::reconcile(
        store,
        core::now() + SAVE_SETTINGS.grace_seconds + 1,
        &SAVE_SETTINGS,
    )
    .await
    .unwrap()
}
/// A saved note at revision 1.
async fn saved_note(store: &Store) -> SaveResult {
    note_saves::save(store, &create("op-create", "client-1"))
        .await
        .unwrap()
}
async fn visible(store: &Store, note_id: &str) -> (i64, String, i64, i64) {
    let row = sqlx::query("SELECT n.revision,c.revision_id FROM notes n JOIN note_current_revisions c ON c.note_id=n.note_id AND c.representation='original' WHERE n.note_id=$1")
        .bind(note_id)
        .fetch_one(&store.db)
        .await
        .unwrap();
    (
        row.get(0),
        row.get(1),
        count(store, "SELECT COUNT(*) FROM note_revisions").await,
        count(store, "SELECT COUNT(*) FROM note_changes").await,
    )
}

#[tokio::test]
async fn crash_after_document_write_leaves_no_visible_change_and_task_removes_orphan() {
    let store = store().await;
    let note = saved_note(&store).await;
    let before = visible(&store, &note.note_id).await;
    let request = update("op-crash-b", &note.note_id, 1, original(64));
    let pending = begin(&store, &request).await;
    pending.write_documents(&store).await.unwrap();
    // The process stops here: steps (c) to (e) never run.
    let orphan = pending.document_ids()[0].clone();
    assert!(has_document(&store, &orphan));
    assert_eq!(visible(&store, &note.note_id).await, before);
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM file_references WHERE reference_kind='note_revision'"
        )
        .await,
        1
    );
    // The running save holds its files until it commits or fails.
    let held = "SELECT COUNT(*) FROM file_references WHERE reference_kind='pending_operation'";
    assert_eq!(count(&store, held).await, 1);
    // Inside the grace period the task leaves a running save alone.
    let early = tasks::reconcile(&store, core::now(), &SAVE_SETTINGS)
        .await
        .unwrap();
    assert_eq!(early, Reconciled::default());
    assert!(has_document(&store, &orphan));
    assert!(matches!(
        note_saves::save(&store, &request).await,
        Err(SaveError::OperationInProgress)
    ));
    let late = reconcile_late(&store).await;
    assert_eq!(late.cleaned, 1);
    assert!(!has_document(&store, &orphan));
    assert_eq!(state(&store, pending.operation_id()).await, "failed");
    assert_eq!(count(&store, held).await, 0);
    assert_eq!(visible(&store, &note.note_id).await, before);
    // A retry of the same operation runs again and saves once.
    let result = note_saves::save(&store, &request).await.unwrap();
    assert_eq!(result.revision, 2);
    assert_eq!(state(&store, pending.operation_id()).await, "done");
    assert_eq!(count(&store, held).await, 0);
    assert_eq!(document_count(&store), 2);
}

#[tokio::test]
async fn crash_after_commit_is_reconciled_without_duplicate_revisions() {
    let store = store().await;
    let note = saved_note(&store).await;
    let request = update("op-crash-d", &note.note_id, 1, original(64));
    let pending = begin(&store, &request).await;
    pending.write_documents(&store).await.unwrap();
    pending.confirm_files(&store).await.unwrap();
    let committed = pending.commit(&store).await.unwrap();
    // The process stops here: step (e) never runs.
    assert_eq!(state(&store, pending.operation_id()).await, "started");
    let revisions = count(&store, "SELECT COUNT(*) FROM note_revisions").await;
    let late = reconcile_late(&store).await;
    assert_eq!(late.finished, 1);
    assert_eq!(late.cleaned, 0);
    assert_eq!(state(&store, pending.operation_id()).await, "done");
    assert!(has_document(&store, &pending.document_ids()[0]));
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        revisions
    );
    let retried = note_saves::save(&store, &request).await.unwrap();
    assert_eq!(retried, committed);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        revisions
    );
    assert_eq!(visible(&store, &note.note_id).await.0, 2);

    // The same crash, finished by a retry before the task runs.
    let request = update("op-crash-d2", &note.note_id, 2, original(80));
    let pending = begin(&store, &request).await;
    pending.write_documents(&store).await.unwrap();
    pending.confirm_files(&store).await.unwrap();
    let committed = pending.commit(&store).await.unwrap();
    let revisions = count(&store, "SELECT COUNT(*) FROM note_revisions").await;
    assert_eq!(note_saves::save(&store, &request).await.unwrap(), committed);
    assert_eq!(state(&store, pending.operation_id()).await, "done");
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        revisions
    );
    assert_eq!(reconcile_late(&store).await.finished, 0);
}

#[tokio::test]
async fn concurrent_saves_with_the_same_if_match_one_wins_the_other_gets_409() {
    let store = store().await;
    let note = saved_note(&store).await;
    // Both pass step (a) before either commits.
    let first = begin(&store, &update("op-a", &note.note_id, 1, original(64))).await;
    let second = begin(&store, &update("op-b", &note.note_id, 1, original(72))).await;
    for p in [&first, &second] {
        p.write_documents(&store).await.unwrap();
        p.confirm_files(&store).await.unwrap();
    }
    let won = first.commit(&store).await.unwrap();
    assert_eq!(won.revision, 2);
    let lost = second.commit(&store).await.unwrap_err();
    assert!(matches!(
        lost,
        SaveError::Conflict {
            current_revision: 2
        }
    ));
    let error = Error::from(lost);
    assert_eq!(error.0, StatusCode::CONFLICT);
    assert!(error.1.starts_with("revision_conflict:"), "{}", error.1);
    assert!(error.1.contains("current revision 2"), "{}", error.1);
    // The loser is abandoned: its document goes, the winner stays current.
    assert!(second.abandon(&store).await.unwrap().is_none());
    assert!(!has_document(&store, &second.document_ids()[0]));
    assert_eq!(state(&store, second.operation_id()).await, "failed");
    let (revision, current, _, _) = visible(&store, &note.note_id).await;
    assert_eq!(revision, 2);
    assert_eq!(current, won.revisions[0].revision_id);

    // The same race through the whole pipeline at the same time.
    let a = update("op-c", &note.note_id, 2, original(88));
    let b = update("op-d", &note.note_id, 2, original(104));
    let (ra, rb) = tokio::join!(note_saves::save(&store, &a), note_saves::save(&store, &b));
    let results = [ra, rb];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(results.iter().any(|r| matches!(
        r,
        Err(SaveError::Conflict {
            current_revision: 3
        })
    )));
    assert_eq!(visible(&store, &note.note_id).await.0, 3);
    // Only documents that a revision references remain.
    assert_eq!(
        document_count(&store) as i64,
        count(&store, "SELECT COUNT(*) FROM note_revisions").await
    );
}

#[tokio::test]
async fn retry_with_the_same_operation_id_gives_the_same_response() {
    let store = store().await;
    let request = create("op-1", "client-1");
    let first = note_saves::save(&store, &request).await.unwrap();
    let again = note_saves::save(&store, &request).await.unwrap();
    assert_eq!(first, again);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM notes").await, 1);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        1
    );
    assert_eq!(count(&store, "SELECT COUNT(*) FROM note_changes").await, 1);

    // Key order inside the document does not change the request.
    let mut reordered = request.clone();
    let text: String = serde_json::to_string(&original(40)).unwrap();
    let value: serde_json::Map<String, Value> = serde_json::from_str(&text).unwrap();
    let reversed: serde_json::Map<String, Value> = value.into_iter().rev().collect();
    reordered.representations[0].content = Content::Document(Value::Object(reversed));
    assert_eq!(note_saves::save(&store, &reordered).await.unwrap(), first);

    // The same id with a different payload is rejected.
    let mut changed = request.clone();
    changed.representations[0].content = Content::Document(original(48));
    let error = note_saves::save(&store, &changed).await.unwrap_err();
    assert!(matches!(error, SaveError::OperationReused));
    assert_eq!(Error::from(error).0, StatusCode::UNPROCESSABLE_ENTITY);

    // An update replays its first result too, also after the note moved on.
    let next = update("op-2", &first.note_id, 1, original(64));
    let saved = note_saves::save(&store, &next).await.unwrap();
    note_saves::save(&store, &update("op-3", &first.note_id, 2, original(72)))
        .await
        .unwrap();
    assert_eq!(note_saves::save(&store, &next).await.unwrap(), saved);
    assert_eq!(visible(&store, &first.note_id).await.0, 3);

    // A rejected save gives the same rejection on retry.
    let stale = update("op-4", &first.note_id, 1, original(80));
    for _ in 0..2 {
        assert!(matches!(
            note_saves::save(&store, &stale).await,
            Err(SaveError::Conflict {
                current_revision: 3
            })
        ));
    }

    // Operation ids belong to their owner: another owner cannot replay them.
    let mut other = update("op-2", &first.note_id, 1, original(64));
    other.owner_id = "u2".into();
    assert!(matches!(
        note_saves::save(&store, &other).await,
        Err(SaveError::NotFound)
    ));
    // Only documents that a revision references remain.
    assert_eq!(
        document_count(&store) as i64,
        count(&store, "SELECT COUNT(*) FROM note_revisions").await
    );
}

#[tokio::test]
async fn failure_before_commit_removes_the_documents_and_cleanup_retries_are_bounded() {
    let store = store().await;
    let note = saved_note(&store).await;
    let before = visible(&store, &note.note_id).await;

    // Step (c) fails: the file stopped being ready after validation.
    let request = update("op-file", &note.note_id, 1, original(64));
    let pending = begin(&store, &request).await;
    pending.write_documents(&store).await.unwrap();
    sqlx::query("UPDATE files SET status='failed' WHERE file_id='img-1'")
        .execute(&store.db)
        .await
        .unwrap();
    let error = pending.confirm_files(&store).await.unwrap_err();
    assert!(Error::from(error).1.starts_with("missing_file"));
    assert!(pending.abandon(&store).await.unwrap().is_none());
    assert!(!has_document(&store, &pending.document_ids()[0]));
    assert_eq!(state(&store, pending.operation_id()).await, "failed");
    assert_eq!(visible(&store, &note.note_id).await, before);
    sqlx::query("UPDATE files SET status='ready' WHERE file_id='img-1'")
        .execute(&store.db)
        .await
        .unwrap();

    // Step (b) fails: CouchDB is down, also for the cleanup.
    let mut down = store.clone();
    down.docs = couch_down();
    let request = update("op-couch", &note.note_id, 1, original(72));
    let error = note_saves::save(&down, &request).await.unwrap_err();
    assert!(matches!(error, SaveError::Unavailable));
    assert_eq!(visible(&store, &note.note_id).await, before);
    let operation_id: String =
        sqlx::query("SELECT p.operation_id FROM pending_operations p WHERE p.state='abandoned'")
            .fetch_one(&store.db)
            .await
            .unwrap()
            .get(0);
    let attempts = || async {
        count(
            &store,
            "SELECT attempts FROM note_save_operations s JOIN pending_operations p ON p.operation_id=s.operation_id WHERE p.state='abandoned'",
        )
        .await
    };
    // The save itself tried the cleanup once.
    assert_eq!(attempts().await, 1);
    for attempt in 2..=SAVE_SETTINGS.max_attempts {
        let run = tasks::reconcile(&down, core::now() + attempt * 30, &SAVE_SETTINGS)
            .await
            .unwrap();
        assert_eq!(run.waiting, 1, "attempt {attempt}");
        assert_eq!(attempts().await, attempt);
    }
    // After the last allowed attempt the task stops retrying this operation.
    assert_eq!(reconcile_late(&down).await, Reconciled::default());
    assert_eq!(state(&store, &operation_id).await, "abandoned");
    assert!(matches!(
        note_saves::save(&store, &request).await,
        Err(SaveError::OperationInProgress)
    ));
}

#[tokio::test]
async fn published_and_current_revisions_are_kept_and_old_ones_are_pruned() {
    let store = store().await;
    let note = saved_note(&store).await;
    let published = note.revisions[0].revision_id.clone();
    sqlx::raw_sql(&format!(
        "INSERT INTO note_publications(note_id,language_code,updated_at) VALUES('{0}','en',1);
         INSERT INTO note_published_representations(note_id,representation,revision_id) VALUES('{0}','original','{published}');",
        note.note_id
    ))
    .execute(&store.db)
    .await
    .unwrap();
    let published_doc: String =
        sqlx::query("SELECT document_id FROM note_revisions WHERE revision_id=$1")
            .bind(&published)
            .fetch_one(&store.db)
            .await
            .unwrap()
            .get(0);
    let snapshot = {
        let Documents::Memory(m) = &store.docs else {
            unreachable!()
        };
        m.lock().unwrap()[&published_doc].clone()
    };
    let second = note_saves::save(&store, &update("op-2", &note.note_id, 1, original(64)))
        .await
        .unwrap();
    let third = note_saves::save(&store, &update("op-3", &note.note_id, 2, original(80)))
        .await
        .unwrap();
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        3
    );
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM file_references").await,
        3
    );
    // Too young: nothing goes.
    assert_eq!(reconcile_late(&store).await.pruned, 0);
    let later = core::now() + SAVE_SETTINGS.revision_retention_seconds + 1;
    let run = tasks::reconcile(&store, later, &SAVE_SETTINGS)
        .await
        .unwrap();
    assert_eq!(run.pruned, 1);
    let left: Vec<String> =
        sqlx::query("SELECT revision_id FROM note_revisions ORDER BY revision_id")
            .fetch_all(&store.db)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect();
    let mut expected = vec![published.clone(), third.revisions[0].revision_id.clone()];
    expected.sort();
    assert_eq!(left, expected);
    assert!(
        !left.contains(&second.revisions[0].revision_id),
        "the old working revision is pruned"
    );
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM file_references").await,
        2
    );
    assert_eq!(document_count(&store), 2);
    // The published document was never overwritten.
    let Documents::Memory(m) = &store.docs else {
        unreachable!()
    };
    assert_eq!(m.lock().unwrap()[&published_doc], snapshot);
}

#[tokio::test]
async fn operations_apply_to_the_current_revision_and_inputs_are_checked() {
    let store = store().await;
    let note = saved_note(&store).await;
    // PATCH style: an editor operation on the current revision.
    let mut patch = update("op-move", &note.note_id, 1, Value::Null);
    patch.representations[0].content =
        Content::Operation(json!({"op": "update", "id": "t1", "changes": {"x": 496, "y": 600}}));
    let moved = note_saves::save(&store, &patch).await.unwrap();
    assert_eq!(moved.revision, 2);
    let doc_id: String = sqlx::query("SELECT document_id FROM note_revisions WHERE revision_id=$1")
        .bind(&moved.revisions[0].revision_id)
        .fetch_one(&store.db)
        .await
        .unwrap()
        .get(0);
    let Documents::Memory(m) = &store.docs else {
        unreachable!()
    };
    let stored = m.lock().unwrap()[&doc_id].clone();
    assert_eq!(stored["type"], "note_revision");
    assert_eq!(stored["schema_name"], "note-original");
    assert_eq!(stored["body"]["items"][0]["x"], 496);

    // An overlap is rejected by the editor rules before any write.
    let mut overlap = update("op-overlap", &note.note_id, 2, Value::Null);
    overlap.representations[0].content =
        Content::Operation(json!({"op": "update", "id": "t1", "changes": {"x": 352, "y": 376}}));
    let documents = document_count(&store);
    let error = Error::from(note_saves::save(&store, &overlap).await.unwrap_err());
    assert_eq!(error.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(error.1.starts_with("block_overlap"), "{}", error.1);
    assert_eq!(document_count(&store), documents);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM pending_operations").await,
        2
    );

    // A typed representation joins the note with its own pointer.
    let mut add_typed = update("op-typed", &note.note_id, 2, Value::Null);
    add_typed.representations = vec![input(
        Representation::Typed,
        Content::Document(typed("Light")),
    )];
    let with_typed = note_saves::save(&store, &add_typed).await.unwrap();
    assert_eq!(with_typed.revision, 3);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_current_revisions").await,
        2
    );

    // Schema errors, missing files and ownership.
    let bad = update("op-bad", &note.note_id, 3, json!({"page": {}, "items": []}));
    assert!(matches!(
        note_saves::save(&store, &bad).await,
        Err(SaveError::Invalid(_))
    ));
    let mut missing = original(40);
    missing["items"][1]["file_id"] = json!("img-2");
    let error = Error::from(
        note_saves::save(&store, &update("op-miss", &note.note_id, 3, missing))
            .await
            .unwrap_err(),
    );
    assert!(error.1.starts_with("missing_file"), "{}", error.1);
    let mut foreign = update("op-foreign", &note.note_id, 3, original(40));
    foreign.owner_id = "u2".into();
    assert!(matches!(
        note_saves::save(&store, &foreign).await,
        Err(SaveError::NotFound)
    ));
    let exists = note_saves::save(&store, &create("op-again", "client-1"))
        .await
        .unwrap_err();
    assert!(matches!(&exists, SaveError::NoteExists { note_id } if *note_id == note.note_id));
    assert_eq!(Error::from(exists).0, StatusCode::CONFLICT);
    let mut empty_key = create("", "client-2");
    assert!(matches!(
        note_saves::save(&store, &empty_key).await,
        Err(SaveError::InvalidRequest("invalid_operation_key"))
    ));
    empty_key.operation_key = "op-x".into();
    empty_key.representations.push(input(
        Representation::Original,
        Content::Document(original(40)),
    ));
    assert!(matches!(
        note_saves::save(&store, &empty_key).await,
        Err(SaveError::InvalidRequest("duplicate_representation"))
    ));
    assert_eq!(visible(&store, &note.note_id).await.0, 3);
    assert_eq!(
        document_count(&store) as i64,
        count(&store, "SELECT COUNT(*) FROM note_revisions").await
    );
}

#[tokio::test]
async fn restarted_attempt_uses_new_document_ids_and_fences_old_pending_handles() {
    let store = store().await;
    let note = saved_note(&store).await;
    let request = update("restart", &note.note_id, 1, original(64));
    let old = begin(&store, &request).await;
    old.write_documents(&store).await.unwrap();
    old.abandon(&store).await.unwrap();
    let next = begin(&store, &request).await;
    assert_ne!(old.document_ids(), next.document_ids());
    assert!(matches!(
        old.write_documents(&store).await,
        Err(SaveError::OperationInProgress)
    ));
    assert!(matches!(
        old.commit(&store).await,
        Err(SaveError::OperationInProgress)
    ));
    assert!(old.abandon(&store).await.unwrap().is_none());
    assert_eq!(state(&store, next.operation_id()).await, "started");
    assert!(matches!(
        next.finish(&store).await,
        Err(SaveError::OperationInProgress)
    ));
    assert_eq!(state(&store, next.operation_id()).await, "started");
    next.write_documents(&store).await.unwrap();
    next.confirm_files(&store).await.unwrap();
    let result = next.commit(&store).await.unwrap();
    next.finish(&store).await.unwrap();
    assert_eq!(note_saves::save(&store, &request).await.unwrap(), result);
}

#[tokio::test]
async fn pruning_commits_sql_before_document_deletion_and_cleanup_survives_failure() {
    let store = store().await;
    let note = saved_note(&store).await;
    note_saves::save(&store, &update("new", &note.note_id, 1, original(64)))
        .await
        .unwrap();
    let mut down = store.clone();
    down.docs = couch_down();
    // Simulate interruption between SQL pruning and external deletion.
    assert_eq!(
        note_saves::prune_revisions(&down, core::now() + 1, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revisions").await,
        1
    );
    assert_eq!(document_count(&store), 2);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revision_cleanup").await,
        1
    );
    let now = core::now();
    note_saves::cleanup_pruned(&down, now, 1, 5).await.unwrap();
    assert_eq!(
        count(&store, "SELECT attempts FROM note_revision_cleanup").await,
        1
    );
    // Backoff prevents hot retries.
    note_saves::cleanup_pruned(&down, now, 1, 5).await.unwrap();
    assert_eq!(
        count(&store, "SELECT attempts FROM note_revision_cleanup").await,
        1
    );
    note_saves::cleanup_pruned(&store, now + 30, 1, 5)
        .await
        .unwrap();
    assert_eq!(document_count(&store), 1);
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revision_cleanup").await,
        0
    );
    // A crash after CouchDB deletion but before queue acknowledgement is safe.
    sqlx::query("INSERT INTO note_revision_cleanup(document_id,created_at) VALUES('missing',0)")
        .execute(&store.db)
        .await
        .unwrap();
    note_saves::cleanup_pruned(&store, now + 30, 1, 5)
        .await
        .unwrap();
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM note_revision_cleanup").await,
        0
    );
}

#[tokio::test]
async fn immutable_document_adapter_never_overwrites_content() {
    let store = store().await;
    store
        .docs
        .put_immutable("revision", &json!({"body": 1}))
        .await
        .unwrap();
    store
        .docs
        .put_immutable("revision", &json!({"body": 1}))
        .await
        .unwrap();
    assert!(
        store
            .docs
            .put_immutable("revision", &json!({"body": 2}))
            .await
            .is_err()
    );
    assert_eq!(
        store.docs.get("revision").await.unwrap(),
        json!({"body": 1})
    );
}

#[tokio::test]
async fn reconciler_waits_for_document_writer_and_blocks_its_later_commit() {
    use axum::{
        Json, Router,
        extract::{Path, State},
        routing::put,
    };
    use std::sync::Arc;
    #[derive(Clone)]
    struct Double {
        docs: Documents,
        started: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    }
    async fn write(
        State(s): State<Double>,
        Path(id): Path<String>,
        Json(body): Json<Value>,
    ) -> StatusCode {
        s.started.notify_one();
        s.resume.notified().await;
        s.docs.put_immutable(&id, &body).await.unwrap();
        StatusCode::CREATED
    }
    async fn read(
        State(s): State<Double>,
        Path(id): Path<String>,
    ) -> Result<Json<Value>, StatusCode> {
        let mut body = s.docs.get(&id).await.map_err(|_| StatusCode::NOT_FOUND)?;
        body["_rev"] = json!("1-test");
        Ok(Json(body))
    }
    async fn remove(State(s): State<Double>, Path(id): Path<String>) -> StatusCode {
        s.docs.delete(&id).await.unwrap();
        StatusCode::OK
    }
    let mut store = store().await;
    let note = saved_note(&store).await;
    let mock = Double {
        docs: Documents::Memory(Arc::default()),
        started: Arc::default(),
        resume: Arc::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/test/{id}", put(write).get(read).delete(remove))
        .with_state(mock.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let Documents::Remote { settings, http } = couch_down() else {
        unreachable!()
    };
    let mut settings = *settings;
    settings.couch_url = format!("http://{address}");
    store.docs = Documents::remote(settings, http);
    let pending = begin(&store, &update("writer", &note.note_id, 1, original(64))).await;
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        pending.write_documents(&writer_store).await.unwrap();
        pending
    });
    mock.started.notified().await;
    let cleanup_store = store.clone();
    let mut reconciler = tokio::spawn(async move { reconcile_late(&cleanup_store).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut reconciler)
            .await
            .is_err()
    );
    mock.resume.notify_one();
    let pending = writer.await.unwrap();
    assert_eq!(reconciler.await.unwrap().cleaned, 1);
    assert!(mock.docs.get(&pending.document_ids()[0]).await.is_err());
    assert!(matches!(
        pending.commit(&store).await,
        Err(SaveError::OperationInProgress)
    ));
    assert_eq!(visible(&store, &note.note_id).await.0, 1);
    server.abort();
}

#[tokio::test]
async fn remote_immutable_retry_accepts_only_the_original_content() {
    use axum::{Json, Router, routing::put};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/test/{id}",
        put(|| async { StatusCode::CONFLICT })
            .get(|| async { Json(json!({"_id": "revision", "_rev": "1-test", "body": 1})) }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let Documents::Remote { settings, http } = couch_down() else {
        unreachable!()
    };
    let mut settings = *settings;
    settings.couch_url = format!("http://{address}");
    let docs = Documents::remote(settings, http);
    docs.put_immutable("revision", &json!({"body": 1}))
        .await
        .unwrap();
    assert!(
        docs.put_immutable("revision", &json!({"body": 2}))
            .await
            .is_err()
    );
    server.abort();
}
