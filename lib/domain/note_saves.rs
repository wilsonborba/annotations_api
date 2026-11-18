//! Save of note revisions across PostgreSQL, CouchDB and FSM.
//!
//! There is no distributed transaction across the stores, so the pipeline is
//! crash-safe by order of its steps:
//! - (a) validate every document against its published schema and the editor
//!   rules, check the expected revision, and record a pending operation with the
//!   ids that the later steps use;
//! - (b) write one new CouchDB document per saved representation. A document id
//!   is never used for other content, so a document that a publication
//!   references is never overwritten;
//! - (c) confirm that every referenced file is `ready` and owned by the caller.
//!   File bytes stay in FSM; a file is `ready` only after its upload was verified;
//! - (d) one PostgreSQL transaction: check the expected revision again, insert the
//!   revision rows, move the current pointers, add the file references, append the
//!   change feed row and record the result;
//! - (e) mark the pending operation `done`.
//!
//! Nothing is visible and nothing is reported as saved before (d) commits. A save
//! that stops before (d) leaves only CouchDB documents that no revision
//! references; the save itself or the reconciliation task removes them. A save
//! that stops after (d) is finished by a retry or by the task, and the fixed
//! revision ids of the operation prevent duplicate revisions.
//!
//! Operation states (`pending_operations.state`): `started`, `abandoned` (stopped
//! before commit, documents not yet removed), `done` and `failed` (documents removed).
use crate::{
    core::{self, Error, NOTE_LIMITS},
    dal::Store,
    domain::{
        editor_rules,
        note_documents::{self, DocumentError, Family, FileRef, FileScope},
    },
};
use axum::http::StatusCode;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;

/// A note representation. Each one has its own document family and revisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Representation {
    Original,
    Typed,
}
impl Representation {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "original" => Some(Self::Original),
            "typed" => Some(Self::Typed),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Typed => "typed",
        }
    }
    pub fn family(self) -> Family {
        match self {
            Self::Original => Family::Original,
            Self::Typed => Family::Typed,
        }
    }
}

/// New content of one representation: a complete document (PUT, POST) or an
/// editor operation applied to the current revision (PATCH).
#[derive(Clone, Debug)]
pub enum Content {
    Document(Value),
    Operation(Value),
}
#[derive(Clone, Debug)]
pub struct RepresentationInput {
    pub representation: Representation,
    /// The published schema version; the default version when absent. An
    /// operation keeps the schema version of the current revision.
    pub schema_version: Option<i64>,
    pub content: Content,
}
#[derive(Clone, Debug)]
pub enum Target {
    /// Create a note. `client_note_id` is unique per owner.
    Create {
        client_note_id: String,
        main_representation: Representation,
    },
    /// Change a note of the caller. `if_match` is the expected note revision.
    Update { note_id: String, if_match: i64 },
}
/// Note metadata saved with the same commit as the representations. Every field
/// is optional: an absent field keeps its stored value. Values are already
/// validated by the caller (`notes`): trimmed title, normalized unique tags.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    /// `Some("")` removes the title.
    pub title: Option<String>,
    /// The complete tag list (display names). `Some(vec![])` removes every tag.
    pub tags: Option<Vec<String>>,
    /// Projects of the caller that a new note joins. Create only.
    pub project_ids: Option<Vec<String>>,
}
/// One save. `owner_id` comes from the gateway identity, never from the body.
/// `operation_key` is the Idempotency-Key header value.
#[derive(Clone, Debug)]
pub struct SaveRequest {
    pub owner_id: String,
    pub operation_key: String,
    pub target: Target,
    pub representations: Vec<RepresentationInput>,
    /// Metadata to save with this revision. An update may change only metadata.
    pub metadata: Option<Metadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SavedRevision {
    pub representation: Representation,
    pub revision_id: String,
}
/// The answer of a committed save. A retry with the same operation gets it again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SaveResult {
    pub note_id: String,
    /// The new note revision (the next If-Match value).
    pub revision: i64,
    pub revisions: Vec<SavedRevision>,
}

/// A rejected or unfinished save. `code` is stable and machine-readable.
#[derive(Debug)]
pub enum SaveError {
    /// The content breaks the document schema, an editor rule or a file rule (422).
    Invalid(Error),
    /// The request shape is wrong (422).
    InvalidRequest(&'static str),
    /// The operation id was used before with a different request (422).
    OperationReused,
    /// The same operation is still running or its cleanup is pending (409).
    OperationInProgress,
    /// The If-Match revision is stale (409). Nothing was overwritten.
    Conflict { current_revision: i64 },
    /// A note with this client note id exists for the owner (409).
    NoteExists { note_id: String },
    /// The note does not exist or belongs to another owner (404).
    NotFound,
    /// A named project does not exist or belongs to another owner (422).
    UnknownProject,
    /// A store is unavailable (503). The save is not reported as done.
    Unavailable,
}
impl SaveError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Invalid(e) => e.0,
            Self::InvalidRequest(_) | Self::OperationReused | Self::UnknownProject => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::OperationInProgress | Self::Conflict { .. } | Self::NoteExists { .. } => {
                StatusCode::CONFLICT
            }
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "invalid_content",
            Self::InvalidRequest(code) => code,
            Self::OperationReused => "idempotency_key_reused",
            Self::OperationInProgress => "operation_in_progress",
            Self::Conflict { .. } => "revision_conflict",
            Self::NoteExists { .. } => "note_exists",
            Self::NotFound => "not_found",
            Self::UnknownProject => "unknown_project",
            Self::Unavailable => "storage_unavailable",
        }
    }
}
impl From<Error> for SaveError {
    fn from(e: Error) -> Self {
        if e.0 == StatusCode::NOT_FOUND {
            Self::NotFound
        } else if e.0.is_server_error() {
            Self::Unavailable
        } else {
            Self::Invalid(e)
        }
    }
}
impl From<sqlx::Error> for SaveError {
    fn from(e: sqlx::Error) -> Self {
        Error::from(e).into()
    }
}
impl From<DocumentError> for SaveError {
    fn from(e: DocumentError) -> Self {
        Error::from(e).into()
    }
}
impl From<editor_rules::RuleError> for SaveError {
    fn from(e: editor_rules::RuleError) -> Self {
        Self::Invalid(e.into())
    }
}
impl From<SaveError> for Error {
    fn from(e: SaveError) -> Self {
        let code = e.code();
        let text = match &e {
            SaveError::Invalid(_) => "",
            SaveError::InvalidRequest(c) => match *c {
                "invalid_operation_key" => {
                    "The operation id uses 1 to 128 visible ASCII characters"
                }
                "invalid_client_note_id" => {
                    "The client note id uses 1 to 128 visible ASCII characters"
                }
                "empty_save" => "A save needs a representation or a metadata change",
                "projects_on_update" => "Project memberships change through the project routes",
                "duplicate_representation" => "Each representation appears once",
                "main_representation_missing" => "A new note needs its main representation",
                "missing_base" => "An operation needs a saved current revision",
                "schema_version_mismatch" => {
                    "An operation keeps the schema version of the current revision"
                }
                _ => "The request is not valid",
            },
            SaveError::OperationReused => {
                "The operation id was already used with a different request"
            }
            SaveError::OperationInProgress => "The operation is still running; retry later",
            SaveError::Conflict { .. } => "The note changed since the given revision",
            SaveError::NoteExists { .. } => "A note with this client note id exists",
            SaveError::NotFound => "",
            SaveError::UnknownProject => "A project does not exist for this owner",
            SaveError::Unavailable => "",
        };
        let status = e.status();
        match e {
            SaveError::Invalid(e) => e,
            SaveError::NotFound => Error::missing(),
            SaveError::Unavailable => Error::unavailable(),
            SaveError::Conflict { current_revision } => Error(
                status,
                format!("{code}: {text} (current revision {current_revision})"),
            ),
            SaveError::NoteExists { note_id } => {
                Error(status, format!("{code}: {text} (note {note_id})"))
            }
            _ => Error(status, format!("{code}: {text}")),
        }
    }
}
type R<T> = std::result::Result<T, SaveError>;

/// One validated representation with the ids that this operation uses.
#[derive(Clone, Debug)]
struct Prepared {
    representation: Representation,
    schema_version: i64,
    revision_id: String,
    document_id: String,
    document: Value,
    content_hash: String,
    files: Vec<FileRef>,
}

/// A started operation between its steps.
#[derive(Debug)]
pub struct Pending {
    operation_id: String,
    owner_id: String,
    note_id: String,
    target: Target,
    items: Vec<Prepared>,
    metadata: Option<Metadata>,
}
/// The outcome of step (a).
#[derive(Debug)]
pub enum Begin {
    /// The operation committed before: its first result.
    Replay(SaveResult),
    /// A new or restarted operation. Run steps (b) to (e).
    Run(Pending),
}

/// Run the whole pipeline. On a failure before commit the documents of the
/// operation are removed; the reconciliation task finishes what a crash leaves.
pub async fn save(store: &Store, request: &SaveRequest) -> R<SaveResult> {
    let pending = match Pending::begin(store, request).await? {
        Begin::Replay(result) => return Ok(result),
        Begin::Run(pending) => pending,
    };
    let committed = async {
        pending.write_documents(store).await?;
        pending.confirm_files(store).await?;
        pending.commit(store).await
    }
    .await;
    match committed {
        Ok(result) => {
            if pending.finish(store).await.is_err() {
                tracing::warn!("A committed note save is not marked done; the task will finish it");
            }
            Ok(result)
        }
        Err(error) => match settle_inner(
            store,
            &pending.operation_id,
            core::now(),
            Some(&pending.items),
        )
        .await
        {
            // The commit reached PostgreSQL although its answer was lost.
            Ok(Settled::Done(result)) => Ok(result),
            _ => Err(error),
        },
    }
}

impl Pending {
    /// Step (a): validate, check the expected revision and record the operation.
    pub async fn begin(store: &Store, request: &SaveRequest) -> R<Begin> {
        check_shape(request)?;
        let owner = &request.owner_id;
        let operation_id =
            sha_hex(format!("note-save\n{owner}\n{}", request.operation_key).as_bytes());
        let request_hash = request_hash(request);
        if let Some(found) = stored(store, &operation_id).await? {
            if found.request_hash != request_hash {
                return Err(SaveError::OperationReused);
            }
            match found.state.as_str() {
                "done" => return Ok(Begin::Replay(found.result()?)),
                "started" | "abandoned" => {
                    if found.result_revision.is_none() {
                        return Err(SaveError::OperationInProgress);
                    }
                    // Committed, but step (e) did not run: finish it now.
                    mark_done(store, &operation_id).await?;
                    return Ok(Begin::Replay(found.result()?));
                }
                _ => {}
            }
            // `failed`: nothing of the earlier attempt remains. Run it again with its ids.
            let ids: Vec<_> = found
                .ids()
                .into_iter()
                .map(|(r, id, _)| (r, id, core::id()))
                .collect();
            let items = prepare(store, request, &found.note_id, &ids).await?;
            let now = core::now();
            let mut tx = store.db.begin().await?;
            let restarted = sqlx::query(
                "UPDATE pending_operations SET state='started' WHERE operation_id=$1 AND state='failed'",
            )
            .bind(&operation_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if restarted != 1 {
                return Err(SaveError::OperationInProgress);
            }
            for (_, revision_id, document_id) in &ids {
                sqlx::query("UPDATE note_save_revisions SET document_id=$1 WHERE operation_id=$2 AND revision_id=$3")
                    .bind(document_id).bind(&operation_id).bind(revision_id)
                    .execute(&mut *tx).await?;
            }
            sqlx::query(
                "UPDATE note_save_operations SET attempts=0,updated_at=$1 WHERE operation_id=$2",
            )
            .bind(now)
            .bind(&operation_id)
            .execute(&mut *tx)
            .await?;
            hold_files(&mut tx, &operation_id, owner, &items).await?;
            tx.commit().await?;
            return Ok(Begin::Run(Pending {
                operation_id,
                owner_id: owner.clone(),
                note_id: found.note_id,
                target: request.target.clone(),
                items,
                metadata: request.metadata.clone(),
            }));
        }
        let note_id = match &request.target {
            Target::Update { note_id, .. } => note_id.clone(),
            Target::Create { .. } => core::id(),
        };
        let ids: Vec<(Representation, String, String)> = request
            .representations
            .iter()
            .map(|r| (r.representation, core::id(), core::id()))
            .collect();
        let items = prepare(store, request, &note_id, &ids).await?;
        let now = core::now();
        let mut tx = store.db.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO pending_operations(operation_id,owner_id,state,created_at) VALUES($1,$2,'started',$3) ON CONFLICT(operation_id) DO NOTHING",
        )
        .bind(&operation_id)
        .bind(owner)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted != 1 {
            // The same operation started at the same moment.
            return Err(SaveError::OperationInProgress);
        }
        sqlx::query(
            "INSERT INTO note_save_operations(operation_id,note_id,request_hash,attempts,updated_at) VALUES($1,$2,$3,0,$4)",
        )
        .bind(&operation_id)
        .bind(&note_id)
        .bind(&request_hash)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        for item in &items {
            sqlx::query(
                "INSERT INTO note_save_revisions(operation_id,representation,revision_id,document_id) VALUES($1,$2,$3,$4)",
            )
            .bind(&operation_id)
            .bind(item.representation.as_str())
            .bind(&item.revision_id)
            .bind(&item.document_id)
            .execute(&mut *tx)
            .await?;
        }
        hold_files(&mut tx, &operation_id, owner, &items).await?;
        tx.commit().await?;
        Ok(Begin::Run(Pending {
            operation_id,
            owner_id: owner.clone(),
            note_id,
            target: request.target.clone(),
            items,
            metadata: request.metadata.clone(),
        }))
    }

    /// Step (b): write one new CouchDB document per representation.
    pub async fn write_documents(&self, store: &Store) -> R<()> {
        // Fence reconciliation while these writes run. Lock order matches commit.
        let mut tx = store.db.begin().await?;
        self.claim(&mut tx).await?;
        for item in &self.items {
            let body = json!({
                "type": "note_revision",
                "note_id": self.note_id,
                "representation": item.representation.as_str(),
                "revision_id": item.revision_id,
                "schema_name": item.representation.family().name(),
                "schema_version": item.schema_version,
                "content_hash": item.content_hash,
                "body": item.document,
            });
            store.docs.put_immutable(&item.document_id, &body).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    // A failed attempt can restart with new document ids. A stale Pending must
    // never write or commit the documents from an earlier attempt.
    async fn claim(&self, tx: &mut sqlx::Transaction<'_, sqlx::Any>) -> R<()> {
        let claimed = sqlx::query(
            "UPDATE pending_operations SET state='started' WHERE operation_id=$1 AND state='started'",
        ).bind(&self.operation_id).execute(&mut **tx).await?.rows_affected();
        if claimed != 1 {
            return Err(SaveError::OperationInProgress);
        }
        for item in &self.items {
            let row = sqlx::query("SELECT document_id FROM note_save_revisions WHERE operation_id=$1 AND revision_id=$2")
                .bind(&self.operation_id).bind(&item.revision_id).fetch_optional(&mut **tx).await?;
            if row.is_none_or(|row| row.get::<String, _>("document_id") != item.document_id) {
                return Err(SaveError::OperationInProgress);
            }
        }
        Ok(())
    }

    /// Step (c): every referenced file is `ready`, verified in FSM and owned by the caller.
    pub async fn confirm_files(&self, store: &Store) -> R<()> {
        for item in &self.items {
            for file in &item.files {
                let row = sqlx::query(
                    "SELECT file_id FROM files WHERE file_id=$1 AND owner_id=$2 AND status='ready' AND fsm_reference IS NOT NULL",
                )
                .bind(&file.file_id)
                .bind(&self.owner_id)
                .fetch_optional(&store.db)
                .await?;
                if row.is_none() {
                    return Err(DocumentError::new("missing_file", file.path.clone()).into());
                }
            }
        }
        Ok(())
    }

    /// Step (d): switch the PostgreSQL pointers in one transaction.
    pub async fn commit(&self, store: &Store) -> R<SaveResult> {
        let now = core::now();
        let mut tx = store.db.begin().await?;
        self.claim(&mut tx).await?;
        let revision = match &self.target {
            Target::Update { note_id, if_match } => {
                let changed = sqlx::query(
                    "UPDATE notes SET revision=revision+1,updated_at=$1 WHERE note_id=$2 AND owner_id=$3 AND revision=$4",
                )
                .bind(now)
                .bind(note_id)
                .bind(&self.owner_id)
                .bind(*if_match)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if changed != 1 {
                    let current = note_revision(&mut *tx, note_id, &self.owner_id).await?;
                    return Err(current
                        .map(|current_revision| SaveError::Conflict { current_revision })
                        .unwrap_or(SaveError::NotFound));
                }
                if_match + 1
            }
            Target::Create {
                client_note_id,
                main_representation,
            } => {
                let created = sqlx::query(
                    "INSERT INTO notes(note_id,owner_id,client_note_id,main_representation,revision,created_at,updated_at) VALUES($1,$2,$3,$4,1,$5,$5) ON CONFLICT DO NOTHING",
                )
                .bind(&self.note_id)
                .bind(&self.owner_id)
                .bind(client_note_id)
                .bind(main_representation.as_str())
                .bind(now)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if created != 1 {
                    let row = sqlx::query(
                        "SELECT note_id FROM notes WHERE owner_id=$1 AND client_note_id=$2",
                    )
                    .bind(&self.owner_id)
                    .bind(client_note_id)
                    .fetch_optional(&mut *tx)
                    .await?;
                    return Err(match row {
                        Some(row) => SaveError::NoteExists {
                            note_id: row.get("note_id"),
                        },
                        None => SaveError::Unavailable,
                    });
                }
                1
            }
        };
        for item in &self.items {
            let representation = item.representation.as_str();
            sqlx::query(
                "INSERT INTO note_revisions(revision_id,note_id,representation,document_id,schema_name,schema_version,content_hash,source_revision_id,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,NULL,$8)",
            )
            .bind(&item.revision_id)
            .bind(&self.note_id)
            .bind(representation)
            .bind(&item.document_id)
            .bind(item.representation.family().name())
            .bind(item.schema_version)
            .bind(&item.content_hash)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO note_current_revisions(note_id,representation,revision_id) VALUES($1,$2,$3) ON CONFLICT(note_id,representation) DO UPDATE SET revision_id=excluded.revision_id",
            )
            .bind(&self.note_id)
            .bind(representation)
            .bind(&item.revision_id)
            .execute(&mut *tx)
            .await?;
            for file in &item.files {
                let added = sqlx::query(
                    "INSERT INTO file_references(file_id,reference_kind,reference_id) SELECT file_id,'note_revision',$2 FROM files WHERE file_id=$1 AND owner_id=$3 AND status='ready' AND fsm_reference IS NOT NULL",
                )
                .bind(&file.file_id)
                .bind(&item.revision_id)
                .bind(&self.owner_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if added != 1 {
                    return Err(DocumentError::new("missing_file", file.path.clone()).into());
                }
            }
        }
        // The revision references now protect the files.
        release_files(&mut tx, &self.operation_id).await?;
        if let Some(metadata) = &self.metadata {
            save_metadata(&mut tx, &self.note_id, &self.owner_id, metadata).await?;
        }
        sqlx::query(
            "INSERT INTO note_changes(owner_id,note_id,change_kind,changed_at) VALUES($1,$2,'upsert',$3)",
        )
        .bind(&self.owner_id)
        .bind(&self.note_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE note_save_operations SET result_revision=$1,updated_at=$2 WHERE operation_id=$3",
        )
        .bind(revision)
        .bind(now)
        .bind(&self.operation_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut revisions: Vec<SavedRevision> = self
            .items
            .iter()
            .map(|i| SavedRevision {
                representation: i.representation,
                revision_id: i.revision_id.clone(),
            })
            .collect();
        revisions.sort_by_key(|r| r.representation);
        Ok(SaveResult {
            note_id: self.note_id.clone(),
            revision,
            revisions,
        })
    }

    /// Step (e): mark the operation done.
    pub async fn finish(&self, store: &Store) -> R<()> {
        mark_done(store, &self.operation_id).await
    }

    /// Stop this operation after a failure: remove its documents unless the
    /// commit happened. Returns the result when the commit happened.
    pub async fn abandon(&self, store: &Store) -> R<Option<SaveResult>> {
        Ok(
            match settle_inner(store, &self.operation_id, core::now(), Some(&self.items)).await? {
                Settled::Done(result) => Some(result),
                _ => None,
            },
        )
    }

    /// The operation id in `pending_operations` (derived from the owner and the key).
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }
    /// The CouchDB document ids that this operation writes.
    pub fn document_ids(&self) -> Vec<String> {
        self.items.iter().map(|i| i.document_id.clone()).collect()
    }
}

/// The result of settling a stopped operation.
#[derive(Debug, PartialEq, Eq)]
pub enum Settled {
    /// The commit happened; the operation is now `done`.
    Done(SaveResult),
    /// The documents are removed; the operation is now `failed`.
    Failed,
    /// A document could not be removed; the operation stays `abandoned`.
    Waiting,
    /// The operation is not `started` or `abandoned`.
    Skipped,
}

/// Take a stopped operation (`started` or `abandoned`) and finish it safely:
/// mark it done when its commit happened, else remove the CouchDB documents
/// that no revision references and mark it failed.
pub async fn settle(store: &Store, operation_id: &str, now: i64) -> R<Settled> {
    settle_inner(store, operation_id, now, None).await
}
async fn settle_inner(
    store: &Store,
    operation_id: &str,
    now: i64,
    expected: Option<&[Prepared]>,
) -> R<Settled> {
    let mut tx = store.db.begin().await?;
    let taken = sqlx::query(
        "UPDATE pending_operations SET state='abandoned' WHERE operation_id=$1 AND state IN ('started','abandoned')",
    )
    .bind(operation_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if taken != 1 {
        return Ok(Settled::Skipped);
    }
    // A commit that is in flight holds the row; after the update above it can no
    // longer commit, and a commit that finished before is visible here.
    let found = stored_on(&mut tx, operation_id)
        .await?
        .ok_or(SaveError::NotFound)?;
    if expected.is_some_and(|items| {
        items.iter().any(|item| {
            !found.revisions.iter().any(|(_, revision_id, document_id)| {
                revision_id == &item.revision_id && document_id == &item.document_id
            })
        })
    }) {
        // Roll back the claim: this handle belongs to an earlier failed attempt.
        return Ok(Settled::Skipped);
    }
    if found.result_revision.is_some() {
        sqlx::query("UPDATE pending_operations SET state='done' WHERE operation_id=$1")
            .bind(operation_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(Settled::Done(found.result()?));
    }
    for (_, _, document_id) in &found.revisions {
        let referenced = sqlx::query("SELECT revision_id FROM note_revisions WHERE document_id=$1")
            .bind(document_id)
            .fetch_optional(&mut *tx)
            .await?;
        if referenced.is_some() {
            continue;
        }
        if remove_document(store, document_id).await.is_err() {
            tracing::warn!(
                "Stopped note save cleanup failed; bounded retries can require operator review"
            );
            sqlx::query(
                "UPDATE note_save_operations SET attempts=attempts+1,updated_at=$1 WHERE operation_id=$2",
            )
            .bind(now)
            .bind(operation_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(Settled::Waiting);
        }
    }
    sqlx::query(
        "UPDATE pending_operations SET state='failed' WHERE operation_id=$1 AND state='abandoned'",
    )
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    release_files(&mut tx, operation_id).await?;
    sqlx::query("UPDATE note_save_operations SET updated_at=$1 WHERE operation_id=$2")
        .bind(now)
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Settled::Failed)
}

/// Protect the files of a running save with `pending_operation` references, so a
/// file release waits (409) until the save commits or fails. A file that is no
/// longer ready for the owner stops the save before any document is written.
async fn hold_files(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    operation_id: &str,
    owner: &str,
    items: &[Prepared],
) -> R<()> {
    release_files(tx, operation_id).await?;
    let mut held: Vec<&str> = vec![];
    for file in items.iter().flat_map(|i| &i.files) {
        if held.contains(&file.file_id.as_str()) {
            continue;
        }
        let added = sqlx::query(
            "INSERT INTO file_references(file_id,reference_kind,reference_id) SELECT file_id,'pending_operation',$2 FROM files WHERE file_id=$1 AND owner_id=$3 AND status='ready' AND fsm_reference IS NOT NULL",
        )
        .bind(&file.file_id)
        .bind(operation_id)
        .bind(owner)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        if added != 1 {
            return Err(DocumentError::new("missing_file", file.path.clone()).into());
        }
        held.push(&file.file_id);
    }
    Ok(())
}

/// Remove the `pending_operation` references of a save that committed or failed.
async fn release_files(tx: &mut sqlx::Transaction<'_, sqlx::Any>, operation_id: &str) -> R<()> {
    sqlx::query(
        "DELETE FROM file_references WHERE reference_kind='pending_operation' AND reference_id=$1",
    )
    .bind(operation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Remove revisions created before `before` that are not current, not published
/// and not the source of another revision, with their file references and their
/// CouchDB documents. At most `batch` revisions. Returns the number removed.
pub async fn prune_revisions(store: &Store, before: i64, batch: i64) -> R<usize> {
    const UNREFERENCED: &str = "NOT EXISTS(SELECT 1 FROM note_current_revisions c WHERE c.revision_id=r.revision_id) AND NOT EXISTS(SELECT 1 FROM note_published_representations p WHERE p.revision_id=r.revision_id) AND NOT EXISTS(SELECT 1 FROM note_revisions s WHERE s.source_revision_id=r.revision_id)";
    let rows = sqlx::query(&format!(
        "SELECT r.revision_id,r.document_id FROM note_revisions r WHERE r.created_at<$1 AND {UNREFERENCED} ORDER BY r.created_at,r.revision_id LIMIT $2"
    ))
    .bind(before)
    .bind(batch)
    .fetch_all(&store.db)
    .await?;
    let mut removed = 0;
    for row in rows {
        let revision_id: String = row.get("revision_id");
        let document_id: String = row.get("document_id");
        let done: R<bool> = async {
            let mut tx = store.db.begin().await?;
            sqlx::query(
                "DELETE FROM file_references WHERE reference_kind='note_revision' AND reference_id=$1",
            )
            .bind(&revision_id)
            .execute(&mut *tx)
            .await?;
            // The row stays locked until commit, so a publication of it cannot start now.
            let deleted = sqlx::query(&format!(
                "DELETE FROM note_revisions WHERE revision_id=$1 AND EXISTS(SELECT 1 FROM note_revisions r WHERE r.revision_id=$1 AND {UNREFERENCED})"
            ))
            .bind(&revision_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if deleted != 1 {
                return Ok(false);
            }
            // Keep durable cleanup work before removing the SQL revision. A
            // crash must never leave a live revision pointing at deleted content.
            sqlx::query("INSERT INTO note_revision_cleanup(document_id,attempts,created_at) VALUES($1,0,$2) ON CONFLICT(document_id) DO NOTHING")
                .bind(&document_id).bind(core::now()).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(true)
        }
        .await;
        match done {
            Ok(true) => removed += 1,
            Ok(false) => {}
            Err(_) => tracing::warn!("Old revision cleanup is pending; it will retry"),
        }
    }
    Ok(removed)
}

/// Remove documents whose revision deletion has committed. Work survives a crash
/// after deletion of either the SQL row or the CouchDB document.
pub async fn cleanup_pruned(store: &Store, now: i64, batch: i64, max_attempts: i64) -> R<()> {
    let rows = sqlx::query("SELECT document_id FROM note_revision_cleanup WHERE attempts<$1 AND next_attempt_at<=$3 ORDER BY next_attempt_at,document_id LIMIT $2")
        .bind(max_attempts).bind(batch).bind(now).fetch_all(&store.db).await?;
    for row in rows {
        let id: String = row.get("document_id");
        if sqlx::query("SELECT revision_id FROM note_revisions WHERE document_id=$1")
            .bind(&id)
            .fetch_optional(&store.db)
            .await?
            .is_some()
        {
            // A protected document must also leave this queue, so it cannot
            // occupy every bounded batch. A later prune can enqueue it again.
            sqlx::query("DELETE FROM note_revision_cleanup WHERE document_id=$1")
                .bind(&id)
                .execute(&store.db)
                .await?;
            continue;
        }
        match remove_document(store, &id).await {
            Ok(()) => {
                sqlx::query("DELETE FROM note_revision_cleanup WHERE document_id=$1")
                    .bind(&id)
                    .execute(&store.db)
                    .await?;
            }
            Err(_) => {
                tracing::warn!(
                    "Pruned document cleanup failed; bounded retries can require operator review"
                );
                sqlx::query(
                    "UPDATE note_revision_cleanup SET attempts=attempts+1,next_attempt_at=$2 WHERE document_id=$1",
                )
                .bind(&id)
                .bind(now + 30)
                .execute(&store.db)
                .await?;
            }
        }
    }
    Ok(())
}

/// Delete a CouchDB document. A missing document counts as removed.
async fn remove_document(store: &Store, document_id: &str) -> R<()> {
    match store.docs.delete(document_id).await {
        Ok(()) => Ok(()),
        Err(e) if e.0 == StatusCode::NOT_FOUND => Ok(()),
        Err(_) => Err(SaveError::Unavailable),
    }
}

async fn mark_done(store: &Store, operation_id: &str) -> R<()> {
    let changed = sqlx::query(
        "UPDATE pending_operations SET state='done' WHERE operation_id=$1 AND state IN ('started','abandoned') AND EXISTS(SELECT 1 FROM note_save_operations s WHERE s.operation_id=$1 AND s.result_revision IS NOT NULL)",
    )
    .bind(operation_id)
    .execute(&store.db)
    .await?.rows_affected();
    if changed != 1 {
        let found = stored(store, operation_id)
            .await?
            .ok_or(SaveError::NotFound)?;
        if found.state != "done" || found.result_revision.is_none() {
            return Err(SaveError::OperationInProgress);
        }
    }
    Ok(())
}

async fn note_revision<'e, E>(db: E, note_id: &str, owner_id: &str) -> R<Option<i64>>
where
    E: sqlx::Executor<'e, Database = sqlx::Any>,
{
    Ok(
        sqlx::query("SELECT revision FROM notes WHERE note_id=$1 AND owner_id=$2")
            .bind(note_id)
            .bind(owner_id)
            .fetch_optional(db)
            .await?
            .map(|r| r.get::<i64, _>("revision")),
    )
}

/// The stored rows of one operation.
struct Stored {
    state: String,
    request_hash: String,
    note_id: String,
    result_revision: Option<i64>,
    revisions: Vec<(Representation, String, String)>,
}
impl Stored {
    fn result(&self) -> R<SaveResult> {
        Ok(SaveResult {
            note_id: self.note_id.clone(),
            revision: self.result_revision.ok_or(SaveError::Unavailable)?,
            revisions: self
                .revisions
                .iter()
                .map(|(representation, revision_id, _)| SavedRevision {
                    representation: *representation,
                    revision_id: revision_id.clone(),
                })
                .collect(),
        })
    }
    fn ids(&self) -> Vec<(Representation, String, String)> {
        self.revisions.clone()
    }
}
async fn stored(store: &Store, operation_id: &str) -> R<Option<Stored>> {
    let mut db = store.db.acquire().await?;
    stored_on(&mut db, operation_id).await
}
async fn stored_on(db: &mut sqlx::AnyConnection, operation_id: &str) -> R<Option<Stored>> {
    let Some(row) = sqlx::query(
        "SELECT p.state,s.request_hash,s.note_id,s.result_revision FROM pending_operations p JOIN note_save_operations s ON s.operation_id=p.operation_id WHERE p.operation_id=$1",
    )
    .bind(operation_id)
    .fetch_optional(&mut *db)
    .await?
    else {
        return Ok(None);
    };
    let revisions = sqlx::query(
        "SELECT representation,revision_id,document_id FROM note_save_revisions WHERE operation_id=$1 ORDER BY representation",
    )
    .bind(operation_id)
    .fetch_all(&mut *db)
    .await?
    .into_iter()
    .map(|r| {
        let representation = Representation::parse(&r.get::<String, _>("representation"))
            .ok_or(SaveError::Unavailable)?;
        Ok((representation, r.get("revision_id"), r.get("document_id")))
    })
    .collect::<R<Vec<_>>>()?;
    Ok(Some(Stored {
        state: row.get("state"),
        request_hash: row.get("request_hash"),
        note_id: row.get("note_id"),
        result_revision: row.try_get::<Option<i64>, _>("result_revision")?,
        revisions,
    }))
}

fn token(s: &str) -> bool {
    (1..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_graphic())
}

fn check_shape(request: &SaveRequest) -> R<()> {
    if !token(&request.operation_key) {
        return Err(SaveError::InvalidRequest("invalid_operation_key"));
    }
    if request.owner_id.is_empty() {
        return Err(SaveError::NotFound);
    }
    let metadata_only = matches!(request.target, Target::Update { .. })
        && request
            .metadata
            .as_ref()
            .is_some_and(|m| m.title.is_some() || m.tags.is_some() || m.project_ids.is_some());
    if request.representations.is_empty() && !metadata_only {
        return Err(SaveError::InvalidRequest("empty_save"));
    }
    let mut seen: Vec<Representation> = request
        .representations
        .iter()
        .map(|r| r.representation)
        .collect();
    seen.sort();
    seen.dedup();
    if seen.len() != request.representations.len() {
        return Err(SaveError::InvalidRequest("duplicate_representation"));
    }
    match &request.target {
        Target::Create {
            client_note_id,
            main_representation,
        } => {
            if !token(client_note_id) {
                return Err(SaveError::InvalidRequest("invalid_client_note_id"));
            }
            if !seen.contains(main_representation) {
                return Err(SaveError::InvalidRequest("main_representation_missing"));
            }
            if request
                .representations
                .iter()
                .any(|r| matches!(r.content, Content::Operation(_)))
            {
                return Err(SaveError::InvalidRequest("missing_base"));
            }
        }
        Target::Update { note_id, .. } => {
            if note_id.is_empty() {
                return Err(SaveError::NotFound);
            }
            if request
                .metadata
                .as_ref()
                .is_some_and(|m| m.project_ids.is_some())
            {
                return Err(SaveError::InvalidRequest("projects_on_update"));
            }
        }
    }
    Ok(())
}

/// Step (a) work: ownership, expected revision, document and rule validation.
async fn prepare(
    store: &Store,
    request: &SaveRequest,
    note_id: &str,
    ids: &[(Representation, String, String)],
) -> R<Vec<Prepared>> {
    let owner = &request.owner_id;
    match &request.target {
        Target::Update { note_id, if_match } => {
            match note_revision(&store.db, note_id, owner).await? {
                None => return Err(SaveError::NotFound),
                Some(current_revision) if current_revision != *if_match => {
                    return Err(SaveError::Conflict { current_revision });
                }
                Some(_) => {}
            }
        }
        Target::Create { client_note_id, .. } => {
            if let Some(row) =
                sqlx::query("SELECT note_id FROM notes WHERE owner_id=$1 AND client_note_id=$2")
                    .bind(owner)
                    .bind(client_note_id)
                    .fetch_optional(&store.db)
                    .await?
            {
                return Err(SaveError::NoteExists {
                    note_id: row.get("note_id"),
                });
            }
        }
    }
    for project_id in request
        .metadata
        .iter()
        .flat_map(|m| m.project_ids.iter().flatten())
    {
        if !owns_project(&store.db, project_id, owner).await? {
            return Err(SaveError::UnknownProject);
        }
    }
    let mut items = vec![];
    for input in &request.representations {
        let family = input.representation.family();
        let (document, version) = match &input.content {
            Content::Document(document) => (document.clone(), input.schema_version),
            Content::Operation(operation) => {
                let (base, version) = base_document(store, note_id, input.representation).await?;
                if input.schema_version.is_some_and(|v| v != version) {
                    return Err(SaveError::InvalidRequest("schema_version_mismatch"));
                }
                let edited = editor_rules::check_edit(
                    family,
                    version,
                    &base,
                    Some(operation),
                    &NOTE_LIMITS,
                )?;
                (edited.document, Some(version))
            }
        };
        let (schema_version, refs) = note_documents::validate_document_files(
            store,
            family.name(),
            version,
            FileScope::Owner(owner),
            &document,
            &NOTE_LIMITS,
        )
        .await?;
        if family != Family::Typed {
            editor_rules::check_placement(&document)?;
        }
        let mut files: Vec<FileRef> = vec![];
        for r in refs {
            if !files.iter().any(|f| f.file_id == r.file_id) {
                files.push(r);
            }
        }
        let (_, revision_id, document_id) = ids
            .iter()
            .find(|(r, _, _)| *r == input.representation)
            .cloned()
            .ok_or(SaveError::Unavailable)?;
        let mut text = String::new();
        canonical(&document, &mut text);
        items.push(Prepared {
            representation: input.representation,
            schema_version,
            revision_id,
            document_id,
            content_hash: sha_hex(text.as_bytes()),
            document,
            files,
        });
    }
    items.sort_by_key(|i| i.representation);
    Ok(items)
}

/// The document body and schema version of the current revision of a representation.
async fn base_document(
    store: &Store,
    note_id: &str,
    representation: Representation,
) -> R<(Value, i64)> {
    let row = sqlx::query(
        "SELECT r.document_id,r.schema_version FROM note_current_revisions c JOIN note_revisions r ON r.revision_id=c.revision_id WHERE c.note_id=$1 AND c.representation=$2",
    )
    .bind(note_id)
    .bind(representation.as_str())
    .fetch_optional(&store.db)
    .await?
    .ok_or(SaveError::InvalidRequest("missing_base"))?;
    let document = store
        .docs
        .get(&row.get::<String, _>("document_id"))
        .await
        .map_err(|_| SaveError::Unavailable)?;
    Ok((document["body"].clone(), row.get("schema_version")))
}

/// Hash of the request content. Key order and representation order do not matter.
fn request_hash(request: &SaveRequest) -> String {
    let target = match &request.target {
        Target::Create {
            client_note_id,
            main_representation,
        } => json!({"create": client_note_id, "main": main_representation.as_str()}),
        Target::Update { note_id, if_match } => json!({"update": note_id, "if_match": if_match}),
    };
    let mut representations: Vec<&RepresentationInput> = request.representations.iter().collect();
    representations.sort_by_key(|r| r.representation);
    let representations: Vec<Value> = representations
        .into_iter()
        .map(|r| {
            let (kind, content) = match &r.content {
                Content::Document(v) => ("document", v),
                Content::Operation(v) => ("operation", v),
            };
            json!({"representation": r.representation.as_str(), "schema_version": r.schema_version, kind: content})
        })
        .collect();
    let mut hashed = json!({"target": target, "representations": representations});
    // Saves without metadata keep the hash of the earlier request format.
    if let Some(m) = &request.metadata {
        hashed["metadata"] =
            json!({"title": m.title, "tags": m.tags, "project_ids": m.project_ids});
    }
    let mut text = String::new();
    canonical(&hashed, &mut text);
    sha_hex(text.as_bytes())
}

async fn owns_project<'e, E>(db: E, project_id: &str, owner_id: &str) -> R<bool>
where
    E: sqlx::Executor<'e, Database = sqlx::Any>,
{
    Ok(
        sqlx::query("SELECT project_id FROM projects WHERE project_id=$1 AND owner_id=$2")
            .bind(project_id)
            .bind(owner_id)
            .fetch_optional(db)
            .await?
            .is_some(),
    )
}

/// Write title, tags and new memberships inside the commit transaction.
async fn save_metadata(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    note_id: &str,
    owner_id: &str,
    metadata: &Metadata,
) -> R<()> {
    if let Some(title) = &metadata.title {
        sqlx::query("DELETE FROM note_titles WHERE note_id=$1")
            .bind(note_id)
            .execute(&mut **tx)
            .await?;
        if !title.is_empty() {
            sqlx::query("INSERT INTO note_titles(note_id,title) VALUES($1,$2)")
                .bind(note_id)
                .bind(title)
                .execute(&mut **tx)
                .await?;
        }
    }
    if let Some(tags) = &metadata.tags {
        sqlx::query("DELETE FROM note_tags WHERE note_id=$1")
            .bind(note_id)
            .execute(&mut **tx)
            .await?;
        for name in tags {
            let normalized = crate::domain::names::normalize(name);
            sqlx::query("INSERT INTO tags(tag_id,name,normalized_name) VALUES($1,$2,$3) ON CONFLICT(normalized_name) DO NOTHING")
                .bind(core::id())
                .bind(name)
                .bind(&normalized)
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO note_tags(note_id,tag_id) SELECT $1,tag_id FROM tags WHERE normalized_name=$2 ON CONFLICT DO NOTHING")
                .bind(note_id)
                .bind(&normalized)
                .execute(&mut **tx)
                .await?;
        }
    }
    for project_id in metadata.project_ids.iter().flatten() {
        // Both foreign keys carry the owner, so a membership never crosses owners.
        let added = sqlx::query("INSERT INTO project_notes(project_id,note_id,owner_id) SELECT project_id,$2,owner_id FROM projects WHERE project_id=$1 AND owner_id=$3 ON CONFLICT DO NOTHING")
            .bind(project_id)
            .bind(note_id)
            .bind(owner_id)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if added != 1 && !owns_project(&mut **tx, project_id, owner_id).await? {
            return Err(SaveError::UnknownProject);
        }
    }
    Ok(())
}

/// JSON text with object keys in sorted order at every level.
fn canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                canonical(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

fn sha_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
