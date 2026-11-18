use crate::{
    core::{self, Error, NOTE_LIMITS, ROOM_SECONDS, Result, UPLOAD_BYTES},
    dal::Store,
    domain::{
        editor_rules,
        note_documents::{self, Family, FileScope},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

/// Completed temporary uploads. These are not positioned editor objects.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct File {
    pub id: String,
    pub file_key: String,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Room {
    pub code: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub revision: i64,
    pub schema_version: i64,
    pub document: Value,
    pub item_revisions: HashMap<String, i64>,
    pub files: Vec<File>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Create {
    pub schema_version: i64,
    pub document: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemWrite {
    /// Zero means a new item. Existing items use their own revision.
    pub expected_revision: i64,
    pub item: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemPatch {
    pub expected_revision: i64,
    pub changes: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomPatch {
    pub expected_revision: i64,
    pub operation: Value,
}
#[derive(Debug)]
pub enum Failure {
    Error(Error),
    StaleItem {
        item: Value,
        item_revision: i64,
        revision: i64,
    },
}
pub type EditResult<T> = std::result::Result<T, Failure>;
impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Self::Error(error)
    }
}
pub fn expired() -> Error {
    Error(
        axum::http::StatusCode::GONE,
        "room_expired: This room has expired".into(),
    )
}
impl Room {
    pub fn public(&self) -> Value {
        json!({"code":self.code,"created_at":self.created_at,"expires_at":self.expires_at,
            "revision":self.revision,"schema_version":self.schema_version,
            "document":self.document,"item_revisions":self.item_revisions,
            "max_file_bytes":UPLOAD_BYTES,"max_ip_bytes":UPLOAD_BYTES,
            "files":self.files.iter().map(|f|json!({"id":f.id,"filename":f.filename,
                "content_type":f.content_type,"size_bytes":f.size_bytes,
                "url":format!("/instant/{}/files/{}",self.code,f.id)})).collect::<Vec<_>>()})
    }
    fn item(&self, id: &str) -> Option<&Value> {
        self.document["items"]
            .as_array()?
            .iter()
            .find(|i| i["id"].as_str() == Some(id))
    }
    fn stale(&self, id: &str) -> Failure {
        Failure::StaleItem {
            item: self.item(id).cloned().unwrap_or(Value::Null),
            item_revision: self.item_revisions.get(id).copied().unwrap_or(0),
            revision: self.revision,
        }
    }
}

pub async fn validate(store: &Store, room: &Room) -> Result<()> {
    let files: HashSet<_> = room.files.iter().map(|f| f.id.clone()).collect();
    note_documents::validate_document(
        store,
        "instant-room",
        Some(room.schema_version),
        FileScope::Room(&files),
        &room.document,
        &NOTE_LIMITS,
    )
    .await
    .map_err(Error::from)?;
    editor_rules::check_edit(
        Family::Instant,
        room.schema_version,
        &room.document,
        None,
        &NOTE_LIMITS,
    )
    .map_err(Error::from)?;
    if room.item_revisions.len() > NOTE_LIMITS.max_items || room.files.len() > NOTE_LIMITS.max_items
    {
        return Err(Error::bad(
            "too_many_items: The room has reached its item limit",
        ));
    }
    // Retain the existing aggregate room text ceiling in addition to the document limit.
    fn text_bytes(v: &Value) -> usize {
        match v {
            Value::String(s) => s.len(),
            Value::Array(v) => v.iter().map(text_bytes).sum(),
            Value::Object(m) => m.values().map(text_bytes).sum(),
            _ => 0,
        }
    }
    if text_bytes(&room.document) > UPLOAD_BYTES as usize {
        return Err(Error::bad(
            "room_text_limit: The room text limit has been reached",
        ));
    }
    Ok(())
}
pub async fn create(store: &Store, code: &str, input: Create, now: i64) -> Result<Room> {
    if !core::slug(code) {
        return Err(Error::bad(
            "invalid_room_code: Use letters, digits, hyphens or underscores",
        ));
    }
    let room = Room {
        code: code.into(),
        created_at: now,
        expires_at: now + ROOM_SECONDS,
        revision: 0,
        schema_version: input.schema_version,
        document: input.document,
        item_revisions: HashMap::new(),
        files: vec![],
    };
    validate(store, &room).await?;
    let mut room = room;
    for item in room.document["items"].as_array().unwrap() {
        room.item_revisions
            .insert(item["id"].as_str().unwrap().into(), 1);
    }
    store.rooms.create(room, now).await
}

/// Item writes retry a whole-room CAS against the latest state. Validation on every
/// retry prevents independent edits from combining into an invalid layout.
async fn change(
    store: &Store,
    code: &str,
    target: Option<(&str, i64)>,
    expected_room: Option<i64>,
    operation: Value,
    now: i64,
) -> EditResult<Room> {
    for _ in 0..core::ROOM_CAS_ATTEMPTS {
        let previous = store.rooms.get(code, now).await?;
        if let Some((id, expected)) = target {
            if previous.item_revisions.get(id).copied().unwrap_or(0) != expected {
                return Err(previous.stale(id));
            }
            if expected > 0 && previous.item(id).is_none() {
                return Err(previous.stale(id));
            }
            if operation["item"]["kind"] == "group"
                || previous.item(id).is_some_and(|i| i["kind"] == "group")
            {
                return Err(Error::bad(
                    "room_operation_required: Edit groups with a room operation",
                )
                .into());
            }
            // An item in a group needs the room revision, since its geometry affects
            // every group member and the group's outside collision boundary.
            if previous.document["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| {
                    item["kind"] == "group"
                        && item["members"]
                            .as_array()
                            .is_some_and(|members| members.iter().any(|member| member == id))
                })
            {
                return Err(Error::bad(
                    "room_operation_required: Edit grouped members with a room operation",
                )
                .into());
            }
        }
        if expected_room.is_some_and(|expected| expected != previous.revision) {
            return Err(Error::conflict(
                "room_revision_conflict: The room changed; reload before editing",
            )
            .into());
        }
        let edited = editor_rules::check_edit(
            Family::Instant,
            previous.schema_version,
            &previous.document,
            Some(&operation),
            &NOTE_LIMITS,
        )
        .map_err(Error::from)?;
        let mut next = previous.clone();
        next.document = edited.document;
        // Deleted ids retain only a revision, not content. Reuse cannot accept stale writes.
        let index = |document: &Value| -> HashMap<String, Value> {
            document["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| (item["id"].as_str().unwrap().to_owned(), item.clone()))
                .collect()
        };
        let previous_items = index(&previous.document);
        let next_items = index(&next.document);
        let ids: HashSet<_> = previous_items.keys().chain(next_items.keys()).collect();
        for id in ids {
            if previous_items.get(id) != next_items.get(id) {
                if !previous_items.contains_key(id) && previous.item_revisions.contains_key(id) {
                    return Err(Error::conflict(
                        "item_id_reused: Deleted item ids cannot be reused",
                    )
                    .into());
                }
                next.item_revisions.insert(
                    id.clone(),
                    previous.item_revisions.get(id).copied().unwrap_or(0) + 1,
                );
            }
        }
        // Even a no-op accepted write consumes the targeted item's revision.
        if let Some((id, expected)) = target {
            next.item_revisions.insert(id.into(), expected + 1);
        }
        let referenced: HashSet<_> = edited.files.iter().map(|f| f.file_id.as_str()).collect();
        let old_refs = note_documents::check_document(
            Family::Instant,
            previous.schema_version,
            &previous.document,
            &NOTE_LIMITS,
        )
        .map_err(Error::from)?;
        let old_refs: HashSet<_> = old_refs.iter().map(|r| r.file_id.as_str()).collect();
        let removed: Vec<_> = previous
            .files
            .iter()
            .filter(|file| {
                old_refs.contains(file.id.as_str()) && !referenced.contains(file.id.as_str())
            })
            .cloned()
            .collect();
        next.files.retain(|f| !removed.iter().any(|r| r.id == f.id));
        next.revision += 1;
        validate(store, &next).await?;
        match store.rooms.compare_set(previous.revision, &next, now).await {
            Ok(true) => {
                for file in removed {
                    release_file(store, &file).await;
                }
                return Ok(next);
            }
            Ok(false) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    let current = store.rooms.get(code, now).await?;
    if let Some((id, expected)) = target
        && current.item_revisions.get(id).copied().unwrap_or(0) != expected
    {
        return Err(current.stale(id));
    }
    if expected_room.is_some_and(|expected| expected != current.revision) {
        return Err(Error::conflict(
            "room_revision_conflict: The room changed; reload before editing",
        )
        .into());
    }
    // Exhaustion means contention, not a false stale-item verdict.
    Err(Error(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "room_busy: The room is busy; reload and try again".into(),
    )
    .into())
}

pub async fn add(store: &Store, code: &str, input: ItemWrite, now: i64) -> EditResult<Room> {
    if input.expected_revision != 0 {
        return Err(Error::bad("invalid_item_revision: New items require revision zero").into());
    }
    let id = input.item["id"]
        .as_str()
        .ok_or_else(|| Error::bad("invalid_id: An item id is required"))?
        .to_string();
    change(
        store,
        code,
        Some((&id, 0)),
        None,
        json!({"op":"add","item":input.item}),
        now,
    )
    .await
}
pub async fn replace(
    store: &Store,
    code: &str,
    id: &str,
    input: ItemWrite,
    now: i64,
) -> EditResult<Room> {
    if input.item["id"] != id {
        return Err(Error::bad("immutable_field: The item id cannot change").into());
    }
    require_existing_revision(input.expected_revision)?;
    change(
        store,
        code,
        Some((id, input.expected_revision)),
        None,
        json!({"op":"replace","item":input.item}),
        now,
    )
    .await
}
pub async fn patch_item(
    store: &Store,
    code: &str,
    id: &str,
    input: ItemPatch,
    now: i64,
) -> EditResult<Room> {
    require_existing_revision(input.expected_revision)?;
    change(
        store,
        code,
        Some((id, input.expected_revision)),
        None,
        json!({"op":"update","id":id,"changes":input.changes}),
        now,
    )
    .await
}
pub async fn remove(
    store: &Store,
    code: &str,
    id: &str,
    expected: i64,
    now: i64,
) -> EditResult<Room> {
    require_existing_revision(expected)?;
    change(
        store,
        code,
        Some((id, expected)),
        None,
        json!({"op":"delete","id":id}),
        now,
    )
    .await
}
fn require_existing_revision(expected: i64) -> Result<()> {
    if expected < 1 {
        return Err(Error::bad(
            "invalid_item_revision: Use the current positive item revision",
        ));
    }
    Ok(())
}
pub async fn patch_room(store: &Store, code: &str, input: RoomPatch, now: i64) -> EditResult<Room> {
    change(
        store,
        code,
        None,
        Some(input.expected_revision),
        input.operation,
        now,
    )
    .await
}
async fn release_file(store: &Store, file: &File) {
    let Ok(Some(reservation)) = store.transfers.get(&file.id).await else {
        return;
    };
    let mut deleted = store.files.delete(&file.file_key).await.is_ok();
    if let Ok(files) = store.files.album(&reservation.album).await {
        for file in files {
            deleted &= store.files.delete(&file.key).await.is_ok();
        }
    }
    if deleted && store.transfers.release(&file.id).await.is_err() {
        tracing::warn!("Removed item quota release is pending until room expiry");
    }
}

/// Access lasts only until the original absolute expiry, including streaming bytes.
pub fn remaining(expires_at: i64) -> Result<std::time::Duration> {
    let deadline = std::time::UNIX_EPOCH + std::time::Duration::from_secs(expires_at.max(0) as u64);
    deadline
        .duration_since(std::time::SystemTime::now())
        .map_err(|_| expired())
}
pub fn download_body(body: axum::body::Body, expires_at: i64) -> Result<axum::body::Body> {
    use futures_util::StreamExt;
    let remaining = remaining(expires_at)?;
    Ok(axum::body::Body::from_stream(
        body.into_data_stream()
            .take_until(tokio::time::sleep(remaining)),
    ))
}
