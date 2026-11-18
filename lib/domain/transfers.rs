use crate::{
    core::{self, Error, Result, UPLOAD_BYTES},
    dal::{
        Store,
        transfers::{Reservation, Status},
    },
    domain::{
        files,
        rooms::{self, File},
    },
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub filename: String,
    pub content_type: String,
    pub size: i64,
    pub checksum: String,
}
fn file_error(error: crate::domain::notes::NoteError) -> Error {
    Error(error.status, format!("{}: {}", error.code, error.message))
}
impl Metadata {
    fn validate(self) -> Result<Self> {
        if self.size > UPLOAD_BYTES {
            return Err(Error(
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                "file_too_large: A file must not exceed 50 MB".into(),
            ));
        }
        let mut value = serde_json::to_value(self)
            .map_err(|_| Error::bad("invalid_request: Invalid upload metadata"))?;
        value["purpose"] = json!("attachment");
        let checked = files::Reservation::parse(
            &serde_json::to_vec(&value).map_err(|_| Error::unavailable())?,
        )
        .map_err(file_error)?;
        Ok(Self {
            filename: checked.filename,
            content_type: checked.content_type,
            size: checked.size,
            checksum: checked.checksum,
        })
    }
}
impl Reservation {
    pub fn public(&self) -> Value {
        json!({"file_id":self.id,"filename":self.filename,"content_type":self.content_type,
            "size":self.bytes,"checksum":self.checksum,"status":self.status,"expires_at":self.expires_at,
            "max_file_bytes":UPLOAD_BYTES,"max_ip_bytes":UPLOAD_BYTES,
            "upload_url":format!("/instant/{}/files/{}/content",self.code,self.id),
            "url":if self.status==Status::Ready {Some(format!("/instant/{}/files/{}",self.code,self.id))} else {None}})
    }
}
pub async fn begin(
    store: &Store,
    code: &str,
    ip: &str,
    input: Metadata,
    idempotency_key: Option<&str>,
    now: i64,
) -> Result<Reservation> {
    let input = input.validate()?;
    let room = store.rooms.get(code, now).await?;
    let id = if let Some(key) = idempotency_key {
        if key.is_empty() || key.len() > 128 || !key.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error::bad(
                "invalid_idempotency_key: Use 1 to 128 visible ASCII characters",
            ));
        }
        format!(
            "{:x}",
            Sha256::digest(format!("{code}\0{ip}\0{key}").as_bytes())
        )
    } else {
        core::id()
    };
    if room.released_files.contains(&id) {
        return Err(Error::missing());
    }
    let requested = Reservation {
        id: id.clone(),
        ip: ip.into(),
        bytes: input.size,
        expires_at: room.expires_at,
        album: format!("instant-{id}"),
        file_key: None,
        code: code.into(),
        filename: input.filename,
        content_type: input.content_type,
        checksum: input.checksum,
        status: Status::Reserved,
        attempt: String::new(),
    };
    let saved = store.transfers.reserve(requested.clone(), now).await?;
    if saved.code != requested.code
        || saved.ip != requested.ip
        || saved.bytes != requested.bytes
        || saved.expires_at != requested.expires_at
        || saved.filename != requested.filename
        || saved.content_type != requested.content_type
        || saved.checksum != requested.checksum
    {
        return Err(Error::conflict(
            "idempotency_conflict: This key already names different upload metadata",
        ));
    }
    // Expired records retain cleanup ownership until the task deletes any bytes.
    store.rooms.get(code, now).await?;
    if saved.status == Status::Ready {
        register(store, &saved, now).await?;
    }
    Ok(saved)
}
async fn authorized(
    store: &Store,
    code: &str,
    id: &str,
    ip: Option<&str>,
    now: i64,
) -> Result<Reservation> {
    let room = store.rooms.get(code, now).await?;
    let record = store.transfers.get(id).await?.ok_or_else(Error::missing)?;
    if record.code != code
        || record.expires_at != room.expires_at
        || ip.is_some_and(|ip| record.ip != ip)
    {
        return Err(Error::missing());
    }
    if record.expires_at <= now {
        return Err(rooms::expired());
    }
    Ok(record)
}

/// Retries of a ready upload verify the same immutable bytes without a second
/// reservation or FSM alias. Only the claimed attempt may finalize a pending file.
pub async fn upload<S, B, E>(
    store: &Store,
    code: &str,
    id: &str,
    ip: &str,
    declared_type: Option<&str>,
    body: S,
    now: i64,
) -> Result<Reservation>
where
    S: Stream<Item = std::result::Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
{
    let record = authorized(store, code, id, Some(ip), now).await?;
    if declared_type.map(files::media_type).as_deref() != Some(record.content_type.as_str()) {
        return Err(Error::bad(
            "content_type_mismatch: Content-Type must match the reservation",
        ));
    }
    if record.status == Status::Uploading {
        return Err(Error::conflict(
            "upload_in_progress: An upload of this file is running",
        ));
    }
    let ready = record.status == Status::Ready;
    let claim = if ready {
        record.clone()
    } else {
        let mut claim = record.clone();
        claim.status = Status::Uploading;
        claim.attempt = core::id();
        if !store.transfers.compare_set(&record, &claim, now).await? {
            return Err(Error::conflict(
                "upload_in_progress: Another request claimed this upload",
            ));
        }
        claim
    };
    let result = tokio::time::timeout(rooms::remaining(record.expires_at)?, async {
        let received = files::receive(store, body, record.bytes, !ready)
            .await
            .map_err(file_error)?;
        if let Err(error) = files::verify_received(
            record.bytes,
            &record.checksum,
            &record.content_type,
            &received,
        ) {
            return Err(if ready {
                Error::conflict("file_immutable: A ready file never changes")
            } else {
                file_error(error)
            });
        }
        authorized(store, code, id, Some(ip), core::now()).await?;
        if ready {
            register(store, &record, core::now()).await?;
            return Ok(record.clone());
        }
        // A prior verified attempt can finish in FSM before its response is lost.
        // Reuse its own matching alias; remove extras without touching other references.
        let mut previous = None;
        for file in store.files.album(&record.album).await? {
            if previous.is_none()
                && file.size_bytes == record.bytes
                && (file.checksum_sha256.is_empty() || file.checksum_sha256 == record.checksum)
            {
                previous = Some(file);
            } else {
                store.files.delete(&file.key).await?;
            }
        }
        let file = match previous {
            Some(file) => file,
            None => {
                let path = received.temp.as_ref().ok_or_else(Error::unavailable)?;
                store
                    .files
                    .upload_path(
                        path,
                        &record.filename,
                        &record.content_type,
                        &record.album,
                        record.bytes,
                    )
                    .await?
            }
        };
        if file.size_bytes != record.bytes
            || (!file.checksum_sha256.is_empty() && file.checksum_sha256 != record.checksum)
        {
            let _ = store.files.delete(&file.key).await;
            return Err(Error::bad(
                "storage_checksum_mismatch: Storage did not confirm the verified bytes",
            ));
        }
        let mut completed = claim.clone();
        completed.status = Status::Ready;
        completed.file_key = Some(file.key);
        if !store
            .transfers
            .compare_set(&claim, &completed, core::now())
            .await?
        {
            return Err(Error::conflict(
                "upload_in_progress: This upload attempt no longer owns the reservation",
            ));
        }
        register(store, &completed, core::now()).await?;
        Ok(completed)
    })
    .await
    .unwrap_or_else(|_| Err(rooms::expired()));
    if result.is_err() && !ready {
        // CAS prevents a failed reset from changing an already finalized record.
        let mut failed = claim.clone();
        failed.status = Status::Failed;
        let _ = store
            .transfers
            .compare_set(&claim, &failed, core::now())
            .await;
    }
    result
}
async fn register(store: &Store, record: &Reservation, now: i64) -> Result<()> {
    if record.status != Status::Ready {
        return Err(Error::conflict(
            "file_not_ready: The file has no verified bytes",
        ));
    }
    let key = record.file_key.as_ref().ok_or_else(Error::unavailable)?;
    for _ in 0..core::ROOM_CAS_ATTEMPTS {
        let current = store
            .transfers
            .get(&record.id)
            .await?
            .ok_or_else(Error::missing)?;
        if current.status != Status::Ready || current.file_key != record.file_key {
            return Err(Error::missing());
        }
        let mut room = store.rooms.get(&record.code, now).await?;
        if room.released_files.contains(&record.id) {
            return Err(Error::missing());
        }
        if room.files.iter().any(|file| file.id == record.id) {
            return Ok(());
        }
        let previous = room.revision;
        room.files.push(File {
            id: record.id.clone(),
            file_key: key.clone(),
            filename: record.filename.clone(),
            content_type: record.content_type.clone(),
            size_bytes: record.bytes,
        });
        rooms::validate(store, &room).await?;
        room.revision += 1;
        if store.rooms.compare_set(previous, &room, now).await? {
            return Ok(());
        }
    }
    Err(Error(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "room_busy: The room is busy; retry the upload".into(),
    ))
}
pub async fn download(store: &Store, code: &str, id: &str, now: i64) -> Result<Reservation> {
    let record = authorized(store, code, id, None, now).await?;
    if record.status != Status::Ready || record.file_key.is_none() {
        return Err(Error::conflict(
            "file_not_ready: The file has no verified bytes",
        ));
    }
    let room = store.rooms.get(code, now).await?;
    if room.released_files.contains(id) || !room.files.iter().any(|file| file.id == id) {
        return Err(Error::missing());
    }
    Ok(record)
}
