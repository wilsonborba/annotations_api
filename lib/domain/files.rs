//! Owner managed files (`/me/files`) and published media (`/media/{file_id}`).
//!
//! - A file row is one authorized logical reference of one owner. Its bytes live
//!   in FSM as one media item; FSM keeps one blob per SHA-256 and counts the items
//!   of each blob, so the same bytes of two files are stored once. A shared blob
//!   never grants access: every read checks the owner or the publication.
//! - Reserve, then upload. The upload checks size, SHA-256 and the real file type
//!   from the bytes before anything reaches FSM, and only then sets `ready`. A
//!   `ready` file is immutable: the same bytes again are accepted, other bytes are
//!   refused. Replace means a new file.
//! - Notes, publications, pending saves, transcriptions and content jobs hold
//!   `file_references` rows. Release (`DELETE`) removes the file row only while no
//!   reference exists, and queues its FSM item in `file_cleanup`. The cleanup task
//!   deletes the item with bounded retries; FSM deletes the blob after its last item.
//! - Clients never receive FSM keys or credentials.
use crate::{
    core::{self, FILE_LIMITS, FILE_SETTINGS, FileSettings},
    dal::Store,
    domain::{note_saves::Representation, notes::NoteError, publication},
};
use axum::http::StatusCode;
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::io::AsyncWriteExt;

type R<T> = std::result::Result<T, NoteError>;

/// Why a file is stored. Each purpose has its byte limit and its accepted types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purpose {
    /// Note attachments: images on paper, file cards and previews.
    Attachment,
    /// Input of a transcription run.
    Transcription,
    /// Input of a content job.
    ContentJob,
}
impl Purpose {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "attachment" => Some(Self::Attachment),
            "transcription" => Some(Self::Transcription),
            "content_job" => Some(Self::ContentJob),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Attachment => "attachment",
            Self::Transcription => "transcription",
            Self::ContentJob => "content_job",
        }
    }
    /// The largest accepted file, from the settings in `core`.
    pub fn max_bytes(self) -> i64 {
        match self {
            Self::Attachment => FILE_LIMITS.attachment_bytes,
            Self::Transcription => FILE_LIMITS.transcription_bytes,
            Self::ContentJob => FILE_LIMITS.content_job_bytes,
        }
    }
    /// The content types accepted for this purpose. Every type has a byte check
    /// in `matches_type`; scriptable types (HTML, SVG, JavaScript) are never accepted.
    pub fn content_types(self) -> &'static [&'static str] {
        match self {
            Self::Attachment => &[
                "image/png",
                "image/jpeg",
                "image/gif",
                "image/webp",
                "image/avif",
                "application/pdf",
                "text/plain",
                "text/csv",
                "application/octet-stream",
            ],
            Self::Transcription => &["image/png", "image/jpeg", "image/webp", "application/pdf"],
            Self::ContentJob => &[
                "image/png",
                "image/jpeg",
                "image/gif",
                "image/webp",
                "image/avif",
                "application/pdf",
                "text/plain",
                "audio/mpeg",
                "audio/wav",
                "audio/ogg",
                "audio/mp4",
                "audio/webm",
                "video/mp4",
                "video/webm",
            ],
        }
    }
}

/// Bytes at the start of a file that the type check reads.
const HEAD_BYTES: usize = 64;

/// True when the first bytes of a file are of the declared type. Text types need
/// UTF-8 without NUL; `TextCheck` checks the rest of the stream.
pub fn matches_type(content_type: &str, head: &[u8]) -> bool {
    let at = |offset: usize, magic: &[u8]| head.get(offset..offset + magic.len()) == Some(magic);
    let brand = || head.get(8..12).unwrap_or_default();
    match content_type {
        "image/png" => at(0, b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => at(0, b"\xff\xd8\xff"),
        "image/gif" => at(0, b"GIF87a") || at(0, b"GIF89a"),
        "image/webp" => at(0, b"RIFF") && at(8, b"WEBP"),
        "image/avif" => at(4, b"ftyp") && matches!(brand(), b"avif" | b"avis"),
        "application/pdf" => at(0, b"%PDF-"),
        "audio/mpeg" => {
            at(0, b"ID3") || (head.len() > 1 && head[0] == 0xff && head[1] & 0xe0 == 0xe0)
        }
        "audio/wav" => at(0, b"RIFF") && at(8, b"WAVE"),
        "audio/ogg" => at(0, b"OggS"),
        "audio/webm" | "video/webm" => at(0, b"\x1a\x45\xdf\xa3"),
        "audio/mp4" | "video/mp4" => at(4, b"ftyp") && !matches!(brand(), b"avif" | b"avis"),
        "text/plain" | "text/csv" => true,
        "application/octet-stream" => true,
        _ => false,
    }
}

/// Streaming UTF-8 check without NUL for text uploads.
#[derive(Default)]
struct TextCheck {
    carry: Vec<u8>,
    bad: bool,
}
impl TextCheck {
    fn push(&mut self, chunk: &[u8]) {
        if self.bad {
            return;
        }
        let mut data = std::mem::take(&mut self.carry);
        data.extend_from_slice(chunk);
        match std::str::from_utf8(&data) {
            Ok(_) => self.bad = data.contains(&0),
            Err(e) if e.error_len().is_none() => {
                self.bad = data[..e.valid_up_to()].contains(&0);
                self.carry = data[e.valid_up_to()..].to_vec();
            }
            Err(_) => self.bad = true,
        }
    }
    fn valid(&self) -> bool {
        !self.bad && self.carry.is_empty()
    }
}

/// The album of the FSM item of one file. The cleanup task finds the item of a
/// stopped upload through it.
fn album(file_id: &str) -> String {
    format!("file-{file_id}")
}

fn invalid(code: &str, message: &str) -> NoteError {
    NoteError::new(StatusCode::UNPROCESSABLE_ENTITY, code, message)
}
fn conflict(code: &str, message: &str) -> NoteError {
    NoteError::new(StatusCode::CONFLICT, code, message)
}
fn unavailable() -> NoteError {
    NoteError::from(core::Error::unavailable())
}

// Reservation -------------------------------------------------------------------

/// A validated `POST /me/files` body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub filename: String,
    pub content_type: String,
    pub size: i64,
    /// Lowercase hexadecimal SHA-256.
    pub checksum: String,
    pub purpose: Purpose,
}
impl Reservation {
    /// Parse and check a request body. Unknown fields are refused.
    pub fn parse(raw: &[u8]) -> R<Self> {
        let value: Value = serde_json::from_slice(raw)
            .map_err(|_| invalid("invalid_json", "The body is not valid JSON"))?;
        let map = value
            .as_object()
            .ok_or_else(|| invalid("invalid_request", "The body is one JSON object"))?;
        if let Some(key) = map.keys().find(|k| {
            !["filename", "content_type", "size", "checksum", "purpose"].contains(&k.as_str())
        }) {
            return Err(invalid("unknown_field", &format!("Unknown field {key}")));
        }
        let text = |key: &str| map.get(key).and_then(Value::as_str);
        let purpose = text("purpose").and_then(Purpose::parse).ok_or_else(|| {
            invalid(
                "invalid_purpose",
                "purpose must be attachment, transcription or content_job",
            )
        })?;
        let filename = text("filename").map(str::trim).unwrap_or_default();
        if filename.is_empty()
            || filename.chars().count() > FILE_LIMITS.max_filename_chars
            || filename
                .chars()
                .any(|c| c.is_control() || c == '/' || c == '\\')
        {
            return Err(invalid(
                "invalid_filename",
                "filename uses 1 to 255 characters without control characters or path separators",
            ));
        }
        let content_type = text("content_type")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !purpose.content_types().contains(&content_type.as_str()) {
            return Err(invalid(
                "unsupported_content_type",
                "content_type is not accepted for this purpose",
            ));
        }
        let size = map
            .get("size")
            .and_then(Value::as_i64)
            .filter(|s| *s > 0)
            .ok_or_else(|| invalid("invalid_size", "size is a positive number of bytes"))?;
        if size > purpose.max_bytes() {
            return Err(invalid(
                "file_too_large",
                "size is above the limit of this purpose",
            ));
        }
        let checksum = text("checksum").unwrap_or_default().to_ascii_lowercase();
        if checksum.len() != 64 || !checksum.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid(
                "invalid_checksum",
                "checksum is the hexadecimal SHA-256 of the bytes",
            ));
        }
        Ok(Self {
            filename: filename.to_string(),
            content_type,
            size,
            checksum,
            purpose,
        })
    }
}

// Stored state ------------------------------------------------------------------

/// One file row. `fsm_reference` stays on the server.
#[derive(Clone, Debug)]
pub struct Stored {
    pub file_id: String,
    pub owner_id: String,
    pub filename: String,
    pub content_type: String,
    pub size: i64,
    pub checksum: String,
    pub purpose: String,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
    fsm_reference: Option<String>,
}
impl Stored {
    fn from_row(row: &sqlx::any::AnyRow) -> R<Self> {
        Ok(Self {
            file_id: row.try_get("file_id")?,
            owner_id: row.try_get("owner_id")?,
            filename: row.try_get("filename")?,
            content_type: row.try_get("content_type")?,
            size: row.try_get("size_bytes")?,
            checksum: row.try_get("sha256")?,
            purpose: row.try_get("purpose")?,
            status: row.try_get("status")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            fsm_reference: row.try_get("fsm_reference")?,
        })
    }
    /// The answer body of the file routes. No FSM key, no owner id.
    pub fn to_json(&self) -> Value {
        json!({
            "file_id": self.file_id,
            "filename": self.filename,
            "content_type": self.content_type,
            "size": self.size,
            "checksum": self.checksum,
            "purpose": self.purpose,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

const COLUMNS: &str = "file_id,owner_id,filename,content_type,size_bytes,sha256,purpose,status,created_at,updated_at,fsm_reference";

async fn load(store: &Store, owner: &str, file_id: &str) -> R<Stored> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM files WHERE file_id=$1 AND owner_id=$2"
    ))
    .bind(file_id)
    .bind(owner)
    .fetch_optional(&store.db)
    .await?
    .ok_or_else(NoteError::not_found)?;
    Stored::from_row(&row)
}

/// The owner of a file, for administrative callers that act for an owner.
pub async fn owner_of(store: &Store, file_id: &str) -> R<Option<String>> {
    Ok(sqlx::query("SELECT owner_id FROM files WHERE file_id=$1")
        .bind(file_id)
        .fetch_optional(&store.db)
        .await?
        .map(|row| row.get("owner_id")))
}

// Routes ------------------------------------------------------------------------

/// `POST /me/files`: reserve a file for `owner`. Nothing reaches FSM yet.
pub async fn reserve(store: &Store, owner: &str, input: &Reservation) -> R<Stored> {
    let now = core::now();
    let file_id = core::id();
    let mut tx = store.db.begin().await?;
    sqlx::query("INSERT INTO owners(id,display_name) VALUES($1,'') ON CONFLICT(id) DO NOTHING")
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO files(file_id,owner_id,filename,sha256,size_bytes,content_type,purpose,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,'reserved',$8,$8)")
        .bind(&file_id)
        .bind(owner)
        .bind(&input.filename)
        .bind(&input.checksum)
        .bind(input.size)
        .bind(&input.content_type)
        .bind(input.purpose.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    load(store, owner, &file_id).await
}

/// `GET /me/files/{file_id}`: the metadata of an own file.
pub async fn get(store: &Store, owner: &str, file_id: &str) -> R<Stored> {
    load(store, owner, file_id).await
}

/// The verified result of reading an upload body.
struct Received {
    size: i64,
    checksum: String,
    head: Vec<u8>,
    text_valid: bool,
    temp: Option<tempfile::TempPath>,
}

/// Read the body. Stop at `limit + 1` bytes. With `spool`, write it to a
/// temporary file for the FSM upload.
async fn receive<S, B, E>(mut body: S, limit: i64, spool: bool) -> R<Received>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
{
    let (temp, mut output) = if spool {
        let temp = tempfile::NamedTempFile::new()
            .map_err(|_| unavailable())?
            .into_temp_path();
        let output = tokio::fs::File::create(&temp)
            .await
            .map_err(|_| unavailable())?;
        (Some(temp), Some(output))
    } else {
        (None, None)
    };
    let mut hasher = Sha256::new();
    let mut size: i64 = 0;
    let mut head = Vec::with_capacity(HEAD_BYTES);
    let mut text = TextCheck::default();
    while let Some(chunk) = body.next().await {
        let chunk =
            chunk.map_err(|_| invalid("invalid_upload", "The upload body could not be read"))?;
        let chunk = chunk.as_ref();
        size += chunk.len() as i64;
        if size > limit {
            return Err(invalid(
                "size_mismatch",
                "The bytes are larger than the reserved size",
            ));
        }
        if head.len() < HEAD_BYTES {
            let take = (HEAD_BYTES - head.len()).min(chunk.len());
            head.extend_from_slice(&chunk[..take]);
        }
        hasher.update(chunk);
        text.push(chunk);
        if let Some(output) = output.as_mut() {
            output.write_all(chunk).await.map_err(|_| unavailable())?;
        }
    }
    if let Some(output) = output.as_mut() {
        output.flush().await.map_err(|_| unavailable())?;
    }
    Ok(Received {
        size,
        checksum: format!("{:x}", hasher.finalize()),
        head,
        text_valid: text.valid(),
        temp,
    })
}

/// Check received bytes against the reservation: size, SHA-256, then the real type.
fn verify(file: &Stored, received: &Received) -> R<()> {
    if received.size != file.size {
        return Err(invalid(
            "size_mismatch",
            "The bytes do not have the reserved size",
        ));
    }
    if received.checksum != file.checksum {
        return Err(invalid(
            "checksum_mismatch",
            "The bytes do not have the reserved checksum",
        ));
    }
    let text = matches!(file.content_type.as_str(), "text/plain" | "text/csv");
    if !matches_type(&file.content_type, &received.head) || (text && !received.text_valid) {
        return Err(invalid(
            "content_type_mismatch",
            "The bytes are not of the reserved content type",
        ));
    }
    Ok(())
}

/// The media type of a Content-Type header value, without parameters.
fn media_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// `PUT /me/files/{file_id}/content`: upload the bytes of a reserved file.
///
/// The bytes go to a temporary file while their size, SHA-256 and real type are
/// checked; only verified bytes reach FSM, and only then is the file `ready`. A
/// failed check leaves the file `failed`, and the owner may upload again. The same
/// bytes for a `ready` file are accepted again; other bytes are refused.
pub async fn upload<S, B, E>(
    store: &Store,
    owner: &str,
    file_id: &str,
    declared_type: Option<&str>,
    body: S,
) -> R<Stored>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
{
    let file = load(store, owner, file_id).await?;
    if declared_type.map(media_type).as_deref() != Some(file.content_type.as_str()) {
        return Err(invalid(
            "content_type_mismatch",
            "Content-Type must be the reserved content type",
        ));
    }
    match file.status.as_str() {
        "ready" => {
            let same = match receive(body, file.size, false).await {
                Ok(r) => r.size == file.size && r.checksum == file.checksum,
                Err(e) if e.code == "size_mismatch" => false,
                Err(e) => return Err(e),
            };
            return if same {
                Ok(file)
            } else {
                Err(conflict(
                    "file_immutable",
                    "A ready file never changes; reserve a new file",
                ))
            };
        }
        "uploading" => {
            return Err(conflict(
                "upload_in_progress",
                "An upload of this file is running",
            ));
        }
        _ => {}
    }
    let claim = core::now();
    let claimed = sqlx::query("UPDATE files SET status='uploading',updated_at=$1 WHERE file_id=$2 AND owner_id=$3 AND status IN ('reserved','failed')")
        .bind(claim)
        .bind(file_id)
        .bind(owner)
        .execute(&store.db)
        .await?
        .rows_affected();
    if claimed != 1 {
        return Err(conflict(
            "upload_in_progress",
            "An upload of this file is running",
        ));
    }
    match store_bytes(store, &file, body, claim).await {
        Ok(()) => load(store, owner, file_id).await,
        Err(error) => {
            let reset = sqlx::query("UPDATE files SET status='failed',updated_at=$1 WHERE file_id=$2 AND status='uploading' AND updated_at=$3")
                .bind(core::now().max(claim + 1))
                .bind(file_id)
                .bind(claim)
                .execute(&store.db)
                .await;
            if reset.is_err() {
                tracing::warn!("A stopped upload is not marked failed; the task will reset it");
            }
            Err(error)
        }
    }
}

/// Receive, verify, send to FSM and set `ready`, for the upload claimed at `claim`.
async fn store_bytes<S, B, E>(store: &Store, file: &Stored, body: S, claim: i64) -> R<()>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
{
    let received = receive(body, file.size, true).await?;
    verify(file, &received)?;
    let temp = received.temp.as_ref().ok_or_else(unavailable)?;
    let item = store
        .files
        .upload_path(
            temp,
            &file.filename,
            &file.content_type,
            &album(&file.file_id),
            file.size,
        )
        .await
        .map_err(|_| unavailable())?;
    let same = item.size_bytes == file.size
        && (item.checksum_sha256.is_empty() || item.checksum_sha256 == file.checksum);
    let finished = if same {
        sqlx::query("UPDATE files SET status='ready',fsm_reference=$1,updated_at=$2 WHERE file_id=$3 AND status='uploading' AND updated_at=$4")
            .bind(&item.key)
            .bind(core::now().max(claim + 1))
            .bind(&file.file_id)
            .bind(claim)
            .execute(&store.db)
            .await
            .map(|r| r.rows_affected() == 1)
    } else {
        Ok(false)
    };
    if matches!(finished, Ok(true)) {
        return Ok(());
    }
    // The item is not referenced: queue it, so a failed delete is retried.
    enqueue(store, &item.key).await?;
    drain(store, core::now(), &FILE_SETTINGS).await.ok();
    match finished {
        Ok(false) if !same => {
            tracing::warn!("FSM stored other bytes than the verified upload");
            Err(unavailable())
        }
        Ok(_) => Err(conflict(
            "upload_in_progress",
            "The upload was stopped; upload again",
        )),
        Err(e) => Err(e.into()),
    }
}

async fn enqueue(store: &Store, key: &str) -> R<()> {
    sqlx::query("INSERT INTO file_cleanup(fsm_reference,attempts,created_at) VALUES($1,0,$2) ON CONFLICT(fsm_reference) DO NOTHING")
        .bind(key)
        .bind(core::now())
        .execute(&store.db)
        .await?;
    Ok(())
}

/// A file ready for download: its metadata and, server side only, its FSM item.
pub struct Download {
    pub file: Stored,
}
impl Download {
    /// The FSM item key. Never sent to a client.
    pub fn fsm_reference(&self) -> &str {
        self.file.fsm_reference.as_deref().unwrap_or_default()
    }
    /// Images open in place; every other type downloads as an attachment.
    pub fn inline(&self) -> bool {
        self.file.content_type.starts_with("image/")
    }
}

/// `GET /me/files/{file_id}/content`: an own `ready` file.
pub async fn download(store: &Store, owner: &str, file_id: &str) -> R<Download> {
    let file = load(store, owner, file_id).await?;
    if file.status != "ready" || file.fsm_reference.is_none() {
        return Err(conflict(
            "file_not_ready",
            "The file has no verified bytes yet",
        ));
    }
    Ok(Download { file })
}

/// `DELETE /me/files/{file_id}`: release the owner's logical file reference.
///
/// Refused with 409 while a note revision (working or published), a pending
/// save, a transcription or a content job references the file, and while an
/// upload runs. The FSM item is queued and deleted after the commit; FSM keeps
/// the blob while another file still has an item of it.
pub async fn release(store: &Store, owner: &str, file_id: &str) -> R<()> {
    let file = load(store, owner, file_id).await?;
    if file.status == "uploading" {
        return Err(conflict(
            "upload_in_progress",
            "An upload of this file is running",
        ));
    }
    let in_use = || {
        conflict(
            "file_in_use",
            "A note, publication, pending save or job still uses this file",
        )
    };
    let mut tx = store.db.begin().await?;
    let deleted = sqlx::query("DELETE FROM files WHERE file_id=$1 AND owner_id=$2 AND status<>'uploading' AND NOT EXISTS(SELECT 1 FROM file_references r WHERE r.file_id=$1)")
        .bind(file_id)
        .bind(owner)
        .execute(&mut *tx)
        .await;
    match deleted {
        Ok(r) if r.rows_affected() == 1 => {}
        Ok(_) => return Err(in_use()),
        // A reference committed at the same moment: the foreign key refused the delete.
        Err(_) if referenced(store, file_id).await? => return Err(in_use()),
        Err(e) => return Err(e.into()),
    }
    if let Some(key) = &file.fsm_reference {
        sqlx::query("INSERT INTO file_cleanup(fsm_reference,attempts,created_at) VALUES($1,0,$2) ON CONFLICT(fsm_reference) DO NOTHING")
            .bind(key)
            .bind(core::now())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    // The item is queued durably; the task retries what fails here.
    if drain(store, core::now(), &FILE_SETTINGS).await.is_err() {
        tracing::warn!("Released file cleanup is pending; it will retry");
    }
    Ok(())
}

async fn referenced(store: &Store, file_id: &str) -> R<bool> {
    Ok(
        sqlx::query("SELECT file_id FROM file_references WHERE file_id=$1 LIMIT 1")
            .bind(file_id)
            .fetch_optional(&store.db)
            .await?
            .is_some(),
    )
}

/// `GET /media/{file_id}?note_id=&view=`: a file of a published note.
///
/// The publication projection (`publication::public_note`, which applies
/// `public_document`) decides what is public: the file must appear in a published
/// representation (the `view` one, when given), and a published revision of that
/// note must reference it. Anything else is not found, so an unpublished note,
/// another note or a private file reveals nothing.
pub async fn public_media(
    store: &Store,
    file_id: &str,
    note_id: &str,
    view: Option<&str>,
) -> R<Download> {
    let view =
        match view {
            None => None,
            Some(v) => Some(Representation::parse(v).ok_or_else(|| {
                NoteError::invalid("invalid_view", "view must be original or typed")
            })?),
        };
    let public = publication::public_note(store, note_id, view).await?;
    let shown = public["representations"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|r| mentions(&r["document"], file_id));
    if !shown {
        return Err(NoteError::not_found());
    }
    let mut sql = format!(
        "SELECT {COLUMNS} FROM files f WHERE f.file_id=$1 AND f.status='ready' AND f.owner_id=(SELECT n.owner_id FROM notes n WHERE n.note_id=$2) AND EXISTS(SELECT 1 FROM file_references r JOIN note_published_representations p ON p.revision_id=r.reference_id WHERE r.file_id=f.file_id AND r.reference_kind='note_revision' AND p.note_id=$2"
    );
    if view.is_some() {
        sql.push_str(" AND p.representation=$3");
    }
    sql.push(')');
    let mut query = sqlx::query(&sql).bind(file_id).bind(note_id);
    if let Some(v) = view {
        query = query.bind(v.as_str());
    }
    let row = query
        .fetch_optional(&store.db)
        .await?
        .ok_or_else(NoteError::not_found)?;
    Ok(Download {
        file: Stored::from_row(&row)?,
    })
}

/// True when a public document names the file in a file field.
fn mentions(value: &Value, file_id: &str) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, v)| {
            ((key == "file_id" || key == "preview_file_id") && v.as_str() == Some(file_id))
                || mentions(v, file_id)
        }),
        Value::Array(list) => list.iter().any(|v| mentions(v, file_id)),
        _ => false,
    }
}

// Cleanup task ------------------------------------------------------------------

/// What one cleanup run did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Cleaned {
    /// FSM items deleted.
    pub deleted: usize,
    /// FSM deletes that failed and will retry (bounded by `max_attempts`).
    pub waiting: usize,
    /// Stopped uploads reset to `failed`.
    pub reset: usize,
}

/// Run the file cleanup: reset uploads that stopped (`uploading` longer than the
/// grace period) and queue their FSM items, then delete queued FSM items.
pub async fn cleanup(store: &Store, now: i64, settings: &FileSettings) -> R<Cleaned> {
    let mut out = Cleaned::default();
    let stopped = sqlx::query("SELECT file_id,updated_at FROM files WHERE status='uploading' AND updated_at<$1 ORDER BY updated_at,file_id LIMIT $2")
        .bind(now - settings.grace_seconds)
        .bind(settings.batch)
        .fetch_all(&store.db)
        .await?;
    for row in stopped {
        let file_id: String = row.get("file_id");
        let claim: i64 = row.get("updated_at");
        // The album holds the item that a stopped upload may have left in FSM.
        let Ok(items) = store.files.album(&album(&file_id)).await else {
            tracing::warn!("A stopped upload could not be checked in FSM; it will retry");
            continue;
        };
        let mut tx = store.db.begin().await?;
        let reset = sqlx::query("UPDATE files SET status='failed',updated_at=$1 WHERE file_id=$2 AND status='uploading' AND updated_at=$3")
            .bind(now)
            .bind(&file_id)
            .bind(claim)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if reset != 1 {
            continue;
        }
        for item in items {
            sqlx::query("INSERT INTO file_cleanup(fsm_reference,attempts,created_at) VALUES($1,0,$2) ON CONFLICT(fsm_reference) DO NOTHING")
                .bind(&item.key)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        out.reset += 1;
    }
    let (deleted, waiting) = drain(store, now, settings).await?;
    out.deleted = deleted;
    out.waiting = waiting;
    Ok(out)
}

/// Delete queued FSM items that no file row uses. Returns (deleted, waiting).
async fn drain(store: &Store, now: i64, settings: &FileSettings) -> R<(usize, usize)> {
    let rows = sqlx::query("SELECT fsm_reference FROM file_cleanup WHERE attempts<$1 AND next_attempt_at<=$2 ORDER BY next_attempt_at,fsm_reference LIMIT $3")
        .bind(settings.max_attempts)
        .bind(now)
        .bind(settings.batch)
        .fetch_all(&store.db)
        .await?;
    let (mut deleted, mut waiting) = (0, 0);
    for row in rows {
        let key: String = row.get("fsm_reference");
        let used = sqlx::query("SELECT file_id FROM files WHERE fsm_reference=$1")
            .bind(&key)
            .fetch_optional(&store.db)
            .await?
            .is_some();
        if !used && store.files.delete(&key).await.is_err() {
            tracing::warn!("FSM file cleanup failed; bounded retries can require operator review");
            sqlx::query("UPDATE file_cleanup SET attempts=attempts+1,next_attempt_at=$2 WHERE fsm_reference=$1")
                .bind(&key)
                .bind(now + settings.retry_seconds)
                .execute(&store.db)
                .await?;
            waiting += 1;
            continue;
        }
        sqlx::query("DELETE FROM file_cleanup WHERE fsm_reference=$1")
            .bind(&key)
            .execute(&store.db)
            .await?;
        if !used {
            deleted += 1;
        }
    }
    Ok((deleted, waiting))
}
