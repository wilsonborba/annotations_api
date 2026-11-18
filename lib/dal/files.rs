//! FSM media adapter. FSM keeps one blob per SHA-256 and counts its media items
//! (aliases): an upload of known bytes adds an item to the existing blob, and a
//! delete removes the item and the blob after its last item. The fixture double
//! behaves the same way, so tests never call the production FSM.
use crate::core::{Error, Result, Settings};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct File {
    pub key: String,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    #[serde(default)]
    pub album: String,
    /// The SHA-256 of the stored blob, as FSM reports it.
    #[serde(default)]
    pub checksum_sha256: String,
}
/// The in-memory FSM double: media items by key, blobs by SHA-256 with their item count.
#[derive(Default)]
struct Memory {
    items: HashMap<String, File>,
    blobs: HashMap<String, (Vec<u8>, i64)>,
}
type FixtureFiles = Arc<Mutex<Memory>>;
#[derive(Clone)]
pub struct Files {
    pub settings: Option<Settings>,
    pub http: reqwest::Client,
    memory: Option<FixtureFiles>,
    /// Folder for upload staging (`UPLOAD_STAGING_DIR`); `None` is the system temp folder.
    staging: Option<std::path::PathBuf>,
}
impl Files {
    pub fn new(settings: Settings, http: reqwest::Client) -> Self {
        let staging = Some(settings.upload_staging_dir.trim())
            .filter(|d| !d.is_empty())
            .map(std::path::PathBuf::from);
        Self {
            settings: Some(settings),
            http,
            memory: None,
            staging,
        }
    }
    pub fn fixture() -> Self {
        Self {
            settings: None,
            http: reqwest::Client::new(),
            memory: Some(Arc::default()),
            staging: None,
        }
    }
    /// The same adapter with uploads staged in `dir`.
    pub fn with_staging_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.staging = Some(dir.into());
        self
    }
    /// A new staging file for an upload before it goes to FSM. It is removed when
    /// the returned path drops, so a rejected or finished upload leaves nothing.
    pub fn staging_file(&self) -> Result<tempfile::TempPath> {
        let file = match &self.staging {
            Some(dir) => tempfile::NamedTempFile::new_in(dir),
            None => tempfile::NamedTempFile::new(),
        };
        file.map(tempfile::NamedTempFile::into_temp_path)
            .map_err(|_| Error::unavailable())
    }
    fn url(&self, key: &str) -> Result<String> {
        let s = self.settings.as_ref().ok_or_else(Error::unavailable)?;
        Ok(format!(
            "{}/annotations/media/{}",
            s.fsm_url.trim_end_matches('/'),
            key
        ))
    }
    pub async fn upload_path(
        &self,
        path: &std::path::Path,
        filename: &str,
        content_type: &str,
        album: &str,
        size: i64,
    ) -> Result<File> {
        if let Some(memory) = &self.memory {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|_| Error::unavailable())?;
            let sha = format!("{:x}", Sha256::digest(&bytes));
            let file = File {
                key: crate::core::id(),
                filename: filename.into(),
                content_type: content_type.into(),
                size_bytes: bytes.len() as i64,
                album: album.into(),
                checksum_sha256: sha.clone(),
            };
            let mut m = memory.lock().unwrap();
            m.blobs.entry(sha).or_insert((bytes, 0)).1 += 1;
            m.items.insert(file.key.clone(), file.clone());
            return Ok(file);
        }
        let s = self.settings.as_ref().ok_or_else(Error::unavailable)?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|_| Error::unavailable())?;
        let part = reqwest::multipart::Part::stream_with_length(
            reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file)),
            size as u64,
        )
        .file_name(filename.to_owned())
        .mime_str(content_type)
        .map_err(|_| Error::bad("Invalid media type"))?;
        let form = reqwest::multipart::Form::new()
            .text("album", album.to_string())
            .part("file", part);
        let value: serde_json::Value = self
            .http
            .post(self.url("")?.trim_end_matches('/'))
            .bearer_auth(&s.fsm_key)
            .multipart(form)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        serde_json::from_value(value["item"].clone()).map_err(|_| Error::unavailable())
    }
    /// Delete one media item. FSM removes the blob after its last item. A missing
    /// item counts as deleted.
    pub async fn delete(&self, key: &str) -> Result<()> {
        if let Some(memory) = &self.memory {
            let mut m = memory.lock().unwrap();
            if let Some(file) = m.items.remove(key) {
                let sha = file.checksum_sha256;
                let last = m.blobs.get_mut(&sha).is_some_and(|blob| {
                    blob.1 -= 1;
                    blob.1 <= 0
                });
                if last {
                    m.blobs.remove(&sha);
                }
            }
            return Ok(());
        }
        let s = self.settings.as_ref().ok_or_else(Error::unavailable)?;
        let r = self
            .http
            .delete(self.url(key)?)
            .bearer_auth(&s.fsm_key)
            .send()
            .await?;
        if r.status() != 404 {
            r.error_for_status()?;
        }
        Ok(())
    }
    pub async fn read(&self, key: &str) -> Result<axum::body::Body> {
        self.read_range(key, None).await
    }
    /// Read the bytes of one item, or only the inclusive byte range `(first, last)`.
    /// FSM answers a Range request with 206 when it supports ranges; otherwise the
    /// adapter skips and cuts the full stream, so the caller always gets the range.
    pub async fn read_range(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<axum::body::Body> {
        if let Some(memory) = &self.memory {
            let m = memory.lock().unwrap();
            let bytes = m
                .items
                .get(key)
                .and_then(|f| m.blobs.get(&f.checksum_sha256))
                .map(|(bytes, _)| bytes)
                .ok_or_else(Error::missing)?;
            let part = match range {
                None => bytes.clone(),
                Some((first, last)) => bytes
                    .get(first as usize..=last as usize)
                    .ok_or_else(Error::unavailable)?
                    .to_vec(),
            };
            return Ok(axum::body::Body::from(part));
        }
        let s = self.settings.as_ref().ok_or_else(Error::unavailable)?;
        let mut request = self.http.get(self.url(key)?).bearer_auth(&s.fsm_key);
        if let Some((first, last)) = range {
            request = request.header(reqwest::header::RANGE, format!("bytes={first}-{last}"));
        }
        let r = request.send().await?.error_for_status()?;
        let Some((first, last)) = range.filter(|_| r.status() != 206) else {
            return Ok(axum::body::Body::from_stream(r.bytes_stream()));
        };
        // A full answer: drop the bytes before `first` and stop after `last`.
        let stream = r
            .bytes_stream()
            .scan((first, last - first + 1), |(skip, left), chunk| {
                let out = match chunk {
                    Ok(_) if *left == 0 => None,
                    Ok(mut bytes) => {
                        let drop = (*skip).min(bytes.len() as u64);
                        *skip -= drop;
                        let mut bytes = bytes.split_off(drop as usize);
                        let keep = (*left).min(bytes.len() as u64);
                        *left -= keep;
                        bytes.truncate(keep as usize);
                        Some(Ok(bytes))
                    }
                    Err(e) => Some(Err(e)),
                };
                futures_util::future::ready(out)
            });
        Ok(axum::body::Body::from_stream(stream))
    }
}

impl Files {
    pub async fn album(&self, album: &str) -> Result<Vec<File>> {
        if let Some(m) = &self.memory {
            return Ok(m
                .lock()
                .unwrap()
                .items
                .values()
                .filter(|f| f.album == album)
                .cloned()
                .collect());
        }
        let s = self.settings.as_ref().ok_or_else(Error::unavailable)?;
        let value: serde_json::Value = self
            .http
            .get(format!(
                "{}/annotations/albums/{album}",
                s.fsm_url.trim_end_matches('/')
            ))
            .bearer_auth(&s.fsm_key)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        serde_json::from_value(value["items"].clone()).map_err(|_| Error::unavailable())
    }
    /// Fixture only: the number of media items and of blobs in the double.
    pub fn fixture_counts(&self) -> Option<(usize, usize)> {
        let m = self.memory.as_ref()?.lock().unwrap();
        Some((m.items.len(), m.blobs.len()))
    }
    /// Fixture only: the item count of the blob with this SHA-256.
    pub fn fixture_refcount(&self, sha256: &str) -> Option<i64> {
        let m = self.memory.as_ref()?.lock().unwrap();
        m.blobs.get(sha256).map(|(_, count)| *count)
    }
}
