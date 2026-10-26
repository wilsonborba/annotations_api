use crate::core::{Error, Result, Settings};
use serde::{Deserialize, Serialize};
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
}
type FixtureFiles = Arc<Mutex<HashMap<String, (File, Vec<u8>)>>>;
#[derive(Clone)]
pub struct Files {
    pub settings: Option<Settings>,
    pub http: reqwest::Client,
    memory: Option<FixtureFiles>,
}
impl Files {
    pub fn new(settings: Settings, http: reqwest::Client) -> Self {
        Self {
            settings: Some(settings),
            http,
            memory: None,
        }
    }
    pub fn fixture() -> Self {
        Self {
            settings: None,
            http: reqwest::Client::new(),
            memory: Some(Arc::default()),
        }
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
            let file = File {
                key: crate::core::id(),
                filename: filename.into(),
                content_type: content_type.into(),
                size_bytes: size,
            };
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|_| Error::unavailable())?;
            memory
                .lock()
                .unwrap()
                .insert(file.key.clone(), (file.clone(), bytes));
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
    pub async fn delete(&self, key: &str) -> Result<()> {
        if let Some(memory) = &self.memory {
            memory.lock().unwrap().remove(key);
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
        if let Some(memory) = &self.memory {
            return memory
                .lock()
                .unwrap()
                .get(key)
                .map(|(_, bytes)| axum::body::Body::from(bytes.clone()))
                .ok_or_else(Error::missing);
        }
        let s = self.settings.as_ref().ok_or_else(Error::unavailable)?;
        let r = self
            .http
            .get(self.url(key)?)
            .bearer_auth(&s.fsm_key)
            .send()
            .await?
            .error_for_status()?;
        Ok(axum::body::Body::from_stream(r.bytes_stream()))
    }
}
