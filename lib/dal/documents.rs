//! The CouchDB document store. A note revision keeps its `body` as one gzip
//! attachment (`body.json.gz`) of its own CouchDB document (annotations#19,
//! decision 32a104ee983b48ca995a7e7820839020): one revision stays one CouchDB
//! document, and a large note does not hit the CouchDB document size.
use crate::core::{Error, Result, Settings};
use base64::{Engine, engine::general_purpose::STANDARD};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Clone)]
pub enum Documents {
    Remote {
        settings: Box<Settings>,
        http: reqwest::Client,
    },
    Memory(Arc<Mutex<HashMap<String, Value>>>),
}
impl Documents {
    pub fn remote(settings: Settings, http: reqwest::Client) -> Self {
        Self::Remote {
            settings: Box::new(settings),
            http,
        }
    }
    fn request(&self, method: reqwest::Method, id: &str) -> reqwest::RequestBuilder {
        let Self::Remote { settings, http } = self else {
            unreachable!()
        };
        http.request(
            method,
            format!(
                "{}/{}/{}",
                settings.couch_url.trim_end_matches('/'),
                settings.couch_database,
                id
            ),
        )
        .basic_auth(&settings.couch_user, Some(&settings.couch_password))
    }
    pub async fn put(&self, id: &str, body: &Value) -> Result<()> {
        match self {
            Self::Memory(m) => {
                m.lock().unwrap().insert(id.into(), body.clone());
            }
            Self::Remote { .. } => {
                self.request(reqwest::Method::PUT, id)
                    .json(body)
                    .send()
                    .await?
                    .error_for_status()?;
            }
        }
        Ok(())
    }
    /// Insert immutable revision content. An identical retry is safe after an
    /// uncertain response; different content never overwrites the document.
    /// A `body` field is stored as the gzip attachment of the document.
    pub async fn put_immutable(&self, id: &str, body: &Value) -> Result<()> {
        let stored = packed(body)?;
        match self {
            Self::Memory(m) => {
                let existing = m.lock().unwrap().get(id).cloned();
                if let Some(existing) = existing {
                    if unpacked(existing)? != *body {
                        return Err(Error::unavailable());
                    }
                } else {
                    m.lock().unwrap().insert(id.into(), stored);
                }
                Ok(())
            }
            Self::Remote { .. } => {
                let response = self
                    .request(reqwest::Method::PUT, id)
                    .json(&stored)
                    .send()
                    .await?;
                if response.status() == reqwest::StatusCode::CONFLICT {
                    let mut existing = self.get(id).await?;
                    if let Some(object) = existing.as_object_mut() {
                        object.remove("_id");
                        object.remove("_rev");
                    }
                    if existing != *body {
                        return Err(Error::unavailable());
                    }
                } else {
                    response.error_for_status()?;
                }
                Ok(())
            }
        }
    }
    /// A document; a stored gzip body comes back as its `body` field.
    pub async fn get(&self, id: &str) -> Result<Value> {
        let raw = match self {
            Self::Memory(m) => m
                .lock()
                .unwrap()
                .get(id)
                .cloned()
                .ok_or_else(Error::missing)?,
            Self::Remote { .. } => {
                let r = self
                    .request(reqwest::Method::GET, id)
                    .query(&[("attachments", "true")])
                    .send()
                    .await?;
                if r.status() == 404 {
                    return Err(Error::missing());
                }
                r.error_for_status()?.json().await?
            }
        };
        unpacked(raw)
    }
    pub async fn delete(&self, id: &str) -> Result<()> {
        match self {
            Self::Memory(m) => {
                m.lock().unwrap().remove(id);
            }
            Self::Remote { .. } => {
                let v = self.get(id).await?;
                let rev = v["_rev"].as_str().ok_or_else(Error::unavailable)?;
                self.request(reqwest::Method::DELETE, id)
                    .query(&[("rev", rev)])
                    .send()
                    .await?
                    .error_for_status()?;
            }
        }
        Ok(())
    }
    pub async fn ready(&self) -> Result<()> {
        if matches!(self, Self::Remote { .. }) {
            self.request(reqwest::Method::GET, "")
                .send()
                .await?
                .error_for_status()?;
        }
        Ok(())
    }
}

/// The name of the gzip body attachment.
const BODY: &str = "body.json.gz";

/// `value` with its `body` field moved into the gzip attachment.
fn packed(value: &Value) -> Result<Value> {
    let Some(body) = value.get("body") else {
        return Ok(value.clone());
    };
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, body).map_err(|_| Error::unavailable())?;
    let bytes = gz.finish().map_err(|_| Error::unavailable())?;
    let mut out = value.clone();
    let object = out.as_object_mut().ok_or_else(Error::unavailable)?;
    object.remove("body");
    object.insert(
        "_attachments".into(),
        json!({BODY: {"content_type": "application/gzip", "data": STANDARD.encode(bytes)}}),
    );
    Ok(out)
}

/// A stored document with its gzip attachment back in `body`. A document
/// without the attachment comes back as stored.
fn unpacked(mut value: Value) -> Result<Value> {
    let Some(data) = value["_attachments"][BODY]["data"].as_str() else {
        return Ok(value);
    };
    let bytes = STANDARD.decode(data).map_err(|_| Error::unavailable())?;
    let mut text = Vec::new();
    GzDecoder::new(&bytes[..])
        .read_to_end(&mut text)
        .map_err(|_| Error::unavailable())?;
    let body: Value = serde_json::from_slice(&text).map_err(|_| Error::unavailable())?;
    let object = value.as_object_mut().ok_or_else(Error::unavailable)?;
    object.remove("_attachments");
    object.insert("body".into(), body);
    Ok(value)
}

/// Compresses a request body for tests and clients that send gzip.
pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    let _ = gz.write_all(bytes);
    gz.finish().unwrap_or_default()
}
