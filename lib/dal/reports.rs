//! Server-side Reports owner-item client for verifying ownership, server-side
//! item creation provenance, and scoped deletion (#37).
use crate::core::{Error, Result};
use axum::http::StatusCode;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const MAX_REPORTS_RESPONSE_BYTES: usize = 256 * 1024;

pub trait ReportsClient: Send + Sync {
    fn create_owned_item<'a>(
        &'a self,
        owner: &'a str,
        project: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<String>>;
    fn verify_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
    ) -> BoxFuture<'a, Result<()>>;
    fn delete_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
        confirm_reused: bool,
    ) -> BoxFuture<'a, Result<()>>;
}

pub struct DisabledReports;

impl ReportsClient for DisabledReports {
    fn create_owned_item<'a>(
        &'a self,
        _owner: &'a str,
        _project: &'a str,
        _payload: Value,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async { Err(Error::unavailable()) })
    }

    fn verify_owned_item<'a>(
        &'a self,
        _owner: &'a str,
        _item_id: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::unavailable()) })
    }

    fn delete_owned_item<'a>(
        &'a self,
        _owner: &'a str,
        _item_id: &'a str,
        _confirm_reused: bool,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::unavailable()) })
    }
}

pub struct HttpReports {
    base_url: String,
    http: reqwest::Client,
}

impl HttpReports {
    pub fn client(base_url: String, http: reqwest::Client) -> Arc<dyn ReportsClient> {
        let trimmed = base_url.trim().trim_end_matches('/').to_string();
        if trimmed.is_empty() {
            return Arc::new(DisabledReports);
        }
        Arc::new(Self {
            base_url: trimmed,
            http,
        })
    }
}

async fn read_bounded_body(mut resp: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    if let Some(len) = resp.content_length()
        && len as usize > limit
    {
        return Err(Error::unavailable());
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|_| Error::unavailable())? {
        if buf.len().saturating_add(chunk.len()) > limit {
            return Err(Error::unavailable());
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

impl ReportsClient for HttpReports {
    fn create_owned_item<'a>(
        &'a self,
        owner: &'a str,
        project: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let clean_project = project.trim();
            if clean_project.is_empty()
                || clean_project.len() > 100
                || !clean_project
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(Error::bad("invalid_project"));
            }
            let url = format!("{}/me/projects/{}/items", self.base_url, clean_project);
            let resp = self
                .http
                .post(url)
                .header("x-uuid", owner)
                .json(&payload)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            let status = resp.status();
            let body = read_bounded_body(resp, MAX_REPORTS_RESPONSE_BYTES).await?;
            match status {
                StatusCode::CREATED => {
                    let parsed: Value =
                        serde_json::from_slice(&body).map_err(|_| Error::unavailable())?;
                    let item_id = parsed
                        .get("item")
                        .and_then(|v| v.get("id"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .ok_or_else(Error::unavailable)?;
                    Ok(item_id.to_string())
                }
                StatusCode::FORBIDDEN => Err(Error(StatusCode::FORBIDDEN, "forbidden".into())),
                StatusCode::NOT_FOUND => Err(Error::missing()),
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                    Err(Error::bad("invalid_report_item"))
                }
                _ => Err(Error::unavailable()),
            }
        })
    }

    fn verify_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let url = format!("{}/me/items/{}", self.base_url, item_id);
            let resp = self
                .http
                .get(url)
                .header("x-uuid", owner)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            let status = resp.status();
            let _ = read_bounded_body(resp, MAX_REPORTS_RESPONSE_BYTES).await?;
            match status {
                StatusCode::OK => Ok(()),
                StatusCode::FORBIDDEN => Err(Error(StatusCode::FORBIDDEN, "forbidden".into())),
                StatusCode::NOT_FOUND => Err(Error::missing()),
                _ => Err(Error::unavailable()),
            }
        })
    }

    fn delete_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
        confirm_reused: bool,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let url = if confirm_reused {
                format!("{}/me/items/{}?confirm=true", self.base_url, item_id)
            } else {
                format!("{}/me/items/{}", self.base_url, item_id)
            };
            let resp = self
                .http
                .delete(url)
                .header("x-uuid", owner)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            let status = resp.status();
            let _ = read_bounded_body(resp, MAX_REPORTS_RESPONSE_BYTES).await?;
            match status {
                StatusCode::OK | StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(()),
                StatusCode::CONFLICT => Err(Error::conflict("reused_confirmation_required")),
                StatusCode::FORBIDDEN => Err(Error(StatusCode::FORBIDDEN, "forbidden".into())),
                _ => Err(Error::unavailable()),
            }
        })
    }
}

#[derive(Default)]
pub struct MockReports {
    items: Mutex<HashMap<String, String>>,
    deleted_calls: Mutex<Vec<(String, String, bool)>>,
    failing_deletes: Mutex<HashSet<String>>,
    next_id: Mutex<u64>,
}

impl MockReports {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn insert_item(&self, item_id: &str, owner_id: &str) {
        self.items
            .lock()
            .unwrap()
            .insert(item_id.to_string(), owner_id.to_string());
    }

    pub fn set_delete_failure(&self, item_id: &str, fail: bool) {
        let mut set = self.failing_deletes.lock().unwrap();
        if fail {
            set.insert(item_id.to_string());
        } else {
            set.remove(item_id);
        }
    }

    pub fn delete_calls(&self) -> Vec<(String, String, bool)> {
        self.deleted_calls.lock().unwrap().clone()
    }
}

impl ReportsClient for MockReports {
    fn create_owned_item<'a>(
        &'a self,
        owner: &'a str,
        _project: &'a str,
        _payload: Value,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let mut n = self.next_id.lock().unwrap();
            *n += 1;
            let item_id = format!("rep-created-{}", *n);
            self.items
                .lock()
                .unwrap()
                .insert(item_id.clone(), owner.to_string());
            Ok(item_id)
        })
    }

    fn verify_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let items = self.items.lock().unwrap();
            match items.get(item_id) {
                Some(real_owner) if real_owner == owner => Ok(()),
                Some(_) => Err(Error(StatusCode::FORBIDDEN, "forbidden".into())),
                None => Err(Error::missing()),
            }
        })
    }

    fn delete_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
        confirm_reused: bool,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.deleted_calls.lock().unwrap().push((
                owner.to_string(),
                item_id.to_string(),
                confirm_reused,
            ));
            if self.failing_deletes.lock().unwrap().contains(item_id) {
                return Err(Error::unavailable());
            }
            let mut items = self.items.lock().unwrap();
            match items.get(item_id) {
                Some(real_owner) if real_owner == owner => {
                    items.remove(item_id);
                    Ok(())
                }
                Some(_) => Err(Error(StatusCode::FORBIDDEN, "forbidden".into())),
                None => Ok(()),
            }
        })
    }
}
