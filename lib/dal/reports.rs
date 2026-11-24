//! Server-side Reports owner-item client for verifying ownership and scoped deletion (#37).
use crate::core::{Error, Result};
use axum::http::StatusCode;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ReportsClient: Send + Sync {
    fn verify_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
    ) -> BoxFuture<'a, Result<()>>;
    fn delete_owned_item<'a>(
        &'a self,
        owner: &'a str,
        item_id: &'a str,
    ) -> BoxFuture<'a, Result<()>>;
}

pub struct DisabledReports;

impl ReportsClient for DisabledReports {
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

impl ReportsClient for HttpReports {
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
            match resp.status() {
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
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let url = format!("{}/me/items/{}?confirm=true", self.base_url, item_id);
            let resp = self
                .http
                .delete(url)
                .header("x-uuid", owner)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            match resp.status() {
                StatusCode::OK | StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(()),
                StatusCode::FORBIDDEN => Err(Error(StatusCode::FORBIDDEN, "forbidden".into())),
                _ => Err(Error::unavailable()),
            }
        })
    }
}

#[derive(Default)]
pub struct MockReports {
    items: Mutex<HashMap<String, String>>,
    deleted_calls: Mutex<Vec<(String, String)>>,
    failing_deletes: Mutex<HashSet<String>>,
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

    pub fn delete_calls(&self) -> Vec<(String, String)> {
        self.deleted_calls.lock().unwrap().clone()
    }
}

impl ReportsClient for MockReports {
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
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.deleted_calls
                .lock()
                .unwrap()
                .push((owner.to_string(), item_id.to_string()));
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
