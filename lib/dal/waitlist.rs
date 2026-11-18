//! Central gateway waitlist client (`POST /apps/api/v1/waitlist`).
use crate::core::{Error, Result};
use axum::http::StatusCode;
use serde_json::json;
use std::{future::Future, pin::Pin, sync::Arc};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait WaitlistClient: Send + Sync {
    fn is_configured(&self) -> bool;
    fn join<'a>(
        &'a self,
        email: &'a str,
        forwarded_for: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>>;
}

pub struct DisabledWaitlist;

impl WaitlistClient for DisabledWaitlist {
    fn is_configured(&self) -> bool {
        false
    }
    fn join<'a>(
        &'a self,
        _email: &'a str,
        _forwarded_for: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::unavailable()) })
    }
}

pub struct HttpWaitlist {
    base_url: String,
    http: reqwest::Client,
}

impl HttpWaitlist {
    pub fn from_settings(base_url: &str) -> Arc<dyn WaitlistClient> {
        let base = base_url.trim().trim_end_matches('/');
        if base.is_empty() {
            return Arc::new(DisabledWaitlist);
        }
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Arc::new(Self {
            base_url: base.to_string(),
            http,
        })
    }
}

impl WaitlistClient for HttpWaitlist {
    fn is_configured(&self) -> bool {
        true
    }
    fn join<'a>(
        &'a self,
        email: &'a str,
        forwarded_for: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut req = self
                .http
                .post(format!("{}/apps/api/v1/waitlist", self.base_url))
                .json(&json!({
                    "app_id": "annotations",
                    "target_type": "interest",
                    "target": "author",
                    "email": email,
                }));
            if let Some(ip) = forwarded_for.filter(|v| !v.trim().is_empty()) {
                req = req.header("x-forwarded-for", ip);
            }
            let resp = req.send().await.map_err(|_| Error::unavailable())?;
            if resp.status().is_success() {
                return Ok(());
            }
            match resp.status() {
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                    Err(Error::bad("Invalid waitlist request"))
                }
                StatusCode::TOO_MANY_REQUESTS => Err(Error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "waitlist_rate_limited: Too many waitlist attempts".into(),
                )),
                _ => Err(Error::unavailable()),
            }
        })
    }
}
