//! Server-side Certifications client for prepared study handoff (#35).
//! Sends `x-uuid` and server-held `x-service-key` (`CERTIFICATIONS_SERVICE_KEY`),
//! never exposing service credentials or raw `fsm_ref` values to the browser.
use crate::core::{Error, Result};
use axum::http::StatusCode;
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const MAX_CERTIFICATIONS_RESPONSE_BYTES: usize = 512 * 1024;

pub trait CertificationsClient: Send + Sync {
    fn create_prepared_study<'a>(
        &'a self,
        owner: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<Value>>;
}

pub struct DisabledCertifications;

impl CertificationsClient for DisabledCertifications {
    fn create_prepared_study<'a>(
        &'a self,
        _owner: &'a str,
        _payload: Value,
    ) -> BoxFuture<'a, Result<Value>> {
        Box::pin(async { Err(Error::unavailable()) })
    }
}

pub struct HttpCertifications {
    base_url: String,
    service_key: String,
    http: reqwest::Client,
}

impl HttpCertifications {
    pub fn client(
        base_url: String,
        service_key: String,
        http: reqwest::Client,
    ) -> Arc<dyn CertificationsClient> {
        let base = base_url.trim().trim_end_matches('/').to_string();
        let key = service_key.trim().to_string();
        if base.is_empty() || key.is_empty() {
            return Arc::new(DisabledCertifications);
        }
        Arc::new(Self {
            base_url: base,
            service_key: key,
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

impl CertificationsClient for HttpCertifications {
    fn create_prepared_study<'a>(
        &'a self,
        owner: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<Value>> {
        Box::pin(async move {
            let url = format!("{}/studies/prepared", self.base_url);
            let resp = self
                .http
                .post(url)
                .header("x-uuid", owner)
                .header("x-service-key", &self.service_key)
                .json(&payload)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            let status = resp.status();
            let body = read_bounded_body(resp, MAX_CERTIFICATIONS_RESPONSE_BYTES).await?;
            match status {
                StatusCode::CREATED | StatusCode::OK => {
                    serde_json::from_slice(&body).map_err(|_| Error::unavailable())
                }
                StatusCode::FORBIDDEN => Err(Error(StatusCode::FORBIDDEN, "forbidden".into())),
                StatusCode::NOT_FOUND => Err(Error::missing()),
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                    Err(Error::bad("invalid_prepared_study"))
                }
                _ => Err(Error::unavailable()),
            }
        })
    }
}

#[derive(Default)]
pub struct MockCertifications {
    pub calls: Mutex<Vec<(String, Value)>>,
}

impl MockCertifications {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl CertificationsClient for MockCertifications {
    fn create_prepared_study<'a>(
        &'a self,
        owner: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<Value>> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((owner.to_string(), payload.clone()));
            Ok(serde_json::json!({
                "study": {
                    "id": "cert-study-1",
                    "owner_id": owner,
                    "title": payload.get("title").and_then(Value::as_str).unwrap_or(""),
                    "source_origin": "annotations",
                    "status": "draft",
                },
                "sources": payload.get("sources").cloned().unwrap_or_else(|| serde_json::json!([]))
            }))
        })
    }
}
