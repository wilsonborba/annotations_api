//! Adapter for the existing Cortex API (`/files`, `/chats`, `/chats/{session_id}/outputs/{output_id}`,
//! `DELETE /chats/{session_id}`). Always sends `app_id=annotations`, `temporary=true`,
//! `use_memory=false`, `persona_id=null`, `needs_web=false`, and the server-verified `x-uuid`.
use crate::core::{Error, Result};
use axum::http::StatusCode;
use reqwest::multipart::{Form, Part};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputStatus {
    Running,
    Completed,
    Partial,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug)]
pub struct CortexOutput {
    pub status: OutputStatus,
    pub text: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct StartedChat {
    pub session_id: String,
    pub output_id: String,
    pub immediate_output: Option<CortexOutput>,
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait CortexClient: Send + Sync {
    fn is_configured(&self) -> bool;
    fn upload_file<'a>(
        &'a self,
        owner: &'a str,
        filename: &'a str,
        content_type: &'a str,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<String>>;
    fn start_chat<'a>(
        &'a self,
        owner: &'a str,
        text: &'a str,
        file_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<StartedChat>>;
    fn get_output<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
        output_id: &'a str,
    ) -> BoxFuture<'a, Result<CortexOutput>>;
    fn cancel_output<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
        output_id: &'a str,
    ) -> BoxFuture<'a, Result<CortexOutput>>;
    fn delete_chat<'a>(&'a self, owner: &'a str, session_id: &'a str) -> BoxFuture<'a, Result<()>>;
}

pub struct DisabledCortex;
impl CortexClient for DisabledCortex {
    fn is_configured(&self) -> bool {
        false
    }
    fn upload_file<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
        _: Vec<u8>,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async { Err(disabled()) })
    }
    fn start_chat<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: Vec<String>,
    ) -> BoxFuture<'a, Result<StartedChat>> {
        Box::pin(async { Err(disabled()) })
    }
    fn get_output<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
    ) -> BoxFuture<'a, Result<CortexOutput>> {
        Box::pin(async { Err(disabled()) })
    }
    fn cancel_output<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
    ) -> BoxFuture<'a, Result<CortexOutput>> {
        Box::pin(async { Err(disabled()) })
    }
    fn delete_chat<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(disabled()) })
    }
}

pub fn disabled() -> Error {
    Error(
        StatusCode::SERVICE_UNAVAILABLE,
        "cortex_unavailable: Optional Cortex processing is disabled".into(),
    )
}

pub struct HttpCortex {
    base_url: String,
    http: reqwest::Client,
}

impl HttpCortex {
    pub fn client(base_url: String, http: reqwest::Client) -> Arc<dyn CortexClient> {
        let trimmed = base_url.trim_end_matches('/').to_string();
        if trimmed.is_empty() {
            Arc::new(DisabledCortex)
        } else {
            Arc::new(Self {
                base_url: trimmed,
                http,
            })
        }
    }
}

fn parse_output(body: &Value) -> Result<CortexOutput> {
    let data = body.get("data").unwrap_or(body);
    let status = match data["status"].as_str().unwrap_or("running") {
        "done" | "completed" => OutputStatus::Completed,
        "partial" => OutputStatus::Partial,
        "cancelled" => OutputStatus::Cancelled,
        "failed" | "error" => OutputStatus::Failed,
        _ => OutputStatus::Running,
    };
    let text = data["text"]
        .as_str()
        .or_else(|| data["content"].as_str())
        .unwrap_or("")
        .to_string();
    let error_code = data["error_code"].as_str().map(String::from);
    let error_message = data["error_message"]
        .as_str()
        .or_else(|| data["error"].as_str())
        .map(String::from);
    Ok(CortexOutput {
        status,
        text,
        error_code,
        error_message,
    })
}

impl CortexClient for HttpCortex {
    fn is_configured(&self) -> bool {
        true
    }
    fn upload_file<'a>(
        &'a self,
        owner: &'a str,
        filename: &'a str,
        content_type: &'a str,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let part = Part::bytes(bytes)
                .file_name(filename.to_string())
                .mime_str(content_type)
                .map_err(|_| Error::bad("invalid_content_type: Invalid file media type"))?;
            let form = Form::new()
                .text("app_id", "annotations")
                .text("temporary", "true")
                .part("file", part);
            let resp = self
                .http
                .post(format!("{}/files", self.base_url))
                .header("x-uuid", owner)
                .multipart(form)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            if !resp.status().is_success() {
                return Err(Error::unavailable());
            }
            let body: Value = resp.json().await.map_err(|_| Error::unavailable())?;
            let data = body.get("data").unwrap_or(&body);
            data["file_id"]
                .as_str()
                .map(String::from)
                .ok_or_else(Error::unavailable)
        })
    }
    fn start_chat<'a>(
        &'a self,
        owner: &'a str,
        text: &'a str,
        file_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<StartedChat>> {
        Box::pin(async move {
            let payload = json!({
                "app_id": "annotations",
                "text": text,
                "file_ids": file_ids,
                "temporary": true,
                "use_memory": false,
                "persona_id": Value::Null,
                "needs_web": false,
                "stream": false,
            });
            let resp = self
                .http
                .post(format!("{}/chats", self.base_url))
                .header("x-uuid", owner)
                .json(&payload)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            if !resp.status().is_success() {
                return Err(Error::unavailable());
            }
            let body: Value = resp.json().await.map_err(|_| Error::unavailable())?;
            let data = body.get("data").unwrap_or(&body);
            let session_id = data["session_id"]
                .as_str()
                .map(String::from)
                .ok_or_else(Error::unavailable)?;
            let output_id = data["output_id"]
                .as_str()
                .map(String::from)
                .ok_or_else(Error::unavailable)?;
            let parsed = parse_output(&body)?;
            let immediate_output = (parsed.status != OutputStatus::Running).then_some(parsed);
            Ok(StartedChat {
                session_id,
                output_id,
                immediate_output,
            })
        })
    }
    fn get_output<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
        output_id: &'a str,
    ) -> BoxFuture<'a, Result<CortexOutput>> {
        Box::pin(async move {
            let resp = self
                .http
                .get(format!(
                    "{}/chats/{session_id}/outputs/{output_id}",
                    self.base_url
                ))
                .header("x-uuid", owner)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            if !resp.status().is_success() {
                return Err(Error::unavailable());
            }
            let body: Value = resp.json().await.map_err(|_| Error::unavailable())?;
            parse_output(&body)
        })
    }
    fn cancel_output<'a>(
        &'a self,
        owner: &'a str,
        session_id: &'a str,
        output_id: &'a str,
    ) -> BoxFuture<'a, Result<CortexOutput>> {
        Box::pin(async move {
            let resp = self
                .http
                .patch(format!(
                    "{}/chats/{session_id}/outputs/{output_id}",
                    self.base_url
                ))
                .header("x-uuid", owner)
                .json(&json!({"action": "cancel"}))
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            if !resp.status().is_success() {
                return Err(Error::unavailable());
            }
            let body: Value = resp.json().await.map_err(|_| Error::unavailable())?;
            parse_output(&body)
        })
    }
    fn delete_chat<'a>(&'a self, owner: &'a str, session_id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let resp = self
                .http
                .delete(format!("{}/chats/{session_id}", self.base_url))
                .header("x-uuid", owner)
                .send()
                .await
                .map_err(|_| Error::unavailable())?;
            if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
                Ok(())
            } else {
                Err(Error::unavailable())
            }
        })
    }
}
