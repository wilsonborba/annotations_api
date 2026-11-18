//! Server-side adapter for the Plane Slim work-items and projects APIs on the LAN.
//! Credentials (`PLANE_API_KEY`) and host (`PLANE_URL`) stay on the server only.
use crate::core::{Error, Result};
use axum::http::StatusCode;
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, sync::Arc};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug)]
pub struct PlaneState {
    pub id: String,
    pub group: String,
}

#[derive(Clone, Debug)]
pub struct PlaneWorkItem {
    pub id: String,
    pub name: String,
    pub description_html: String,
    pub target_date: Option<String>,
    pub state: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PlaneWorkItemPage {
    pub results: Vec<PlaneWorkItem>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateWorkItem {
    pub name: String,
    pub description_html: Option<String>,
    pub target_date: Option<String>,
    pub state: String,
}

#[derive(Clone, Debug, Default)]
pub struct UpdateWorkItem {
    pub name: Option<String>,
    pub description_html: Option<String>,
    pub target_date: Option<Option<String>>,
    pub state: Option<String>,
}

pub trait PlaneClient: Send + Sync {
    fn is_configured(&self) -> bool;
    fn create_project<'a>(
        &'a self,
        name: &'a str,
        identifier: &'a str,
    ) -> BoxFuture<'a, Result<String>>;
    fn list_states<'a>(&'a self, project_id: &'a str) -> BoxFuture<'a, Result<Vec<PlaneState>>>;
    fn list_work_items<'a>(
        &'a self,
        project_id: &'a str,
        cursor: Option<&'a str>,
        limit: i64,
    ) -> BoxFuture<'a, Result<PlaneWorkItemPage>>;
    fn create_work_item<'a>(
        &'a self,
        project_id: &'a str,
        input: &'a CreateWorkItem,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>>;
    fn get_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>>;
    fn update_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
        input: &'a UpdateWorkItem,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>>;
    fn delete_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<()>>;
}

pub fn disabled() -> Error {
    Error(
        StatusCode::SERVICE_UNAVAILABLE,
        "plane_unavailable: Task integration is not configured".into(),
    )
}

pub struct DisabledPlane;
impl PlaneClient for DisabledPlane {
    fn is_configured(&self) -> bool {
        false
    }
    fn create_project<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<String>> {
        Box::pin(async { Err(disabled()) })
    }
    fn list_states<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<PlaneState>>> {
        Box::pin(async { Err(disabled()) })
    }
    fn list_work_items<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: i64,
    ) -> BoxFuture<'a, Result<PlaneWorkItemPage>> {
        Box::pin(async { Err(disabled()) })
    }
    fn create_work_item<'a>(
        &'a self,
        _: &'a str,
        _: &'a CreateWorkItem,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>> {
        Box::pin(async { Err(disabled()) })
    }
    fn get_work_item<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<PlaneWorkItem>> {
        Box::pin(async { Err(disabled()) })
    }
    fn update_work_item<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a UpdateWorkItem,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>> {
        Box::pin(async { Err(disabled()) })
    }
    fn delete_work_item<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(disabled()) })
    }
}

pub struct HttpPlane {
    base_url: String,
    api_key: String,
    workspace: String,
    http: reqwest::Client,
}

impl HttpPlane {
    pub fn client(
        base_url: String,
        api_key: String,
        workspace: String,
        http: reqwest::Client,
    ) -> Arc<dyn PlaneClient> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if base_url.is_empty() || api_key.trim().is_empty() || workspace.trim().is_empty() {
            Arc::new(DisabledPlane)
        } else {
            Arc::new(Self {
                base_url,
                api_key,
                workspace,
                http,
            })
        }
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/api/v1/workspaces/{}{}",
            self.base_url, self.workspace, path
        )
    }
}

fn map_status(status: StatusCode) -> Error {
    match status {
        StatusCode::NOT_FOUND => Error::missing(),
        StatusCode::CONFLICT => Error(
            StatusCode::CONFLICT,
            "plane_identifier_exists: Plane project identifier already exists".into(),
        ),
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            Error::bad("plane_rejected: Plane rejected the task request".to_string())
        }
        StatusCode::TOO_MANY_REQUESTS => Error(
            StatusCode::TOO_MANY_REQUESTS,
            "plane_rate_limited: Plane rate limit reached".into(),
        ),
        _ => Error(
            StatusCode::SERVICE_UNAVAILABLE,
            "plane_unavailable: Plane service is unavailable".into(),
        ),
    }
}

fn parse_work_item(v: &Value) -> Result<PlaneWorkItem> {
    Ok(PlaneWorkItem {
        id: v["id"]
            .as_str()
            .map(String::from)
            .ok_or_else(Error::unavailable)?,
        name: v["name"].as_str().unwrap_or_default().to_string(),
        description_html: v["description_html"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        target_date: v["target_date"].as_str().map(String::from),
        state: v["state"].as_str().unwrap_or_default().to_string(),
        created_at: v["created_at"].as_str().map(String::from),
        updated_at: v["updated_at"].as_str().map(String::from),
    })
}

impl PlaneClient for HttpPlane {
    fn is_configured(&self) -> bool {
        true
    }
    fn create_project<'a>(
        &'a self,
        name: &'a str,
        identifier: &'a str,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let resp = self
                .http
                .post(self.url("/projects/"))
                .header("X-Api-Key", &self.api_key)
                .json(&json!({"name": name, "identifier": identifier, "network": 0}))
                .send()
                .await
                .map_err(|_| disabled())?;
            if !resp.status().is_success() {
                let status = resp.status();
                if status == StatusCode::CONFLICT {
                    return Err(map_status(status));
                }
                if matches!(
                    status,
                    StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
                ) {
                    let text = resp.text().await.unwrap_or_default().to_ascii_lowercase();
                    if text.contains("identifier")
                        || text.contains("already exists")
                        || text.contains("duplicate")
                    {
                        return Err(map_status(StatusCode::CONFLICT));
                    }
                }
                return Err(map_status(status));
            }
            let body: Value = resp.json().await.map_err(|_| disabled())?;
            body["id"].as_str().map(String::from).ok_or_else(disabled)
        })
    }
    fn list_states<'a>(&'a self, project_id: &'a str) -> BoxFuture<'a, Result<Vec<PlaneState>>> {
        Box::pin(async move {
            let resp = self
                .http
                .get(self.url(&format!("/projects/{project_id}/states/")))
                .header("X-Api-Key", &self.api_key)
                .send()
                .await
                .map_err(|_| disabled())?;
            if !resp.status().is_success() {
                return Err(map_status(resp.status()));
            }
            let body: Value = resp.json().await.map_err(|_| disabled())?;
            let list = body["results"]
                .as_array()
                .or_else(|| body.as_array())
                .ok_or_else(disabled)?;
            Ok(list
                .iter()
                .filter_map(|s| {
                    Some(PlaneState {
                        id: s["id"].as_str()?.to_string(),
                        group: s["group"].as_str().unwrap_or("").to_string(),
                    })
                })
                .collect())
        })
    }
    fn list_work_items<'a>(
        &'a self,
        project_id: &'a str,
        cursor: Option<&'a str>,
        limit: i64,
    ) -> BoxFuture<'a, Result<PlaneWorkItemPage>> {
        Box::pin(async move {
            let mut req = self
                .http
                .get(self.url(&format!("/projects/{project_id}/work-items/")))
                .header("X-Api-Key", &self.api_key)
                .query(&[("per_page", limit.to_string())]);
            if let Some(c) = cursor {
                req = req.query(&[("cursor", c)]);
            }
            let resp = req.send().await.map_err(|_| disabled())?;
            if !resp.status().is_success() {
                return Err(map_status(resp.status()));
            }
            let body: Value = resp.json().await.map_err(|_| disabled())?;
            let list = body["results"]
                .as_array()
                .or_else(|| body.as_array())
                .ok_or_else(disabled)?;
            let results = list
                .iter()
                .map(parse_work_item)
                .collect::<Result<Vec<_>>>()?;
            let next_cursor = body["next_cursor"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from);
            Ok(PlaneWorkItemPage {
                results,
                next_cursor,
            })
        })
    }
    fn create_work_item<'a>(
        &'a self,
        project_id: &'a str,
        input: &'a CreateWorkItem,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>> {
        Box::pin(async move {
            let mut payload = json!({
                "name": input.name,
                "state": input.state,
            });
            if let Some(desc) = &input.description_html {
                payload["description_html"] = json!(desc);
            }
            if let Some(date) = &input.target_date {
                payload["target_date"] = json!(date);
            }
            let resp = self
                .http
                .post(self.url(&format!("/projects/{project_id}/work-items/")))
                .header("X-Api-Key", &self.api_key)
                .json(&payload)
                .send()
                .await
                .map_err(|_| disabled())?;
            if !resp.status().is_success() {
                return Err(map_status(resp.status()));
            }
            let body: Value = resp.json().await.map_err(|_| disabled())?;
            parse_work_item(&body)
        })
    }
    fn get_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>> {
        Box::pin(async move {
            let resp = self
                .http
                .get(self.url(&format!("/projects/{project_id}/work-items/{task_id}/")))
                .header("X-Api-Key", &self.api_key)
                .send()
                .await
                .map_err(|_| disabled())?;
            if !resp.status().is_success() {
                return Err(map_status(resp.status()));
            }
            let body: Value = resp.json().await.map_err(|_| disabled())?;
            parse_work_item(&body)
        })
    }
    fn update_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
        input: &'a UpdateWorkItem,
    ) -> BoxFuture<'a, Result<PlaneWorkItem>> {
        Box::pin(async move {
            let mut payload = serde_json::Map::new();
            if let Some(name) = &input.name {
                payload.insert("name".into(), json!(name));
            }
            if let Some(desc) = &input.description_html {
                payload.insert("description_html".into(), json!(desc));
            }
            if let Some(date) = &input.target_date {
                payload.insert("target_date".into(), json!(date));
            }
            if let Some(state) = &input.state {
                payload.insert("state".into(), json!(state));
            }
            let resp = self
                .http
                .patch(self.url(&format!("/projects/{project_id}/work-items/{task_id}/")))
                .header("X-Api-Key", &self.api_key)
                .json(&Value::Object(payload))
                .send()
                .await
                .map_err(|_| disabled())?;
            if !resp.status().is_success() {
                return Err(map_status(resp.status()));
            }
            let body: Value = resp.json().await.map_err(|_| disabled())?;
            parse_work_item(&body)
        })
    }
    fn delete_work_item<'a>(
        &'a self,
        project_id: &'a str,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let resp = self
                .http
                .delete(self.url(&format!("/projects/{project_id}/work-items/{task_id}/")))
                .header("X-Api-Key", &self.api_key)
                .send()
                .await
                .map_err(|_| disabled())?;
            if !resp.status().is_success() {
                return Err(map_status(resp.status()));
            }
            Ok(())
        })
    }
}
