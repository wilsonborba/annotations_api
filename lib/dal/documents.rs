use crate::core::{Error, Result, Settings};
use serde_json::Value;
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
    pub async fn get(&self, id: &str) -> Result<Value> {
        match self {
            Self::Memory(m) => m
                .lock()
                .unwrap()
                .get(id)
                .cloned()
                .ok_or_else(Error::missing),
            Self::Remote { .. } => {
                let r = self.request(reqwest::Method::GET, id).send().await?;
                if r.status() == 404 {
                    return Err(Error::missing());
                }
                Ok(r.error_for_status()?.json().await?)
            }
        }
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
