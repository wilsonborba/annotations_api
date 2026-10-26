use redis::AsyncCommands;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Default)]
struct Memory {
    epoch: i64,
    values: HashMap<String, String>,
}
#[derive(Clone)]
pub struct Cache {
    redis: Option<redis::Client>,
    memory: Option<Arc<Mutex<Memory>>>,
}
impl Cache {
    pub fn new(redis: Option<redis::Client>) -> Self {
        Self {
            redis,
            memory: None,
        }
    }
    pub fn fixture() -> Self {
        Self {
            redis: None,
            memory: Some(Arc::default()),
        }
    }
    pub async fn epoch(&self) -> Option<i64> {
        if let Some(m) = &self.memory {
            return Some(m.lock().unwrap().epoch);
        }
        let mut c = self
            .redis
            .as_ref()?
            .get_multiplexed_async_connection()
            .await
            .ok()?;
        let epoch: Option<i64> = c.get("annotations:cache-generation").await.ok()?;
        Some(epoch.unwrap_or(0))
    }
    pub async fn get_at(&self, epoch: i64, key: &str) -> Option<String> {
        let key = format!("annotations:cache:{epoch}:{key}");
        if let Some(m) = &self.memory {
            return m.lock().unwrap().values.get(&key).cloned();
        }
        let mut c = self
            .redis
            .as_ref()?
            .get_multiplexed_async_connection()
            .await
            .ok()?;
        c.get(key).await.ok()
    }
    pub async fn put_at(&self, epoch: i64, key: &str, value: &str) {
        let key = format!("annotations:cache:{epoch}:{key}");
        if let Some(m) = &self.memory {
            m.lock().unwrap().values.insert(key, value.into());
            return;
        }
        if let Some(client) = &self.redis
            && let Ok(mut c) = client.get_multiplexed_async_connection().await
        {
            let _: redis::RedisResult<()> = c.set_ex(key, value, crate::core::CACHE_SECONDS).await;
        }
    }
    pub async fn invalidate(&self) {
        if let Some(m) = &self.memory {
            let mut m = m.lock().unwrap();
            m.epoch += 1;
            m.values.clear();
            return;
        }
        if let Some(client) = &self.redis
            && let Ok(mut c) = client.get_multiplexed_async_connection().await
        {
            let _: redis::RedisResult<i64> = c.incr("annotations:cache-generation", 1).await;
        }
    }
}
