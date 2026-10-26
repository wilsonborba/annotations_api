use redis::AsyncCommands;
#[derive(Clone)]
pub struct Cache(pub Option<redis::Client>);
impl Cache {
    pub async fn get(&self, key: &str) -> Option<String> {
        let client = self.0.as_ref()?;
        let mut c = client.get_multiplexed_async_connection().await.ok()?;
        let version: i64 = c.get("annotations:cache-generation").await.unwrap_or(0);
        c.get(format!("annotations:cache:{version}:{key}"))
            .await
            .ok()
    }
    pub async fn put(&self, key: &str, value: &str) {
        if let Some(client) = &self.0
            && let Ok(mut c) = client.get_multiplexed_async_connection().await
        {
            let version: i64 = c.get("annotations:cache-generation").await.unwrap_or(0);
            let _: redis::RedisResult<()> = c
                .set_ex(
                    format!("annotations:cache:{version}:{key}"),
                    value,
                    crate::core::CACHE_SECONDS,
                )
                .await;
        }
    }
    pub async fn invalidate(&self) {
        if let Some(client) = &self.0
            && let Ok(mut c) = client.get_multiplexed_async_connection().await
        {
            let _: redis::RedisResult<i64> = c.incr("annotations:cache-generation", 1).await;
        }
    }
}
