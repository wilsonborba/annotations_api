use crate::core::{Error, Result, UPLOAD_BYTES};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Clone, Serialize, Deserialize)]
pub struct Reservation {
    pub id: String,
    pub ip: String,
    pub bytes: i64,
    pub expires_at: i64,
    pub album: String,
    pub file_key: Option<String>,
}
#[derive(Clone)]
pub enum Transfers {
    Redis(redis::Client),
    Memory(Arc<Mutex<HashMap<String, Reservation>>>),
    Unavailable,
}
impl Transfers {
    pub fn new(redis: Option<redis::Client>) -> Self {
        redis.map(Self::Redis).unwrap_or(Self::Unavailable)
    }
    pub fn fixture() -> Self {
        Self::Memory(Arc::default())
    }
    pub async fn reserve(&self, input: Reservation) -> Result<()> {
        if input.bytes <= 0 || input.bytes > UPLOAD_BYTES {
            return Err(Error(
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                "A file must be between 1 byte and 50 MB".into(),
            ));
        }
        match self {
            Self::Memory(m) => {
                let mut m = m.lock().unwrap();
                let used: i64 = m
                    .values()
                    .filter(|r| r.ip == input.ip)
                    .map(|r| r.bytes)
                    .sum();
                if used + input.bytes > UPLOAD_BYTES {
                    return Err(Error(
                        axum::http::StatusCode::TOO_MANY_REQUESTS,
                        "The active 50 MB upload allowance for this IP is full".into(),
                    ));
                }
                m.insert(input.id.clone(), input);
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let saved:i64=redis::Script::new("local used=tonumber(redis.call('GET',KEYS[1]) or '0'); if used+tonumber(ARGV[1])>tonumber(ARGV[2]) then return 0 end; redis.call('INCRBY',KEYS[1],ARGV[1]); redis.call('SET',KEYS[2],ARGV[3]); redis.call('ZADD',KEYS[3],ARGV[4],ARGV[5]); return 1").key(format!("annotations:quota:{}",input.ip)).key(format!("annotations:transfer:{}",input.id)).key("annotations:cleanup").arg(input.bytes).arg(UPLOAD_BYTES).arg(serde_json::to_string(&input).unwrap()).arg(input.expires_at).arg(&input.id).invoke_async(&mut c).await.map_err(|_|Error::unavailable())?;
                if saved != 1 {
                    return Err(Error(
                        axum::http::StatusCode::TOO_MANY_REQUESTS,
                        "The active 50 MB upload allowance for this IP is full".into(),
                    ));
                }
            }
            Self::Unavailable => return Err(Error::unavailable()),
        }
        Ok(())
    }
    pub async fn get(&self, id: &str) -> Result<Option<Reservation>> {
        match self {
            Self::Memory(m) => Ok(m.lock().unwrap().get(id).cloned()),
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let value: Option<String> = c
                    .get(format!("annotations:transfer:{id}"))
                    .await
                    .map_err(|_| Error::unavailable())?;
                Ok(value.and_then(|v| serde_json::from_str(&v).ok()))
            }
            Self::Unavailable => Err(Error::unavailable()),
        }
    }
    pub async fn attach(&self, id: &str, key: &str) -> Result<()> {
        match self {
            Self::Memory(m) => {
                m.lock()
                    .unwrap()
                    .get_mut(id)
                    .ok_or_else(Error::missing)?
                    .file_key = Some(key.into());
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let saved:i64=redis::Script::new("local s=redis.call('GET',KEYS[1]); if not s then return 0 end;local r=cjson.decode(s);r.file_key=ARGV[1];redis.call('SET',KEYS[1],cjson.encode(r));return 1").key(format!("annotations:transfer:{id}")).arg(key).invoke_async(&mut c).await.map_err(|_|Error::unavailable())?;
                if saved != 1 {
                    return Err(Error::missing());
                }
            }
            Self::Unavailable => return Err(Error::unavailable()),
        }
        Ok(())
    }
    pub async fn release(&self, id: &str) -> Result<()> {
        match self {
            Self::Memory(m) => {
                m.lock().unwrap().remove(id);
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let _:i64=redis::Script::new("local s=redis.call('GET',KEYS[1]);if s then local r=cjson.decode(s);local q='annotations:quota:'..r.ip;local used=tonumber(redis.call('GET',q) or '0');if used<=r.bytes then redis.call('DEL',q) else redis.call('DECRBY',q,r.bytes) end;redis.call('DEL',KEYS[1]);end;redis.call('ZREM',KEYS[2],ARGV[1]);return 1").key(format!("annotations:transfer:{id}")).key("annotations:cleanup").arg(id).invoke_async(&mut c).await.map_err(|_|Error::unavailable())?;
            }
            Self::Unavailable => return Err(Error::unavailable()),
        }
        Ok(())
    }
    pub async fn due(&self, now: i64) -> Result<Vec<Reservation>> {
        match self {
            Self::Memory(m) => Ok(m
                .lock()
                .unwrap()
                .values()
                .filter(|r| r.expires_at <= now)
                .take(100)
                .cloned()
                .collect()),
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                    .arg("annotations:cleanup")
                    .arg("-inf")
                    .arg(now)
                    .arg("LIMIT")
                    .arg(0)
                    .arg(100)
                    .query_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
                let mut out = vec![];
                for id in ids {
                    if let Some(r) = self.get(&id).await? {
                        out.push(r)
                    } else {
                        self.release(&id).await?
                    }
                }
                Ok(out)
            }
            Self::Unavailable => Err(Error::unavailable()),
        }
    }
}
