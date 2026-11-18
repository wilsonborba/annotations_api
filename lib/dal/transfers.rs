use crate::{
    core::{Error, Result, UPLOAD_BYTES},
    domain::rooms::expired,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    Reserved,
    Uploading,
    Failed,
    Ready,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub id: String,
    pub ip: String,
    pub bytes: i64,
    pub expires_at: i64,
    pub album: String,
    pub file_key: Option<String>,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub checksum: String,
    #[serde(default)]
    pub status: Status,
    #[serde(default)]
    pub attempt: String,
    /// The verified bytes are of a preview type (`domain::files::PREVIEW_TYPES`).
    #[serde(default)]
    pub preview: bool,
}
#[derive(Clone)]
pub enum Transfers {
    Redis(redis::Client),
    Memory(Arc<Mutex<HashMap<String, Reservation>>>),
    Unavailable,
}
const RESERVE: &str = r#"
local now=math.max(tonumber(ARGV[4]),tonumber(redis.call('TIME')[1]))
local input=cjson.decode(ARGV[1])
if input.expires_at<=now then return {3,''} end
local previous=redis.call('GET',KEYS[2])
if previous then return {1,previous} end
local used=tonumber(redis.call('GET',KEYS[1]) or '0')
local due=redis.call('ZRANGEBYSCORE',KEYS[4],'-inf',now)
for _,member in ipairs(due) do
    used=math.max(0,used-tonumber(string.match(member,':(%d+)$')))
    redis.call('ZREM',KEYS[4],member)
end
redis.call('SET',KEYS[1],used)
if used+input.bytes>tonumber(ARGV[2]) then return {2,''} end
redis.call('SET',KEYS[1],used+input.bytes)
redis.call('SET',KEYS[2],ARGV[1])
redis.call('ZADD',KEYS[3],input.expires_at,input.id)
redis.call('ZADD',KEYS[4],input.expires_at,ARGV[3])
return {1,ARGV[1]}
"#;
const COMPARE_SET: &str = r#"
local now=math.max(tonumber(ARGV[3]),tonumber(redis.call('TIME')[1]))
local text=redis.call('GET',KEYS[1])
if not text then return 0 end
if cjson.decode(text).expires_at<=now then return 3 end
if text~=ARGV[1] then return 2 end
redis.call('SET',KEYS[1],ARGV[2])
return 1
"#;
const RELEASE: &str = r#"
local text=redis.call('GET',KEYS[1])
if text then
    local r=cjson.decode(text)
    local q='annotations:quota:'..r.ip
    local index='annotations:quota-active:'..r.ip
    local member=r.id..':'..tostring(r.bytes)
    if redis.call('ZREM',index,member)==1 or not r.code or r.code=='' then
        local used=tonumber(redis.call('GET',q) or '0')
        if used<=r.bytes then redis.call('DEL',q) else redis.call('DECRBY',q,r.bytes) end
    end
    redis.call('DEL',KEYS[1])
end
redis.call('ZREM',KEYS[2],ARGV[1])
return 1
"#;
impl Transfers {
    pub fn new(redis: Option<redis::Client>) -> Self {
        redis.map(Self::Redis).unwrap_or(Self::Unavailable)
    }
    pub fn fixture() -> Self {
        Self::Memory(Arc::default())
    }
    pub async fn reserve(&self, input: Reservation, now: i64) -> Result<Reservation> {
        if input.bytes <= 0 || input.bytes > UPLOAD_BYTES {
            return Err(Error(
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                "file_too_large: A file must be between 1 byte and 50 MB".into(),
            ));
        }
        if input.expires_at <= now {
            return Err(expired());
        }
        match self {
            Self::Memory(m) => {
                let mut m = m.lock().unwrap();
                if let Some(previous) = m.get(&input.id) {
                    return Ok(previous.clone());
                }
                let used: i64 = m
                    .values()
                    .filter(|r| r.ip == input.ip && r.expires_at > now)
                    .map(|r| r.bytes)
                    .sum();
                if used + input.bytes > UPLOAD_BYTES {
                    return Err(quota_full());
                }
                m.insert(input.id.clone(), input.clone());
                Ok(input)
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let (status, text): (i64, String) = redis::Script::new(RESERVE)
                    .key(format!("annotations:quota:{}", input.ip))
                    .key(format!("annotations:transfer:{}", input.id))
                    .key("annotations:cleanup")
                    .key(format!("annotations:quota-active:{}", input.ip))
                    .arg(serde_json::to_string(&input).map_err(|_| Error::unavailable())?)
                    .arg(UPLOAD_BYTES)
                    .arg(format!("{}:{}", input.id, input.bytes))
                    .arg(now)
                    .invoke_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
                match status {
                    1 => serde_json::from_str(&text).map_err(|_| Error::unavailable()),
                    3 => Err(expired()),
                    _ => Err(quota_full()),
                }
            }
            Self::Unavailable => Err(Error::unavailable()),
        }
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
                value
                    .map(|v| serde_json::from_str(&v).map_err(|_| Error::unavailable()))
                    .transpose()
            }
            Self::Unavailable => Err(Error::unavailable()),
        }
    }
    /// Atomic upload ownership and finalization. Unchanged metadata stays bound to
    /// the original room, uploader and absolute expiry throughout the operation.
    pub async fn compare_set(
        &self,
        previous: &Reservation,
        next: &Reservation,
        now: i64,
    ) -> Result<bool> {
        if next.id != previous.id
            || next.ip != previous.ip
            || next.code != previous.code
            || next.bytes != previous.bytes
            || next.expires_at != previous.expires_at
            || next.album != previous.album
            || next.filename != previous.filename
            || next.content_type != previous.content_type
            || next.checksum != previous.checksum
        {
            return Err(Error::bad(
                "invalid_upload_state: Upload metadata cannot change",
            ));
        }
        if previous.expires_at <= now {
            return Err(expired());
        }
        match self {
            Self::Memory(m) => {
                let mut m = m.lock().unwrap();
                if m.get(&previous.id) != Some(previous) {
                    return Ok(false);
                }
                m.insert(next.id.clone(), next.clone());
                Ok(true)
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let status: i64 = redis::Script::new(COMPARE_SET)
                    .key(format!("annotations:transfer:{}", previous.id))
                    .arg(serde_json::to_string(previous).map_err(|_| Error::unavailable())?)
                    .arg(serde_json::to_string(next).map_err(|_| Error::unavailable())?)
                    .arg(now)
                    .invoke_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
                match status {
                    1 => Ok(true),
                    3 => Err(expired()),
                    _ => Ok(false),
                }
            }
            Self::Unavailable => Err(Error::unavailable()),
        }
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
                let _: i64 = redis::Script::new(RELEASE)
                    .key(format!("annotations:transfer:{id}"))
                    .key("annotations:cleanup")
                    .arg(id)
                    .invoke_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
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
fn quota_full() -> Error {
    Error(
        axum::http::StatusCode::TOO_MANY_REQUESTS,
        "ip_quota_exceeded: The active 50 MB upload allowance for this IP is full".into(),
    )
}
