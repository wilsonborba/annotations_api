use crate::core::{Error, Result};
use crate::domain::rooms::Room;
use redis::AsyncCommands;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Clone)]
pub enum Rooms {
    Redis(redis::Client),
    Memory(Arc<Mutex<HashMap<String, Room>>>),
    Unavailable,
}
impl Rooms {
    pub fn new(redis: Option<redis::Client>) -> Self {
        redis.map(Self::Redis).unwrap_or(Self::Unavailable)
    }
    pub fn fixture() -> Self {
        Self::Memory(Arc::default())
    }
    fn key(code: &str) -> String {
        format!("annotations:room:{code}")
    }
    pub async fn get(&self, code: &str, now: i64) -> Result<Room> {
        let room = match self {
            Self::Memory(m) => m.lock().unwrap().get(code).cloned(),
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let text: Option<String> = c
                    .get(Self::key(code))
                    .await
                    .map_err(|_| Error::unavailable())?;
                text.and_then(|s| serde_json::from_str(&s).ok())
            }
            Self::Unavailable => return Err(Error::unavailable()),
        }
        .ok_or_else(Error::missing)?;
        if room.expires_at <= now {
            return Err(Error(
                axum::http::StatusCode::GONE,
                "This room has expired".into(),
            ));
        }
        Ok(room)
    }
    pub async fn create(&self, room: Room, now: i64) -> Result<Room> {
        match self {
            Self::Memory(m) => {
                let mut state = m.lock().unwrap();
                if let Some(existing) = state.get(&room.code)
                    && existing.expires_at > now
                {
                    return Ok(existing.clone());
                }
                state.insert(room.code.clone(), room.clone());
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let saved: Option<String> = redis::cmd("SET")
                    .arg(Self::key(&room.code))
                    .arg(serde_json::to_string(&room).unwrap())
                    .arg("NX")
                    .arg("EX")
                    .arg(room.expires_at - now)
                    .query_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
                if saved.is_none() {
                    return self.get(&room.code, now).await;
                }
            }
            Self::Unavailable => return Err(Error::unavailable()),
        }
        Ok(room)
    }
    pub async fn compare_set(&self, old: i64, room: &Room, now: i64) -> Result<()> {
        if room.expires_at <= now {
            return Err(Error(
                axum::http::StatusCode::GONE,
                "This room has expired".into(),
            ));
        }
        match self {
            Self::Memory(m) => {
                let mut state = m.lock().unwrap();
                let previous = state.get(&room.code).ok_or_else(Error::missing)?;
                if previous.expires_at <= now || previous.revision != old {
                    return Err(Error::conflict("The room changed; reload before editing"));
                }
                state.insert(room.code.clone(), room.clone());
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let changed:i64=redis::Script::new("local s=redis.call('GET',KEYS[1]); if not s then return 0 end; local r=cjson.decode(s); if r.revision~=tonumber(ARGV[1]) or r.expires_at<=tonumber(ARGV[4]) then return 0 end; redis.call('SET',KEYS[1],ARGV[2],'EX',ARGV[3]); return 1").key(Self::key(&room.code)).arg(old).arg(serde_json::to_string(room).unwrap()).arg(room.expires_at-now).arg(now).invoke_async(&mut c).await.map_err(|_|Error::unavailable())?;
                if changed != 1 {
                    return Err(Error::conflict("The room changed; reload before editing"));
                }
            }
            Self::Unavailable => return Err(Error::unavailable()),
        }
        Ok(())
    }
}
