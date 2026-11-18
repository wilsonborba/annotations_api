use crate::{
    core::{Error, Result},
    domain::rooms::{Room, expired},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Expired content is discarded. Only code and original expiry remain as tombstones.
#[derive(Default)]
pub struct Memory {
    live: HashMap<String, Room>,
    used: HashMap<String, i64>,
}
#[derive(Clone)]
pub enum Rooms {
    Redis(redis::Client),
    Memory(Arc<Mutex<Memory>>),
    Unavailable,
}

// Use Redis TIME at the atomic boundary as well as the caller's observation.
// An old caller timestamp cannot prolong access or recreate an expired key.
const READ: &str = r#"
local now=math.max(tonumber(ARGV[1]),tonumber(redis.call('TIME')[1]))
local text=redis.call('GET',KEYS[1])
if text then
    local room=cjson.decode(text)
    if room.expires_at>now then return {1,text} end
    redis.call('SETNX',KEYS[2],tostring(room.expires_at))
    redis.call('DEL',KEYS[1])
end
if redis.call('EXISTS',KEYS[2])==1 then return {3,''} end
return {0,''}
"#;
const CREATE: &str = r#"
local now=math.max(tonumber(ARGV[2]),tonumber(redis.call('TIME')[1]))
local text=redis.call('GET',KEYS[1])
if text then
    local previous=cjson.decode(text)
    redis.call('SETNX',KEYS[2],tostring(previous.expires_at))
    if previous.expires_at>now then return {2,''} end
    redis.call('DEL',KEYS[1])
    return {3,''}
end
if redis.call('EXISTS',KEYS[2])==1 then return {3,''} end
local room=cjson.decode(ARGV[1])
if room.expires_at<=now then return {3,''} end
redis.call('SET',KEYS[2],tostring(room.expires_at))
redis.call('SET',KEYS[1],ARGV[1],'EXAT',room.expires_at)
return {1,ARGV[1]}
"#;
const COMPARE_SET: &str = r#"
local now=math.max(tonumber(ARGV[3]),tonumber(redis.call('TIME')[1]))
local text=redis.call('GET',KEYS[1])
if not text then
    if redis.call('EXISTS',KEYS[2])==1 then return {3,''} end
    return {0,''}
end
local previous=cjson.decode(text)
if previous.expires_at<=now then
    redis.call('SETNX',KEYS[2],tostring(previous.expires_at))
    redis.call('DEL',KEYS[1])
    return {3,''}
end
if previous.revision~=tonumber(ARGV[1]) then return {2,''} end
local next=cjson.decode(ARGV[2])
if next.code~=previous.code or next.created_at~=previous.created_at
    or next.expires_at~=previous.expires_at or next.schema_version~=previous.schema_version
    or next.revision~=previous.revision+1 then return {4,''} end
redis.call('SET',KEYS[1],ARGV[2],'EXAT',previous.expires_at)
return {1,''}
"#;
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
    fn used(code: &str) -> String {
        format!("annotations:room-used:{code}")
    }
    pub async fn get(&self, code: &str, now: i64) -> Result<Room> {
        match self {
            Self::Memory(m) => {
                let mut state = m.lock().unwrap();
                if state.live.get(code).is_some_and(|r| r.expires_at <= now) {
                    state.live.remove(code);
                }
                if let Some(room) = state.live.get(code) {
                    return Ok(room.clone());
                }
                Err(if state.used.contains_key(code) {
                    expired()
                } else {
                    Error::missing()
                })
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let (status, text): (i64, String) = redis::Script::new(READ)
                    .key(Self::key(code))
                    .key(Self::used(code))
                    .arg(now)
                    .invoke_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
                match status {
                    1 => serde_json::from_str(&text).map_err(|_| Error::unavailable()),
                    3 => Err(expired()),
                    _ => Err(Error::missing()),
                }
            }
            Self::Unavailable => Err(Error::unavailable()),
        }
    }
    pub async fn create(&self, room: Room, now: i64) -> Result<Room> {
        if room.expires_at <= now {
            return Err(expired());
        }
        match self {
            Self::Memory(m) => {
                let mut state = m.lock().unwrap();
                if state.used.contains_key(&room.code) {
                    if state
                        .live
                        .get(&room.code)
                        .is_some_and(|r| r.expires_at > now)
                    {
                        return Err(Error::conflict(
                            "room_exists: The room code is already in use",
                        ));
                    }
                    state.live.remove(&room.code);
                    return Err(expired());
                }
                state.used.insert(room.code.clone(), room.expires_at);
                state.live.insert(room.code.clone(), room.clone());
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let (status, _): (i64, String) = redis::Script::new(CREATE)
                    .key(Self::key(&room.code))
                    .key(Self::used(&room.code))
                    .arg(serde_json::to_string(&room).map_err(|_| Error::unavailable())?)
                    .arg(now)
                    .invoke_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
                match status {
                    1 => (),
                    2 => {
                        return Err(Error::conflict(
                            "room_exists: The room code is already in use",
                        ));
                    }
                    3 => return Err(expired()),
                    _ => return Err(Error::unavailable()),
                }
            }
            Self::Unavailable => return Err(Error::unavailable()),
        }
        Ok(room)
    }
    /// False only means the snapshot changed. Missing and expired states are errors.
    pub async fn compare_set(&self, old: i64, room: &Room, now: i64) -> Result<bool> {
        match self {
            Self::Memory(m) => {
                let mut state = m.lock().unwrap();
                let Some(previous) = state.live.get(&room.code) else {
                    return Err(if state.used.contains_key(&room.code) {
                        expired()
                    } else {
                        Error::missing()
                    });
                };
                if previous.expires_at <= now {
                    state.live.remove(&room.code);
                    return Err(expired());
                }
                if previous.revision != old {
                    return Ok(false);
                }
                if previous.created_at != room.created_at
                    || previous.expires_at != room.expires_at
                    || previous.schema_version != room.schema_version
                    || room.revision != old + 1
                {
                    return Err(Error::bad(
                        "invalid_room_write: Room lifetime and schema cannot change",
                    ));
                }
                state.live.insert(room.code.clone(), room.clone());
                Ok(true)
            }
            Self::Redis(client) => {
                let mut c = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::unavailable())?;
                let (status, _): (i64, String) = redis::Script::new(COMPARE_SET)
                    .key(Self::key(&room.code))
                    .key(Self::used(&room.code))
                    .arg(old)
                    .arg(serde_json::to_string(room).map_err(|_| Error::unavailable())?)
                    .arg(now)
                    .invoke_async(&mut c)
                    .await
                    .map_err(|_| Error::unavailable())?;
                match status {
                    1 => Ok(true),
                    2 => Ok(false),
                    3 => Err(expired()),
                    4 => Err(Error::bad(
                        "invalid_room_write: Room lifetime and schema cannot change",
                    )),
                    _ => Err(Error::missing()),
                }
            }
            Self::Unavailable => Err(Error::unavailable()),
        }
    }
}
