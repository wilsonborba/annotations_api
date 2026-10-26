use crate::{
    core::{self, Error, ROOM_SECONDS, Result, UPLOAD_BYTES},
    dal::rooms::Rooms,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[derive(Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub kind: String,
    pub text: String,
    #[serde(default)]
    pub file_key: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub size_bytes: i64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Room {
    pub code: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub revision: i64,
    pub items: Vec<Item>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edit {
    pub id: Option<String>,
    pub revision: Option<i64>,
    pub kind: String,
    pub text: String,
}
impl Room {
    pub fn public(&self) -> Value {
        json!({"code":self.code,"created_at":self.created_at,"expires_at":self.expires_at,"revision":self.revision,"max_file_bytes":UPLOAD_BYTES,"max_ip_bytes":UPLOAD_BYTES,"items":self.items.iter().map(|i|json!({"id":i.id,"kind":i.kind,"text":i.text,"filename":i.filename,"content_type":i.content_type,"size_bytes":i.size_bytes,"url":if i.file_key.is_empty(){String::new()}else{format!("/instant/{}/files/{}",self.code,i.id)}})).collect::<Vec<_>>()})
    }
}
pub async fn create(store: &Rooms, code: &str, now: i64) -> Result<Room> {
    if !core::slug(code) {
        return Err(Error::bad(
            "Use a room code with letters, digits, hyphens or underscores",
        ));
    }
    store
        .create(
            Room {
                code: code.into(),
                created_at: now,
                expires_at: now + ROOM_SECONDS,
                revision: 0,
                items: vec![],
            },
            now,
        )
        .await
}
pub async fn edit(store: &Rooms, code: &str, input: Edit, now: i64) -> Result<Room> {
    if !matches!(input.kind.as_str(), "text" | "link")
        || input.text.trim().is_empty()
        || input.text.len() > UPLOAD_BYTES as usize
    {
        return Err(Error::bad("A text or link item is required"));
    }
    if input.kind == "link" {
        crate::domain::schemas::validate_url(&input.text)?;
    }
    for _ in 0..5 {
        let mut room = store.get(code, now).await?;
        let previous = room.revision;
        if let Some(id) = &input.id {
            if input.revision != Some(previous) {
                return Err(Error::conflict("The room changed; reload before editing"));
            }
            let item = room
                .items
                .iter_mut()
                .find(|i| &i.id == id && i.file_key.is_empty())
                .ok_or_else(Error::missing)?;
            item.text = input.text.clone();
            item.kind = input.kind.clone();
        } else {
            room.items.push(Item {
                id: core::id(),
                kind: input.kind.clone(),
                text: input.text.clone(),
                file_key: String::new(),
                filename: String::new(),
                content_type: String::new(),
                size_bytes: 0,
            });
        }
        let retained: usize = room.items.iter().map(|i| i.text.len()).sum();
        if retained > UPLOAD_BYTES as usize {
            return Err(Error::bad("The room text limit has been reached"));
        }
        room.revision += 1;
        match store.compare_set(previous, &room, now).await {
            Ok(()) => return Ok(room),
            Err(e) if e.0 == axum::http::StatusCode::CONFLICT && input.id.is_none() => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Error::conflict("The room changed; try again"))
}
