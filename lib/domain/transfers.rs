use crate::{
    core::{self, Error, Result},
    dal::{Store, transfers::Reservation},
    domain::rooms::Item,
};
pub async fn begin(
    store: &Store,
    code: &str,
    ip: &str,
    size: i64,
    now: i64,
) -> Result<Reservation> {
    let room = store.rooms.get(code, now).await?;
    let id = core::id();
    let reservation = Reservation {
        id: id.clone(),
        ip: ip.into(),
        bytes: size,
        expires_at: room.expires_at,
        album: format!("instant-{id}"),
        file_key: None,
    };
    store.transfers.reserve(reservation.clone()).await?;
    Ok(reservation)
}
pub async fn attach(
    store: &Store,
    code: &str,
    reservation: &Reservation,
    file: &crate::dal::files::File,
    now: i64,
) -> Result<serde_json::Value> {
    store.transfers.attach(&reservation.id, &file.key).await?;
    for _ in 0..5 {
        let mut room = store.rooms.get(code, now).await?;
        let old = room.revision;
        room.items.push(Item {
            id: reservation.id.clone(),
            kind: if file.content_type.starts_with("image/") {
                "image".into()
            } else {
                "file".into()
            },
            text: String::new(),
            file_key: file.key.clone(),
            filename: file.filename.clone(),
            content_type: file.content_type.clone(),
            size_bytes: file.size_bytes,
        });
        room.revision += 1;
        match store.rooms.compare_set(old, &room, now).await {
            Ok(()) => return Ok(room.public()),
            Err(e) if e.0 == axum::http::StatusCode::CONFLICT => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Error::conflict("The room changed; try again"))
}
