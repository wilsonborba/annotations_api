use crate::{
    core::{Error, Result, now},
    dal::Store,
};
use serde::Deserialize;
use serde_json::{Value, json};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub email: String,
    pub message: String,
}
pub async fn register(store: &Store, input: Request) -> Result<Value> {
    let email = input.email.trim().to_lowercase();
    if email.len() > 254
        || email.contains(char::is_whitespace)
        || !email.split_once('@').is_some_and(|(name, host)| {
            !name.is_empty() && host.contains('.') && !host.starts_with('.') && !host.ends_with('.')
        })
        || input.message.trim().is_empty()
        || input.message.len() > 5000
    {
        return Err(Error::bad(
            "Provide an email and a message of up to 5000 characters",
        ));
    }
    let result=sqlx::query("INSERT INTO waitlist(email,message,created_at) VALUES($1,$2,$3) ON CONFLICT(email) DO NOTHING").bind(email).bind(input.message.trim()).bind(now()).execute(&store.db).await?;
    Ok(json!({"status":if result.rows_affected()==1{"registered"}else{"already_registered"}}))
}
