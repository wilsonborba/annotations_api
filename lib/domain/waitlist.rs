use crate::{
    core::{Error, Result},
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
pub async fn register(store: &Store, input: Request, forwarded_for: Option<&str>) -> Result<Value> {
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
    store.waitlist.join(&email, forwarded_for).await?;
    Ok(json!({"status": "registered"}))
}
