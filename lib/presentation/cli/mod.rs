//! Host-permission protected commands. There is no HTTP promotion interface.
use crate::domain::models::{self, Manifest};
use sqlx::AnyPool;

pub enum Command {
    Register(String),
    Activate(String),
    Rollback,
}
pub fn parse(args: &[String]) -> anyhow::Result<Option<Command>> {
    let usage = "Usage: annotations-api models register <manifest> | models activate <version> | models rollback";
    Ok(match args {
        [] => None,
        [group, verb, path] if group == "models" && verb == "register" => {
            Some(Command::Register(path.clone()))
        }
        [group, verb, version] if group == "models" && verb == "activate" => {
            Some(Command::Activate(version.clone()))
        }
        [group, verb] if group == "models" && verb == "rollback" => Some(Command::Rollback),
        _ => anyhow::bail!(usage),
    })
}
pub async fn run(db: &AnyPool, command: Command) -> anyhow::Result<String> {
    let result = match command {
        Command::Register(path) => {
            let data = tokio::fs::read(path).await.map_err(|_| {
                anyhow::anyhow!("model_manifest_unreadable: Cannot read model manifest")
            })?;
            let manifest: Manifest = serde_json::from_slice(&data).map_err(|_| {
                anyhow::anyhow!("invalid_model_manifest: Model manifest is invalid")
            })?;
            models::register(db, manifest).await
        }
        Command::Activate(version) => models::activate(db, &version).await,
        Command::Rollback => models::rollback(db).await,
    };
    result.map_err(|e| anyhow::anyhow!("{}: {}", e.code, e.message))
}
