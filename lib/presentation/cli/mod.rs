//! Host-permission protected commands. There is no HTTP promotion interface and no
//! HTTP route for training data: collected feedback leaves the service only through
//! `training export`, and only `training remove` deletes it.
use crate::{
    dal::{Store, training::Selection},
    domain::{
        models::{self, Manifest},
        training,
    },
};
use sqlx::AnyPool;

/// Which collected examples `training remove` deletes.
pub enum Target {
    Run(String),
    Note(String),
    Account(String),
}
pub enum Command {
    Register(String),
    Activate(String),
    Rollback,
    Withdraw(String),
    /// Write received and approved examples as JSON Lines to standard output.
    TrainingExport {
        include_flagged: bool,
    },
    TrainingRemove(Target),
}
impl Command {
    /// Training commands read CouchDB documents too; model commands use SQL only.
    pub fn training(&self) -> bool {
        matches!(self, Self::TrainingExport { .. } | Self::TrainingRemove(_))
    }
}
const USAGE: &str = "Usage: annotations-api models register <manifest> | models activate <version> | models rollback | models withdraw <version> | training export [--include-flagged] | training remove (--run <run_id> | --note <note_id> | --account <account_id>)";
pub fn parse(args: &[String]) -> anyhow::Result<Option<Command>> {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Ok(match args.as_slice() {
        [] => None,
        ["models", "register", path] => Some(Command::Register(path.to_string())),
        ["models", "activate", version] => Some(Command::Activate(version.to_string())),
        ["models", "rollback"] => Some(Command::Rollback),
        ["models", "withdraw", version] => Some(Command::Withdraw(version.to_string())),
        ["training", "export"] => Some(Command::TrainingExport {
            include_flagged: false,
        }),
        ["training", "export", "--include-flagged"] => Some(Command::TrainingExport {
            include_flagged: true,
        }),
        ["training", "remove", "--run", id] => {
            Some(Command::TrainingRemove(Target::Run(id.to_string())))
        }
        ["training", "remove", "--note", id] => {
            Some(Command::TrainingRemove(Target::Note(id.to_string())))
        }
        ["training", "remove", "--account", id] => {
            Some(Command::TrainingRemove(Target::Account(id.to_string())))
        }
        _ => anyhow::bail!(USAGE),
    })
}
/// Run a model registry command. Returns the affected model release.
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
        Command::Withdraw(version) => models::withdraw(db, &version).await,
        Command::TrainingExport { .. } | Command::TrainingRemove(_) => {
            anyhow::bail!("Training commands run with document storage")
        }
    };
    result.map_err(|e| anyhow::anyhow!("{}: {}", e.code, e.message))
}
/// Run a training command. Export lines go to `out`; the returned text is a summary.
pub async fn training(
    store: &Store,
    command: Command,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<String> {
    let result = match command {
        Command::TrainingExport { include_flagged } => {
            training::export(store, include_flagged, out)
                .await
                .map(|n| format!("Exported training examples: {n}"))
        }
        Command::TrainingRemove(target) => {
            let selection = match &target {
                Target::Run(id) => Selection::Run(id),
                Target::Note(id) => Selection::Note(id),
                Target::Account(id) => Selection::Account(id),
            };
            training::remove(store, selection)
                .await
                .map(|n| format!("Removed training examples: {n}"))
        }
        _ => anyhow::bail!("Model commands run with registry storage only"),
    };
    result.map_err(|e| anyhow::anyhow!("{}: {}", e.code, e.message))
}
