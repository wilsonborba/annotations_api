//! Host-permission protected commands. There is no HTTP promotion interface and no
//! HTTP route for training data: collected feedback leaves the service only through
//! `training export`, and only `training remove` deletes it.
use crate::{
    dal::{
        Store,
        training::{ReviewTarget, Selection},
    },
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
/// Which collected examples `training approve` or `training flag` updates.
pub enum ReviewSelection {
    Run(String),
    Event(String),
    RunEvent(String, String),
}
pub enum Command {
    Register(String),
    Activate(String),
    Rollback,
    Withdraw(String),
    /// Write flagged items and a representative sample of received items as JSON Lines.
    TrainingReview {
        limit: Option<i64>,
    },
    TrainingApprove(ReviewSelection),
    TrainingFlag(ReviewSelection),
    /// Write approved examples (and optionally received/flagged) as JSON Lines to standard output.
    TrainingExport {
        include_received: bool,
        include_flagged: bool,
    },
    TrainingRemove(Target),
}
impl Command {
    /// Training commands read CouchDB documents too; model commands use SQL only.
    pub fn training(&self) -> bool {
        matches!(
            self,
            Self::TrainingReview { .. }
                | Self::TrainingApprove(_)
                | Self::TrainingFlag(_)
                | Self::TrainingExport { .. }
                | Self::TrainingRemove(_)
        )
    }
}
const USAGE: &str = "Usage: annotations-api models register <manifest> | models activate <version> | models rollback | models withdraw <version> | training review [--limit <n>] | training approve (--run <run_id> [--event <id>] | --event <id>) | training flag (--run <run_id> [--event <id>] | --event <id>) | training export [--include-received] [--include-flagged] | training remove (--run <run_id> | --note <note_id> | --account <account_id>)";
pub fn parse(args: &[String]) -> anyhow::Result<Option<Command>> {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Ok(match args.as_slice() {
        [] => None,
        ["models", "register", path] => Some(Command::Register(path.to_string())),
        ["models", "activate", version] => Some(Command::Activate(version.to_string())),
        ["models", "rollback"] => Some(Command::Rollback),
        ["models", "withdraw", version] => Some(Command::Withdraw(version.to_string())),
        ["training", "review"] => Some(Command::TrainingReview { limit: None }),
        ["training", "review", "--limit", n] => {
            let limit = n.parse::<i64>().map_err(|_| anyhow::anyhow!(USAGE))?;
            Some(Command::TrainingReview { limit: Some(limit) })
        }
        ["training", "approve", "--run", run_id] => Some(Command::TrainingApprove(
            ReviewSelection::Run(run_id.to_string()),
        )),
        ["training", "approve", "--event", id] => Some(Command::TrainingApprove(
            ReviewSelection::Event(id.to_string()),
        )),
        ["training", "approve", "--run", run_id, "--event", id] => Some(Command::TrainingApprove(
            ReviewSelection::RunEvent(run_id.to_string(), id.to_string()),
        )),
        ["training", "flag", "--run", run_id] => Some(Command::TrainingFlag(ReviewSelection::Run(
            run_id.to_string(),
        ))),
        ["training", "flag", "--event", id] => Some(Command::TrainingFlag(ReviewSelection::Event(
            id.to_string(),
        ))),
        ["training", "flag", "--run", run_id, "--event", id] => Some(Command::TrainingFlag(
            ReviewSelection::RunEvent(run_id.to_string(), id.to_string()),
        )),
        ["training", "export", flags @ ..] => {
            let mut include_received = false;
            let mut include_flagged = false;
            for flag in flags {
                match *flag {
                    "--include-received" if !include_received => include_received = true,
                    "--include-flagged" if !include_flagged => include_flagged = true,
                    _ => anyhow::bail!(USAGE),
                }
            }
            Some(Command::TrainingExport {
                include_received,
                include_flagged,
            })
        }
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
        _ => {
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
    let result =
        match command {
            Command::TrainingReview { limit } => training::review(store, limit, out).await.map(
                |(flagged, received)| {
                    format!(
                        "Reviewed training examples: {} (flagged: {flagged}, received: {received})",
                        flagged + received
                    )
                },
            ),
            Command::TrainingApprove(target) => {
                let selection = match &target {
                    ReviewSelection::Run(id) => ReviewTarget::Run(id),
                    ReviewSelection::Event(id) => ReviewTarget::Event(id),
                    ReviewSelection::RunEvent(run_id, id) => ReviewTarget::RunEvent(run_id, id),
                };
                training::approve(store, selection)
                    .await
                    .map(|n| format!("Approved training examples: {n}"))
            }
            Command::TrainingFlag(target) => {
                let selection = match &target {
                    ReviewSelection::Run(id) => ReviewTarget::Run(id),
                    ReviewSelection::Event(id) => ReviewTarget::Event(id),
                    ReviewSelection::RunEvent(run_id, id) => ReviewTarget::RunEvent(run_id, id),
                };
                training::flag(store, selection)
                    .await
                    .map(|n| format!("Flagged training examples: {n}"))
            }
            Command::TrainingExport {
                include_received,
                include_flagged,
            } => training::export(store, include_received, include_flagged, out)
                .await
                .map(|n| format!("Exported training examples: {n}")),
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
