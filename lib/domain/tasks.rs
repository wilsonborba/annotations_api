use crate::{
    core::{FILE_SETTINGS, Result, SAVE_SETTINGS, SaveSettings},
    dal::Store,
    domain::{
        files,
        note_saves::{self, Settled},
    },
};
/// Room access expires independently. Ownership stays until deletion succeeds.
pub async fn cleanup(store: &Store, now: i64) -> Result<usize> {
    let due = store.transfers.due(now).await?;
    let mut removed = 0;
    for record in due {
        // A timed-out FSM request can finish remotely after the HTTP future drops.
        // Keep cleanup ownership for one request timeout before the final sweep.
        if matches!(
            record.status,
            crate::dal::transfers::Status::Uploading | crate::dal::transfers::Status::Failed
        ) && now < record.expires_at + crate::core::STORAGE_REQUEST_TIMEOUT.as_secs() as i64
        {
            continue;
        }
        let Ok(files) = store.files.album(&record.album).await else {
            continue;
        };
        let mut failed = false;
        for file in files {
            if store.files.delete(&file.key).await.is_err() {
                failed = true;
            }
        }
        if let Some(key) = &record.file_key
            && store.files.delete(key).await.is_err()
        {
            failed = true;
        }
        if !failed {
            store.transfers.release(&record.id).await?;
            removed += 1;
        }
    }
    Ok(removed)
}
/// What one reconciliation run did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Reconciled {
    /// Committed saves that were marked done.
    pub finished: usize,
    /// Stopped saves whose orphan documents were removed.
    pub cleaned: usize,
    /// Stopped saves whose cleanup failed and will retry (bounded by `max_attempts`).
    pub waiting: usize,
    /// Old revisions removed (not current, not published, not a source).
    pub pruned: usize,
}

/// Reconcile note saves. Takes at most `settings.batch` operations that stopped:
/// `started` longer than the grace period, or `abandoned`, with fewer than
/// `settings.max_attempts` failed cleanups. Then removes at most `settings.batch`
/// old revisions that nothing references.
pub async fn reconcile(store: &Store, now: i64, settings: &SaveSettings) -> Result<Reconciled> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT p.operation_id FROM pending_operations p JOIN note_save_operations s ON s.operation_id=p.operation_id WHERE ((p.state='started' AND s.updated_at<$1) OR (p.state='abandoned' AND s.updated_at<=$4)) AND s.attempts<$2 ORDER BY s.updated_at,p.operation_id LIMIT $3",
    )
    .bind(now - settings.grace_seconds)
    .bind(settings.max_attempts)
    .bind(settings.batch)
    .bind(now - 30)
    .fetch_all(&store.db)
    .await?;
    let mut out = Reconciled::default();
    for row in rows {
        let id: String = row.get("operation_id");
        match note_saves::settle(store, &id, now).await {
            Ok(Settled::Done(_)) => out.finished += 1,
            Ok(Settled::Failed) => out.cleaned += 1,
            Ok(Settled::Waiting) => out.waiting += 1,
            Err(_) => {
                tracing::warn!("Note save cleanup could not complete; it will retry");
                out.waiting += 1;
            }
            Ok(Settled::Skipped) => {}
        }
    }
    out.pruned = note_saves::prune_revisions(
        store,
        now - settings.revision_retention_seconds,
        settings.batch,
    )
    .await
    .map_err(crate::core::Error::from)?;
    note_saves::cleanup_pruned(store, now, settings.batch, settings.max_attempts)
        .await
        .map_err(crate::core::Error::from)?;
    Ok(out)
}

pub fn spawn(store: Store) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            if cleanup(&store, crate::core::now()).await.is_err() {
                tracing::warn!("Temporary media cleanup is pending; it will retry");
            }
            if reconcile(&store, crate::core::now(), &SAVE_SETTINGS)
                .await
                .is_err()
            {
                tracing::warn!("Note save reconciliation is pending; it will retry");
            }
            if files::cleanup(&store, crate::core::now(), &FILE_SETTINGS)
                .await
                .is_err()
            {
                tracing::warn!("Managed file cleanup is pending; it will retry");
            }
        }
    })
}
