use crate::{core::Result, dal::Store};
/// Room access expires independently. Ownership stays until deletion succeeds.
pub async fn cleanup(store: &Store, now: i64) -> Result<usize> {
    let due = store.transfers.due(now).await?;
    let mut removed = 0;
    for record in due {
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
pub fn spawn(store: Store) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            if cleanup(&store, crate::core::now()).await.is_err() {
                tracing::warn!("Temporary media cleanup is pending; it will retry");
            }
        }
    })
}
