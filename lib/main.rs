use annotations_api::{core::Settings, dal::Store, domain::App, presentation};
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut command = presentation::cli::parse(&std::env::args().skip(1).collect::<Vec<_>>())?;
    dotenvy::dotenv().ok();
    let settings = Settings::load();
    annotations_api::core::logs::init(&settings, annotations_api::core::logs::sender());
    anyhow::ensure!(
        !settings.database_url.is_empty(),
        "DATABASE_URL is required"
    );
    if let Some(command) = command.take_if(|c| c.training()) {
        // Training commands use the relational and document stores only. They never
        // start HTTP or tasks. Export lines go to standard output, the summary to stderr.
        let store = Store::connect(settings)
            .await
            .map_err(|_| anyhow::anyhow!("storage_unavailable: Storage is unavailable"))?;
        let mut out = std::io::BufWriter::new(std::io::stdout().lock());
        let summary = presentation::cli::training(&store, command, &mut out).await?;
        std::io::Write::flush(&mut out)?;
        eprintln!("{summary}");
        store.db.close().await;
        return Ok(());
    }
    if let Some(command) = command {
        // Registry commands use only SQL. They never start HTTP, tasks or other services.
        sqlx::any::install_default_drivers();
        let db = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&settings.database_url)
            .await
            .map_err(|_| anyhow::anyhow!("storage_unavailable: Registry storage is unavailable"))?;
        let version = presentation::cli::run(&db, command).await?;
        println!("Model release: {version}");
        db.close().await;
        return Ok(());
    }
    let address = format!("{}:{}", settings.host, settings.port);
    let store = Store::connect(settings)
        .await
        .map_err(|_| anyhow::anyhow!("Storage configuration is unavailable"))?;
    store
        .ready()
        .await
        .map_err(|_| anyhow::anyhow!("Storage is not ready. Run the explicit migration script."))?;
    let _cleanup = annotations_api::domain::tasks::spawn(store.clone());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address,"Annotations API listening");
    axum::serve(
        listener,
        presentation::router(App { store })
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
