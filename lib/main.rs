use annotations_api::{core::Settings, dal::Store, domain::App, presentation};
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let settings = Settings::load();
    annotations_api::core::logs::init(&settings, annotations_api::core::logs::sender());
    anyhow::ensure!(
        !settings.database_url.is_empty(),
        "DATABASE_URL is required"
    );
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
