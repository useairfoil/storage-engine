use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use object_store::memory::InMemory;
use se_common::clock::DefaultSystemClock;
use se_metadata::{CatalogConfig, CatalogId, Metadata, RestCatalogConfig};
use se_secret_store::memory::InMemorySecretStore;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::format::FmtSpan;

struct LogWriter(Arc<Mutex<Vec<u8>>>);

// Isolate capture from tests that exercise the same tracing callsites without a subscriber.
#[tokio::test]
async fn traces_operations_without_catalog_configuration() -> Result<(), Box<dyn std::error::Error>>
{
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer_output = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .without_time()
        .with_ansi(false)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(move || LogWriter(writer_output.clone()))
        .finish();

    async {
        let metadata = Metadata::new(
            "metadata",
            Arc::new(InMemory::new()),
            Arc::new(InMemorySecretStore::new()),
            Arc::new(DefaultSystemClock::new()),
        )
        .await?;
        let id = CatalogId::new("traced-catalog")?;
        let mut config = RestCatalogConfig {
            uri: "https://private-config.example".into(),
            warehouse: Some("private-warehouse".into()),
            properties: [("credential".into(), "private-credential".into())].into(),
        };

        metadata
            .create_catalog(id.clone(), CatalogConfig::Rest(config.clone()))
            .await?;
        assert_eq!(
            metadata.get_catalog(&id).await?,
            Some(CatalogConfig::Rest(config.clone()))
        );

        config.uri = "https://private-update.example".into();
        metadata
            .update_catalog(&id, CatalogConfig::Rest(config))
            .await?;
        metadata.delete_catalog(&id).await?;
        metadata.close().await?;

        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .with_subscriber(subscriber)
    .await?;

    let logs = String::from_utf8(output.lock().map_err(|_| "log mutex poisoned")?.clone())?;
    let missing: Vec<_> = [
        "metadata.open",
        "metadata.from_db",
        "metadata.create_catalog",
        "metadata.get_catalog",
        "metadata.update_catalog",
        "metadata.delete_catalog",
        "metadata.close",
        "catalog.id=traced-catalog",
    ]
    .into_iter()
    .filter(|field| !logs.contains(field))
    .collect();
    let leaked: Vec<_> = [
        "private-config.example",
        "private-update.example",
        "private-credential",
        "private-warehouse",
        "config=",
    ]
    .into_iter()
    .filter(|secret| logs.contains(secret))
    .collect();

    insta::assert_debug_snapshot!(missing, @"[]");
    insta::assert_debug_snapshot!(leaked, @"[]");

    Ok(())
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("log mutex poisoned"))?
            .extend_from_slice(buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
