//! The metadata database at the heart of the Storage Engine.

mod catalog;
mod encoding;

use std::sync::Arc;

pub use catalog::{CatalogConfig, CatalogId, RestCatalogConfig};
use object_store::{ObjectStore, path::Path};
use se_common::clock::SystemClock;
use se_secret_store::SecretStore;
use slatedb::Db;

#[derive(Clone)]
pub struct Metadata {
    db: Db,
    secret_store: Arc<dyn SecretStore>,
    clock: Arc<dyn SystemClock>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("slatedb error: {0}")]
    SlateDb(#[from] slatedb::Error),

    #[error("catalog id must contain 1–32 bytes: {0:?}")]
    InvalidCatalogId(String),

    #[error("catalog already exists: {0}")]
    CatalogAlreadyExists(CatalogId),

    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogId),

    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),

    #[error("catalog configuration encoding error: {0}")]
    CatalogConfigEncoding(#[from] serde_json::Error),

    #[error("invalid catalog secret ULID: {0}")]
    InvalidSecretId(#[from] std::array::TryFromSliceError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Metadata {
    /// Opens the metadata database at an object-store prefix using SlateDB defaults.
    #[tracing::instrument(name = "metadata.open", skip_all)]
    pub async fn new(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        secret_store: Arc<dyn SecretStore>,
        clock: Arc<dyn SystemClock>,
    ) -> Result<Self> {
        let db = Db::builder(path, object_store)
            .with_system_clock(clock.clone())
            .build()
            .await?;

        Ok(Self::from_db(db, secret_store, clock))
    }

    /// Takes ownership of a pre-built database, preserving its configuration and data.
    #[tracing::instrument(name = "metadata.from_db", skip_all)]
    pub fn from_db(
        db: Db,
        secret_store: Arc<dyn SecretStore>,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            db,
            secret_store,
            clock,
        }
    }

    /// Returns the store used for secrets such as catalog configuration.
    pub fn secret_store(&self) -> &dyn SecretStore {
        self.secret_store.as_ref()
    }

    /// Closes the database, including all clones, using SlateDB's default close options.
    #[tracing::instrument(name = "metadata.close", skip_all)]
    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;

        Ok(())
    }
}
