//! The metadata database at the heart of the Storage Engine.

mod catalog;
mod encoding;
mod files;
mod reconciliation;
mod table;
mod tasks;
mod wal;

use std::sync::Arc;

pub use catalog::{CatalogConfig, CatalogId, RestCatalogConfig};
pub use files::{VersionedDataFile, VersionedDeletionVector};
use object_store::{ObjectStore, path::Path};
pub use reconciliation::{
    PreparedCommit, RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY, ReconciliationInput,
    ReconciliationPlan,
};
use se_common::clock::SystemClock;
use se_secret_store::SecretStore;
use slatedb::Db;
pub use table::TableDebugState;
pub use tasks::{
    DataFileMetadata, DeletionVectorMetadata, DeletionVectorReplacement, IndexDelta,
    MaterializationInput, MaterializationResult, Scheduled, TableWork, Task, TaskAssignment,
    TaskRequest, TaskToken, WalRange,
};
pub use wal::{BatchDescriptor, BatchMetadata};

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

    #[error("table UUID has not been resolved: {0}")]
    TableNotResolved(uuid::Uuid),

    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),

    #[error("catalog configuration encoding error: {0}")]
    CatalogConfigEncoding(#[from] serde_json::Error),

    #[error("invalid catalog secret ULID: {0}")]
    InvalidSecretId(#[from] std::array::TryFromSliceError),

    #[error("invalid WAL state encoding")]
    InvalidWalState,

    #[error("invalid WAL batch encoding")]
    InvalidWalBatch,

    #[error("WAL sequence numbers exhausted")]
    WalSequenceExhausted,

    #[error("materializer epochs exhausted")]
    WalEpochExhausted,

    #[error("table revisions exhausted")]
    TableRevisionExhausted,

    #[error("reconciliation does not support data file removals")]
    UnsupportedReconciliation,

    #[error("Iceberg table UUID or main snapshot differs from the reconciliation baseline")]
    ReconciliationConflict,

    #[error("prepared Iceberg commit does not match the reconciliation plan")]
    InvalidPreparedCommit,

    #[error("task attempt is no longer active")]
    TaskFenced,

    #[error(
        "materialization result does not match its assignment or contains invalid file metadata"
    )]
    InvalidTaskResult,

    #[error("invalid persisted task state")]
    InvalidTaskState,

    #[error("task encoding error: {0}")]
    TaskEncoding(serde_json::Error),
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
