//! The metadata database at the heart of the Storage Engine.

mod catalog;
mod encoding;
mod files;
mod reconciliation;
mod status;
mod table;
mod tasks;
mod wal;

use std::{sync::Arc, time::Duration};

pub use catalog::{CatalogConfig, CatalogId, RestCatalogConfig};
pub use files::{VersionedDataFile, VersionedDeletionVector};
use object_store::{ObjectStore, path::Path};
pub use reconciliation::{
    PreparedCommit, RECONCILIATION_ID_PROPERTY, REVISION_PROPERTY, ReconciliationInput,
    ReconciliationPlan,
};
use se_common::clock::SystemClock;
use se_secret_store::SecretStore;
use slatedb::{
    Db,
    config::{GarbageCollectorDirectoryOptions, GarbageCollectorOptions, Settings},
};
pub use status::{
    TableCondition, TableIcebergStatus, TableStatus, TableTaskState, TableTaskStatus,
    TableWalStatus, TableWorkStatus,
};
pub use table::TableDebugState;
pub use tasks::{
    CHECKPOINT_INTERVAL_PROPERTY, DataFileMetadata, DeletionVectorMetadata,
    DeletionVectorReplacement, IndexDelta, MaterializationInput, MaterializationResult, Scheduled,
    TableWork, Task, TaskAssignment, TaskAssignmentResponse, TaskRequest, TaskToken, WalDeletion,
    WalRange,
};
use tracing::instrument;
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

    #[error("catalog id must be valid UUID v7: {0:?}")]
    InvalidCatalogId(String),

    #[error("catalog already exists: {0}")]
    CatalogAlreadyExists(CatalogId),

    #[error("catalog not found: {0}")]
    CatalogNotFound(CatalogId),

    #[error("table UUID has not been resolved: {0}")]
    TableNotResolved(uuid::Uuid),

    #[error("table is being deleted: {0}")]
    TableDeleting(uuid::Uuid),

    #[error(
        "invalid airfoil.checkpoint.interval-sec: {0:?}; expected whole seconds between 0 and 900 inclusive"
    )]
    InvalidCheckpointInterval(String),

    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("secret store error: {0}")]
    SecretStore(#[from] se_secret_store::Error),

    #[error("catalog configuration encoding error: {0}")]
    CatalogConfigEncoding(#[from] serde_json::Error),

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

    #[error("invalid metadata encoding: {0}")]
    InvalidEncoding(&'static str),

    #[error("Iceberg commit encoding error: {0}")]
    PreparedCommitEncoding(serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Metadata {
    /// Opens the metadata database with WAL fence cleanup after 24 hours.
    #[instrument(name = "metadata.open", skip_all)]
    pub async fn new(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        secret_store: Arc<dyn SecretStore>,
        clock: Arc<dyn SystemClock>,
    ) -> Result<Self> {
        let db = Db::builder(path, object_store)
            .with_settings(Settings {
                garbage_collector_options: Some(GarbageCollectorOptions {
                    // Writers must not resume after being stale for 24 hours.
                    wal_fence_options: Some(GarbageCollectorDirectoryOptions {
                        min_age: Duration::from_secs(24 * 60 * 60),
                        dry_run: false,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .with_system_clock(clock.clone())
            .build()
            .await?;

        Ok(Self::from_db(db, secret_store, clock))
    }

    /// Takes ownership of a pre-built database, preserving its configuration and data.
    #[instrument(name = "metadata.from_db", skip_all)]
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
    #[instrument(name = "metadata.close", skip_all)]
    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;

        Ok(())
    }
}
