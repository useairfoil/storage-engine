//! Materialization of assigned WAL ranges without publication or WAL acknowledgment.

pub mod append;
mod iceberg;
mod index;
mod read;
mod sort;
pub mod upsert;
mod write;

pub use index::TableIndex;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("iceberg error: {0}")]
    Iceberg(#[from] ::iceberg::Error),

    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("wal error: {0}")]
    Wal(#[from] se_wal::Error),

    #[error("task heartbeat failed: {0}")]
    Heartbeat(#[from] se_metadata_client::Error),

    #[error("table schema missing: {0}")]
    SchemaMissing(i32),

    #[error("invalid or unavailable WAL range")]
    InvalidWalRange,

    #[error("WAL batch {0} does not match the assigned schema")]
    BatchSchemaMismatch(u64),

    #[error("partitioned append materialization is not implemented")]
    PartitionedTableUnsupported,

    #[error("slatedb error: {0}")]
    SlateDb(#[from] slatedb::Error),

    #[error("index encoding error: {0}")]
    IndexEncoding(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("upsert materialization requires a recovered table index")]
    UpsertIndexRequired,

    #[error("table index writer epoch is no longer current")]
    IndexFenced,

    #[error("table index does not match the accepted materialization history")]
    InvalidIndexState,

    #[error("identifier fields or their types changed")]
    IdentifierSchemaChanged,

    #[error("upserts require Iceberg v3 and an initially empty table")]
    UnsupportedUpsertTable,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
