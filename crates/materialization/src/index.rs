use std::sync::Arc;

use object_store::{ObjectStore, path::Path};
use se_common::clock::SystemClock;
use se_metadata::{DeletionVectorMetadata, IndexDelta, MaterializationResult};
use se_metadata_client::Heartbeat;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use slatedb::{Db, DbTransaction, IsolationLevel};
use uuid::Uuid;

use crate::{Error, Result};

const EPOCH: &[u8] = b"epoch";
const APPLIED: &[u8] = b"applied";
const IDENTIFIERS: &[u8] = b"identifiers";

pub struct TableIndex {
    db: Db,
    epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RowLocation {
    pub(crate) file: String,
    pub(crate) row: u64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VectorState {
    pub(crate) metadata: DeletionVectorMetadata,
    pub(crate) bitmap: Vec<u8>,
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PreparedDelta {
    pub(crate) result: MaterializationResult,
    pub(crate) identifiers: Vec<(i32, String)>,
    pub(crate) rows: Vec<(Vec<u8>, RowLocation)>,
    pub(crate) vectors: Vec<VectorState>,
}

impl TableIndex {
    /// Opens a table's index, fencing previous writers and rejecting older task epochs.
    pub async fn open(
        path: impl Into<Path>,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn SystemClock>,
        epoch: u64,
    ) -> Result<Self> {
        let db = Db::builder(path, store)
            .with_system_clock(clock)
            .build()
            .await?;
        let index = Self { db, epoch };
        let result = index.acquire().await;
        if let Err(error) = result {
            index.close().await?;
            return Err(error);
        }
        Ok(index)
    }

    /// Returns the exclusive WAL cursor durably reflected in the committed index.
    pub async fn applied_through(&self) -> Result<u64> {
        Ok(self.read(APPLIED).await?.unwrap_or(0))
    }

    /// Applies only the metadata-authorized deltas before allowing further materialization.
    pub async fn catch_up(&self, deltas: &[IndexDelta], heartbeat: &Heartbeat) -> Result<()> {
        for delta in deltas {
            self.apply(delta).await?;
            heartbeat.beat().await?;
        }
        Ok(())
    }

    /// Atomically applies an accepted delta and advances the durable index cursor.
    pub async fn apply(&self, accepted: &IndexDelta) -> Result<()> {
        let tx = self.transaction().await?;
        let applied: u64 = read_tx(&tx, APPLIED).await?.unwrap_or(0);
        if applied >= accepted.range.end {
            return Ok(());
        }
        let delta: PreparedDelta = read_tx(&tx, &delta_key(accepted.id))
            .await?
            .ok_or(Error::InvalidIndexState)?;
        if delta.result.index_delta != Some(accepted.id)
            || delta.result.range != accepted.range
            || applied != accepted.range.start
        {
            return Err(Error::InvalidIndexState);
        }
        for (key, location) in &delta.rows {
            tx.put(row_key(key), serde_json::to_vec(location)?)?;
        }
        for vector in &delta.vectors {
            tx.put(
                vector_key(&vector.metadata.target_file),
                serde_json::to_vec(vector)?,
            )?;
        }
        tx.put(IDENTIFIERS, serde_json::to_vec(&delta.identifiers)?)?;
        tx.put(APPLIED, serde_json::to_vec(&accepted.range.end)?)?;
        commit(tx).await
    }

    /// Closes the table writer without discarding prepared or accepted state.
    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub(crate) async fn check_identifiers(&self, identifiers: &[(i32, String)]) -> Result<()> {
        let current: Option<Vec<(i32, String)>> = self.read(IDENTIFIERS).await?;
        if current.is_some_and(|current| current != identifiers) {
            return Err(Error::IdentifierSchemaChanged);
        }
        Ok(())
    }

    pub(crate) async fn location(&self, key: &[u8]) -> Result<Option<RowLocation>> {
        self.read(&row_key(key)).await
    }

    pub(crate) async fn vector(&self, target: &str) -> Result<Option<VectorState>> {
        self.read(&vector_key(target)).await
    }

    pub(crate) async fn prepared(&self, id: Uuid) -> Result<Option<MaterializationResult>> {
        let delta: Option<PreparedDelta> = self.read(&delta_key(id)).await?;
        if delta.is_some() {
            self.db.flush().await?;
        }
        Ok(delta.map(|delta| delta.result))
    }

    pub(crate) async fn prepare(&self, delta: PreparedDelta) -> Result<MaterializationResult> {
        let id = delta.result.index_delta.ok_or(Error::InvalidIndexState)?;
        let tx = self.transaction().await?;
        let applied: u64 = read_tx(&tx, APPLIED).await?.unwrap_or(0);
        if applied != delta.result.range.start {
            return Err(Error::InvalidIndexState);
        }
        if let Some(previous) = read_tx::<PreparedDelta>(&tx, &delta_key(id)).await? {
            if previous != delta {
                return Err(Error::InvalidIndexState);
            }
            self.db.flush().await?;
            return Ok(previous.result);
        }
        tx.put(delta_key(id), serde_json::to_vec(&delta)?)?;
        commit(tx).await?;
        // TODO: Reclaim rejected deltas and orphan files once metadata proves they cannot commit.
        Ok(delta.result)
    }

    async fn acquire(&self) -> Result<()> {
        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let previous: u64 = read_tx(&tx, EPOCH).await?.unwrap_or(0);
        if self.epoch < previous {
            return Err(Error::IndexFenced);
        }
        tx.put(EPOCH, serde_json::to_vec(&self.epoch)?)?;
        commit(tx).await
    }

    async fn transaction(&self) -> Result<DbTransaction> {
        let tx = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        if read_tx::<u64>(&tx, EPOCH).await? != Some(self.epoch) {
            return Err(Error::IndexFenced);
        }
        Ok(tx)
    }

    async fn read<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        let tx = self.transaction().await?;
        read_tx(&tx, key).await
    }
}

fn row_key(key: &[u8]) -> Vec<u8> {
    [b"row/", key].concat()
}

fn delta_key(id: Uuid) -> Vec<u8> {
    [b"delta/".as_slice(), id.as_bytes()].concat()
}

fn vector_key(target: &str) -> Vec<u8> {
    [b"vector/".as_slice(), target.as_bytes()].concat()
}

async fn read_tx<T: DeserializeOwned>(tx: &DbTransaction, key: &[u8]) -> Result<Option<T>> {
    tx.get(key)
        .await?
        .map(|value| serde_json::from_slice(&value).map_err(Error::from))
        .transpose()
}

async fn commit(tx: DbTransaction) -> Result<()> {
    if let Some(handle) = tx.commit().await? {
        handle.await_durable().await?;
    }
    Ok(())
}
