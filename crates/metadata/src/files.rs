use serde::{Deserialize, Serialize};
use slatedb::DbTransaction;

use crate::{
    DataFileMetadata, DeletionVectorMetadata, Error, MaterializationResult, Result, TaskToken,
    encoding::{TableKey, decode, encode},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedDataFile {
    pub metadata: DataFileMetadata,
    pub schema_id: i32,
    pub added_revision: u64,
    pub removed_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedDeletionVector {
    pub metadata: DeletionVectorMetadata,
    pub added_revision: u64,
    pub removed_revision: Option<u64>,
}

/// Persists desired file changes in the same transaction that acknowledges their WAL range.
pub(crate) async fn apply_materialization(
    tx: &DbTransaction,
    token: &TaskToken,
    result: &MaterializationResult,
    revision: u64,
) -> Result<()> {
    let table = TableKey::for_token(token);
    for metadata in &result.data_files {
        let key = table.data_file(&metadata.path);
        if tx.get(&key).await?.is_some() {
            return Err(Error::InvalidTaskResult);
        }
        let file = VersionedDataFile {
            metadata: metadata.clone(),
            schema_id: result.schema_id,
            added_revision: revision,
            removed_revision: None,
        };
        tx.put(key, encode(&file)?)?;
    }

    for replacement in &result.deletion_vectors {
        let target = &replacement.current.target_file;
        let file: VersionedDataFile = decode(
            &tx.get(table.data_file(target))
                .await?
                .ok_or(Error::InvalidTaskResult)?,
        )?;
        if file.removed_revision.is_some()
            || replacement.current.cardinality > file.metadata.num_rows
        {
            return Err(Error::InvalidTaskResult);
        }

        let current_key = table.current_deletion_vector(target);
        let previous: Option<VersionedDeletionVector> = tx
            .get(&current_key)
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if previous.as_ref().map(|vector| &vector.metadata) != replacement.previous.as_ref() {
            return Err(Error::InvalidTaskResult);
        }
        if let Some(mut previous) = previous {
            if replacement.current.cardinality < previous.metadata.cardinality {
                return Err(Error::InvalidTaskResult);
            }
            previous.removed_revision = Some(revision);
            tx.put(
                table.deletion_vector(target, previous.added_revision),
                encode(&previous)?,
            )?;
        }

        let vector = VersionedDeletionVector {
            metadata: replacement.current.clone(),
            added_revision: revision,
            removed_revision: None,
        };
        tx.put(table.deletion_vector(target, revision), encode(&vector)?)?;
        tx.put(current_key, encode(&vector)?)?;
    }

    Ok(())
}
