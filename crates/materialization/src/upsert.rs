//! Full-row, last-WAL-row-wins upserts. SlateDB tracks materialized, not published, state.
//! All mutations staged here must be committed with the pending work and WAL frontier.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use datafusion::arrow::{
    array::{RecordBatch, UInt32Array},
    compute::{concat_batches, take_record_batch},
    datatypes::SchemaRef,
    row::{RowConverter, SortField},
};
use iceberg::{
    arrow::record_batch_projector::RecordBatchProjector,
    io::FileIO,
    puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter},
    spec::{Schema, SortOrder, TableProperties},
};
use roaring::RoaringTreemap;
use se_common::id::IdGenerator;
use se_meta_store::table::{
    DeletionVectorReplacement, MaterializedDataFile, MaterializedDeletionVector, RowLocation,
    TableRead, TableWrite,
};

use crate::{Error, Result, append, sort::sort_batch};

const PUFFIN_TARGET_BYTES: u64 = 64 * 1024 * 1024;
const PUFFIN_MAX_VECTORS: usize = 1024;
const DV_MAGIC: [u8; 4] = [0xd1, 0xd3, 0x39, 0x64];

/// Writes immutable artifacts and stages their derived index state in `tx`.
/// Inputs must already be adapted to the canonical schema, in WAL order.
/// The caller must atomically accept the returned work with its WAL checkpoint.
#[allow(clippy::too_many_arguments)]
pub async fn write_to_upsert_table(
    record_batches: Vec<RecordBatch>,
    table_schema: &Schema,
    arrow_schema: SchemaRef,
    sort_order: &SortOrder,
    file_io: &FileIO,
    table_location: &str,
    table_properties: &TableProperties,
    tx: &(impl TableWrite + TableRead),
) -> Result<(Vec<MaterializedDataFile>, Vec<DeletionVectorReplacement>)> {
    let mut ids = table_schema.identifier_field_ids().collect::<Vec<_>>();
    ids.sort_unstable();
    if ids.is_empty() {
        return Err(Error::InvalidUpsert("identifier fields are required"));
    }
    if let Some(id) = tx.get_upsert_schema_id().await? {
        let original = tx.get_schema(id).await?.ok_or(Error::SchemaMissing(id))?;
        let mut original_ids = original.identifier_field_ids().collect::<Vec<_>>();
        original_ids.sort_unstable();
        if original_ids != ids
            || ids.iter().any(|id| {
                original.field_by_id(*id).map(|f| &f.field_type)
                    != table_schema.field_by_id(*id).map(|f| &f.field_type)
            })
        {
            return Err(Error::InvalidUpsert(
                "identifier fields or their types changed",
            ));
        }
    } else {
        tx.put_upsert_schema_id(table_schema.schema_id())?;
    }

    let projector =
        RecordBatchProjector::from_iceberg_schema(Arc::new(table_schema.clone()), &ids)?;
    let batch = concat_batches(&arrow_schema, &record_batches)?;
    let columns = projector.project_column(batch.columns())?;
    if columns.iter().any(|column| column.null_count() != 0) {
        return Err(Error::InvalidUpsert(
            "identifier fields must not contain nulls",
        ));
    }
    let converter = RowConverter::new(
        columns
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect(),
    )?;
    let keys = converter.convert_columns(&columns)?;
    let mut winners = HashMap::new();
    for (index, key) in keys.iter().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| Error::InvalidUpsert("batch exceeds Arrow row index range"))?;
        winners.insert(key.as_ref().to_vec(), index);
    }
    // Preserve WAL order for unsorted tables; sorting never decides the winning row.
    let mut indices = winners.into_values().collect::<Vec<_>>();
    indices.sort_unstable();
    let batch = take_record_batch(&batch, &UInt32Array::from(indices))?;
    let batch = sort_batch(batch, table_schema, sort_order)?;
    let keys = converter.convert_columns(&projector.project_column(batch.columns())?)?;

    let data_files = append::write_sorted_batches(
        vec![batch],
        table_schema,
        sort_order,
        file_io,
        table_location,
        table_properties,
    )
    .await?;
    let mut deleted = BTreeMap::<String, RoaringTreemap>::new();
    let mut keys = keys.iter();
    // The rolling writer returns files in write order. Row positions restart per file.
    for file in &data_files {
        for row_index in 0..file.record_count {
            // PANIC: The writer preserves all rows and returns their counts in write order.
            let key = keys.next().expect("one key per written row");
            if let Some(old) = tx.get_row_location(key.as_ref()).await? {
                if !deleted.contains_key(&old.file_path) {
                    let bitmap = match tx.get_deletion_bitmap(&old.file_path).await? {
                        Some(bytes) => RoaringTreemap::deserialize_from(bytes.as_ref())?,
                        None => RoaringTreemap::new(),
                    };
                    deleted.insert(old.file_path.clone(), bitmap);
                }
                // PANIC: This file's bitmap was loaded immediately above.
                deleted
                    .get_mut(&old.file_path)
                    .expect("loaded bitmap")
                    .insert(old.row_index);
            }
            tx.put_row_location(
                key.as_ref(),
                &RowLocation {
                    file_path: file.file_path.clone(),
                    row_index,
                },
            )?;
        }
    }

    let mut replacements = Vec::new();
    let mut writer = None;
    let mut path = String::new();
    let mut offset = 4u64; // Puffin header.
    let mut object_start = 0;
    for (file, bitmap) in deleted {
        let mut serialized = Vec::new();
        bitmap.serialize_into(&mut serialized)?;
        let data = encode_deletion_vector(&serialized)?;
        if writer.is_some()
            && (offset + data.len() as u64 > PUFFIN_TARGET_BYTES
                || replacements.len() - object_start >= PUFFIN_MAX_VECTORS)
        {
            finish_puffin(
                writer.take(),
                file_io,
                table_location,
                &path,
                &mut replacements[object_start..],
            )
            .await?;
        }
        if writer.is_none() {
            path = format!("data/{}.puffin", rand::rng().gen_uuid());
            writer = Some(
                PuffinWriter::new(
                    &file_io
                        .new_output(format!("{}/{path}", table_location.trim_end_matches('/')))?,
                    HashMap::new(),
                    false,
                )
                .await?,
            );
            offset = 4;
            object_start = replacements.len();
        }
        let current = MaterializedDeletionVector {
            data_file_path: file.clone(),
            puffin_path: path.clone(),
            file_size_in_bytes: 0,
            offset: i64::try_from(offset)
                .map_err(|_| Error::InvalidUpsert("Puffin offset exceeds signed range"))?,
            length: data.len() as i64,
            cardinality: bitmap.len(),
        };
        let blob = Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_owned())
            .fields(Vec::new())
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(data)
            .properties(HashMap::from([
                (
                    "referenced-data-file".into(),
                    format!("{}/{file}", table_location.trim_end_matches('/')),
                ),
                ("cardinality".into(), bitmap.len().to_string()),
            ]))
            .build();
        // PANIC: A writer is opened before adding each blob.
        writer
            .as_mut()
            .expect("open Puffin writer")
            .add(blob, CompressionCodec::None)
            .await?;
        offset += current.length as u64;
        replacements.push(DeletionVectorReplacement {
            previous: tx.get_deletion_vector(&file).await?,
            current,
        });
        // Descriptor size is completed after closing the object, before transaction commit.
        // PANIC: The replacement was pushed immediately above.
        tx.put_deletion_vector(
            &replacements.last().expect("just pushed vector").current,
            &serialized,
        )?;
    }
    finish_puffin(
        writer,
        file_io,
        table_location,
        &path,
        &mut replacements[object_start..],
    )
    .await?;
    for replacement in &replacements {
        // Preserve the bitmap staged above while completing its descriptor.
        // PANIC: Every emitted vector has a bitmap staged in this transaction.
        let bitmap = tx
            .get_deletion_bitmap(&replacement.current.data_file_path)
            .await?
            .expect("staged bitmap");
        tx.put_deletion_vector(&replacement.current, &bitmap)?;
    }
    Ok((data_files, replacements))
}

fn encode_deletion_vector(bitmap: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(bitmap.len() + DV_MAGIC.len())
        .map_err(|_| Error::InvalidUpsert("deletion vector exceeds 32-bit payload length"))?;
    let mut data = Vec::with_capacity(bitmap.len() + 12);
    data.extend_from_slice(&length.to_be_bytes());
    data.extend_from_slice(&DV_MAGIC);
    data.extend_from_slice(bitmap);
    data.extend_from_slice(&crc32fast::hash(&data[4..]).to_be_bytes());
    Ok(data)
}

async fn finish_puffin(
    writer: Option<PuffinWriter>,
    file_io: &FileIO,
    table_location: &str,
    path: &str,
    replacements: &mut [DeletionVectorReplacement],
) -> Result<()> {
    if let Some(writer) = writer {
        writer.close().await?;
        let size = file_io
            .new_input(format!("{}/{path}", table_location.trim_end_matches('/')))?
            .metadata()
            .await?
            .size;
        for replacement in replacements {
            replacement.current.file_size_in_bytes = size;
        }
    }
    Ok(())
}
