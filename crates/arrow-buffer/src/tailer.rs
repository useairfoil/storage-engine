use std::sync::Arc;

use slatedb::object_store::ObjectStore;

use crate::{
    consumer::{BatchDescriptor, ConsumedBatch, ConsumerFetchHandle},
    error::Result,
    queue::{ManifestStore, ManifestView},
};

/// Reads the retained manifest and Arrow IPC batches without fencing or acknowledging consumers.
pub struct Tailer {
    manifest_store: ManifestStore,
    fetcher: ConsumerFetchHandle,
    last_sequence: Option<u64>,
}

impl Tailer {
    /// Create a read-only tailer on a manifest in the given object store.
    pub fn with_object_store(manifest_path: String, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            manifest_store: ManifestStore {
                object_store: object_store.clone(),
                manifest_path,
            },
            fetcher: ConsumerFetchHandle::with_object_store(object_store),
            last_sequence: None,
        }
    }

    /// Read the current manifest. A missing manifest is an empty view.
    pub async fn manifest(&self) -> Result<ManifestView> {
        let (manifest, _) = self.manifest_store.read().await?;
        ManifestView::from_manifest(manifest)
    }

    /// Return the next retained batch, or `None` if no new batch is available.
    ///
    /// The cursor advances only after a successful fetch. Poll again after `None`
    /// to see later writes; the cursor is local and never changes the manifest.
    pub async fn next_batch(&mut self) -> Result<Option<ConsumedBatch>> {
        let (manifest, _) = self.manifest_store.read().await?;
        let mut next = None;
        for entry in manifest.iter() {
            let entry = entry?;
            if self.last_sequence.is_none_or(|last| entry.sequence > last) {
                next = Some(entry);
                break;
            }
        }
        let Some(entry) = next else {
            return Ok(None);
        };

        let batch = self
            .fetcher
            .fetch(BatchDescriptor {
                sequence: entry.sequence,
                location: entry.location,
                metadata: entry.metadata,
            })
            .await?;
        self.last_sequence = Some(batch.sequence);
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use slatedb::object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use super::*;
    use crate::queue::{QueueConsumer, QueueProducer};

    const MANIFEST: &str = "ingest/manifest";

    async fn write_batch(store: &Arc<dyn ObjectStore>, location: &str, value: i32) {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![value]))],
        )
        .unwrap();
        let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        store
            .put(
                &Path::from(location),
                PutPayload::from(writer.into_inner().unwrap()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tails_new_batches_without_writing_or_fencing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut tailer = Tailer::with_object_store(MANIFEST.into(), store.clone());
        assert_eq!(tailer.manifest().await.unwrap().entries().len(), 0);
        assert!(store.get(&Path::from(MANIFEST)).await.is_err());

        let consumer = QueueConsumer::with_object_store(MANIFEST.into(), store.clone());
        consumer.initialize().await.unwrap();
        let producer = QueueProducer::with_object_store(MANIFEST.into(), store.clone());
        write_batch(&store, "ingest/first.batch", 17).await;
        producer
            .enqueue("ingest/first.batch".into(), vec![])
            .await
            .unwrap();

        let before = store
            .get(&Path::from(MANIFEST))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let first = tailer.next_batch().await.unwrap().unwrap();
        assert_eq!(first.sequence, 0);
        assert_eq!(
            first.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            17
        );
        assert!(tailer.next_batch().await.unwrap().is_none());
        let after = store
            .get(&Path::from(MANIFEST))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(consumer.descriptors_after(None, 10).await.unwrap().len(), 1);

        write_batch(&store, "ingest/second.batch", 29).await;
        producer
            .enqueue("ingest/second.batch".into(), vec![])
            .await
            .unwrap();
        let second = tailer.next_batch().await.unwrap().unwrap();
        assert_eq!(second.sequence, 1);
        assert_eq!(
            second.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            29
        );
        assert_eq!(tailer.manifest().await.unwrap().entries().len(), 2);
        assert_eq!(consumer.descriptors_after(None, 10).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn retries_a_batch_that_is_not_yet_fetchable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(MANIFEST.into(), store.clone());
        producer
            .enqueue("ingest/missing.batch".into(), vec![])
            .await
            .unwrap();
        let mut tailer = Tailer::with_object_store(MANIFEST.into(), store.clone());

        assert!(tailer.next_batch().await.is_err());
        write_batch(&store, "ingest/missing.batch", 41).await;
        let batch = tailer.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.sequence, 0);
        assert_eq!(
            batch.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            41
        );
    }
}
