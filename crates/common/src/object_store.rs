//! Object store configuration and a single-object [`TransactionalStorageProtocol`].
//!
//! Unlike slatedb's `ObjectStoreSequencedStorageProtocol`, which writes a new file per
//! version, this protocol always reads and writes the same object and relies on
//! conditional puts (`PutMode::Create`/`PutMode::Update`) for compare-and-swap semantics.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use object_store::{
    Error::{AlreadyExists, NotFound, Precondition},
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, PutResult, UpdateVersion,
    aws::AmazonS3Builder,
    azure::MicrosoftAzureBuilder,
    path::Path,
    prefix::PrefixStore,
};
use serde::Deserialize;
use slatedb_txn_obj::{ObjectCodec, TransactionalObjectError, TransactionalStorageProtocol};

#[derive(Deserialize)]
#[serde(tag = "scheme", content = "options", rename_all = "snake_case")]
pub enum ObjectStoreConfig {
    S3(HashMap<String, String>),
    Azblob(HashMap<String, String>),
}

/// The version of an object as observed in an [`ObjectStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion(pub Option<UpdateVersion>);

/// Implements [`TransactionalStorageProtocol<T, ObjectVersion>`] on object storage using
/// a single object and compare-and-swap (CAS) updates.
pub struct ObjectStoreStorageProtocol<T> {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    codec: Box<dyn ObjectCodec<T>>,
}

impl ObjectStoreConfig {
    /// Builds a native object store using ambient configuration, explicit overrides, and an optional root.
    pub fn build(&self) -> object_store::Result<Arc<dyn ObjectStore>> {
        let (store, options): (Arc<dyn ObjectStore>, _) = match self {
            Self::S3(options) => {
                let mut builder = AmazonS3Builder::from_env();
                for (key, value) in options {
                    if key != "root" {
                        let key = match key.as_str() {
                            "enable_virtual_host_style" => "virtual_hosted_style_request",
                            key => key,
                        };
                        builder = builder.with_config(key.parse()?, value);
                    }
                }
                (Arc::new(builder.build()?), options)
            }
            Self::Azblob(options) => {
                let mut builder = MicrosoftAzureBuilder::from_env();
                for (key, value) in options {
                    if key != "root" {
                        let key = match key.as_str() {
                            "container" => "container_name",
                            key => key,
                        };
                        builder = builder.with_config(key.parse()?, value);
                    }
                }
                (Arc::new(builder.build()?), options)
            }
        };
        Ok(match options.get("root") {
            Some(root) => Arc::new(PrefixStore::new(store, root.as_str())),
            None => store,
        })
    }
}

impl ObjectVersion {
    /// The version of an object that does not exist yet.
    pub fn initial() -> Self {
        Self(None)
    }
}

impl<T> ObjectStoreStorageProtocol<T> {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        codec: Box<dyn ObjectCodec<T>>,
    ) -> Self {
        Self {
            object_store,
            path,
            codec,
        }
    }
}

impl From<Option<UpdateVersion>> for ObjectVersion {
    fn from(version: Option<UpdateVersion>) -> Self {
        Self(version)
    }
}

impl From<UpdateVersion> for ObjectVersion {
    fn from(version: UpdateVersion) -> Self {
        Self(Some(version))
    }
}

impl From<PutResult> for ObjectVersion {
    fn from(result: PutResult) -> Self {
        Self(Some(UpdateVersion::from(result)))
    }
}

#[async_trait]
impl<T: Send + Sync> TransactionalStorageProtocol<T, ObjectVersion>
    for ObjectStoreStorageProtocol<T>
{
    async fn write(
        &self,
        current_id: Option<ObjectVersion>,
        new_value: &T,
    ) -> Result<ObjectVersion, TransactionalObjectError> {
        let payload = self.codec.encode(new_value);
        let mode = match current_id {
            None | Some(ObjectVersion(None)) => PutMode::Create,
            Some(ObjectVersion(Some(version))) => PutMode::Update(version),
        };

        match self
            .object_store
            .put_opts(
                &self.path,
                PutPayload::from_bytes(payload),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(result) => Ok(ObjectVersion::from(result)),
            Err(AlreadyExists { .. } | Precondition { .. }) => {
                Err(TransactionalObjectError::ObjectVersionExists)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn try_read_latest(
        &self,
    ) -> Result<Option<(ObjectVersion, T)>, TransactionalObjectError> {
        match self.object_store.get(&self.path).await {
            Ok(result) => {
                let version = ObjectVersion::from(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result.bytes().await?;
                let value = self
                    .codec
                    .decode(&bytes)
                    .map_err(TransactionalObjectError::CallbackError)?;
                Ok(Some((version, value)))
            }
            Err(NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use slatedb_txn_obj::{
        ObjectCodec, SimpleTransactionalObject, TransactionalObject, TransactionalObjectError,
        TransactionalStorageProtocol,
    };

    use super::{ObjectStoreStorageProtocol, ObjectVersion};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestVal {
        epoch: u64,
        payload: u64,
    }

    struct TestValCodec;

    impl ObjectCodec<TestVal> for TestValCodec {
        fn encode(&self, value: &TestVal) -> Bytes {
            Bytes::from(format!("{}:{}", value.epoch, value.payload))
        }

        fn decode(
            &self,
            bytes: &Bytes,
        ) -> Result<TestVal, Box<dyn std::error::Error + Send + Sync>> {
            let s = std::str::from_utf8(bytes)?;
            let mut parts = s.split(':');
            let epoch = parts.next().ok_or("missing epoch")?.parse()?;
            let payload = parts.next().ok_or("missing payload")?.parse()?;
            Ok(TestVal { epoch, payload })
        }
    }

    fn val(epoch: u64, payload: u64) -> TestVal {
        TestVal { epoch, payload }
    }

    fn new_protocol(store: &Arc<dyn ObjectStore>) -> Arc<ObjectStoreStorageProtocol<TestVal>> {
        Arc::new(ObjectStoreStorageProtocol::new(
            Arc::clone(store),
            Path::from("/root/state"),
            Box::new(TestValCodec),
        ))
    }

    #[tokio::test]
    async fn test_write_and_read_latest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = new_protocol(&store);

        insta::assert_compact_debug_snapshot!(
            protocol.try_read_latest().await.unwrap(),
            @"None"
        );

        let version = protocol.write(None, &val(1, 10)).await.unwrap();
        insta::assert_compact_debug_snapshot!(
            version,
            @r#"ObjectVersion(Some(UpdateVersion { e_tag: Some("\"0\""), version: None }))"#
        );
        insta::assert_compact_debug_snapshot!(
            protocol.try_read_latest().await.unwrap(),
            @r#"Some((ObjectVersion(Some(UpdateVersion { e_tag: Some("\"0\""), version: None })), TestVal { epoch: 1, payload: 10 }))"#
        );

        let latest = protocol.write(Some(version), &val(1, 20)).await.unwrap();
        insta::assert_compact_debug_snapshot!(
            latest,
            @r#"ObjectVersion(Some(UpdateVersion { e_tag: Some("\"1\""), version: None }))"#
        );
        insta::assert_compact_debug_snapshot!(
            protocol.try_read_latest().await.unwrap(),
            @r#"Some((ObjectVersion(Some(UpdateVersion { e_tag: Some("\"1\""), version: None })), TestVal { epoch: 1, payload: 20 }))"#
        );
    }

    #[tokio::test]
    async fn test_write_conflicts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = new_protocol(&store);
        let second = new_protocol(&store);

        let version = first.write(None, &val(1, 10)).await.unwrap();

        // Create conflicts when the object already exists.
        let err = second.write(None, &val(1, 20)).await.unwrap_err();
        insta::assert_compact_debug_snapshot!(err, @"ObjectVersionExists");

        // A write without version metadata fails closed when the object exists.
        let err = second
            .write(Some(ObjectVersion::initial()), &val(1, 20))
            .await
            .unwrap_err();
        insta::assert_compact_debug_snapshot!(err, @"ObjectVersionExists");

        // A write with a stale version conflicts after a concurrent update.
        first
            .write(Some(version.clone()), &val(1, 20))
            .await
            .unwrap();
        let err = second.write(Some(version), &val(1, 30)).await.unwrap_err();
        insta::assert_compact_debug_snapshot!(err, @"ObjectVersionExists");

        insta::assert_compact_debug_snapshot!(
            second.try_read_latest().await.unwrap(),
            @r#"Some((ObjectVersion(Some(UpdateVersion { e_tag: Some("\"1\""), version: None })), TestVal { epoch: 1, payload: 20 }))"#
        );
    }

    #[tokio::test]
    async fn test_transactional_object_retries_conflict() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = new_protocol(&store);
        let mut object = SimpleTransactionalObject::<TestVal, ObjectVersion>::init(
            Arc::clone(&first) as Arc<dyn TransactionalStorageProtocol<TestVal, ObjectVersion>>,
            val(1, 1),
        )
        .await
        .unwrap();

        // Another client updates the same object concurrently.
        let second = new_protocol(&store);
        let (version, _) = second.try_read_latest().await.unwrap().unwrap();
        second.write(Some(version), &val(1, 2)).await.unwrap();

        object
            .maybe_apply_update(|o| {
                let mut dirty = o.prepare_dirty()?;
                dirty.value.payload += 1;
                Ok::<_, TransactionalObjectError>(Some(dirty))
            })
            .await
            .unwrap();

        insta::assert_compact_debug_snapshot!(
            second.try_read_latest().await.unwrap(),
            @r#"Some((ObjectVersion(Some(UpdateVersion { e_tag: Some("\"2\""), version: None })), TestVal { epoch: 1, payload: 3 }))"#
        );
    }
}
