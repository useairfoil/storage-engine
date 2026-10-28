//! Iceberg storage backed by native object_store clients.

use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use iceberg::{
    Error, ErrorKind, Result,
    io::{
        FileMetadata, FileRead, FileWrite, InputFile, OutputFile, S3Config, Storage, StorageConfig,
        StorageFactory,
    },
};
use object_store::{
    ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, buffered::BufWriter, path::Path,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreStorageFactory;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectStoreStorage {
    config: S3Config,
}

struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

struct ObjectStoreWriter(BufWriter);

#[typetag::serde(name = "se.object_store_storage_factory")]
impl StorageFactory for ObjectStoreStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(ObjectStoreStorage {
            config: S3Config::try_from(config)?,
        }))
    }
}

#[typetag::serde(name = "se.object_store_storage")]
#[async_trait]
impl Storage for ObjectStoreStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (store, key) = self.store(path)?;
        match store.head(&key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(io_error(error)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (store, key) = self.store(path)?;
        Ok(FileMetadata {
            size: store.head(&key).await.map_err(io_error)?.size,
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (store, key) = self.store(path)?;
        store
            .get(&key)
            .await
            .map_err(io_error)?
            .bytes()
            .await
            .map_err(io_error)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, path) = self.store(path)?;
        Ok(Box::new(ObjectStoreReader { store, path }))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        let (store, key) = self.store(path)?;
        store.put(&key, bytes.into()).await.map_err(io_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (store, key) = self.store(path)?;
        Ok(Box::new(ObjectStoreWriter(BufWriter::new(store, key))))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (store, key) = self.store(path)?;
        store.delete(&key).await.map_err(io_error)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (store, key) = self.store(path)?;
        let mut objects = store.list(Some(&key));
        while let Some(object) = objects.next().await {
            let object = object.map_err(io_error)?;
            store.delete(&object.location).await.map_err(io_error)?;
        }
        Ok(())
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}

impl ObjectStoreStorage {
    fn store(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
        let (scheme, location) = path.split_once("://").ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "expected an S3 URL with bucket and path",
            )
        })?;
        let (bucket, key) = location
            .split_once('/')
            .filter(|(bucket, _)| !bucket.is_empty())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "expected an S3 URL with bucket and path",
                )
            })?;
        if scheme != "s3" && scheme != "s3a" {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "only S3 table storage is supported",
            ));
        }

        let config = &self.config;
        if config.role_arn.is_some()
            || config.external_id.is_some()
            || config.role_session_name.is_some()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "assume-role table storage is not supported by the native backend",
            ));
        }
        if (config.disable_config_load || config.disable_ec2_metadata)
            && !config.allow_anonymous
            && (config.access_key_id.is_none() || config.secret_access_key.is_none())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "catalog must supply S3 credentials when credential discovery is disabled",
            ));
        }

        let mut builder = if config.disable_config_load {
            AmazonS3Builder::new()
        } else {
            AmazonS3Builder::from_env()
        }
        .with_bucket_name(bucket)
        .with_virtual_hosted_style_request(config.enable_virtual_host_style)
        .with_skip_signature(config.allow_anonymous);

        for (key, value) in [
            ("endpoint", config.endpoint.as_ref()),
            ("region", config.region.as_ref()),
            ("access_key_id", config.access_key_id.as_ref()),
            ("secret_access_key", config.secret_access_key.as_ref()),
            ("session_token", config.session_token.as_ref()),
            (
                "server_side_encryption",
                config.server_side_encryption.as_ref(),
            ),
            (
                "sse_kms_key_id",
                config.server_side_encryption_aws_kms_key_id.as_ref(),
            ),
            (
                "sse_customer_key_base64",
                config.server_side_encryption_customer_key.as_ref(),
            ),
        ] {
            if let Some(value) = value {
                builder = builder.with_config(key.parse().map_err(io_error)?, value);
            }
        }
        if config
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.starts_with("http://"))
        {
            builder = builder.with_allow_http(true);
        }

        Ok((
            Arc::new(builder.build().map_err(io_error)?),
            Path::from(key),
        ))
    }
}

#[async_trait]
impl FileRead for ObjectStoreReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(io_error)
    }
}

#[async_trait]
impl FileWrite for ObjectStoreWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.0.put(bytes).await.map_err(io_error)
    }

    async fn close(&mut self) -> Result<()> {
        self.0.shutdown().await.map_err(io_error)
    }
}

fn io_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Unexpected, "object store operation failed").with_source(error)
}
