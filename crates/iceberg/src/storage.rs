//! Iceberg's storage interface backed by OpenDAL clients with tracing enabled.

use std::{collections::HashMap, ops::Range, sync::Arc};

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
use opendal::{Operator, layers::TimeoutLayer};
use se_common::storage_tracing::TracedRetryLayer;
use serde::{Deserialize, Serialize};

/// Builds traced S3 storage using the configuration supplied by the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedStorageFactory;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TracedStorage {
    config: opendal::services::S3Config,
}

// Bridges Iceberg's traits to OpenDAL; tracing is handled by TracedRetryLayer.
struct OpenDalReader(opendal::Reader);
struct OpenDalWriter(opendal::Writer);

#[typetag::serde(name = "se.traced_storage_factory")]
impl StorageFactory for TracedStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        // TODO(fran): support non-S3 config
        Ok(Arc::new(TracedStorage {
            config: s3_config(S3Config::try_from(config)?),
        }))
    }
}

#[typetag::serde(name = "se.traced_storage")]
#[async_trait]
impl Storage for TracedStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, key) = self.operator(path)?;
        op.exists(key).await.map_err(io_error)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, key) = self.operator(path)?;
        let metadata = op.stat(key).await.map_err(io_error)?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, key) = self.operator(path)?;
        Ok(op.read(key).await.map_err(io_error)?.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, key) = self.operator(path)?;
        Ok(Box::new(OpenDalReader(
            op.reader(key).await.map_err(io_error)?,
        )))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        let (op, key) = self.operator(path)?;
        op.write(key, bytes).await.map_err(io_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, key) = self.operator(path)?;
        Ok(Box::new(OpenDalWriter(
            op.writer(key).await.map_err(io_error)?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, key) = self.operator(path)?;
        op.delete(key).await.map_err(io_error)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, key) = self.operator(path)?;
        let prefix = format!("{}/", key.trim_end_matches('/'));
        op.delete_with(&prefix)
            .recursive(true)
            .await
            .map_err(io_error)
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        // Match iceberg-storage-opendal's batching across buckets.
        let mut deleters = HashMap::<String, opendal::Deleter>::new();
        while let Some(path) = paths.next().await {
            let (bucket, key) = split_location(&path)?;
            let deleter = match deleters.entry(bucket.to_owned()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let (op, _) = self.operator(&path)?;
                    entry.insert(op.deleter().await.map_err(io_error)?)
                }
            };
            deleter.delete(key).await.map_err(io_error)?;
        }
        for mut deleter in deleters.into_values() {
            deleter.close().await.map_err(io_error)?;
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

impl TracedStorage {
    fn operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        let (bucket, key) = split_location(path)?;
        let mut config = self.config.clone();
        config.bucket = bucket.to_owned();
        let op = Operator::from_config(config)
            .map_err(io_error)?
            .layer(TimeoutLayer::default())
            .layer(TracedRetryLayer::default());
        Ok((op, key))
    }
}

#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(self.0.read(range).await.map_err(io_error)?.to_bytes())
    }
}

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.0.write(bytes).await.map_err(io_error)
    }

    async fn close(&mut self) -> Result<()> {
        self.0.close().await.map_err(io_error)?;
        Ok(())
    }
}

/// Maps Iceberg's parsed properties without dropping vended credentials or S3 options.
fn s3_config(s3: S3Config) -> opendal::services::S3Config {
    let mut config = opendal::services::S3Config::default();
    config.endpoint = s3.endpoint;
    config.region = s3.region;
    config.access_key_id = s3.access_key_id;
    config.secret_access_key = s3.secret_access_key;
    config.session_token = s3.session_token;
    config.enable_virtual_host_style = s3.enable_virtual_host_style;
    config.server_side_encryption = s3.server_side_encryption;
    config.server_side_encryption_aws_kms_key_id = s3.server_side_encryption_aws_kms_key_id;
    config.server_side_encryption_customer_algorithm = s3.server_side_encryption_customer_algorithm;
    config.server_side_encryption_customer_key = s3.server_side_encryption_customer_key;
    config.server_side_encryption_customer_key_md5 = s3.server_side_encryption_customer_key_md5;
    config.role_arn = s3.role_arn;
    config.external_id = s3.external_id;
    config.role_session_name = s3.role_session_name;
    config.skip_signature = s3.allow_anonymous;
    config.disable_config_load = s3.disable_config_load;
    config.disable_ec2_metadata = s3.disable_ec2_metadata;
    config
}

fn split_location(path: &str) -> Result<(&str, &str)> {
    path.split_once("://")
        .and_then(|(_, location)| location.split_once('/'))
        .filter(|(bucket, _)| !bucket.is_empty())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "expected an S3 URL with bucket and path",
            )
        })
}

fn io_error(error: opendal::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "OpenDAL I/O operation failed").with_source(error)
}
