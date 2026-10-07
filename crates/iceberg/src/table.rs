use std::sync::Arc;

use iceberg::{
    io::{S3_PATH_STYLE_ACCESS, S3Config, StorageConfig},
    table::Table,
};
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{
    Operator,
    layers::{RetryLayer, TimeoutLayer},
};

/// Errors constructing a table storage operator.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid table storage config: {0}")]
    InvalidConfig(&'static str),
    #[error("iceberg config error: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[error("storage operator error: {0}")]
    OpenDal(#[from] opendal::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Storage extensions for Iceberg tables.
pub trait TableExt {
    /// Builds an S3 object store rooted at the table's base location.
    ///
    /// Paths are table-relative, not full S3 URLs. Credentials are taken from the
    /// FileIO config without ambient AWS fallback or automatic refresh.
    /// Path-style addressing is the default unless `s3.path-style-access` is set.
    fn object_store(&self) -> Result<Arc<dyn ObjectStore>>;
}

impl TableExt for Table {
    fn object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let operator = storage_operator(self.metadata().location(), self.file_io().config())?;
        Ok(Arc::new(OpendalStore::new(operator)))
    }
}

fn storage_operator(location: &str, props: &StorageConfig) -> Result<Operator> {
    let location = location
        .strip_prefix("s3://")
        .ok_or(Error::InvalidConfig("expected an s3:// table location"))?;
    let (bucket, root) = location.split_once('/').unwrap_or((location, ""));
    if bucket.is_empty() {
        return Err(Error::InvalidConfig("missing S3 bucket"));
    }
    let s3 = S3Config::try_from(props)?;
    if s3.access_key_id.as_deref().is_none_or(str::is_empty)
        || s3.secret_access_key.as_deref().is_none_or(str::is_empty)
    {
        return Err(Error::InvalidConfig("missing S3 access key or secret key"));
    }
    if s3.region.as_deref().is_none_or(str::is_empty) {
        return Err(Error::InvalidConfig("missing S3 region"));
    }
    let mut config = opendal::services::S3Config::default();
    config.bucket = bucket.to_owned();
    config.root = Some(root.to_owned());
    config.endpoint = s3.endpoint;
    config.region = s3.region;
    config.access_key_id = s3.access_key_id;
    config.secret_access_key = s3.secret_access_key;
    config.session_token = s3.session_token;
    config.enable_virtual_host_style =
        props.get(S3_PATH_STYLE_ACCESS).is_some() && s3.enable_virtual_host_style;
    config.server_side_encryption = s3.server_side_encryption;
    config.server_side_encryption_aws_kms_key_id = s3.server_side_encryption_aws_kms_key_id;
    config.server_side_encryption_customer_algorithm = s3.server_side_encryption_customer_algorithm;
    config.server_side_encryption_customer_key = s3.server_side_encryption_customer_key;
    config.server_side_encryption_customer_key_md5 = s3.server_side_encryption_customer_key_md5;
    config.disable_config_load = true;
    config.disable_ec2_metadata = true;
    Ok(Operator::from_config(config)?
        .layer(TimeoutLayer::default())
        .layer(RetryLayer::default().with_jitter()))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use iceberg::{
        NamespaceIdent, TableIdent,
        io::{FileIOBuilder, MemoryStorageFactory},
        spec::{FormatVersion, Schema, SortOrder, TableMetadataBuilder, UnboundPartitionSpec},
    };

    use super::*;

    fn config() -> StorageConfig {
        StorageConfig::new()
            .with_prop("s3.endpoint", "http://127.0.0.1:8333")
            .with_prop("client.region", "local")
            .with_prop("s3.access-key-id", "table-key")
            .with_prop("s3.secret-access-key", "table-secret")
            .with_prop("s3.session-token", "table-token")
    }

    #[tokio::test]
    async fn signs_table_relative_paths_with_vended_credentials() {
        let metadata = TableMetadataBuilder::new(
            Schema::builder().build().unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "s3://warehouse/tables/events/".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let table = Table::builder()
            .runtime(iceberg::Runtime::try_current().unwrap())
            .identifier(TableIdent::new(
                NamespaceIdent::new("test".into()),
                "events".into(),
            ))
            .metadata(metadata)
            .file_io(
                FileIOBuilder::new(Arc::new(MemoryStorageFactory))
                    .with_props(config().props().clone())
                    .build(),
            )
            .build()
            .unwrap();
        let _store: Arc<dyn ObjectStore> = table.object_store().unwrap();
        let operator =
            storage_operator(table.metadata().location(), table.file_io().config()).unwrap();
        let request = operator
            .presign_read("data/part.parquet", Duration::from_secs(60))
            .await
            .unwrap();
        let uri = request.uri();
        assert_eq!(uri.authority().unwrap().as_str(), "127.0.0.1:8333");
        assert_eq!(uri.path(), "/warehouse/tables/events/data/part.parquet");
        let query = uri.query().unwrap();
        assert!(query.contains("table-key"));
        assert!(query.contains("table-token"));
        assert!(query.contains("local"));
    }

    #[tokio::test]
    async fn honors_explicit_virtual_host_addressing() {
        let props = config()
            .with_prop("s3.endpoint", "https://s3.example.com")
            .with_prop(S3_PATH_STYLE_ACCESS, "false");
        let operator = storage_operator("s3://warehouse/events", &props).unwrap();
        let request = operator
            .presign_read("part.parquet", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(request.uri().host(), Some("warehouse.s3.example.com"));
        assert_eq!(request.uri().path(), "/events/part.parquet");
    }

    #[test]
    fn rejects_unsupported_locations_and_missing_credentials() {
        for (location, props) in [
            ("file:///tmp/table", config()),
            ("s3:///table", config()),
            ("s3://warehouse/table", StorageConfig::new()),
            (
                "s3://warehouse/table",
                config().with_prop("s3.secret-access-key", ""),
            ),
            (
                "s3://warehouse/table",
                config().with_prop("client.region", ""),
            ),
        ] {
            assert!(matches!(
                storage_operator(location, &props),
                Err(Error::InvalidConfig(_))
            ));
        }
    }
}
