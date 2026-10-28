use std::{collections::HashMap, sync::Arc};

use clap::{Args, ValueEnum};
use object_store::ObjectStore;
use se_common::object_store::ObjectStoreConfig;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ObjectStoreType {
    S3,
    Azblob,
}

#[derive(Debug, Clone, Args)]
pub struct ObjectStoreArgs {
    /// Specifies the object store backend, configured through environment variables.
    #[arg(
        long = "object-store.type",
        default_value = "s3",
        env = "SE_OBJECT_STORE_TYPE"
    )]
    pub store_type: ObjectStoreType,
}

impl ObjectStoreArgs {
    /// Builds a native object store using standard environment settings and SE-prefixed overrides.
    pub fn create_object_store(&self) -> object_store::Result<Arc<dyn ObjectStore>> {
        let config = match self.store_type {
            ObjectStoreType::S3 => ObjectStoreConfig::S3(collect_config_from_env("SE_S3_")),
            ObjectStoreType::Azblob => {
                ObjectStoreConfig::Azblob(collect_config_from_env("SE_AZBLOB_"))
            }
        };
        config.build()
    }
}

fn collect_config_from_env(prefix: &str) -> HashMap<String, String> {
    std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix(prefix)
                .map(|field| (field.to_ascii_lowercase(), value))
        })
        .collect()
}
