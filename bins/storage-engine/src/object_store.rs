use std::sync::Arc;

use clap::{Args, ValueEnum};
use object_store::{
    ObjectStore, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, prefix::PrefixStore,
};

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
        let (store, config): (Arc<dyn ObjectStore>, _) = match self.store_type {
            ObjectStoreType::S3 => {
                let config = collect_config_from_env("SE_S3_");
                let mut builder = AmazonS3Builder::from_env();

                for (key, value) in &config {
                    if key != "root" {
                        let key = match key.as_str() {
                            "enable_virtual_host_style" => "virtual_hosted_style_request",
                            key => key,
                        };
                        builder = builder.with_config(key.parse()?, value);
                    }
                }

                (Arc::new(builder.build()?), config)
            }
            ObjectStoreType::Azblob => {
                let config = collect_config_from_env("SE_AZBLOB_");
                let mut builder = MicrosoftAzureBuilder::from_env();

                for (key, value) in &config {
                    if key != "root" {
                        let key = match key.as_str() {
                            "container" => "container_name",
                            key => key,
                        };
                        builder = builder.with_config(key.parse()?, value);
                    }
                }

                (Arc::new(builder.build()?), config)
            }
        };

        match config.iter().find(|(key, _)| key == "root") {
            Some((_, root)) => Ok(Arc::new(PrefixStore::new(store, root.as_str()))),
            None => Ok(store),
        }
    }
}

fn collect_config_from_env(prefix: &str) -> Vec<(String, String)> {
    std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix(prefix)
                .map(|field| (field.to_ascii_lowercase(), value))
        })
        .collect()
}
