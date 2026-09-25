use std::sync::Arc;

use clap::{Args, ValueEnum};
use object_store::{
    ObjectStore, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ObjectStoreType {
    Aws,
    Gcp,
    Azure,
}

#[derive(Debug, Clone, Args)]
pub struct ObjectStoreArgs {
    /// Specifies the type of object store to use.
    ///
    /// Based on this value, the appropriate object store will be created from environment variables.
    #[arg(
        long = "object-store.type",
        default_value = "aws",
        env = "SE_OBJECT_STORE_TYPE"
    )]
    pub object_store_type: ObjectStoreType,
    #[arg(
        long = "object-store.bucket-name",
        default_value = "aws",
        env = "SE_OBJECT_STORE_BUCKET_NAME"
    )]
    pub object_store_bucket_name: String,
}

impl ObjectStoreArgs {
    pub fn create_object_store(&self) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        match self.object_store_type {
            ObjectStoreType::Aws => {
                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(&self.object_store_bucket_name)
                    .build()?;
                Ok(Arc::new(store))
            }
            ObjectStoreType::Gcp => {
                let store = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&self.object_store_bucket_name)
                    .build()?;
                Ok(Arc::new(store))
            }
            ObjectStoreType::Azure => {
                let store = MicrosoftAzureBuilder::from_env()
                    .with_container_name(&self.object_store_bucket_name)
                    .build()?;
                Ok(Arc::new(store))
            }
        }
    }
}
