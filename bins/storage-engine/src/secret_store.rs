use std::sync::Arc;

use clap::{Args, ValueEnum};
use object_store::ObjectStore;
use se_secret_store::{SecretStore, file::FileSecretStoreBuilder};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SecretStoreType {
    File,
}

#[derive(Debug, Clone, Args)]
pub struct SecretStoreArgs {
    /// Specifies the type of secret store to use.
    #[arg(
        long = "secret-store.type",
        default_value = "file",
        env = "SE_SECRET_STORE_TYPE"
    )]
    pub secret_store_type: SecretStoreType,
}

impl SecretStoreArgs {
    pub async fn create_secret_store(
        &self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn SecretStore>, se_secret_store::Error> {
        match self.secret_store_type {
            SecretStoreType::File => {
                let store = FileSecretStoreBuilder::new(object_store).build().await?;
                Ok(Arc::new(store))
            }
        }
    }
}
