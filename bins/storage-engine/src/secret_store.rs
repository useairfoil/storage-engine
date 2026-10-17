use std::sync::Arc;

use clap::{Args, ValueEnum};
use object_store::ObjectStore;
use se_secret_store::{SecretStore, azure::AzureKeyVaultSecretStore, file::FileSecretStoreBuilder};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SecretStoreType {
    File,
    AzureKeyVault,
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

    /// Azure Key Vault endpoint, required when using the azure-key-vault secret store.
    #[arg(
        long = "secret-store.azure-key-vault-url",
        env = "SE_AZURE_KEY_VAULT_URL",
        required_if_eq("secret_store_type", "azure-key-vault")
    )]
    pub azure_key_vault_url: Option<String>,
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
            SecretStoreType::AzureKeyVault => {
                let url = self.azure_key_vault_url.as_deref().ok_or_else(|| {
                    se_secret_store::Error::Generic {
                        store: "azure-key-vault",
                        source: "SE_AZURE_KEY_VAULT_URL or --secret-store.azure-key-vault-url is required".into(),
                    }
                })?;
                Ok(Arc::new(AzureKeyVaultSecretStore::from_url(url)?))
            }
        }
    }
}
