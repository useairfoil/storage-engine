//! Azure Key Vault SecretStore
//!
//! Names retain lowercase ASCII letters and digits, every other UTF-8 byte is
//! escaped as `-xx` (lowercase hex).
//! For example, `catalog/lakekeeper` becomes `catalog-2flakekeeper`, and
//! `object-store/ci-s3` becomes `object-2dstore-2fci-2ds3`.
//! Escaping uppercase letters and hyphens prevents collisions in Key Vault's
//! case-insensitive name space.
//! Encoded names must fit Key Vault's 127-character limit.
//!
//! Values are read and written as plain strings.
//! Deletion is soft deletion, never purge.
//! Azure may reject recreating a deleted name until it is recovered or purged.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use azure_core::{credentials::TokenCredential, error::ErrorKind, http::StatusCode};
use azure_identity::{DeveloperToolsCredential, WorkloadIdentityCredential};
use azure_security_keyvault_secrets::{SecretClient, models::SetSecretParameters};

use crate::{Error, Result, Secret, SecretName, SecretStore};

/// A secret store using a caller-configured Azure Key Vault client.
///
/// The caller supplies the client so authentication and transport can be chosen
/// independently of the store. Latest secret versions are read.
pub struct AzureKeyVaultSecretStore {
    client: SecretClient,
}

#[derive(Debug, thiserror::Error)]
enum AzureKeyVaultError {
    #[error("azure sdk error: {0}")]
    Sdk(#[from] azure_core::Error),
    #[error("Key Vault returned a secret without a value")]
    MissingValue,
}

impl AzureKeyVaultSecretStore {
    pub fn new(client: SecretClient) -> Self {
        Self { client }
    }

    /// Uses a federated token file when `AZURE_FEDERATED_TOKEN_FILE` is set,
    /// otherwise authenticates using Azure CLI or Azure Developer CLI credentials.
    pub fn from_url(vault_url: &str) -> Result<Self> {
        let credential: Arc<dyn TokenCredential> =
            if std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some() {
                WorkloadIdentityCredential::new(None).map_err(AzureKeyVaultError::from)?
            } else {
                DeveloperToolsCredential::new(None).map_err(AzureKeyVaultError::from)?
            };
        let client =
            SecretClient::new(vault_url, credential, None).map_err(AzureKeyVaultError::from)?;
        Ok(Self::new(client))
    }
}

impl fmt::Debug for AzureKeyVaultSecretStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureKeyVaultSecretStore")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretStore for AzureKeyVaultSecretStore {
    async fn get(&self, name: &SecretName) -> Result<Secret> {
        let vault_name = vault_secret_name(name)?;

        let response = match self.client.get_secret(&vault_name, None).await {
            Ok(response) => response,
            Err(error) if is_not_found(&error) => return Err(Error::NotFound(name.clone())),
            Err(error) => return Err(AzureKeyVaultError::from(error).into()),
        };

        let secret = response.into_model().map_err(AzureKeyVaultError::from)?;

        let value = secret.value.ok_or(AzureKeyVaultError::MissingValue)?;

        Ok(Secret::new(name.clone(), value))
    }

    async fn put(&self, name: &SecretName, value: String) -> Result<()> {
        let vault_name = vault_secret_name(name)?;

        let parameters = SetSecretParameters {
            value: Some(value),
            content_type: Some("text/plain; charset=utf-8".into()),
            ..Default::default()
        };

        self.client
            .set_secret(
                &vault_name,
                parameters.try_into().map_err(AzureKeyVaultError::from)?,
                None,
            )
            .await
            .map_err(AzureKeyVaultError::from)?;

        Ok(())
    }

    async fn delete(&self, name: &SecretName) -> Result<()> {
        let vault_name = vault_secret_name(name)?;

        match self.client.delete_secret(&vault_name, None).await {
            Ok(_) => Ok(()),
            Err(error) if is_not_found(&error) => Ok(()),
            Err(error) => Err(AzureKeyVaultError::from(error).into()),
        }
    }
}

fn vault_secret_name(name: &SecretName) -> Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::new();

    for byte in name.as_str().bytes() {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
            encoded.push(char::from(byte));
        } else {
            encoded.push('-');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0xf)]));
        }
    }

    if encoded.len() > 127 {
        return Err(Error::InvalidName(name.as_str().into()));
    }

    Ok(encoded)
}

fn is_not_found(error: &azure_core::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::HttpResponse {
            status: StatusCode::NotFound,
            ..
        }
    )
}

impl From<AzureKeyVaultError> for Error {
    fn from(source: AzureKeyVaultError) -> Self {
        Self::Generic {
            store: "azure-key-vault",
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_vault_names() -> Result<()> {
        for (name, expected) in [
            ("abc123", "abc123"),
            ("catalog/lakekeeper", "catalog-2flakekeeper"),
            ("object-store/ci-s3", "object-2dstore-2fci-2ds3"),
            ("catalog/a", "catalog-2fa"),
            ("catalog-2fa", "catalog-2d2fa"),
            ("Catalog/a", "-43atalog-2fa"),
            ("catalog_a", "catalog-5fa"),
            ("catalog/é", "catalog-2f-c3-a9"),
        ] {
            assert_eq!(vault_secret_name(&SecretName::new(name)?)?, expected);
        }

        Ok(())
    }

    #[test]
    fn validates_encoded_name_length() -> Result<()> {
        for name in ["a".repeat(127), format!("{}/", "a".repeat(124))] {
            assert_eq!(vault_secret_name(&SecretName::new(name)?)?.len(), 127);
        }
        for name in ["a".repeat(128), format!("{}/", "a".repeat(125))] {
            assert!(matches!(
                vault_secret_name(&SecretName::new(name.clone())?),
                Err(Error::InvalidName(invalid)) if invalid == name
            ));
        }
        Ok(())
    }
}
