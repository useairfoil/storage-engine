//! # Secret Store
//!
//! This crate provides a uniform interface for interacting with secret stores,
//! such as AWS Secrets Manager, Azure Key Vault, and HashiCorp Vault.
//!
//! For development, the crate also provides a [`memory::InMemorySecretStore`]
//! that stores secrets in memory and a [`file::FileSecretStore`] that stores
//! secrets on the filesystem.
//!
//! ## Examples
//!
//! ```rust
//! # use bytes::Bytes;
//! use se_secret_store::{SecretName, SecretStore, memory::InMemorySecretStore};
//!
//! # tokio_test::block_on(async {
//! let store = InMemorySecretStore::new();
//!
//! let key = SecretName::new_unchecked("password");
//! store.put(&key, Bytes::from_static(b"hunter2")).await.unwrap();
//!
//! let secret = store.get(&key).await.unwrap();
//! assert_eq!(secret.into_value(), Bytes::from_static(b"hunter2"));
//! # });
//! ```
pub mod file;
pub mod memory;

use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;

/// The error type for secret store operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid secret name.
    #[error("invalid secret name: {0:?}")]
    InvalidName(String),
    /// Secret not found.
    #[error("secret not found: {0}")]
    NotFound(SecretName),
    /// Fallback error.
    #[error("{} store error: {}", store, source)]
    Generic {
        store: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SecretName(String);

/// A secret, made of a name and its value.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret {
    name: SecretName,
    value: Bytes,
}

/// A store used to safely manage secrets.
#[async_trait]
pub trait SecretStore: fmt::Debug + Send + Sync + 'static {
    /// Retrieves the secret by name.
    async fn get(&self, name: &SecretName) -> Result<Secret>;

    /// Stores a secret with the given name.
    async fn put(&self, name: &SecretName, value: Bytes) -> Result<()>;

    /// Deletes the secret by name.
    async fn delete(&self, name: &SecretName) -> Result<()>;
}

impl SecretName {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::InvalidName(name));
        }
        Ok(Self(name))
    }

    pub fn new_unchecked(name: impl Into<String>) -> Self {
        Self::new(name).expect("secret name must not be empty")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecretName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Secret {
    /// Creates a new secret with the given name and value.
    pub fn new(name: SecretName, value: impl Into<Bytes>) -> Self {
        Self {
            name,
            value: value.into(),
        }
    }

    /// Returns a reference to the secret's name.
    pub fn name(&self) -> &SecretName {
        &self.name
    }

    /// Returns a reference to the secret's value.
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Consumes the secret, returning its value.
    pub fn into_value(self) -> Bytes {
        self.value
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secret")
            .field("name", &self.name)
            .field("value", &"***")
            .finish()
    }
}
