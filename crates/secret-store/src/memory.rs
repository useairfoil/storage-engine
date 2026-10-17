use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::{Error, Result, Secret, SecretName, SecretStore};

#[derive(Debug, Default, Clone)]
pub struct InMemorySecretStore {
    secrets: Arc<DashMap<SecretName, String>>,
}

impl InMemorySecretStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn get(&self, name: &SecretName) -> Result<Secret> {
        let value = self
            .secrets
            .get(name)
            .ok_or_else(|| Error::NotFound(name.clone()))?
            .value()
            .clone();

        Ok(Secret::new(name.clone(), value))
    }

    async fn put(&self, name: &SecretName, value: String) -> Result<()> {
        self.secrets.insert(name.clone(), value);
        Ok(())
    }

    async fn delete(&self, name: &SecretName) -> Result<()> {
        self.secrets.remove(name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> SecretName {
        SecretName::new_unchecked(s)
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let store = InMemorySecretStore::new();
        let name = name("database/password");

        store.put(&name, "hunter2".into()).await.unwrap();

        let secret = store.get(&name).await.unwrap();
        assert_eq!(secret.name(), &name);
        assert_eq!(secret.value(), "hunter2");
        assert_eq!(secret.into_value(), "hunter2");
    }

    #[tokio::test]
    async fn get_missing_returns_not_found() {
        let store = InMemorySecretStore::new();

        let err = store.get(&name("missing")).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn put_overwrites_existing_value() {
        let store = InMemorySecretStore::new();
        let name = name("api/token");

        store.put(&name, "v1".into()).await.unwrap();
        store.put(&name, "v2".into()).await.unwrap();

        assert_eq!(store.get(&name).await.unwrap().value(), "v2");
    }

    #[tokio::test]
    async fn delete_removes_secret() {
        let store = InMemorySecretStore::new();
        let name = name("api/token");

        store.put(&name, "v1".into()).await.unwrap();
        store.delete(&name).await.unwrap();

        assert!(matches!(
            store.get(&name).await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn delete_missing_is_ok() {
        let store = InMemorySecretStore::new();

        store.delete(&name("missing")).await.unwrap();
    }

    #[tokio::test]
    async fn empty_name_is_invalid() {
        assert!(matches!(
            SecretName::new("").unwrap_err(),
            Error::InvalidName(_)
        ));
    }
}
