//! Controllable in-memory secret store for tests.

use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;

use crate::{Error, Result, Secret, SecretName, SecretStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretOperation {
    Get(SecretName),
    Put(SecretName),
    Delete(SecretName),
}

#[derive(Debug, Default)]
pub struct MockSecretStore {
    inner: Mutex<InnerState>,
}

#[derive(Debug, Default)]
struct InnerState {
    secrets: BTreeMap<SecretName, String>,
    operations: Vec<SecretOperation>,
    fail_gets: bool,
    fail_puts: bool,
    fail_deletes: bool,
}

impl MockSecretStore {
    /// Creates an empty controllable mock secret store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces a secret directly without recording an operation.
    pub fn insert(&self, name: SecretName, value: impl Into<String>) {
        self.lock().secrets.insert(name, value.into());
    }

    /// Removes a secret directly without recording an operation.
    pub fn remove(&self, name: &SecretName) -> Option<String> {
        self.lock().secrets.remove(name)
    }

    /// Returns the stored value for a secret name if present.
    pub fn value(&self, name: &SecretName) -> Option<String> {
        self.lock().secrets.get(name).cloned()
    }

    /// Returns whether a secret with the given name is currently stored.
    pub fn contains(&self, name: &SecretName) -> bool {
        self.lock().secrets.contains_key(name)
    }

    /// Returns the sorted names of all currently stored secrets.
    pub fn names(&self) -> Vec<SecretName> {
        self.lock().secrets.keys().cloned().collect()
    }

    /// Returns the number of currently stored secrets.
    pub fn len(&self) -> usize {
        self.lock().secrets.len()
    }

    /// Returns whether no secrets are currently stored.
    pub fn is_empty(&self) -> bool {
        self.lock().secrets.is_empty()
    }

    /// Configures whether `get` calls fail with a store error.
    pub fn fail_gets(&self, fail: bool) {
        self.lock().fail_gets = fail;
    }

    /// Configures whether `put` calls fail with a store error.
    pub fn fail_puts(&self, fail: bool) {
        self.lock().fail_puts = fail;
    }

    /// Configures whether `delete` calls fail with a store error.
    pub fn fail_deletes(&self, fail: bool) {
        self.lock().fail_deletes = fail;
    }

    /// Returns all recorded secret store operations in arrival order.
    pub fn operations(&self) -> Vec<SecretOperation> {
        self.lock().operations.clone()
    }

    /// Clears the recorded operation log.
    pub fn clear_operations(&self) {
        self.lock().operations.clear();
    }

    fn lock(&self) -> MutexGuard<'_, InnerState> {
        // PANIC: MockSecretStore mutex is only poisoned if a test panics while holding it.
        self.inner.lock().unwrap()
    }
}

#[async_trait]
impl SecretStore for MockSecretStore {
    async fn get(&self, name: &SecretName) -> Result<Secret> {
        let mut inner = self.lock();
        inner.operations.push(SecretOperation::Get(name.clone()));

        if inner.fail_gets {
            return Err(unavailable("secret retrieval unavailable"));
        }

        let value = inner
            .secrets
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(name.clone()))?;

        Ok(Secret::new(name.clone(), value))
    }

    async fn put(&self, name: &SecretName, value: String) -> Result<()> {
        let mut inner = self.lock();
        inner.operations.push(SecretOperation::Put(name.clone()));

        if inner.fail_puts {
            return Err(unavailable("secret write unavailable"));
        }

        inner.secrets.insert(name.clone(), value);

        Ok(())
    }

    async fn delete(&self, name: &SecretName) -> Result<()> {
        let mut inner = self.lock();
        inner.operations.push(SecretOperation::Delete(name.clone()));

        if inner.fail_deletes {
            return Err(unavailable("secret deletion unavailable"));
        }

        inner.secrets.remove(name);

        Ok(())
    }
}

fn unavailable(message: &'static str) -> Error {
    Error::Generic {
        store: "mock",
        source: std::io::Error::other(message).into(),
    }
}
