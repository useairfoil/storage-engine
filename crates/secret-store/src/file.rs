use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use dashmap::DashMap;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, path::Path,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::{Error, Result, Secret, SecretName, SecretStore};

pub const DEFAULT_LOCATION: &str = "secrets.json";

#[derive(Debug)]
pub struct FileSecretStore {
    store: Arc<dyn ObjectStore>,
    location: Path,
    secrets: DashMap<SecretName, Bytes>,
    version: Mutex<Option<UpdateVersion>>,
}

pub struct FileSecretStoreBuilder {
    store: Arc<dyn ObjectStore>,
    location: Path,
}

#[derive(Debug, thiserror::Error)]
pub enum FileSecretStoreError {
    #[error("snapshot encoding error: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("invalid snapshot value encoding: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Snapshot {
    secrets: BTreeMap<String, String>,
}

enum WriteError {
    Conflict,
    Store(FileSecretStoreError),
}

#[async_trait]
impl SecretStore for FileSecretStore {
    async fn get(&self, name: &SecretName) -> Result<Secret> {
        let value = self
            .secrets
            .get(name)
            .ok_or_else(|| Error::NotFound(name.clone()))?
            .value()
            .clone();

        Ok(Secret::new(name.clone(), value))
    }

    async fn put(&self, name: &SecretName, value: Bytes) -> Result<()> {
        self.write_snapshot(|secrets| {
            secrets.insert(name.clone(), value.clone());
        })
        .await
    }

    async fn delete(&self, name: &SecretName) -> Result<()> {
        self.write_snapshot(|secrets| {
            secrets.remove(name);
        })
        .await
    }
}

impl FileSecretStoreBuilder {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            location: Path::from(DEFAULT_LOCATION),
        }
    }

    pub fn with_location(mut self, location: impl Into<Path>) -> Self {
        self.location = location.into();
        self
    }

    pub async fn build(self) -> Result<FileSecretStore> {
        warn!(location = %self.location, "Writing secrets to object storage. DO NOT USE IN PRODUCTION.");

        let store = FileSecretStore {
            store: self.store,
            location: self.location,
            secrets: DashMap::default(),
            version: Mutex::new(None),
        };

        let version = store.reload().await?;
        *store.version.lock().await = version;

        Ok(store)
    }
}

impl From<FileSecretStoreError> for Error {
    fn from(value: FileSecretStoreError) -> Self {
        Self::Generic {
            store: "file",
            source: Box::new(value),
        }
    }
}

impl FileSecretStore {
    async fn write_snapshot<F>(&self, apply: F) -> Result<()>
    where
        F: Fn(&DashMap<SecretName, Bytes>),
    {
        let mut version = self.version.lock().await;

        loop {
            apply(&self.secrets);

            match self.persist(version.as_ref()).await {
                Ok(new_version) => {
                    *version = new_version;
                    return Ok(());
                }
                Err(WriteError::Conflict) => {
                    debug!(location = %self.location, "snapshot changed by another writer, reloading");
                    *version = self.reload().await?;
                }
                Err(WriteError::Store(e)) => return Err(e.into()),
            }
        }
    }

    async fn persist(
        &self,
        version: Option<&UpdateVersion>,
    ) -> Result<Option<UpdateVersion>, WriteError> {
        let snapshot = Snapshot::from_secrets(&self.secrets);
        let payload =
            serde_json::to_vec_pretty(&snapshot).map_err(|e| WriteError::Store(e.into()))?;

        let put_mode = match version {
            None => PutMode::Create,
            Some(version) => PutMode::Update(version.clone()),
        };

        match self
            .store
            .put_opts(&self.location, payload.into(), put_mode.into())
            .await
        {
            Ok(result) => Ok(Some(UpdateVersion {
                e_tag: result.e_tag,
                version: result.version,
            })),
            Err(
                e @ ObjectStoreError::Precondition { .. }
                | e @ ObjectStoreError::AlreadyExists { .. },
            ) => {
                debug!(%e, "snapshot update rejected");
                Err(WriteError::Conflict)
            }
            Err(e) => Err(WriteError::Store(e.into())),
        }
    }

    async fn reload(&self) -> Result<Option<UpdateVersion>, FileSecretStoreError> {
        self.secrets.clear();

        match self.store.get(&self.location).await {
            Ok(response) => {
                let meta = response.meta.clone();
                let bytes = response.bytes().await?;
                let snapshot: Snapshot = serde_json::from_slice(&bytes)?;

                for (name, value) in snapshot.secrets {
                    let value = BASE64.decode(value)?.into();
                    self.secrets.insert(SecretName::new_unchecked(name), value);
                }

                Ok(Some(UpdateVersion {
                    e_tag: meta.e_tag,
                    version: meta.version,
                }))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl Snapshot {
    fn from_secrets(secrets: &DashMap<SecretName, Bytes>) -> Self {
        let mut snapshot = BTreeMap::new();

        for entry in secrets.iter() {
            snapshot.insert(
                entry.key().as_str().to_owned(),
                BASE64.encode(entry.value()),
            );
        }

        Self { secrets: snapshot }
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn name(s: &str) -> SecretName {
        SecretName::new_unchecked(s)
    }

    async fn snapshot(store: &InMemory, location: &str) -> Snapshot {
        let bytes = store
            .get(&Path::from(location))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        serde_json::from_slice(&bytes).unwrap()
    }

    async fn external_write(store: &InMemory, location: &str, mutate: impl FnOnce(&mut Snapshot)) {
        let path = Path::from(location);
        let response = store.get(&path).await.unwrap();
        let e_tag = response.meta.e_tag.clone().unwrap();
        let mut snapshot: Snapshot =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();

        mutate(&mut snapshot);

        store
            .put_opts(
                &path,
                serde_json::to_vec(&snapshot).unwrap().into(),
                PutMode::Update(UpdateVersion {
                    e_tag: Some(e_tag),
                    version: None,
                })
                .into(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_persists_snapshot_and_get_roundtrips() {
        let object_store = Arc::new(InMemory::new());
        let store = FileSecretStoreBuilder::new(object_store.clone())
            .build()
            .await
            .unwrap();

        store
            .put(&name("database/password"), Bytes::from_static(b"hunter2"))
            .await
            .unwrap();

        assert_eq!(
            store.get(&name("database/password")).await.unwrap().value(),
            &Bytes::from_static(b"hunter2")
        );

        let snapshot = snapshot(&object_store, DEFAULT_LOCATION).await;
        assert_eq!(
            snapshot
                .secrets
                .get("database/password")
                .map(String::as_str),
            Some(BASE64.encode(b"hunter2").as_str())
        );
    }

    #[tokio::test]
    async fn delete_persists_snapshot() {
        let object_store = Arc::new(InMemory::new());
        let store = FileSecretStoreBuilder::new(object_store.clone())
            .build()
            .await
            .unwrap();

        store
            .put(&name("database/password"), Bytes::from_static(b"hunter2"))
            .await
            .unwrap();
        store.delete(&name("database/password")).await.unwrap();

        assert!(matches!(
            store.get(&name("database/password")).await.unwrap_err(),
            Error::NotFound(_)
        ));

        let snapshot = snapshot(&object_store, DEFAULT_LOCATION).await;
        assert!(snapshot.secrets.is_empty());
    }

    #[tokio::test]
    async fn build_loads_existing_snapshot() {
        let object_store = Arc::new(InMemory::new());

        let mut initial = Snapshot::default();
        initial
            .secrets
            .insert("database/password".to_owned(), BASE64.encode(b"hunter2"));

        object_store
            .put(
                &Path::from(DEFAULT_LOCATION),
                serde_json::to_vec(&initial).unwrap().into(),
            )
            .await
            .unwrap();

        let store = FileSecretStoreBuilder::new(object_store)
            .build()
            .await
            .unwrap();

        assert_eq!(
            store.get(&name("database/password")).await.unwrap().value(),
            &Bytes::from_static(b"hunter2")
        );
    }

    #[tokio::test]
    async fn with_location_overrides_default_location() {
        let object_store = Arc::new(InMemory::new());
        let store = FileSecretStoreBuilder::new(object_store.clone())
            .with_location("custom/secrets.json")
            .build()
            .await
            .unwrap();

        store
            .put(&name("database/password"), Bytes::from_static(b"hunter2"))
            .await
            .unwrap();

        let snapshot = snapshot(&object_store, "custom/secrets.json").await;
        assert!(snapshot.secrets.contains_key("database/password"));
    }

    #[tokio::test]
    async fn conflicting_writer_triggers_reload_and_retry() {
        let object_store = Arc::new(InMemory::new());
        let store = FileSecretStoreBuilder::new(object_store.clone())
            .build()
            .await
            .unwrap();

        store
            .put(&name("database/password"), Bytes::from_static(b"hunter2"))
            .await
            .unwrap();

        external_write(&object_store, DEFAULT_LOCATION, |snapshot| {
            snapshot
                .secrets
                .insert("api/token".to_owned(), BASE64.encode(b"token"));
        })
        .await;

        store
            .put(&name("service/api-key"), Bytes::from_static(b"key"))
            .await
            .unwrap();

        assert_eq!(
            store.get(&name("api/token")).await.unwrap().value(),
            &Bytes::from_static(b"token")
        );
        assert_eq!(
            store.get(&name("service/api-key")).await.unwrap().value(),
            &Bytes::from_static(b"key")
        );

        let snapshot = snapshot(&object_store, DEFAULT_LOCATION).await;
        assert_eq!(snapshot.secrets.len(), 3);
    }
}
