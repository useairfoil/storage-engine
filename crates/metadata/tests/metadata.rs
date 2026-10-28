use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use se_common::clock::{DefaultSystemClock, MockSystemClock};
use se_metadata::{CatalogConfig, CatalogId, Error, Metadata, RestCatalogConfig};
use se_secret_store::{
    Secret, SecretName, SecretStore,
    file::{FileSecretStore, FileSecretStoreBuilder},
    memory::InMemorySecretStore,
};
use slatedb::Db;
use ulid::Ulid;
use uuid::Uuid;

#[derive(Debug)]
struct UnavailableSecretStore;

struct FaultySecretStore {
    inner: Arc<FileSecretStore>,
    db: Db,
    close_on_put: bool,
    fail_delete: bool,
}

#[tokio::test]
async fn opens_at_requested_prefix_and_can_be_reopened() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(DefaultSystemClock::new());
    let metadata = Metadata::new(
        "tenants/acme/metadata",
        store.clone(),
        secrets.clone(),
        clock.clone(),
    )
    .await?;

    let name = SecretName::new("catalog-config")?;
    metadata
        .secret_store()
        .put(&name, "config-v1".into())
        .await?;

    let clone = metadata.clone();
    drop(metadata);

    assert_eq!(clone.secret_store().get(&name).await?.value(), "config-v1");

    clone.secret_store().put(&name, "config-v2".into()).await?;
    clone.close().await?;

    let objects: Vec<_> = store
        .list(Some(&Path::from("tenants/acme/metadata")))
        .try_collect()
        .await?;

    insta::assert_debug_snapshot!(objects.is_empty(), @"false");

    let reopened = Metadata::new("tenants/acme/metadata", store, secrets, clock).await?;

    assert_eq!(
        reopened.secret_store().get(&name).await?.value(),
        "config-v2"
    );

    reopened.close().await?;

    Ok(())
}

#[tokio::test]
async fn catalogs_resolve_ulid_secrets_and_survive_reopening()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(MockSystemClock::with_time(123_456_789));
    let db = Db::open("metadata", store.clone()).await?;
    let metadata = Metadata::from_db(db.clone(), secrets.clone(), clock.clone());
    let first = CatalogId::new("Catalog /\0\x01")?;
    let second = CatalogId::new("é".repeat(16))?;
    let first_config = catalog_config("https://first.example");
    let second_config = catalog_config("https://second.example");

    insta::assert_debug_snapshot!(metadata.get_catalog(&first).await?, @"None");

    metadata
        .create_catalog(first.clone(), first_config.clone())
        .await?;

    clock.set(987_654_321);
    metadata
        .clone()
        .create_catalog(second.clone(), second_config.clone())
        .await?;

    assert_eq!(
        metadata.get_catalog(&first).await?,
        Some(first_config.clone())
    );
    assert_eq!(
        metadata.get_catalog(&second).await?,
        Some(second_config.clone())
    );

    // SlateDB holds only the ULID reference; configuration lives under that secret key.
    let key = b"\x01\x01Catalog /\x01\x01\x01\x02\x00";
    let reference = db.get(key).await?.ok_or("missing catalog reference")?;
    let secret_id = Ulid::from_bytes(reference.as_ref().try_into()?);

    assert_eq!(secret_id.timestamp_ms(), 123_456_789);

    let second_key = [b"\x01\x01".as_slice(), second.as_str().as_bytes(), b"\0"].concat();
    let second_reference = db
        .get(second_key)
        .await?
        .ok_or("missing second catalog reference")?;
    let second_secret_id = Ulid::from_bytes(second_reference.as_ref().try_into()?);

    assert_eq!(second_secret_id.timestamp_ms(), 987_654_321);

    let name = SecretName::new(secret_id.to_string())?;
    let secret = secrets.get(&name).await?;

    assert_eq!(
        serde_json::from_str::<CatalogConfig>(secret.value())?,
        first_config
    );
    insta::assert_debug_snapshot!(secrets.get(&SecretName::new(first.as_str())?).await.is_err(), @"true");

    let error = metadata
        .create_catalog(first.clone(), second_config.clone())
        .await
        .err()
        .ok_or("duplicate create succeeded")?;

    insta::assert_debug_snapshot!(matches!(error, Error::CatalogAlreadyExists(_)), @"true");
    assert_eq!(
        metadata.get_catalog(&first).await?,
        Some(first_config.clone())
    );

    metadata.close().await?;

    let reopened = Metadata::new("metadata", store, secrets.clone(), clock).await?;

    assert_eq!(reopened.get_catalog(&first).await?, Some(first_config));
    assert_eq!(reopened.get_catalog(&second).await?, Some(second_config));

    secrets.delete(&name).await?;

    let error = reopened
        .get_catalog(&first)
        .await
        .err()
        .ok_or("missing secret was ignored")?;

    insta::assert_debug_snapshot!(matches!(error, Error::SecretStore(se_secret_store::Error::NotFound(_))), @"true");

    reopened.close().await?;

    Ok(())
}

#[tokio::test]
async fn catalog_updates_and_deletes_are_durable_and_isolated()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(MockSystemClock::with_time(123));
    let db = Db::open("metadata", store.clone()).await?;
    let metadata = Metadata::from_db(db.clone(), secrets.clone(), clock.clone());
    let id = CatalogId::new("catalog")?;
    let neighbor = CatalogId::new("catalog-extra")?;
    let original = catalog_config("https://original.example");
    let replacement = catalog_config("https://replacement.example");
    let neighbor_config = catalog_config("https://neighbor.example");

    metadata.delete_catalog(&id).await?;

    let error = metadata.update_catalog(&id, replacement.clone()).await;

    insta::assert_debug_snapshot!(matches!(error, Err(Error::CatalogNotFound(ref missing)) if missing == &id), @"true");
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

    metadata
        .create_catalog(id.clone(), original.clone())
        .await?;
    metadata
        .create_catalog(neighbor.clone(), neighbor_config.clone())
        .await?;

    let old_reference = db
        .get(b"\x01\x01catalog\0")
        .await?
        .ok_or("missing reference")?;
    let old_name =
        SecretName::new(Ulid::from_bytes(old_reference.as_ref().try_into()?).to_string())?;

    clock.set(456);
    metadata
        .clone()
        .update_catalog(&id, replacement.clone())
        .await?;

    assert_eq!(metadata.get_catalog(&id).await?, Some(replacement.clone()));
    insta::assert_debug_snapshot!(matches!(secrets.get(&old_name).await, Err(se_secret_store::Error::NotFound(_))), @"true");

    let reference = db
        .get(b"\x01\x01catalog\0")
        .await?
        .ok_or("missing updated reference")?;
    let secret_id = Ulid::from_bytes(reference.as_ref().try_into()?);

    assert_eq!(secret_id.timestamp_ms(), 456);

    let name = SecretName::new(secret_id.to_string())?;

    assert_eq!(
        serde_json::from_str::<CatalogConfig>(secrets.get(&name).await?.value())?,
        replacement
    );

    metadata.close().await?;

    let metadata = Metadata::from_db(
        Db::open("metadata", store.clone()).await?,
        secrets.clone(),
        clock.clone(),
    );

    assert_eq!(metadata.get_catalog(&id).await?, Some(replacement));

    metadata.delete_catalog(&id).await?;
    metadata.delete_catalog(&id).await?;

    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");
    insta::assert_debug_snapshot!(matches!(secrets.get(&name).await, Err(se_secret_store::Error::NotFound(_))), @"true");
    assert_eq!(
        metadata.get_catalog(&neighbor).await?,
        Some(neighbor_config.clone())
    );

    metadata.close().await?;

    let metadata = Metadata::from_db(Db::open("metadata", store).await?, secrets, clock);

    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");
    assert_eq!(
        metadata.get_catalog(&neighbor).await?,
        Some(neighbor_config)
    );

    metadata
        .create_catalog(id.clone(), original.clone())
        .await?;

    assert_eq!(metadata.get_catalog(&id).await?, Some(original));

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn concurrent_creates_do_not_overwrite_the_winner() -> Result<(), Box<dyn std::error::Error>>
{
    let metadata = Metadata::new(
        "metadata",
        Arc::new(InMemory::new()),
        Arc::new(InMemorySecretStore::new()),
        Arc::new(DefaultSystemClock::new()),
    )
    .await?;
    let id = CatalogId::new("shared")?;
    let first_config = catalog_config("https://first.example");
    let second_config = catalog_config("https://second.example");

    let (first, second) = tokio::join!(
        metadata.create_catalog(id.clone(), first_config.clone()),
        metadata.create_catalog(id.clone(), second_config.clone()),
    );

    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);

    let expected = if first.is_ok() {
        first_config
    } else {
        second_config
    };

    assert_eq!(metadata.get_catalog(&id).await?, Some(expected));

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn secret_write_failure_does_not_publish_a_catalog() -> Result<(), Box<dyn std::error::Error>>
{
    let metadata = Metadata::new(
        "metadata",
        Arc::new(InMemory::new()),
        Arc::new(UnavailableSecretStore),
        Arc::new(DefaultSystemClock::new()),
    )
    .await?;
    let id = CatalogId::new("unavailable")?;

    let error = metadata
        .create_catalog(id.clone(), catalog_config("https://catalog.example"))
        .await
        .err()
        .ok_or("secret write unexpectedly succeeded")?;

    insta::assert_debug_snapshot!(matches!(error, Error::SecretStore(_)), @"true");
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn injected_database_is_shared_and_closed_by_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let db = Db::open("custom-prefix", store.clone()).await?;
    db.put(b"existing-key", b"existing-value").await?;

    let metadata = Metadata::from_db(
        db.clone(),
        Arc::new(InMemorySecretStore::new()),
        Arc::new(DefaultSystemClock::new()),
    );
    let clone = metadata.clone();
    drop(metadata);

    insta::assert_debug_snapshot!(db.status().close_reason, @"None");

    clone.close().await?;

    insta::assert_debug_snapshot!(db.status().close_reason, @r"
    Some(
        Clean,
    )
    ");

    let reopened = Db::open("custom-prefix", store).await?;

    assert_eq!(
        reopened.get(b"existing-key").await?.as_deref(),
        Some(b"existing-value".as_slice()),
    );

    reopened.close().await?;

    Ok(())
}

#[tokio::test]
async fn database_failure_cleans_up_secret_and_preserves_original_error()
-> Result<(), Box<dyn std::error::Error>> {
    for (update, fail_delete) in [(false, false), (false, true), (true, false), (true, true)] {
        let store = Arc::new(InMemory::new());
        let secret_objects = Arc::new(InMemory::new());
        let inner = Arc::new(
            FileSecretStoreBuilder::new(secret_objects.clone())
                .build()
                .await?,
        );
        let db = Db::open("metadata", store.clone()).await?;
        let clock = Arc::new(DefaultSystemClock::new());
        let id = CatalogId::new("db-failure")?;
        let original = catalog_config("https://original.example");

        if update {
            Metadata::from_db(db.clone(), inner.clone(), clock.clone())
                .create_catalog(id.clone(), original.clone())
                .await?;
        }

        let secrets = Arc::new(FaultySecretStore {
            inner,
            db: db.clone(),
            close_on_put: true,
            fail_delete,
        });
        let metadata = Metadata::from_db(db, secrets.clone(), clock.clone());

        let config = catalog_config("https://catalog.example");
        let result = if update {
            metadata.update_catalog(&id, config).await
        } else {
            metadata.create_catalog(id.clone(), config).await
        };
        let error = result
            .err()
            .ok_or("database write unexpectedly succeeded")?;

        insta::allow_duplicates! {
            insta::assert_debug_snapshot!(matches!(error, Error::SlateDb(_)), @"true");
        }

        let snapshot = secret_objects
            .get(&Path::from("secrets.json"))
            .await?
            .bytes()
            .await?;
        let snapshot: serde_json::Value = serde_json::from_slice(&snapshot)?;
        let secrets_left = snapshot["secrets"]
            .as_object()
            .ok_or("invalid secret snapshot")?
            .len();

        assert_eq!(secrets_left, usize::from(update) + usize::from(fail_delete));

        let reopened = Metadata::new("metadata", store, secrets, clock).await?;
        let catalog = reopened.get_catalog(&id).await?;

        assert_eq!(catalog, update.then_some(original));

        reopened.close().await?;
    }

    Ok(())
}

#[tokio::test]
async fn cleanup_errors_leave_the_committed_catalog_state() -> Result<(), Box<dyn std::error::Error>>
{
    let store = Arc::new(InMemory::new());
    let inner = Arc::new(
        FileSecretStoreBuilder::new(Arc::new(InMemory::new()))
            .build()
            .await?,
    );
    let db = Db::open("metadata", store.clone()).await?;
    let secrets = Arc::new(FaultySecretStore {
        inner,
        db: db.clone(),
        close_on_put: false,
        fail_delete: true,
    });
    let clock = Arc::new(DefaultSystemClock::new());
    let metadata = Metadata::from_db(db, secrets.clone(), clock.clone());
    let id = CatalogId::new("cleanup-failure")?;

    metadata
        .create_catalog(id.clone(), catalog_config("https://old.example"))
        .await?;

    let replacement = catalog_config("https://new.example");
    let result = metadata.update_catalog(&id, replacement.clone()).await;

    insta::assert_debug_snapshot!(matches!(result, Err(Error::SecretStore(_))), @"true");
    assert_eq!(metadata.get_catalog(&id).await?, Some(replacement.clone()));

    metadata.close().await?;

    let metadata = Metadata::new("metadata", store.clone(), secrets.clone(), clock.clone()).await?;

    assert_eq!(metadata.get_catalog(&id).await?, Some(replacement));

    let result = metadata.delete_catalog(&id).await;

    insta::assert_debug_snapshot!(matches!(result, Err(Error::SecretStore(_))), @"true");
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

    metadata.delete_catalog(&id).await?;
    metadata.close().await?;

    let metadata = Metadata::new("metadata", store, secrets, clock).await?;

    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn wal_appends_are_atomic_isolated_and_survive_reopening()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let secrets = Arc::new(InMemorySecretStore::new());
    let clock = Arc::new(MockSystemClock::with_time(123_456_789));
    let db = Db::open("metadata", store.clone()).await?;
    let metadata = Metadata::from_db(db.clone(), secrets.clone(), clock.clone());
    let catalog = CatalogId::new("A/\0\x01é")?;
    let table = Uuid::from_bytes([17; 16]);
    let timestamp = chrono::DateTime::from_timestamp(-2, 16_909_060).ok_or("invalid time")?;
    let state_key = [
        b"\x01\x02A/\x01\x01\x01\x02\xc3\xa9\0".as_slice(),
        table.as_bytes(),
    ]
    .concat();
    let batch_prefix = [
        b"\x01\x03A/\x01\x01\x01\x02\xc3\xa9\0".as_slice(),
        table.as_bytes(),
    ]
    .concat();

    // Appending must preserve an existing reader's epoch.
    db.put(
        &state_key,
        [0_u64.to_le_bytes(), 7_u64.to_le_bytes()].concat(),
    )
    .await?;

    futures::future::try_join_all((0..16).map(|i| {
        let metadata = &metadata;
        let catalog = &catalog;

        async move {
            metadata
                .wal_append(catalog, table, -17, &format!("wal/{i}"), timestamp)
                .await
        }
    }))
    .await?;

    let state = db.get(&state_key).await?.ok_or("missing WAL state")?;
    assert_eq!(
        state.as_ref(),
        [16_u64.to_le_bytes(), 7_u64.to_le_bytes()].concat()
    );

    let mut scan = db.scan_prefix(&batch_prefix, ..).await?;
    let mut locations = Vec::new();
    let mut sequence = 0_u64;

    while let Some(entry) = scan.next().await? {
        assert_eq!(
            entry.key.as_ref(),
            [batch_prefix.as_slice(), &sequence.to_be_bytes()].concat()
        );
        assert_eq!(
            &entry.value[..16],
            b"\xef\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\x04\x03\x02\x01"
        );
        locations.push(String::from_utf8(
            entry.value[16..entry.value.len() - 1].to_vec(),
        )?);
        assert_eq!(entry.value.last(), Some(&0));
        sequence += 1;
    }

    assert_eq!(sequence, 16);
    locations.sort();
    let mut expected: Vec<_> = (0..16).map(|i| format!("wal/{i}")).collect();
    expected.sort();
    assert_eq!(locations, expected);

    metadata
        .wal_append(&CatalogId::new("other")?, table, 2, "elsewhere", timestamp)
        .await?;
    metadata
        .wal_append(
            &catalog,
            Uuid::from_bytes([18; 16]),
            3,
            "another-table",
            timestamp,
        )
        .await?;
    assert_eq!(
        db.get(&state_key).await?.ok_or("missing state")?.as_ref(),
        state.as_ref()
    );

    metadata.close().await?;

    let db = Db::open("metadata", store).await?;
    let metadata = Metadata::from_db(db.clone(), secrets, clock);
    metadata
        .wal_append(&catalog, table, 5, "new/\0\x01é", timestamp)
        .await?;

    let key = [batch_prefix.as_slice(), &16_u64.to_be_bytes()].concat();
    let value = db.get(key).await?.ok_or("missing reopened batch")?;
    assert_eq!(&value[16..], b"new/\x01\x01\x01\x02\xc3\xa9\0");
    assert_eq!(
        db.get(&state_key).await?.ok_or("missing state")?.as_ref(),
        [17_u64.to_le_bytes(), 7_u64.to_le_bytes()].concat()
    );

    let descriptors = metadata
        .wal_read_after(&catalog, table, Some(15), 2)
        .await?;
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].sequence, 16);
    assert_eq!(descriptors[0].metadata.schema_id, 5);
    assert_eq!(descriptors[0].metadata.location, "new/\0\x01é");
    assert_eq!(descriptors[0].metadata.timestamp, timestamp);

    // Invalid or exhausted state must not produce a partially committed batch.
    for (state, expected_error) in [
        (vec![0], "invalid WAL state encoding"),
        (
            [u64::MAX.to_le_bytes(), 7_u64.to_le_bytes()].concat(),
            "WAL sequence numbers exhausted",
        ),
    ] {
        db.put(&state_key, state.clone()).await?;
        let error = metadata
            .wal_append(&catalog, table, 9, "rejected", timestamp)
            .await
            .err()
            .ok_or("append unexpectedly succeeded")?;
        assert_eq!(error.to_string(), expected_error);
        assert_eq!(
            db.get(&state_key).await?.ok_or("missing state")?.as_ref(),
            state
        );
        let key = [batch_prefix.as_slice(), &17_u64.to_be_bytes()].concat();
        let batch = db.get(key).await?;

        insta::allow_duplicates! {
            insta::assert_debug_snapshot!(batch, @"None");
        }
    }

    metadata.close().await?;

    Ok(())
}

fn catalog_config(uri: &str) -> CatalogConfig {
    CatalogConfig::Rest(RestCatalogConfig {
        uri: uri.into(),
        warehouse: Some("warehouse-one".into()),
        properties: [("credential".into(), "test-credential".into())].into(),
    })
}

#[async_trait]
impl SecretStore for UnavailableSecretStore {
    async fn get(&self, name: &SecretName) -> se_secret_store::Result<Secret> {
        Err(se_secret_store::Error::NotFound(name.clone()))
    }

    async fn put(&self, _name: &SecretName, _value: String) -> se_secret_store::Result<()> {
        Err(se_secret_store::Error::Generic {
            store: "unavailable",
            source: std::io::Error::other("secret store unavailable").into(),
        })
    }

    async fn delete(&self, _name: &SecretName) -> se_secret_store::Result<()> {
        Ok(())
    }
}

impl std::fmt::Debug for FaultySecretStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultySecretStore").finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretStore for FaultySecretStore {
    async fn get(&self, name: &SecretName) -> se_secret_store::Result<Secret> {
        self.inner.get(name).await
    }

    async fn put(&self, name: &SecretName, value: String) -> se_secret_store::Result<()> {
        self.inner.put(name, value).await?;

        if self.close_on_put {
            self.db
                .close()
                .await
                .map_err(|source| se_secret_store::Error::Generic {
                    store: "closing",
                    source: source.into(),
                })?;
        }

        Ok(())
    }

    async fn delete(&self, name: &SecretName) -> se_secret_store::Result<()> {
        if self.fail_delete {
            return Err(se_secret_store::Error::Generic {
                store: "closing",
                source: std::io::Error::other("secret deletion unavailable").into(),
            });
        }

        self.inner.delete(name).await
    }
}
