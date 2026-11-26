mod common;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common::TestEnvironment;
use se_metadata::{
    CatalogConfig, CatalogConfigUpdate, CatalogId, Error, Metadata, RestCatalogConfig,
    RestCatalogUpdate,
};
use se_secret_store::{
    Secret, SecretName, SecretStore,
    testing::{MockSecretStore, SecretOperation},
};
use slatedb::Db;
use ulid::Ulid;
use uuid::Uuid;

struct CloseDbOnPut {
    inner: Arc<MockSecretStore>,
    db: Db,
}

#[tokio::test]
async fn creates_gets_and_stores_catalog_under_ulid_secret()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        secrets,
        clock,
        ..
    } = TestEnvironment::with_time(123_456_789).await?;

    let first = CatalogId::from(Uuid::from_u128(1));
    let second = CatalogId::from(Uuid::from_u128(2));
    let first_config = catalog_config(&server.uri_with_prefix("first"));
    let second_config = catalog_config(&server.uri_with_prefix("second"));

    insta::assert_debug_snapshot!(metadata.get_catalog(&first).await?, @"None");

    metadata
        .create_catalog(first.clone(), first_config.clone())
        .await?;

    let first_secret = single_secret_name(&secrets);
    assert_ne!(first_secret.as_str(), first.to_string());
    insta::assert_debug_snapshot!(
        Ulid::from_string(first_secret.as_str())?.timestamp_ms(),
        @"123456789"
    );
    assert_eq!(
        serde_json::from_str::<CatalogConfig>(&secrets.value(&first_secret).unwrap())?,
        first_config
    );

    clock.set(987_654_321);
    metadata
        .create_catalog(second.clone(), second_config.clone())
        .await?;

    let second_secret = secrets
        .names()
        .into_iter()
        .find(|name| name != &first_secret)
        .ok_or("missing second secret")?;
    insta::assert_debug_snapshot!(
        Ulid::from_string(second_secret.as_str())?.timestamp_ms(),
        @"987654321"
    );

    assert_eq!(
        metadata.get_catalog(&first).await?,
        Some(first_config.clone())
    );
    assert_eq!(
        metadata.get_catalog(&second).await?,
        Some(second_config.clone())
    );

    // Duplicate create is rejected without overwriting the existing catalog.
    let error = metadata
        .create_catalog(first.clone(), second_config)
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        error,
        @"catalog already exists: 00000000-0000-0000-0000-000000000001"
    );
    assert_eq!(metadata.get_catalog(&first).await?, Some(first_config));
    insta::assert_debug_snapshot!(secrets.len(), @"2");

    // Missing backing secret surfaces a SecretStore::NotFound error.
    secrets.remove(&first_secret);
    let error = metadata.get_catalog(&first).await.unwrap_err();
    assert!(matches!(
        error,
        Error::SecretStore(se_secret_store::Error::NotFound(_))
    ));

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn updates_catalog_properties_and_rotates_secret() -> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        secrets,
        clock,
        ..
    } = TestEnvironment::with_time(123).await?;

    let id = CatalogId::from(Uuid::from_u128(1));
    let neighbor = CatalogId::from(Uuid::from_u128(2));
    let original = CatalogConfig::Rest(RestCatalogConfig {
        uri: server.uri_with_prefix("original"),
        warehouse: Some("warehouse-one".into()),
        properties: HashMap::from([
            ("credential".into(), "test-credential".into()),
            ("scope".into(), "old-scope".into()),
            ("stale".into(), "remove-me".into()),
        ]),
    });
    let update = CatalogConfigUpdate::Rest(RestCatalogUpdate {
        properties: HashMap::from([
            ("scope".into(), Some("new-scope".into())),
            ("header.x-custom".into(), Some("value".into())),
            ("stale".into(), None),
            ("absent".into(), None),
        ]),
    });
    let expected = CatalogConfig::Rest(RestCatalogConfig {
        uri: server.uri_with_prefix("original"),
        warehouse: Some("warehouse-one".into()),
        properties: HashMap::from([
            ("credential".into(), "test-credential".into()),
            ("scope".into(), "new-scope".into()),
            ("header.x-custom".into(), "value".into()),
        ]),
    });
    let neighbor_config = catalog_config(&server.uri_with_prefix("neighbor"));

    // Updating a missing catalog returns CatalogNotFound.
    let error = metadata
        .update_catalog(&id, update.clone())
        .await
        .unwrap_err();
    insta::assert_snapshot!(
        error,
        @"catalog not found: 00000000-0000-0000-0000-000000000001"
    );

    metadata
        .create_catalog(id.clone(), original.clone())
        .await?;
    let old_secret = single_secret_name(&secrets);

    metadata
        .create_catalog(neighbor.clone(), neighbor_config.clone())
        .await?;

    clock.set(456);
    metadata.update_catalog(&id, update).await?;

    assert_eq!(metadata.get_catalog(&id).await?, Some(expected.clone()));
    assert_eq!(
        metadata.get_catalog(&neighbor).await?,
        Some(neighbor_config)
    );
    assert!(!secrets.contains(&old_secret));
    insta::assert_debug_snapshot!(secrets.len(), @"2");

    let rotated_secret = secrets
        .names()
        .into_iter()
        .find(|name| {
            Ulid::from_string(name.as_str())
                .map(|ulid| ulid.timestamp_ms() == 456)
                .unwrap_or(false)
        })
        .ok_or("missing rotated secret")?;
    assert_eq!(
        serde_json::from_str::<CatalogConfig>(&secrets.value(&rotated_secret).unwrap())?,
        expected
    );

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn noop_update_probes_catalog_without_rotating_secret()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        secrets,
        clock,
        ..
    } = TestEnvironment::with_time(123).await?;

    let id = CatalogId::from(Uuid::from_u128(1));
    let config = CatalogConfig::Rest(RestCatalogConfig {
        uri: server.uri(),
        warehouse: Some("warehouse-one".into()),
        properties: HashMap::from([("scope".into(), "current".into())]),
    });

    metadata.create_catalog(id.clone(), config.clone()).await?;
    let secret_before = single_secret_name(&secrets);
    secrets.clear_operations();

    clock.set(789);
    metadata
        .update_catalog(
            &id,
            CatalogConfigUpdate::Rest(RestCatalogUpdate {
                properties: HashMap::from([
                    ("scope".into(), Some("current".into())),
                    ("missing".into(), None),
                ]),
            }),
        )
        .await?;

    insta::assert_debug_snapshot!(server.config_probes(), @"2");
    assert_eq!(secrets.names(), vec![secret_before.clone()]);
    assert_eq!(
        secrets.operations(),
        vec![SecretOperation::Get(secret_before)]
    );
    assert_eq!(metadata.get_catalog(&id).await?, Some(config));

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn deletes_catalog_idempotently_and_allows_recreation()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        secrets,
        ..
    } = TestEnvironment::new().await?;

    let id = CatalogId::from(Uuid::from_u128(1));
    let neighbor = CatalogId::from(Uuid::from_u128(2));
    let config = catalog_config(&server.uri_with_prefix("first"));
    let neighbor_config = catalog_config(&server.uri_with_prefix("neighbor"));

    // Deleting a non-existent catalog is a no-op.
    metadata.delete_catalog(&id).await?;
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

    metadata.create_catalog(id.clone(), config.clone()).await?;
    let secret_name = single_secret_name(&secrets);
    metadata
        .create_catalog(neighbor.clone(), neighbor_config.clone())
        .await?;

    metadata.delete_catalog(&id).await?;
    metadata.delete_catalog(&id).await?;

    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");
    assert!(!secrets.contains(&secret_name));
    insta::assert_debug_snapshot!(secrets.len(), @"1");
    assert_eq!(
        metadata.get_catalog(&neighbor).await?,
        Some(neighbor_config)
    );

    metadata.create_catalog(id.clone(), config.clone()).await?;
    assert_eq!(metadata.get_catalog(&id).await?, Some(config));

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn validates_reserved_properties_and_probes_on_create_and_update()
-> Result<(), Box<dyn std::error::Error>> {
    let TestEnvironment {
        metadata,
        server,
        secrets,
        ..
    } = TestEnvironment::new().await?;
    server.require_header("x-api-key", "good-key");
    let id = CatalogId::from(Uuid::from_u128(1));

    // Reserved keys are rejected on create before probing.
    let mut create_errors = Vec::new();
    for key in ["uri", "warehouse"] {
        let error = metadata
            .create_catalog(
                id.clone(),
                CatalogConfig::Rest(RestCatalogConfig {
                    uri: server.uri(),
                    warehouse: None,
                    properties: HashMap::from([(key.into(), "disallowed".into())]),
                }),
            )
            .await
            .unwrap_err();
        create_errors.push(error.to_string());
    }
    insta::assert_debug_snapshot!(create_errors, @r#"
    [
        "catalog property \"uri\" cannot be set in properties",
        "catalog property \"warehouse\" cannot be set in properties",
    ]
    "#);
    insta::assert_debug_snapshot!(server.config_probes(), @"0");

    // Failed probe prevents creating the catalog or writing a secret.
    let bad_config = CatalogConfig::Rest(RestCatalogConfig {
        uri: server.uri(),
        warehouse: Some("warehouse-one".into()),
        properties: HashMap::from([("header.x-api-key".into(), "wrong-key".into())]),
    });
    let error = metadata.create_catalog(id.clone(), bad_config).await;

    assert!(matches!(error, Err(Error::CatalogProbeFailed(_))));
    insta::assert_debug_snapshot!(server.config_probes(), @"1");
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");
    assert!(secrets.is_empty());

    // Valid config passes probe and is persisted.
    let good_config = CatalogConfig::Rest(RestCatalogConfig {
        uri: server.uri(),
        warehouse: Some("warehouse-one".into()),
        properties: HashMap::from([("header.x-api-key".into(), "good-key".into())]),
    });
    metadata
        .create_catalog(id.clone(), good_config.clone())
        .await?;
    insta::assert_debug_snapshot!(server.config_probes(), @"2");

    // Reserved keys are rejected on update (both set and delete) before probing.
    let mut update_errors = Vec::new();
    for (key, value) in [
        ("uri", Some("https://other.example".into())),
        ("uri", None),
        ("warehouse", Some("other".into())),
        ("warehouse", None),
    ] {
        let error = metadata
            .update_catalog(
                &id,
                CatalogConfigUpdate::Rest(RestCatalogUpdate {
                    properties: HashMap::from([(key.into(), value)]),
                }),
            )
            .await
            .unwrap_err();
        update_errors.push(error.to_string());
    }
    insta::assert_debug_snapshot!(update_errors, @r#"
    [
        "catalog property \"uri\" cannot be set in properties",
        "catalog property \"uri\" cannot be set in properties",
        "catalog property \"warehouse\" cannot be set in properties",
        "catalog property \"warehouse\" cannot be set in properties",
    ]
    "#);
    insta::assert_debug_snapshot!(server.config_probes(), @"2");

    // Failed probe on update leaves the existing catalog intact.
    let error = metadata
        .update_catalog(
            &id,
            CatalogConfigUpdate::Rest(RestCatalogUpdate {
                properties: HashMap::from([("header.x-api-key".into(), Some("wrong-key".into()))]),
            }),
        )
        .await;

    assert!(matches!(error, Err(Error::CatalogProbeFailed(_))));
    insta::assert_debug_snapshot!(server.config_probes(), @"3");
    assert_eq!(metadata.get_catalog(&id).await?, Some(good_config));

    // Rotating to a new valid key succeeds while preserving uri and warehouse.
    server.require_header("x-api-key", "rotated-key");
    metadata
        .update_catalog(
            &id,
            CatalogConfigUpdate::Rest(RestCatalogUpdate {
                properties: HashMap::from([(
                    "header.x-api-key".into(),
                    Some("rotated-key".into()),
                )]),
            }),
        )
        .await?;

    assert_eq!(
        metadata.get_catalog(&id).await?,
        Some(CatalogConfig::Rest(RestCatalogConfig {
            uri: server.uri(),
            warehouse: Some("warehouse-one".into()),
            properties: HashMap::from([("header.x-api-key".into(), "rotated-key".into())]),
        }))
    );

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn concurrent_creates_do_not_overwrite_the_winner() -> Result<(), Box<dyn std::error::Error>>
{
    let TestEnvironment {
        metadata, server, ..
    } = TestEnvironment::new().await?;
    let id = CatalogId::from(Uuid::from_u128(1));
    let first_config = catalog_config(&server.uri_with_prefix("first"));
    let second_config = catalog_config(&server.uri_with_prefix("second"));

    let (first, second) = tokio::join!(
        metadata.create_catalog(id.clone(), first_config.clone()),
        metadata.create_catalog(id.clone(), second_config.clone()),
    );

    insta::assert_debug_snapshot!(usize::from(first.is_ok()) + usize::from(second.is_ok()), @"1");

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
    let TestEnvironment {
        metadata,
        server,
        secrets,
        ..
    } = TestEnvironment::new().await?;
    secrets.fail_puts(true);
    let id = CatalogId::from(Uuid::from_u128(1));

    let error = metadata
        .create_catalog(id.clone(), catalog_config(&server.uri()))
        .await
        .unwrap_err();

    insta::assert_snapshot!(error, @"secret store error: mock store error: secret write unavailable");
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

    metadata.close().await?;

    Ok(())
}

#[tokio::test]
async fn database_failure_cleans_up_staged_secret_and_preserves_original_error()
-> Result<(), Box<dyn std::error::Error>> {
    for (update, fail_delete) in [(false, false), (false, true), (true, false), (true, true)] {
        let TestEnvironment {
            store,
            db,
            secrets,
            clock,
            server,
            metadata: setup_metadata,
        } = TestEnvironment::new().await?;
        let id = CatalogId::from(Uuid::from_u128(1));
        let original = catalog_config(&server.uri());

        if update {
            setup_metadata
                .create_catalog(id.clone(), original.clone())
                .await?;
        }

        secrets.fail_deletes(fail_delete);
        let faulty = Arc::new(CloseDbOnPut {
            inner: secrets.clone(),
            db: db.clone(),
        });
        let metadata = Metadata::from_db(db, faulty, clock.clone());

        let result = if update {
            metadata
                .update_catalog(
                    &id,
                    CatalogConfigUpdate::Rest(RestCatalogUpdate {
                        properties: HashMap::from([("scope".into(), Some("updated".into()))]),
                    }),
                )
                .await
        } else {
            metadata.create_catalog(id.clone(), original.clone()).await
        };

        assert!(matches!(result, Err(Error::SlateDb(_))));
        assert_eq!(
            secrets.len(),
            usize::from(update) + usize::from(fail_delete)
        );

        let reopened = Metadata::from_db(Db::open("metadata", store).await?, secrets, clock);
        assert_eq!(reopened.get_catalog(&id).await?, update.then_some(original));
        reopened.close().await?;
    }

    Ok(())
}

#[tokio::test]
async fn cleanup_errors_leave_the_committed_catalog_state() -> Result<(), Box<dyn std::error::Error>>
{
    let TestEnvironment {
        metadata,
        server,
        secrets,
        ..
    } = TestEnvironment::new().await?;
    secrets.fail_deletes(true);
    let id = CatalogId::from(Uuid::from_u128(1));

    metadata
        .create_catalog(id.clone(), catalog_config(&server.uri()))
        .await?;

    let update = CatalogConfigUpdate::Rest(RestCatalogUpdate {
        properties: HashMap::from([("scope".into(), Some("new-scope".into()))]),
    });
    let expected = CatalogConfig::Rest(RestCatalogConfig {
        uri: server.uri(),
        warehouse: Some("warehouse-one".into()),
        properties: HashMap::from([
            ("credential".into(), "test-credential".into()),
            ("scope".into(), "new-scope".into()),
        ]),
    });

    let result = metadata.update_catalog(&id, update).await.unwrap_err();
    insta::assert_snapshot!(result, @"secret store error: mock store error: secret deletion unavailable");
    assert_eq!(metadata.get_catalog(&id).await?, Some(expected));

    let result = metadata.delete_catalog(&id).await.unwrap_err();
    insta::assert_snapshot!(result, @"secret store error: mock store error: secret deletion unavailable");
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

    // Repeating delete once the mapping is gone succeeds without calling delete again.
    metadata.delete_catalog(&id).await?;
    insta::assert_debug_snapshot!(metadata.get_catalog(&id).await?, @"None");

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

fn single_secret_name(secrets: &MockSecretStore) -> SecretName {
    let names = secrets.names();
    assert_eq!(names.len(), 1);
    names.into_iter().next().unwrap()
}

impl std::fmt::Debug for CloseDbOnPut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloseDbOnPut").finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretStore for CloseDbOnPut {
    async fn get(&self, name: &SecretName) -> se_secret_store::Result<Secret> {
        self.inner.get(name).await
    }

    async fn put(&self, name: &SecretName, value: String) -> se_secret_store::Result<()> {
        self.inner.put(name, value).await?;
        self.db
            .close()
            .await
            .map_err(|source| se_secret_store::Error::Generic {
                store: "closing",
                source: source.into(),
            })
    }

    async fn delete(&self, name: &SecretName) -> se_secret_store::Result<()> {
        self.inner.delete(name).await
    }
}
