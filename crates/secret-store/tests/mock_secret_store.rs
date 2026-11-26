use se_secret_store::{SecretName, SecretStore, testing::MockSecretStore};

#[tokio::test]
async fn stores_inspects_and_records_secret_operations() -> Result<(), Box<dyn std::error::Error>> {
    let store = MockSecretStore::new();
    let first = SecretName::new("alpha")?;
    let second = SecretName::new("beta")?;

    assert!(store.is_empty());

    store.put(&first, "one".into()).await?;
    store.put(&second, "two".into()).await?;

    insta::assert_debug_snapshot!(store.len(), @"2");
    assert!(store.contains(&first));
    insta::assert_debug_snapshot!(store.value(&first), @r#"
    Some(
        "one",
    )
    "#);
    insta::assert_snapshot!(store.get(&second).await?.value(), @"two");
    insta::assert_debug_snapshot!(store.names(), @r#"
    [
        SecretName(
            "alpha",
        ),
        SecretName(
            "beta",
        ),
    ]
    "#);

    store.delete(&first).await?;
    assert!(!store.contains(&first));
    insta::assert_snapshot!(store.get(&first).await.unwrap_err(), @"secret not found: alpha");

    insta::assert_debug_snapshot!(store.operations(), @r#"
    [
        Put(
            SecretName(
                "alpha",
            ),
        ),
        Put(
            SecretName(
                "beta",
            ),
        ),
        Get(
            SecretName(
                "beta",
            ),
        ),
        Delete(
            SecretName(
                "alpha",
            ),
        ),
        Get(
            SecretName(
                "alpha",
            ),
        ),
    ]
    "#);

    store.clear_operations();
    assert!(store.operations().is_empty());

    Ok(())
}

#[tokio::test]
async fn injects_get_put_and_delete_failures() -> Result<(), Box<dyn std::error::Error>> {
    let store = MockSecretStore::new();
    let name = SecretName::new("secret")?;

    store.fail_puts(true);
    insta::assert_snapshot!(
        store.put(&name, "v1".into()).await.unwrap_err(),
        @"mock store error: secret write unavailable"
    );
    assert!(store.is_empty());

    store.fail_puts(false);
    store.insert(name.clone(), "v1");

    store.fail_gets(true);
    insta::assert_snapshot!(
        store.get(&name).await.unwrap_err(),
        @"mock store error: secret retrieval unavailable"
    );

    store.fail_gets(false);
    insta::assert_snapshot!(store.get(&name).await?.value(), @"v1");

    store.fail_deletes(true);
    insta::assert_snapshot!(
        store.delete(&name).await.unwrap_err(),
        @"mock store error: secret deletion unavailable"
    );
    assert!(store.contains(&name));

    store.fail_deletes(false);
    store.delete(&name).await?;
    assert!(store.is_empty());

    insta::assert_debug_snapshot!(store.operations(), @r#"
    [
        Put(
            SecretName(
                "secret",
            ),
        ),
        Get(
            SecretName(
                "secret",
            ),
        ),
        Get(
            SecretName(
                "secret",
            ),
        ),
        Delete(
            SecretName(
                "secret",
            ),
        ),
        Delete(
            SecretName(
                "secret",
            ),
        ),
    ]
    "#);

    Ok(())
}
