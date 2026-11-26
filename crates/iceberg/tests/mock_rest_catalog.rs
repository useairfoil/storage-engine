use std::{collections::HashMap, sync::Arc};

use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCommit, TableIdent, TableRequirement,
    TableUpdate, spec::FormatVersion,
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use se_iceberg::{
    storage::ObjectStoreStorageFactory,
    testing::{CommitFault, MockRestCatalog, test_table},
};
use uuid::Uuid;

#[tokio::test]
async fn probes_config_oauth_headers_and_prefixes() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockRestCatalog::start().await?;
    server.require_header("x-api-key", "good-key");

    let bad = rest_catalog(
        &server.uri(),
        HashMap::from([("header.x-api-key".into(), "wrong-key".into())]),
    )
    .await?;
    assert!(bad.invalidate_token().await.is_err());
    insta::assert_debug_snapshot!(server.config_probes(), @"1");

    let good = rest_catalog(
        &server.uri_with_prefix("tenant-a"),
        HashMap::from([
            ("credential".into(), "client:secret".into()),
            ("header.x-api-key".into(), "good-key".into()),
        ]),
    )
    .await?;
    good.invalidate_token().await?;

    insta::assert_debug_snapshot!(server.config_probes(), @"2");
    insta::assert_debug_snapshot!(server.requests(), @r#"
    [
        Config {
            prefix: None,
        },
        OAuthToken {
            prefix: Some(
                "tenant-a",
            ),
        },
        Config {
            prefix: Some(
                "tenant-a",
            ),
        },
    ]
    "#);

    server.clear_requests();
    insta::assert_debug_snapshot!(server.requests(), @"[]");

    Ok(())
}

#[tokio::test]
async fn lists_nested_namespaces_and_tables() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockRestCatalog::start().await?;
    let nested_ns = NamespaceIdent::from_strs(["analytics", "daily"])?;
    let empty_ns = NamespaceIdent::new("staging".into());
    server.add_namespace(&empty_ns);

    let first = test_table(
        TableIdent::new(nested_ns.clone(), "events".into()),
        Uuid::from_u128(1),
        FormatVersion::V2,
        HashMap::new(),
    )?;
    let second = test_table(
        TableIdent::new(nested_ns.clone(), "clicks".into()),
        Uuid::from_u128(2),
        FormatVersion::V2,
        HashMap::new(),
    )?;
    server.put_table(first);
    server.put_table(second);

    let catalog = rest_catalog(&server.uri(), HashMap::new()).await?;

    let roots = catalog.list_namespaces(None).await?;
    insta::assert_debug_snapshot!(roots, @r#"
    [
        NamespaceIdent(
            [
                "analytics",
            ],
        ),
        NamespaceIdent(
            [
                "staging",
            ],
        ),
    ]
    "#);

    let children = catalog
        .list_namespaces(Some(&NamespaceIdent::new("analytics".into())))
        .await?;
    insta::assert_debug_snapshot!(children, @r#"
    [
        NamespaceIdent(
            [
                "analytics",
                "daily",
            ],
        ),
    ]
    "#);

    let tables = catalog.list_tables(&nested_ns).await?;
    insta::assert_debug_snapshot!(tables, @r#"
    [
        TableIdent {
            namespace: NamespaceIdent(
                [
                    "analytics",
                    "daily",
                ],
            ),
            name: "clicks",
        },
        TableIdent {
            namespace: NamespaceIdent(
                [
                    "analytics",
                    "daily",
                ],
            ),
            name: "events",
        },
    ]
    "#);

    let missing_ns = NamespaceIdent::new("missing".into());
    assert!(catalog.list_tables(&missing_ns).await.is_err());

    Ok(())
}

#[tokio::test]
async fn commits_tables_and_injects_commit_faults() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockRestCatalog::start().await?;
    let ident = TableIdent::from_strs(["test", "orders"])?;
    let table = test_table(
        ident.clone(),
        Uuid::from_u128(42),
        FormatVersion::V3,
        HashMap::from([("owner".into(), "initial".into())]),
    )?;
    server.put_table(table);

    let catalog = rest_catalog(&server.uri(), HashMap::new()).await?;

    let loaded = catalog.load_table(&ident).await?;
    insta::assert_snapshot!(
        loaded.metadata().uuid(),
        @"00000000-0000-0000-0000-00000000002a"
    );

    // Pre-apply fault returns an error without mutating catalog state.
    server.fail_next_commit(CommitFault::FailBeforeApply);
    let err = catalog
        .update_table(set_property_commit(&ident, Uuid::from_u128(42), "v1"))
        .await;
    assert!(err.is_err());
    insta::assert_debug_snapshot!(server.commit_count(), @"0");
    insta::assert_debug_snapshot!(
        server.table(&ident).unwrap().metadata().properties().get("owner"),
        @r#"
    Some(
        "initial",
    )
    "#
    );

    // Uncertain commit applies on the server, then returns an error to the client.
    server.fail_next_commit(CommitFault::ApplyThenFail);
    let err = catalog
        .update_table(set_property_commit(&ident, Uuid::from_u128(42), "v2"))
        .await;
    assert!(err.is_err());
    insta::assert_debug_snapshot!(server.commit_count(), @"1");
    insta::assert_debug_snapshot!(
        server.table(&ident).unwrap().metadata().properties().get("owner"),
        @r#"
    Some(
        "v2",
    )
    "#
    );

    // Requirement mismatch returns a conflict without mutating the table.
    let conflict = catalog
        .update_table(set_property_commit(&ident, Uuid::from_u128(999), "v3"))
        .await;
    assert!(conflict.is_err());
    insta::assert_debug_snapshot!(server.commit_count(), @"1");

    // Normal commit succeeds and updates the stored table.
    let updated = catalog
        .update_table(set_property_commit(&ident, Uuid::from_u128(42), "v4"))
        .await?;
    insta::assert_debug_snapshot!(server.commit_count(), @"2");
    insta::assert_debug_snapshot!(
        updated.metadata().properties().get("owner"),
        @r#"
    Some(
        "v4",
    )
    "#
    );

    insta::assert_debug_snapshot!(server.requests(), @r#"
    [
        Config {
            prefix: None,
        },
        LoadTable {
            prefix: None,
            ident: TableIdent {
                namespace: NamespaceIdent(
                    [
                        "test",
                    ],
                ),
                name: "orders",
            },
        },
        CommitTable {
            prefix: None,
            ident: TableIdent {
                namespace: NamespaceIdent(
                    [
                        "test",
                    ],
                ),
                name: "orders",
            },
            applied: false,
        },
        CommitTable {
            prefix: None,
            ident: TableIdent {
                namespace: NamespaceIdent(
                    [
                        "test",
                    ],
                ),
                name: "orders",
            },
            applied: true,
        },
        CommitTable {
            prefix: None,
            ident: TableIdent {
                namespace: NamespaceIdent(
                    [
                        "test",
                    ],
                ),
                name: "orders",
            },
            applied: false,
        },
        CommitTable {
            prefix: None,
            ident: TableIdent {
                namespace: NamespaceIdent(
                    [
                        "test",
                    ],
                ),
                name: "orders",
            },
            applied: true,
        },
    ]
    "#);

    Ok(())
}

async fn rest_catalog(
    uri: &str,
    mut properties: HashMap<String, String>,
) -> iceberg::Result<RestCatalog> {
    properties.insert(REST_CATALOG_PROP_URI.into(), uri.into());

    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(ObjectStoreStorageFactory))
        .load("mock", properties)
        .await
}

fn set_property_commit(ident: &TableIdent, uuid: Uuid, value: &str) -> TableCommit {
    TableCommit::builder()
        .ident(ident.clone())
        .requirements(vec![TableRequirement::UuidMatch { uuid }])
        .updates(vec![TableUpdate::SetProperties {
            updates: HashMap::from([("owner".into(), value.into())]),
        }])
        .build()
}
