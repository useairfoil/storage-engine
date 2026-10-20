# Integration tests

This unpublished workspace crate owns cross-component tests against real object
stores and catalogs. Its library contains shared test fixtures, not production code.
Tests for a single component should remain in that component's crate.

## Shared setup

`TestEnvironment::from_env()` reads three required environment variables:

- `SE_CI_CATALOG_NAME`: catalog ID, such as `lakekeeper` (not `catalogs/lakekeeper`).
- `SE_CI_CATALOG_CONFIG`: catalog configuration as a JSON string.
- `SE_CI_OBJECT_STORE_CONFIG`: object store configuration as a JSON string.

The object store configuration has this shape:

```json
{
  "scheme": "s3",
  "options": {
    "bucket": "integration-tests",
    "region": "eu-west-1"
  }
}
```

`options` are OpenDAL service configuration keys and string values, including
credentials when needed. The scheme must be enabled in the workspace's OpenDAL
build. The operator is adapted to an `ObjectStore`, then the catalog store is
constructed using that store and an in-memory secret store. The catalog JSON is
inserted into the secret store under `catalog/<SE_CI_CATALOG_NAME>`, using the
existing `CatalogConfig` format:

```json
{
  "rest": {
    "uri": "https://catalog.example.com",
    "warehouse": "test-warehouse",
    "properties": {}
  }
}
```

`uri` is required; `warehouse` and `properties` are optional. Include catalog
authentication settings in `properties` and object store credentials in `options`
as needed. Providers using ambient authentication may require additional
provider-specific environment variables.

## SecretSpec setup

The crate's `secretspec.toml` declares these variables in a separate SecretSpec
project, `storage-engine-integration-tests`. Configure their values in your
chosen SecretSpec provider. `just test-integration` runs SecretSpec with this
manifest to inject the variables before launching the tests. SecretSpec handles
its provider's authentication; the Rust fixture does not access Key Vault or
require `SE_CI_KEY_VAULT_URL` or `SE_CI_OBJECT_STORE`.

Missing environment variables, invalid configuration, and provider authentication
or permission failures return errors rather than skipping tests.

## Per-test isolation

Each fixture generates one namespace-safe identifier, `test_<uuid-without-hyphens>`.
That exact identifier is both its engine storage prefix and a namespace created
in the shared catalog. All test tables belong to that namespace. For example:

```text
Engine storage: <configured-root>/test_7f3a19e17f9b42dcaac991ca3765fe42/
Catalog namespace: test_7f3a19e17f9b42dcaac991ca3765fe42
Catalog table: test_7f3a19e17f9b42dcaac991ca3765fe42.events
```

The fixture loads catalog configuration from its in-memory secret store. Its ingestor and
reconciler share the same catalog store; configuration is not written to the
engine object store. The real catalog and its warehouse must already exist;
setup contacts the catalog and fails if namespace creation fails.
Tests own table provisioning, credentials, and cleanup. Current production
catalog/warehouse support still applies; this fixture adds no new providers.

For a test table:

1. Construct a `TestEnvironment`.
2. Create a table in `environment.namespace` in the real catalog. Use
   `environment.table_ident("events")` to get its fully qualified identifier.
3. Call `environment.ingestor.for_table` with `environment.catalog_name` and the
   table's namespace/name. This links the table into the engine if needed.
4. Call `environment.reconciler.for_table` for the same table.
5. Write batches through the table ingestor and call
   `table_reconciler.run_reconciliation()` to materialize and publish them.
6. Close `environment.ingestor`, then call
   `table_reconciler.table().clone().flush_and_close()` for each opened table.

The fixture exposes its rooted `object_store` and `catalog_store` so tests can
recreate components against the same persisted engine state and catalog configuration.
Constructing a new `TestEnvironment` instead creates a fresh prefix and namespace.
Remote data is never deleted automatically; `storage_prefix` identifies both engine artifacts
and the catalog namespace for diagnosis/cleanup. Dropping catalog tables does not
necessarily delete their warehouse data; follow the catalog's purge semantics.

## Suites

The compatibility suite creates an unpartitioned table, ingests one batch, waits
for its durable write acknowledgement, and runs reconciliation. The test expects
the sequence and component shutdown to succeed without errors.

Add further compatibility scenarios alongside `tests/compatibility/main.rs`, and
failure scenarios under `tests/recovery/main.rs` when needed. Run each suite
against one explicitly configured environment per CI matrix job, rather
than looping through providers within a test. Missing required configuration must
fail setup, not silently skip tests.

```sh
just check -p se-integration-tests
just test-integration
just test-integration --test compatibility
# If the three variables are already exported, SecretSpec can be bypassed:
cargo nextest run -p se-integration-tests --profile integration --test compatibility
```

Provider provisioning, catalog fault decorators, and warehouse operator factories
are intentionally left to the scenarios that first need them.
