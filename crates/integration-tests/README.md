# Integration tests

This unpublished workspace crate owns cross-component tests against real object
stores and catalogs. Its library contains shared test fixtures, not production code.
Tests for a single component should remain in that component's crate.

## Shared setup

`TestEnvironment::from_env()` selects existing configurations using:

- `SE_CI_CATALOG_NAME`: catalog ID, such as `lakekeeper` (not `catalogs/lakekeeper`).
- `SE_CI_OBJECT_STORE`: object store ID, such as `ci-s3`.
- `SE_CI_KEY_VAULT_URL`: vault endpoint, such as `https://my-vault.vault.azure.net/`.

It reads the object store secret at `object-store/<id>` as JSON:

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
constructed using that store and the same secret store. The selected catalog is
loaded using the existing `catalog/<id>` secret format; it is not registered or
overwritten. Namespace creation and per-fixture isolation work as below.

## Key Vault authentication and provisioning

Locally, run `az login`; `DeveloperToolsCredential` uses your Azure CLI or Azure
Developer CLI identity. In GitHub Actions, `azure/login` with OIDC can establish
the CLI identity without a stored client secret. Alternatively, when
`AZURE_FEDERATED_TOKEN_FILE` is set, the fixture uses `WorkloadIdentityCredential`
with that assertion file plus `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`. The token
file must be supplied and refreshed by the CI environment; the SDK does not fetch
GitHub's OIDC token itself. A configured but invalid federated identity fails
rather than falling back to a developer identity.

The identity needs permission to read secret values, for example the `Key Vault
Secrets User` role scoped to the test vault. Provisioning/writing secrets requires
separate write permissions. No vault, permissions, or secrets are provisioned by
the fixture.

`AzureKeyVaultSecretStore` maps logical secret names to valid vault names by
retaining lowercase ASCII letters and digits and escaping every other UTF-8 byte
as `-xx` (lowercase hex). Thus:

| Logical name | Key Vault name |
| --- | --- |
| `catalog/lakekeeper` | `catalog-2flakekeeper` |
| `object-store/ci-s3` | `object-2dstore-2fci-2ds3` |

Encoded names must fit Azure's 127-character limit. Uppercase letters and hyphens
are escaped too, avoiding collisions in Azure's case-insensitive namespace.

Provision JSON as an ordinary secret string with no special content type. Reads
return its UTF-8 bytes unchanged. Writes through the `SecretStore` trait use base64
and content type `application/vnd.storage-engine.secret+base64`, which the adapter
decodes on read. Do not set that content type on a plain JSON secret. Deletion is
Azure soft-delete, not purge; recreating a deleted name can require recovery.

Missing environment variables, missing secrets, invalid configuration, and Azure
authentication/permission failures return errors rather than skipping tests.

## Per-test isolation

Each fixture generates one namespace-safe identifier, `test_<uuid-without-hyphens>`.
That exact identifier is both its engine storage prefix and a namespace created
in the shared catalog. All test tables belong to that namespace. For example:

```text
Engine storage: <configured-root>/test_7f3a19e17f9b42dcaac991ca3765fe42/
Catalog namespace: test_7f3a19e17f9b42dcaac991ca3765fe42
Catalog table: test_7f3a19e17f9b42dcaac991ca3765fe42.events
```

The fixture loads catalog configuration from Key Vault. Its ingestor and
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
just test -p se-integration-tests
cargo test -p se-integration-tests --test compatibility
cargo nextest run -p se-integration-tests --test compatibility
```

Provider provisioning, catalog fault decorators, and warehouse operator factories
are intentionally left to the scenarios that first need them.
