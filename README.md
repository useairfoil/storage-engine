# Storage Engine

Storage Engine is the streaming storage engine for Apache Iceberg. It's designed to ingest high-volume data concurrently while keeping Iceberg's metadata up-to-date.

In a medallion architecture data lakehouse, Storage Engine is used for bronze (raw) and silver (cleaned, deduplicated) data.

Storage Engine supports two modes, configurable by table:

 - entities: row values are merged so that only the latest value is stored, with no duplicates. In this mode, partial updates are supported.
 - append only: every write adds data to the table.

The ingestion API is being redesigned. The current HTTP server exposes health checks and catalog management only.

At the moment, Storage Engine _is not_ a general purpose storage engine. Data MUST go through the Storage Engine ingestion service so that it can be validated and added to the table.

### Features

 - Multi tenant: manage multiple Iceberg catalogs with a single Storage Engine cluster. Data from different tenants is never mixed together.
 - Separate cluster and catalog storage: operational data is stored separately from the catalogs data.
 - Operationally simple: there is no stateful dependency other than object storage. All components are stateless and can serve any tenant.
 - Apache Iceberg v3: support positional deletes with deletion vectors, semi structured data, and row lineage.
 - Rust + Arrow + DataFusion (RAD) stack: Storage Engine builds on a solid foundation.
 - Open Source: Apache 2.0 license. You can run it locally or in your cloud without any vendor lock-in.

### Roadmap

 - [ ] Entity tables with configurable primary key and version columns.
 - [ ] Append only tables.
 - [ ] Clustering and sorting by non-PK column.
 - [ ] Apache Iceberg v4.

## Getting Started

Run `cargo run --bin se -- dev` to start the HTTP server at
`http://127.0.0.1:7777`. Use `--server.address` or `SE_SERVER_ADDRESS` to
change the listen address. The storage service defaults to S3; set
`--opendal-service s3` or `SE_OPENDAL_SERVICE=s3` explicitly if needed. Configure
the bucket with `OPENDAL_S3_BUCKET` and optionally the endpoint with
`OPENDAL_S3_ENDPOINT`. OpenDAL reads standard `AWS_*` variables for credentials
and region. The file secret store uses that object store.

Send a SQL query through Arrow Flight SQL with:

```sh
cargo run --bin se -- sql --catalog-id warehouse 'SELECT * FROM default."analytics.events".page_views LIMIT 10'
cargo run --bin se -- sql --catalog-id warehouse 'DESCRIBE default."analytics.events".page_views'
```

Use `--server-address <url>` to override the default `http://127.0.0.1:7777`.
The command sends the selected catalog in the JSON `query-metadata` header and
prints results as a table. DataFusion executes read-only SQL against Iceberg
snapshots, including filters, projections, aggregates, joins, and limits. Reads
use S3 credentials supplied by the selected catalog, without falling back to
ambient AWS credentials. The selected catalog is exposed as `default`; nested
Iceberg namespaces are dot-separated schema names and must be quoted in SQL.
Use `DESCRIBE` for table schemas or Flight SQL's metadata APIs for discovery.
DataFusion's `information_schema` is enabled, supporting `SHOW TABLES`,
`SHOW COLUMNS`, and metadata queries. SQL mutations and transactions are not
implemented.

| Method | Path | Success response |
| --- | --- | --- |
| GET | `/health` | `200 OK` (liveness check, empty body) |
| POST | `/catalogs` | `201 Created`, catalog JSON and `Location` header |
| GET | `/catalogs/{id}` | `200 OK`, catalog JSON |
| DELETE | `/catalogs/{id}` | `204 No Content` (also when already absent) |
| GET | `/catalogs/{id}/v1/config` | `200 OK`, Iceberg REST client configuration |

Catalog configurations are persisted in the configured secret store, not a
database. There is intentionally no listing endpoint (`GET /catalogs` returns
`405 Method Not Allowed`).

```sh
curl -f http://127.0.0.1:7777/health
curl -f http://127.0.0.1:7777/catalogs \
  -H 'Content-Type: application/json' \
  -d '{"catalog_id":"example","config":{"type":"rest","uri":"http://localhost:8181"}}'
curl -f http://127.0.0.1:7777/catalogs/example
curl -f -X DELETE http://127.0.0.1:7777/catalogs/example
```

Create and get return `{"name":"catalogs/example","config":{"type":"rest","uri":"http://localhost:8181"}}`.
REST config also accepts optional `warehouse` and `properties` fields.
IDs use the existing catalog ID validation. Invalid IDs return `400`, missing
catalogs return `404`, and duplicate creation returns `409`. Invalid JSON returns
`400`, an invalid JSON schema returns `422`, and a missing JSON content type
returns `415`. Backend failures return `500` without exposing backend details.
The API has no authentication yet; keep it on a trusted interface, especially
because catalog properties may contain credentials.

### Iceberg REST catalog API

Each catalog has its own Iceberg REST base URI, for example
`http://127.0.0.1:7777/catalogs/example`. Currently only `GET /v1/config`
is implemented under that base URI:

```sh
curl -f 'http://127.0.0.1:7777/catalogs/example/v1/config?warehouse=example'
```

It returns `{"defaults":{},"overrides":{},"endpoints":[]}`. The optional
`warehouse` query parameter is accepted but ignored: the URL selects the catalog.
There are no client property defaults or overrides yet, and no namespace or table
operations are advertised. Upstream catalog connection properties and credentials
are not returned. Missing catalogs return `404`, invalid IDs return `400`, and
backend failures return `500`, using the Iceberg REST error response format.

Iceberg endpoint implementations live in `crates/server/src/iceberg/`, starting
with `config.rs`; namespace and table handlers will live alongside it as they are
implemented. Catalog-management endpoints remain separate.

```txt
storage-engine
├── bins
│   ├── storage-engine: the main binary (`se`).
│   └── stress: a stress testing tool (`se-stress`).
└── crates
    ├── common: common utilities shared across crates, e.g. DST.
    ├── ingestion: ingestion pipeline.
    ├── flight-server: Flight SQL ingestion and metadata discovery server.
    ├── meta-store: crate to interact with the metadata.
    ├── observability: utilities to set up observability.
    ├── secret-store: abstraction over secret stores (e.g. AWS Secrets Manager, HashiCorp Vault).
    └── server: Axum HTTP server for health checks and catalog management.
```

## Development

Enter the development environment with `nix develop`, which includes
`cargo-nextest`, then run:

```sh
just test                 # Default profile: excludes remote integration tests.
just test --profile ci    # CI profile: excludes remote integration tests.
just test-integration     # Integration profile: only remote integration tests.
just test-doc             # Doctests, which nextest does not run.
```

The integration profile requires Azure authentication and these environment
variables:

```sh
az login
export SE_CI_KEY_VAULT_URL="https://<vault-name>.vault.azure.net/"
export SE_CI_CATALOG_NAME="<catalog-name>"
export SE_CI_OBJECT_STORE="<object-store-id>"
```

The vault must already contain plain JSON values for `catalog/<catalog-name>`
and `object-store/<object-store-id>`, using the secret store's Azure name encoding.
Integration tests create real remote resources under unique namespaces and engine
storage prefixes; they do not delete those resources automatically.

Profiles are configured in `.config/nextest.toml`. They only affect nextest;
`cargo test --workspace` still runs remote integration tests.

## License

Copyright 2026 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
