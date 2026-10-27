# Airfoil Storage Engine

The Airfoil Storage Engine is a streaming storage engine for Apache Iceberg. It's designed to ingest high-volume data concurrently while keeping Iceberg's tables up-to-date with latency measured in seconds.

In a medallion architecture data lakehouse, the Airfoil Storage Engine is used for bronze (raw) and silver (cleaned, deduplicated) data.

The Airfoil Storage Engine supports two modes, configurable by table:

 - append: every write adds data to the table.
 - upsert: row values are merged so that only the latest value is stored, with no duplicates. In this mode, partial updates are supported.

A table is in upsert mode if it has one or more identifier field IDs. Changing a table type is not supported.

At the moment, the Airfoil Storage Engine _is not_ a general purpose storage engine. Data MUST go through the storage engine ingestion service so that it can be validated and added to the table.

### Features

 - Multi tenant: manage multiple Iceberg catalogs with a single storage engine cluster. Data from different catalogs is never mixed together.
 - Separate cluster and catalog storage: operational data is stored separately from the catalogs data.
 - Operationally simple: there is no stateful dependency other than object storage and a secret store. All components are stateless and can serve any tenant.
 - Apache Iceberg v3: support positional deletes with deletion vectors, semi structured data, and row lineage.
 - Rust + Arrow + DataFusion (RAD) stack: the Airfoil Storage Engine builds on a solid foundation.
 - Open Source: Apache 2.0 license. You can run it locally or in your cloud without any vendor lock-in.

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
