//! Arrow Flight SQL query, metadata discovery, and ingestion service.

mod ingestion;
mod permit;
mod query;
mod stream;

use std::{collections::HashSet, sync::Arc};

use arrow_array::RecordBatch;
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService as ArrowFlightService, FlightServiceServer},
    sql::{
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables,
        CommandStatementQuery, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use arrow_schema::Schema;
use iceberg::{Catalog, NamespaceIdent, arrow::schema_to_arrow_schema, io::StorageFactory};
pub use ingestion::IngestionOptions;
use object_store::ObjectStore;
use se_common::{clock::SystemClock, rand::SeRand};
use se_ingestion::Ingestor;
use se_metadata_client::{CatalogConfig, CatalogId, MetadataClient};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{Span, instrument};

use crate::{permit::IngestionSemaphore, stream::TracedStream};

const SQL_CATALOG: &str = "default";

#[derive(Clone)]
pub struct FlightService {
    metadata: Arc<dyn MetadataClient>,
    storage_factory: Arc<dyn StorageFactory>,
    ingestor: Ingestor,
    ct: CancellationToken,
    options: IngestionOptions,
    global_memory: Arc<Semaphore>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryMetadata {
    catalog: String,
}

/// Builds a Flight SQL endpoint with WAL-backed ingestion.
pub fn service(
    metadata: Arc<dyn MetadataClient>,
    storage_factory: Arc<dyn StorageFactory>,
    object_store: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
    rng: Arc<SeRand>,
    ct: CancellationToken,
) -> FlightServiceServer<FlightService> {
    FlightServiceServer::new(FlightService::new(
        metadata,
        storage_factory,
        object_store,
        clock,
        rng,
        ct,
    ))
}

impl FlightService {
    /// Creates a Flight SQL service using remote metadata and WAL-backed ingestion.
    pub fn new(
        metadata: Arc<dyn MetadataClient>,
        storage_factory: Arc<dyn StorageFactory>,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn SystemClock>,
        rng: Arc<SeRand>,
        ct: CancellationToken,
    ) -> Self {
        let options = IngestionOptions::default();
        let global_memory = IngestionSemaphore::global_memory(&options);
        let ingestor = Ingestor::new(metadata.clone(), object_store, clock, rng);

        Self {
            metadata,
            storage_factory,
            ingestor,
            ct,
            options,
            global_memory,
        }
    }

    /// Flushes and closes the service's ingestion producers.
    pub async fn close(&self) -> se_ingestion::Result<()> {
        self.ingestor.close().await
    }

    async fn ingest(
        &self,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoPutStream>, Status> {
        let output = ingestion::ingest(
            request.into_inner(),
            &self.ingestor,
            &self.options,
            Arc::clone(&self.global_memory),
            &self.ct,
        )
        .await?;
        Ok(Response::new(Box::pin(TracedStream::new(output))))
    }

    async fn get_catalog<T>(
        &self,
        request: &Request<T>,
    ) -> Result<(CatalogId, CatalogConfig), Status> {
        let metadata = request
            .metadata()
            .get("query-metadata")
            .ok_or_else(|| Status::invalid_argument("query-metadata header is required"))?;
        let metadata: QueryMetadata = serde_json::from_slice(metadata.as_encoded_bytes())
            .map_err(|_| Status::invalid_argument("invalid query-metadata header"))?;
        let id = CatalogId::new(metadata.catalog)
            .map_err(|_| Status::invalid_argument("invalid catalog ID"))?;

        Span::current().record("catalog", tracing::field::display(&id));

        let config = self
            .metadata
            .get_catalog(&id)
            .await
            .map_err(|_| Status::internal("failed to load catalog"))?
            .ok_or_else(|| Status::not_found("catalog not found"))?;

        Ok((id, config))
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightService {
    type FlightService = Self;

    #[instrument(skip_all, fields(catalog, query_bytes = command.query.len(), transaction = command.transaction_id.is_some()), err)]
    async fn get_flight_info_statement(
        &self,
        command: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (id, config) = self.get_catalog(&request).await?;
        query::flight_info(
            id,
            config,
            self.storage_factory.clone(),
            command,
            request.into_inner(),
            &self.ct,
        )
        .await
    }

    #[instrument(skip_all, fields(catalog, response_messages), err)]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        let (id, config) = self.get_catalog(&request).await?;
        query::do_get(id, config, self.storage_factory.clone(), ticket, &self.ct).await
    }

    #[instrument(skip_all, fields(catalog), err)]
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.get_catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    #[instrument(skip_all, fields(catalog, requested_catalog = ?query.catalog, schema_filter = ?query.db_schema_filter_pattern), err)]
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.get_catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    #[instrument(skip_all, fields(catalog, requested_catalog = ?query.catalog, schema_filter = ?query.db_schema_filter_pattern, table_filter = ?query.table_name_filter_pattern, table_types = ?query.table_types, include_schema = query.include_schema), err)]
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.get_catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    #[instrument(skip_all, fields(catalog), err)]
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.get_catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    #[instrument(skip_all, fields(catalog, response_messages), err)]
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        self.get_catalog(&request).await?;
        let mut builder = query.into_builder();
        builder.append(SQL_CATALOG);
        batch_response(builder.build().map_err(metadata_status)?)
    }

    #[instrument(skip_all, fields(catalog, requested_catalog = ?query.catalog, schema_filter = ?query.db_schema_filter_pattern, response_messages), err)]
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        let (id, config) = self.get_catalog(&request).await?;
        let catalog = config
            .to_catalog(&id, self.storage_factory.clone())
            .await
            .map_err(|_| Status::internal("failed to connect to catalog"))?;

        let mut builder = query.into_builder();
        for namespace in get_all_namespaces(catalog.as_ref())
            .await
            .map_err(iceberg_status)?
        {
            builder.append(SQL_CATALOG, namespace.join("."));
        }

        batch_response(builder.build().map_err(metadata_status)?)
    }

    #[instrument(skip_all, fields(catalog, requested_catalog = ?query.catalog, schema_filter = ?query.db_schema_filter_pattern, table_filter = ?query.table_name_filter_pattern, table_types = ?query.table_types, include_schema = query.include_schema, response_messages), err)]
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        let (id, config) = self.get_catalog(&request).await?;
        let catalog = config
            .to_catalog(&id, self.storage_factory.clone())
            .await
            .map_err(|_| Status::internal("failed to connect to catalog"))?;

        let mut builder = query.into_builder();
        for namespace in get_all_namespaces(catalog.as_ref())
            .await
            .map_err(iceberg_status)?
        {
            for ident in catalog
                .list_tables(&namespace)
                .await
                .map_err(iceberg_status)?
            {
                let schema = if builder.include_schema() {
                    let table = catalog.load_table(&ident).await.map_err(iceberg_status)?;
                    schema_to_arrow_schema(table.metadata().current_schema())
                        .map_err(iceberg_status)?
                } else {
                    Schema::empty()
                };

                builder
                    .append(
                        SQL_CATALOG,
                        namespace.join("."),
                        ident.name,
                        "TABLE",
                        &schema,
                    )
                    .map_err(metadata_status)?;
            }
        }

        batch_response(builder.build().map_err(metadata_status)?)
    }

    #[instrument(skip_all, fields(catalog, response_messages), err)]
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        self.get_catalog(&request).await?;
        let mut builder = query.into_builder();
        builder.append("TABLE");
        batch_response(builder.build().map_err(metadata_status)?)
    }

    #[instrument(skip_all, fields(command_type = %_message.type_url, catalog, namespace, table, request_id, response_messages), err)]
    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        _message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self as ArrowFlightService>::DoPutStream>, Status> {
        self.ingest(request).await
    }

    #[instrument(skip_all, fields(sql_info_id = _id))]
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

fn flight_info(
    descriptor: FlightDescriptor,
    schema: &Schema,
) -> Result<Response<FlightInfo>, Status> {
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(descriptor.cmd.clone()));
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|_| Status::internal("failed to encode metadata schema"))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint);
    Ok(Response::new(info))
}

fn batch_response(
    batch: RecordBatch,
) -> Result<Response<<FlightService as ArrowFlightService>::DoGetStream>, Status> {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(batch.schema())
        .build(tokio_stream::iter([Ok(batch)]))
        .map(|result| result.map_err(|_| Status::internal("failed to encode metadata")));
    Ok(Response::new(Box::pin(TracedStream::new(stream))))
}

#[instrument(skip_all, err(Debug))]
async fn get_all_namespaces(catalog: &dyn Catalog) -> iceberg::Result<Vec<NamespaceIdent>> {
    let mut pending = catalog.list_namespaces(None).await?;
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    while let Some(namespace) = pending.pop() {
        if !seen.insert(namespace.clone()) {
            continue;
        }
        pending.extend(catalog.list_namespaces(Some(&namespace)).await?);
        result.push(namespace);
    }
    Ok(result)
}

fn iceberg_status(_error: iceberg::Error) -> Status {
    // Backend errors can contain credentials and must not be returned to clients.
    Status::internal("failed to read catalog metadata")
}

fn metadata_status(_error: arrow_flight::error::FlightError) -> Status {
    Status::internal("failed to build metadata response")
}
