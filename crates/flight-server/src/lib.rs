//! Arrow Flight SQL ingestion and metadata discovery service.

mod ingestion;
mod permit;
mod query;

use std::{collections::HashSet, sync::Arc};

use arrow_array::RecordBatch;
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService as ArrowFlightService, FlightServiceServer},
    sql::{
        Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables,
        CommandStatementQuery, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use arrow_schema::Schema;
use iceberg::{Catalog, NamespaceIdent, arrow::schema_to_arrow_schema};
pub use ingestion::IngestionOptions;
use se_ingestion::Ingestor;
use se_meta_store::catalog::{CatalogName, CatalogStore, StoredCatalog};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::permit::IngestionSemaphore;

const SQL_CATALOG: &str = "default";

#[derive(Debug, Clone)]
pub struct FlightService {
    catalogs: CatalogStore,
    ingestor: Ingestor,
    ct: CancellationToken,
    options: IngestionOptions,
    global_memory: Arc<Semaphore>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryMetadata {
    catalog: CatalogName,
}

/// Builds the shared ingestion and metadata discovery endpoint.
///
/// Discovery requires a `query-metadata` gRPC header containing JSON such as
/// `{"catalog":"catalogs/warehouse"}` on both GetFlightInfo and DoGet. SQL clients
/// see the selected catalog as `default`, and nested namespaces as dot-separated
/// schema names. GetTables with `include_schema` returns Arrow IPC table schemas.
/// DataFusion executes read-only SQL against Iceberg tables using catalog-vended
/// S3 credentials. DESCRIBE and SHOW COLUMNS return table schemas; information_schema
/// supports metadata SQL. Ingestion retains its app-metadata protocol.
pub fn service(
    catalogs: CatalogStore,
    ingestor: Ingestor,
    ct: CancellationToken,
) -> FlightServiceServer<FlightService> {
    service_with_options(catalogs, ingestor, IngestionOptions::default(), ct)
}

pub fn service_with_options(
    catalogs: CatalogStore,
    ingestor: Ingestor,
    options: IngestionOptions,
    ct: CancellationToken,
) -> FlightServiceServer<FlightService> {
    FlightServiceServer::new(FlightService::new(catalogs, ingestor, options, ct))
}

impl FlightService {
    /// Creates the shared Flight SQL service with the specified ingestion limits.
    pub fn new(
        catalogs: CatalogStore,
        ingestor: Ingestor,
        options: IngestionOptions,
        ct: CancellationToken,
    ) -> Self {
        let global_memory = IngestionSemaphore::global_memory(&options);
        Self {
            catalogs,
            ingestor,
            ct,
            options,
            global_memory,
        }
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
        Ok(Response::new(Box::pin(output)))
    }

    async fn catalog<T>(&self, request: &Request<T>) -> Result<StoredCatalog, Status> {
        let metadata = request
            .metadata()
            .get("query-metadata")
            .ok_or_else(|| Status::invalid_argument("query-metadata header is required"))?;
        let metadata: QueryMetadata = serde_json::from_slice(metadata.as_encoded_bytes())
            .map_err(|_| Status::invalid_argument("invalid query-metadata header"))?;
        self.catalogs
            .get(metadata.catalog)
            .await
            .map_err(|_| Status::internal("failed to load catalog"))?
            .ok_or_else(|| Status::not_found("catalog not found"))
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightService {
    type FlightService = Self;

    async fn get_flight_info_statement(
        &self,
        command: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let catalog = self.catalog(&request).await?;
        query::flight_info(catalog, command, request.into_inner(), &self.ct).await
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        let catalog = self.catalog(&request).await?;
        query::do_get(catalog, ticket, &self.ct).await
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.catalog(&request).await?;
        flight_info(request.into_inner(), &query.into_builder().schema())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        self.catalog(&request).await?;
        let mut builder = query.into_builder();
        builder.append(SQL_CATALOG);
        batch_response(builder.build().map_err(metadata_status)?)
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        let catalog = self
            .catalog(&request)
            .await?
            .to_catalog()
            .await
            .map_err(|_| Status::internal("failed to connect to catalog"))?;

        let mut builder = query.into_builder();
        for namespace in get_all_namespaces(catalog.as_ref()).await? {
            builder.append(SQL_CATALOG, namespace.join("."));
        }

        batch_response(builder.build().map_err(metadata_status)?)
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        let catalog = self
            .catalog(&request)
            .await?
            .to_catalog()
            .await
            .map_err(|_| Status::internal("failed to connect to catalog"))?;

        let mut builder = query.into_builder();
        for namespace in get_all_namespaces(catalog.as_ref()).await? {
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

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as ArrowFlightService>::DoGetStream>, Status> {
        self.catalog(&request).await?;
        let mut builder = query.into_builder();
        builder.append("TABLE");
        batch_response(builder.build().map_err(metadata_status)?)
    }

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        _message: Any,
    ) -> Result<Response<<Self as ArrowFlightService>::DoPutStream>, Status> {
        self.ingest(request).await
    }

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
    Ok(Response::new(Box::pin(stream)))
}

async fn get_all_namespaces(catalog: &dyn Catalog) -> Result<Vec<NamespaceIdent>, Status> {
    let mut pending = catalog
        .list_namespaces(None)
        .await
        .map_err(iceberg_status)?;
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    while let Some(namespace) = pending.pop() {
        if !seen.insert(namespace.clone()) {
            continue;
        }
        pending.extend(
            catalog
                .list_namespaces(Some(&namespace))
                .await
                .map_err(iceberg_status)?,
        );
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
