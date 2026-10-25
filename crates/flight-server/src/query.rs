use std::sync::Arc;

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightService as ArrowFlightService,
    sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery},
};
use datafusion::{
    catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider},
    dataframe::DataFrame,
    error::DataFusionError,
    execution::context::SQLOptions,
    prelude::{SessionConfig, SessionContext},
};
use iceberg_datafusion::IcebergStaticTableProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use prost::Message;
use se_meta_store::catalog::{CatalogName, StoredCatalog};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};

use crate::{FlightService, SQL_CATALOG, get_all_namespaces, iceberg_status};

/// Stateless tickets are scoped to the catalog selected when planning the query.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StatementTicket {
    catalog: CatalogName,
    sql: String,
}

pub(super) async fn flight_info(
    catalog: StoredCatalog,
    command: CommandStatementQuery,
    descriptor: FlightDescriptor,
    ct: &CancellationToken,
) -> Result<Response<FlightInfo>, Status> {
    if command.transaction_id.is_some() {
        return Err(Status::unimplemented("transactions are not supported"));
    }

    let plan = ct
        .run_until_cancelled(async {
            plan(&catalog, &command.query)
                .await?
                .create_physical_plan()
                .await
                .map_err(sql_status)
        })
        .await
        .ok_or_else(|| Status::cancelled("server is shutting down"))??;

    let handle = serde_json::to_vec(&StatementTicket {
        catalog: catalog.name().clone(),
        sql: command.query,
    })
    .map_err(|_| Status::internal("failed to encode statement ticket"))?;

    let ticket = TicketStatementQuery {
        statement_handle: handle.into(),
    };

    let info = FlightInfo::new()
        .try_with_schema(plan.schema().as_ref())
        .map_err(|_| Status::internal("failed to encode query schema"))?
        .with_descriptor(descriptor)
        .with_endpoint(
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec())),
        );

    Ok(Response::new(info))
}

pub(super) async fn do_get(
    catalog: StoredCatalog,
    ticket: TicketStatementQuery,
    ct: &CancellationToken,
) -> Result<Response<<FlightService as ArrowFlightService>::DoGetStream>, Status> {
    let ticket: StatementTicket = serde_json::from_slice(&ticket.statement_handle)
        .map_err(|_| Status::invalid_argument("invalid statement ticket"))?;

    if &ticket.catalog != catalog.name() {
        return Err(Status::invalid_argument(
            "statement ticket belongs to a different catalog",
        ));
    }

    let stream = ct
        .run_until_cancelled(async {
            plan(&catalog, &ticket.sql)
                .await?
                .execute_stream()
                .await
                .map_err(sql_status)
        })
        .await
        .ok_or_else(|| Status::cancelled("server is shutting down"))??;
    let schema = stream.schema();
    let stream = stream.map(|batch| batch.map_err(|error| FlightError::from(sql_status(error))));

    let ct = ct.clone();

    let output = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream)
        .map(move |result| {
            if ct.is_cancelled() {
                Err(Status::cancelled("server is shutting down"))
            } else {
                result.map_err(|_| Status::internal("failed to stream query results"))
            }
        });

    Ok(Response::new(Box::pin(output)))
}

async fn plan(catalog: &StoredCatalog, sql: &str) -> Result<DataFrame, Status> {
    let catalog = catalog
        .to_catalog_with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .await
        .map_err(|_| Status::internal("failed to connect to catalog"))?;

    let provider = Arc::new(MemoryCatalogProvider::new());

    for namespace in get_all_namespaces(catalog.as_ref()).await? {
        let schema = Arc::new(MemorySchemaProvider::new());
        for ident in catalog
            .list_tables(&namespace)
            .await
            .map_err(iceberg_status)?
        {
            let table = catalog.load_table(&ident).await.map_err(iceberg_status)?;
            let table = IcebergStaticTableProvider::try_new_from_table(table)
                .await
                .map_err(iceberg_status)?;

            schema
                .register_table(ident.name, Arc::new(table))
                .map_err(sql_status)?;
        }

        provider
            .register_schema(&namespace.join("."), schema)
            .map_err(sql_status)?;
    }

    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_default_catalog_and_schema(SQL_CATALOG, "public")
            .with_information_schema(true),
    );

    context.register_catalog(SQL_CATALOG, provider);

    context
        .sql_with_options(
            sql,
            SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false)
                .with_allow_statements(false),
        )
        .await
        .map_err(sql_status)
}

fn sql_status(error: DataFusionError) -> Status {
    match error.find_root() {
        DataFusionError::NotImplemented(_) => {
            Status::unimplemented("SQL operation is not supported")
        }
        DataFusionError::Plan(_) | DataFusionError::SQL(_, _) => {
            Status::invalid_argument(error.to_string())
        }
        _ => Status::internal("failed to execute SQL query"),
    }
}
