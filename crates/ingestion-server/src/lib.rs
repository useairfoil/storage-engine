//! Arrow Flight SQL ingestion service.

mod permit;

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use arrow_flight::{
    FlightData, PutResult,
    decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        Any, SqlInfo,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use se_ingestion::{Error as IngestionError, Ingestor, TableIngestor, WriteError};
use se_meta_store::catalog::{CatalogName, CatalogStore, NamespaceIdent};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::permit::IngestionSemaphore;

const DEFAULT_MAX_IN_FLIGHT_WRITES: usize = 32;
const DEFAULT_MAX_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_GLOBAL_IN_FLIGHT_BYTES: u64 = 512 * 1024 * 1024 * 1024;

/// Configuration for the ingestion service.
#[derive(Debug, Clone)]
pub struct IngestionOptions {
    /// Maximum number of record batch writes running concurrently for one stream.
    pub max_in_flight_writes: NonZeroUsize,
    /// Maximum estimated memory used by record batches being written for one stream.
    pub max_in_flight_bytes: NonZeroU64,
    /// Maximum estimated memory used by record batches being written across all streams.
    pub max_global_in_flight_bytes: NonZeroU64,
}

#[derive(Debug, Clone)]
pub struct IngestionService {
    ingestor: Ingestor,
    ct: CancellationToken,
    options: IngestionOptions,
    global_memory: Arc<Semaphore>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaMetadata {
    request_id: u64,
    catalog: String,
    namespace: Vec<String>,
    table_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestMetadata {
    request_id: u64,
}

#[derive(Debug, Serialize)]
struct AckMetadata {
    request_id: u64,
}

pub fn service(
    catalog_store: CatalogStore,
    ct: CancellationToken,
) -> FlightServiceServer<IngestionService> {
    service_with_options(catalog_store, ct, IngestionOptions::default())
}

pub fn service_with_options(
    catalog_store: CatalogStore,
    ct: CancellationToken,
    options: IngestionOptions,
) -> FlightServiceServer<IngestionService> {
    let global_memory = IngestionSemaphore::global_memory(&options);
    FlightServiceServer::new(IngestionService {
        ingestor: Ingestor::new(catalog_store),
        ct,
        options,
        global_memory,
    })
}

impl Default for IngestionOptions {
    fn default() -> Self {
        Self {
            max_in_flight_writes: NonZeroUsize::new(DEFAULT_MAX_IN_FLIGHT_WRITES)
                .unwrap_or(NonZeroUsize::MIN),
            max_in_flight_bytes: NonZeroU64::new(DEFAULT_MAX_IN_FLIGHT_BYTES)
                .unwrap_or(NonZeroU64::MIN),
            max_global_in_flight_bytes: NonZeroU64::new(DEFAULT_MAX_GLOBAL_IN_FLIGHT_BYTES)
                .unwrap_or(NonZeroU64::MIN),
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for IngestionService {
    type FlightService = Self;

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        let input = request
            .into_inner()
            .map(|message| message.map_err(FlightError::from));
        let mut input = FlightDataDecoder::new(input);

        let schema_message = tokio::select! {
            _ = self.ct.cancelled() => return Err(Status::cancelled("server shutting down")),
            message = input.next() => message
                .ok_or_else(|| Status::invalid_argument("schema message is required"))?
                .map_err(|_| Status::invalid_argument("first message must be a schema message"))?,
        };
        let schema_metadata = parse_schema_metadata(schema_message)?;

        debug!(
            request_id = schema_metadata.request_id,
            catalog = %schema_metadata.catalog,
            namespace = ?schema_metadata.namespace,
            table_name = %schema_metadata.table_name,
            "ingestion started"
        );

        let catalog = CatalogName::new(&schema_metadata.catalog)
            .map_err(|_| Status::invalid_argument("invalid catalog id"))?;
        let namespace = NamespaceIdent::from_vec(schema_metadata.namespace)
            .map_err(|_| Status::invalid_argument("invalid namespace"))?;
        let table_ingestor = tokio::select! {
            _ = self.ct.cancelled() => return Err(Status::cancelled("server shutting down")),
            result = self.ingestor.for_table(catalog, namespace, schema_metadata.table_name) => {
                result.map_err(ingestion_status)?
            },
        };

        let (sender, receiver) = mpsc::channel(32);

        let request_id = schema_metadata.request_id;
        sender
            .send(Ok(acknowledgement(request_id)?))
            .await
            .map_err(|_| Status::internal("acknowledgement stream closed"))?;

        debug!(request_id, "ingestion response sent");

        let semaphore = IngestionSemaphore::new(&self.options, Arc::clone(&self.global_memory));
        tokio::spawn(process_messages(
            input,
            table_ingestor,
            sender,
            self.ct.clone(),
            semaphore,
        ));

        let output: <Self as FlightService>::DoPutStream = Box::pin(ReceiverStream::new(receiver));

        Ok(Response::new(output))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

async fn process_messages(
    mut input: FlightDataDecoder,
    table_ingestor: TableIngestor,
    sender: mpsc::Sender<Result<PutResult, Status>>,
    ct: CancellationToken,
    semaphore: IngestionSemaphore,
) {
    let mut tasks = JoinSet::new();
    let mut input_done = false;

    loop {
        if input_done && tasks.is_empty() {
            break;
        }

        tokio::select! {
            _ = ct.cancelled() => break,
            message = input.next(), if !input_done
                && tasks.len() < semaphore.max_in_flight_writes()
                && semaphore.has_write_capacity() => match message {
                Some(Ok(message)) => match message.payload {
                    DecodedPayload::Schema(_) => {
                        let _ = sender
                            .send(Err(Status::invalid_argument(
                                "schema message must be first",
                            )))
                            .await;
                        return;
                    }
                    DecodedPayload::RecordBatch(batch) => {
                        let request_id = match parse_request_metadata(&message.inner) {
                            Ok(request_id) => request_id,
                            Err(error) => {
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                        };

                        debug!(request_id, "ingestion message received");

                        let batch_bytes = u64::try_from(batch.get_array_memory_size())
                            .unwrap_or(u64::MAX);
                        let permit = match semaphore.try_acquire(batch_bytes) {
                            Ok(permit) => permit,
                            Err(error) => {
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                        };

                        let write = table_ingestor.write(batch);
                        tasks.spawn(async move {
                            let _permit = permit;
                            (request_id, write.await)
                        });
                    }
                    DecodedPayload::None => {
                        let _ = sender
                            .send(Err(Status::invalid_argument("record batch is required")))
                            .await;
                        return;
                    }
                },
                Some(Err(_)) => {
                    let _ = sender
                        .send(Err(Status::invalid_argument("invalid Arrow Flight data")))
                        .await;
                    return;
                }
                None => input_done = true,
            },
            result = tasks.join_next(), if !tasks.is_empty() => {
                let request_id = match result {
                    Some(Ok((request_id, Ok(())))) => request_id,
                    Some(Ok((_, Err(error)))) => {
                        let _ = sender.send(Err(write_status(error))).await;
                        return;
                    }
                    Some(Err(_)) => {
                        let _ = sender
                            .send(Err(Status::internal("ingestion task failed")))
                            .await;
                        return;
                    }
                    None => continue,
                };

                let response = match acknowledgement(request_id) {
                    Ok(response) => response,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };

                if sender.send(Ok(response)).await.is_err() {
                    return;
                }

                debug!(request_id, "ingestion response sent");
            },
        }
    }
}

fn write_status(error: WriteError) -> Status {
    match error {
        WriteError::InvalidSchema => Status::invalid_argument(error.to_string()),
        WriteError::EmptyBatch => Status::invalid_argument(error.to_string()),
        WriteError::Flush => Status::internal(error.to_string()),
        _ => Status::internal("failed to write record batch"),
    }
}

fn ingestion_status(error: IngestionError) -> Status {
    match error {
        IngestionError::CatalogNotFound(_) => Status::not_found("catalog not found"),
        IngestionError::TableNotFound(_) => Status::not_found("table not found"),
        _ => Status::internal("failed to create table ingestor"),
    }
}

fn parse_schema_metadata(message: DecodedFlightData) -> Result<SchemaMetadata, Status> {
    if !matches!(message.payload, DecodedPayload::Schema(_)) {
        return Err(Status::invalid_argument(
            "first message must be a schema message",
        ));
    }

    let metadata: SchemaMetadata = serde_json::from_slice(&message.inner.app_metadata)
        .map_err(|_| Status::invalid_argument("invalid schema app metadata"))?;
    if metadata.request_id != 0 {
        return Err(Status::invalid_argument("schema request_id must be zero"));
    }

    Ok(metadata)
}

fn parse_request_metadata(message: &FlightData) -> Result<u64, Status> {
    let metadata: RequestMetadata = serde_json::from_slice(&message.app_metadata)
        .map_err(|_| Status::invalid_argument("invalid app metadata"))?;
    if metadata.request_id == 0 {
        return Err(Status::invalid_argument(
            "request_id zero is reserved for the schema message",
        ));
    }
    Ok(metadata.request_id)
}

fn acknowledgement(request_id: u64) -> Result<PutResult, Status> {
    let app_metadata = serde_json::to_vec(&AckMetadata { request_id })
        .map_err(|_| Status::internal("failed to serialize acknowledgement"))?
        .into();

    Ok(PutResult { app_metadata })
}
