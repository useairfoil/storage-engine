use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_flight::{
    FlightData, PutResult,
    decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder},
    error::FlightError,
    sql::server::PeekableFlightDataStream,
};
use arrow_schema::SchemaRef;
use datafusion::physical_expr_adapter::{BatchAdapter, BatchAdapterFactory};
use se_ingestion::{Error as IngestionError, Ingestor, TableIngestor, WriteError};
use se_meta_store::catalog::{CatalogName, NamespaceIdent};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::Status;
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

struct StreamAdapter {
    // Incoming Flight schema; each batch must match it before adaptation.
    source_schema: SchemaRef,
    // Iceberg table schema, also used for the WAL.
    table_schema: SchemaRef,
    // Reorders supplied columns and fills missing nullable columns with nulls.
    adapter: BatchAdapter,
}

pub(super) async fn ingest(
    input: PeekableFlightDataStream,
    ingestor: &Ingestor,
    options: &IngestionOptions,
    global_memory: Arc<Semaphore>,
    ct: &CancellationToken,
) -> Result<ReceiverStream<Result<PutResult, Status>>, Status> {
    let input = input.map(|message| message.map_err(FlightError::from));
    let mut input = FlightDataDecoder::new(input);

    let schema_message = tokio::select! {
        _ = ct.cancelled() => return Err(Status::cancelled("server shutting down")),
        message = input.next() => message
            .ok_or_else(|| Status::invalid_argument("schema message is required"))?
            .map_err(|_| Status::invalid_argument("first message must be a schema message"))?,
    };
    let source_schema = match &schema_message.payload {
        DecodedPayload::Schema(schema) => Arc::clone(schema),
        _ => {
            return Err(Status::invalid_argument(
                "first message must be a schema message",
            ));
        }
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
        _ = ct.cancelled() => return Err(Status::cancelled("server shutting down")),
        result = ingestor.for_table(catalog, namespace, schema_metadata.table_name) => {
            result.map_err(ingestion_status)?
        },
    };
    let adapter = StreamAdapter::new(source_schema, table_ingestor.table_schema())?;

    let (sender, receiver) = mpsc::channel(32);

    let request_id = schema_metadata.request_id;
    sender
        .send(Ok(acknowledgement(request_id)?))
        .await
        .map_err(|_| Status::internal("acknowledgement stream closed"))?;

    debug!(request_id, "ingestion response sent");

    let semaphore = IngestionSemaphore::new(options, global_memory);
    tokio::spawn(process_messages(
        input,
        table_ingestor,
        adapter,
        sender,
        semaphore,
        ct.clone(),
    ));

    Ok(ReceiverStream::new(receiver))
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

async fn process_messages(
    mut input: FlightDataDecoder,
    table_ingestor: TableIngestor,
    adapter: StreamAdapter,
    sender: mpsc::Sender<Result<PutResult, Status>>,
    semaphore: IngestionSemaphore,
    ct: CancellationToken,
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

                        let batch = match adapter.adapt_batch(&batch) {
                            Ok(batch) => batch,
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
        WriteError::Write | WriteError::Flush => Status::internal(error.to_string()),
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

impl StreamAdapter {
    fn new(source_schema: SchemaRef, table_schema: SchemaRef) -> Result<Self, Status> {
        for field in source_schema.fields() {
            let target = table_schema
                .field_with_name(field.name())
                .map_err(|_| Status::invalid_argument("invalid source schema"))?;
            // Incoming fields may be declared nullable even when every value is present.
            // Validate actual nulls against the table schema after adaptation.
            if !target.contains(&field.as_ref().clone().with_nullable(false)) {
                return Err(Status::invalid_argument("invalid source schema"));
            }
        }
        let adapter = BatchAdapterFactory::new(table_schema.clone())
            .make_adapter(&source_schema)
            .map_err(|_| Status::invalid_argument("invalid source schema"))?;
        Ok(Self {
            source_schema,
            table_schema,
            adapter,
        })
    }

    fn adapt_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, Status> {
        if batch.schema() != self.source_schema {
            return Err(Status::invalid_argument("invalid record batch schema"));
        }
        let adapted = self
            .adapter
            .adapt_batch(batch)
            .map_err(|_| Status::invalid_argument("invalid record batch schema"))?;
        RecordBatch::try_new(self.table_schema.clone(), adapted.columns().to_vec())
            .map_err(|_| Status::invalid_argument("invalid record batch schema"))
    }
}
