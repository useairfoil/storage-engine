use std::{collections::HashMap, future::Future, sync::Arc};

use arrow_array::RecordBatch;
use arrow_flight::{
    FlightClient, FlightData, FlightDescriptor, SchemaAsIpc, encode::FlightDataEncoderBuilder,
    error::FlightError,
};
use arrow_schema::SchemaRef;
use iceberg::NamespaceIdent;
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::{Channel, Endpoint};
use tracing::debug;

const INGEST_COMMAND: &[u8] = b"se.ingestion.v1.Ingest";
const PUBLISH_QUEUE_SIZE: usize = 32;

#[derive(Debug, Error)]
pub enum Error {
    #[error("flight connection failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("flight upload failed: {0}")]
    Flight(#[from] FlightError),
    #[error("invalid flight metadata: {0}")]
    Metadata(#[from] serde_json::Error),
    #[error("missing or unexpected flight acknowledgement: {0}")]
    Acknowledgement(&'static str),
    #[error("ingestion publisher closed")]
    Closed,
    #[error("ingestion failed: {0}")]
    Rejected(String),
    #[error("ingestion task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

pub struct IngestorClient {
    channel: Channel,
    schema: SchemaRef,
    schema_metadata: Vec<u8>,
}

pub struct Publisher {
    sender: mpsc::Sender<Command>,
}

struct Command {
    batch: RecordBatch,
    response: oneshot::Sender<Result<(), String>>,
}

struct Pending {
    remaining: usize,
    response: oneshot::Sender<Result<(), String>>,
}

#[derive(Deserialize)]
struct AckMetadata {
    request_id: u64,
}

impl IngestorClient {
    pub async fn connect(
        engine_url: &str,
        catalog_id: &str,
        namespace: &NamespaceIdent,
        table_name: &str,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let channel = Endpoint::from_shared(engine_url.trim_end_matches('/').to_owned())?
            .connect()
            .await?;
        let schema_metadata = serde_json::to_vec(&serde_json::json!({
            "request_id": 0,
            "catalog": catalog_id,
            "namespace": namespace.as_ref(),
            "table_name": table_name,
        }))?;

        Ok(Self {
            channel,
            schema,
            schema_metadata,
        })
    }

    pub fn start(self) -> (JoinHandle<Result<(), Error>>, Publisher) {
        let (sender, receiver) = mpsc::channel(PUBLISH_QUEUE_SIZE);
        let task = tokio::spawn(self.run(receiver));
        (task, Publisher { sender })
    }

    async fn run(self, commands: mpsc::Receiver<Command>) -> Result<(), Error> {
        let mut schema_message: FlightData =
            SchemaAsIpc::new(&self.schema, &Default::default()).into();
        let mut descriptor = vec![0x0a, INGEST_COMMAND.len() as u8];
        descriptor.extend_from_slice(INGEST_COMMAND);
        schema_message.flight_descriptor = Some(FlightDescriptor::new_cmd(descriptor));
        schema_message.app_metadata = self.schema_metadata.into();

        let (outgoing, incoming) = mpsc::channel(PUBLISH_QUEUE_SIZE);
        outgoing
            .send(schema_message)
            .await
            .map_err(|_| Error::Closed)?;
        let mut client = FlightClient::new(self.channel);
        let mut results = client.do_put(ReceiverStream::new(incoming).map(Ok)).await?;
        let first = results
            .next()
            .await
            .ok_or(Error::Acknowledgement("schema was not acknowledged"))??;
        if serde_json::from_slice::<AckMetadata>(&first.app_metadata)?.request_id != 0 {
            return Err(Error::Acknowledgement("expected schema acknowledgement"));
        }
        debug!("received ingestion schema acknowledgement");

        let pending = Arc::new(Mutex::new(HashMap::<u64, Pending>::new()));
        let producer = tokio::spawn(pump(commands, outgoing, self.schema, pending.clone()));
        let responses: Result<(), Error> = async {
            while let Some(result) = results.next().await {
                let result = result?;
                let request_id =
                    serde_json::from_slice::<AckMetadata>(&result.app_metadata)?.request_id;
                let mut pending = pending.lock().await;
                let response = match pending.get_mut(&request_id) {
                    Some(entry) if entry.remaining > 1 => {
                        entry.remaining -= 1;
                        None
                    }
                    Some(_) => pending.remove(&request_id).map(|entry| entry.response),
                    None => return Err(Error::Acknowledgement("unexpected batch request id")),
                };
                debug!(request_id, "received batch acknowledgement");
                if let Some(response) = response {
                    debug!(request_id, "batch fully acknowledged");
                    let _ = response.send(Ok(()));
                }
            }
            Ok(())
        }
        .await;

        let early_end = responses.is_ok() && !producer.is_finished();
        if responses.is_err() || early_end {
            producer.abort();
        }
        let produced = producer.await;
        let outcome = if let Err(error) = responses {
            Err(error)
        } else if early_end {
            Err(Error::Acknowledgement(
                "response stream ended before input finished",
            ))
        } else {
            match produced {
                Ok(Ok(())) if pending.lock().await.is_empty() => Ok(()),
                Ok(Ok(())) => Err(Error::Acknowledgement("not all batches were acknowledged")),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(error.into()),
            }
        };
        if let Err(ref error) = outcome {
            let message = error.to_string();
            for (_, entry) in pending.lock().await.drain() {
                let _ = entry.response.send(Err(message.clone()));
            }
        }
        outcome
    }
}

impl Publisher {
    #[tracing::instrument(
        name = "stress.publish",
        skip_all,
        fields(batch.size_rows = batch.num_rows(), batch.size_bytes = batch.get_array_memory_size()),
        err
    )]
    pub fn publish(
        &self,
        batch: RecordBatch,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        let sender = self.sender.clone();

        async move {
            let (response, result) = oneshot::channel();
            sender
                .send(Command { batch, response })
                .await
                .map_err(|_| Error::Closed)?;
            result
                .await
                .map_err(|_| Error::Closed)?
                .map_err(Error::Rejected)
        }
    }
}

async fn pump(
    mut commands: mpsc::Receiver<Command>,
    outgoing: mpsc::Sender<FlightData>,
    schema: SchemaRef,
    pending: Arc<Mutex<HashMap<u64, Pending>>>,
) -> Result<(), Error> {
    let mut next_request_id = 1_u64;
    while let Some(command) = commands.recv().await {
        let rows = command.batch.num_rows();
        let mut messages = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .build(tokio_stream::iter([Ok(command.batch)]));
        // The stream already has its schema; the encoder's first message is redundant.
        messages.next().await.transpose()?;
        let mut frames = Vec::new();
        while let Some(frame) = messages.next().await {
            frames.push(frame?);
        }
        if frames.is_empty() {
            let _ = command.response.send(Ok(()));
            continue;
        }
        let request_id = next_request_id;
        next_request_id += 1;
        let metadata = serde_json::to_vec(&serde_json::json!({ "request_id": request_id }))?;
        // A batch can span several Flight messages; resolve its publisher once all are acknowledged.
        pending.lock().await.insert(
            request_id,
            Pending {
                remaining: frames.len(),
                response: command.response,
            },
        );
        debug!(
            request_id,
            batch.size_rows = rows,
            fragments = frames.len(),
            "publishing batch"
        );
        for mut frame in frames {
            frame.app_metadata = metadata.clone().into();
            outgoing.send(frame).await.map_err(|_| Error::Closed)?;
        }
        debug!(request_id, "batch queued for Flight upload");
    }
    Ok(())
}
