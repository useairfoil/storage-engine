use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow_flight::{
    FlightData, FlightDescriptor, SchemaAsIpc, flight_service_client::FlightServiceClient,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema;
use object_store::memory::InMemory;
use se_ingestion_server::service;
use se_meta_store::catalog::{CatalogConfig, CatalogName, CatalogStore, RestCatalogConfig};
use se_secret_store::memory::InMemorySecretStore;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Code, service::Routes};

#[derive(Debug, Deserialize)]
struct AckMetadata {
    request_id: u64,
}

#[tokio::test]
async fn acknowledges_schema_first_and_every_data_message() {
    let store = catalog_store();
    store
        .create(CatalogName::new("example").unwrap(), catalog_config())
        .await
        .unwrap();
    let (mut client, server, _ct) = client(store).await;

    let messages = std::iter::once(schema_message("example"))
        .chain((1..=8).map(data_message))
        .collect::<Vec<_>>();
    let mut results = client
        .do_put(tokio_stream::iter(messages))
        .await
        .unwrap()
        .into_inner();

    let first = results.message().await.unwrap().unwrap();
    assert_eq!(request_id(first), 0);

    let mut acknowledged = Vec::new();
    while let Some(result) = results.message().await.unwrap() {
        acknowledged.push(request_id(result));
    }
    acknowledged.sort_unstable();
    assert_eq!(acknowledged, (1..=8).collect::<Vec<_>>());

    server.abort();
}

#[tokio::test]
async fn rejects_a_missing_catalog_before_acknowledging_schema() {
    let (mut client, server, _ct) = client(catalog_store()).await;

    let error = client
        .do_put(tokio_stream::iter([schema_message("missing")]))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::NotFound);
    assert_eq!(error.message(), "catalog not found");

    server.abort();
}

#[tokio::test]
async fn rejects_a_non_schema_first_message() {
    let (mut client, server, _ct) = client(catalog_store()).await;
    let mut message = data_message(1);
    message.flight_descriptor = Some(ingestion_descriptor());

    let error = client
        .do_put(tokio_stream::iter([message]))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(error.message(), "first message must be a schema message");

    server.abort();
}

#[tokio::test]
async fn rejects_zero_for_a_message_after_the_schema() {
    let store = catalog_store();
    store
        .create(CatalogName::new("example").unwrap(), catalog_config())
        .await
        .unwrap();
    let (mut client, server, _ct) = client(store).await;
    let mut results = client
        .do_put(tokio_stream::iter([
            schema_message("example"),
            data_message(0),
        ]))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(request_id(results.message().await.unwrap().unwrap()), 0);
    let error = results.message().await.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        error.message(),
        "request_id zero is reserved for the schema message"
    );

    server.abort();
}

#[tokio::test]
async fn cancellation_closes_an_active_ingestion_stream() {
    let store = catalog_store();
    store
        .create(CatalogName::new("example").unwrap(), catalog_config())
        .await
        .unwrap();
    let (mut client, server, ct) = client(store).await;
    let (sender, receiver) = mpsc::channel(1);
    sender.send(schema_message("example")).await.unwrap();
    let mut results = client
        .do_put(tokio_stream::wrappers::ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(request_id(results.message().await.unwrap().unwrap()), 0);

    ct.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), results.message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, None);

    drop(sender);
    server.abort();
}

async fn client(
    store: CatalogStore,
) -> (
    FlightServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<()>,
    CancellationToken,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let ct = CancellationToken::new();
    let router = Routes::new(service(store, ct.clone())).into_axum_router();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (FlightServiceClient::new(channel), server, ct)
}

fn catalog_store() -> CatalogStore {
    CatalogStore::new(
        Arc::new(InMemorySecretStore::new()),
        Arc::new(InMemory::new()),
    )
}

fn catalog_config() -> CatalogConfig {
    CatalogConfig::Rest(RestCatalogConfig {
        uri: "https://catalog.example.com".to_string(),
        warehouse: None,
        properties: HashMap::new(),
    })
}

fn schema_message(catalog: &str) -> FlightData {
    let schema = Schema::empty();
    let options = IpcWriteOptions::default();
    let mut message: FlightData = SchemaAsIpc::new(&schema, &options).into();
    message.flight_descriptor = Some(ingestion_descriptor());
    message.app_metadata = serde_json::to_vec(&serde_json::json!({
        "request_id": 0,
        "catalog": catalog,
        "namespace": ["analytics", "events"],
        "table_name": "page_views"
    }))
    .unwrap()
    .into();
    message
}

fn ingestion_descriptor() -> FlightDescriptor {
    let type_url = b"se.ingestion.v1.Ingest";
    let mut command = vec![0x0a, u8::try_from(type_url.len()).unwrap()];
    command.extend_from_slice(type_url);
    FlightDescriptor::new_cmd(command)
}

fn data_message(request_id: u64) -> FlightData {
    FlightData {
        app_metadata: serde_json::to_vec(&serde_json::json!({ "request_id": request_id }))
            .unwrap()
            .into(),
        ..Default::default()
    }
}

fn request_id(result: arrow_flight::PutResult) -> u64 {
    serde_json::from_slice::<AckMetadata>(&result.app_metadata)
        .unwrap()
        .request_id
}
