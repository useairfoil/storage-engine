use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_flight::{
    FlightData, FlightDescriptor, SchemaAsIpc, flight_service_client::FlightServiceClient,
};
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use axum::{Json, Router, http::StatusCode, routing::get};
use object_store::memory::InMemory;
use se_ingestion::start_ingestion_loop;
use se_ingestion_server::service;
use se_meta_store::catalog::{
    CatalogConfig, CatalogName, CatalogStore, NamespaceIdent, RestCatalogConfig, TableIdent,
};
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
async fn acknowledges_schema_but_not_uncommitted_data() {
    let (store, catalog_server, _) = store_with_catalog().await;
    let (mut client, server, _ct) = client(store).await;

    let messages = [schema_message("example"), data_message(1)];
    let mut results = client
        .do_put(tokio_stream::iter(messages))
        .await
        .unwrap()
        .into_inner();

    let first = results.message().await.unwrap().unwrap();
    assert_eq!(request_id(first), 0);

    let error = results.message().await.unwrap_err();
    assert_eq!(error.code(), Code::Internal);
    assert_eq!(error.message(), "failed to flush record batch");

    server.abort();
    catalog_server.abort();
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
    let (store, catalog_server, _) = store_with_catalog().await;
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
    catalog_server.abort();
}

#[tokio::test]
async fn cancellation_closes_an_active_ingestion_stream() {
    let (store, catalog_server, _) = store_with_catalog().await;
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
    catalog_server.abort();
}

#[tokio::test]
async fn rejects_a_missing_table_before_acknowledging_schema() {
    let (store, catalog_server, _) = store_with_catalog().await;
    let (mut client, server, _ct) = client(store).await;

    let error = client
        .do_put(tokio_stream::iter([schema_message_for(
            "example", "missing",
        )]))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::NotFound);
    assert_eq!(error.message(), "table not found");

    server.abort();
    catalog_server.abort();
}

#[tokio::test]
async fn concurrent_missing_table_lookups_both_fail_without_sticking_the_gate() {
    let (store, catalog_server, _) = store_with_catalog().await;
    let (_ingestion_task, ingestor) = start_ingestion_loop(store);
    let name = CatalogName::new("example").unwrap();

    let (first, second) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            ingestor.for_table(name.clone(), namespace(), "missing".into()),
            ingestor.for_table(name, namespace(), "missing".into()),
        )
    })
    .await
    .unwrap();
    assert_eq!(
        first.unwrap_err().to_string(),
        "table not found: analytics.events.missing"
    );
    assert_eq!(
        second.unwrap_err().to_string(),
        "table not found: analytics.events.missing"
    );
    catalog_server.abort();
}

#[tokio::test]
async fn concurrent_requests_share_initialization_and_reuse_stored_metadata() {
    let (store, catalog_server, loads) = store_with_catalog().await;
    let (_ingestion_task, ingestor) = start_ingestion_loop(store.clone());
    let name = CatalogName::new("example").unwrap();
    let ident = TableIdent::new(namespace(), "page_views".to_string());

    let (first, second) = tokio::join!(
        ingestor.for_table(name.clone(), namespace(), ident.name.clone()),
        ingestor.for_table(name.clone(), namespace(), ident.name.clone()),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    let catalog = store.get(name.clone()).await.unwrap().unwrap();
    let stored = catalog
        .try_load_table(ident.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.metadata().uuid.to_string(),
        "00000000-0000-0000-0000-000000000007"
    );

    store.delete(name.clone()).await.unwrap();
    let third = ingestor
        .for_table(name.clone(), namespace(), ident.name.clone())
        .await
        .unwrap();
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    store
        .create(name.clone(), catalog.config().clone())
        .await
        .unwrap();
    catalog_server.abort();
    drop((first, second, third));
    ingestor
        .for_table(name, namespace(), ident.name)
        .await
        .unwrap();
    assert_eq!(loads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn writes_validate_before_enqueue_and_wait_for_the_group_flush() {
    let (store, catalog_server, _) = store_with_catalog().await;
    let (_ingestion_task, ingestor) = start_ingestion_loop(store);
    let table = ingestor
        .for_table(
            CatalogName::new("example").unwrap(),
            namespace(),
            "page_views".into(),
        )
        .await
        .unwrap();

    let valid = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![Some(1)]))],
    )
    .unwrap();
    let invalid = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![Some("one")]))],
    )
    .unwrap();
    let renamed = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            true,
        )])),
        vec![Arc::new(Int32Array::from(vec![Some(2)]))],
    )
    .unwrap();

    let mut first = Box::pin(table.write(valid.clone()));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut first)
            .await
            .is_err()
    );
    assert_eq!(
        table.write(invalid).await.unwrap_err().to_string(),
        "invalid record batch schema"
    );
    assert_eq!(
        table.write(renamed).await.unwrap_err().to_string(),
        "invalid record batch schema"
    );
    assert_eq!(
        table
            .write(RecordBatch::new_empty(valid.schema()))
            .await
            .unwrap_err()
            .to_string(),
        "record batch is empty"
    );

    let mut second = Box::pin(table.write(valid));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut second)
            .await
            .is_err()
    );
    let (first, second) = tokio::join!(first, second);
    assert_eq!(
        first.unwrap_err().to_string(),
        "failed to flush record batch"
    );
    assert_eq!(
        second.unwrap_err().to_string(),
        "failed to flush record batch"
    );
    catalog_server.abort();
}

#[tokio::test]
async fn cancelled_caller_leaves_queued_table_work_discoverable() {
    let (store, catalog_server, _) = store_with_catalog().await;
    let (ingestion_task, ingestor) = start_ingestion_loop(store.clone());
    let name = CatalogName::new("example").unwrap();
    let table = ingestor
        .for_table(name.clone(), namespace(), "page_views".into())
        .await
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![Some(1)]))],
    )
    .unwrap();
    let mut write = Box::pin(table.write(batch));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut write)
            .await
            .is_err()
    );

    store.delete(name.clone()).await.unwrap();
    drop((write, table));
    let table = ingestor
        .for_table(name, namespace(), "page_views".into())
        .await
        .unwrap();
    drop((table, ingestor));
    tokio::time::timeout(Duration::from_secs(2), ingestion_task)
        .await
        .unwrap()
        .unwrap();
    catalog_server.abort();
}

#[tokio::test]
async fn one_loop_task_owns_writes_for_distinct_catalogs() {
    let (store, catalog_server, _) = store_with_catalog().await;
    let other = CatalogName::new("other").unwrap();
    let config = store
        .get(CatalogName::new("example").unwrap())
        .await
        .unwrap()
        .unwrap()
        .config()
        .clone();
    store.create(other.clone(), config).await.unwrap();
    let (ingestion_task, ingestor) = start_ingestion_loop(store);
    let (first, second) = tokio::join!(
        ingestor.for_table(
            CatalogName::new("example").unwrap(),
            namespace(),
            "page_views".into()
        ),
        ingestor.for_table(other, namespace(), "page_views".into()),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![Some(1)]))],
    )
    .unwrap();
    let mut first_write = Box::pin(first.write(batch.clone()));
    let mut second_write = Box::pin(second.write(batch));
    let (first_wait, second_wait) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(50), &mut first_write),
        tokio::time::timeout(Duration::from_millis(50), &mut second_write),
    );
    assert!(first_wait.is_err());
    assert!(second_wait.is_err());

    ingestion_task.abort();
    assert!(ingestion_task.await.unwrap_err().is_cancelled());
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), first_write)
            .await
            .unwrap()
            .unwrap_err()
            .to_string(),
        "failed to flush record batch"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), second_write)
            .await
            .unwrap()
            .unwrap_err()
            .to_string(),
        "failed to flush record batch"
    );
    catalog_server.abort();
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
    let (_ingestion_task, ingestor) = start_ingestion_loop(store);
    let router = Routes::new(service(ingestor, ct.clone())).into_axum_router();
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

fn catalog_config(uri: String) -> CatalogConfig {
    CatalogConfig::Rest(RestCatalogConfig {
        uri,
        warehouse: None,
        properties: HashMap::new(),
    })
}

fn schema_message(catalog: &str) -> FlightData {
    schema_message_for(catalog, "page_views")
}

fn schema_message_for(catalog: &str, table_name: &str) -> FlightData {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let options = IpcWriteOptions::default();
    let mut message: FlightData = SchemaAsIpc::new(&schema, &options).into();
    message.flight_descriptor = Some(ingestion_descriptor());
    message.app_metadata = serde_json::to_vec(&serde_json::json!({
        "request_id": 0,
        "catalog": catalog,
        "namespace": ["analytics", "events"],
        "table_name": table_name
    }))
    .unwrap()
    .into();
    message
}

fn namespace() -> NamespaceIdent {
    NamespaceIdent::from_strs(["analytics", "events"]).unwrap()
}

async fn store_with_catalog() -> (CatalogStore, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let loads = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&loads);
    let router = Router::new()
        .route("/v1/config", get(|| async { Json(serde_json::json!({"defaults": {}, "overrides": {}})) }))
        .route("/v1/namespaces/{*path}", get(move |axum::extract::Path(path): axum::extract::Path<String>| {
            let count = Arc::clone(&count);
            async move {
                if !path.ends_with("/tables/page_views") {
                    return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"})));
                }
                count.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, Json(serde_json::json!({
                    "metadata-location": "file:///tmp/warehouse/page_views/metadata.json",
                    "metadata": {
                        "format-version": 1,
                        "table-uuid": "00000000-0000-0000-0000-000000000007",
                        "location": "file:///tmp/warehouse/page_views",
                        "last-updated-ms": 1,
                        "last-column-id": 1,
                        "schema": {"type": "struct", "schema-id": 0, "fields": [
                            {"id": 1, "name": "id", "required": false, "type": "int"}
                        ]},
                        "current-schema-id": 0,
                        "partition-spec": [],
                        "default-spec-id": 0,
                        "sort-orders": [{"order-id": 0, "fields": []}],
                        "default-sort-order-id": 0,
                        "properties": {}
                    },
                    "config": {}
                })))
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let store = catalog_store();
    store
        .create(CatalogName::new("example").unwrap(), catalog_config(uri))
        .await
        .unwrap();
    (store, server, loads)
}

fn ingestion_descriptor() -> FlightDescriptor {
    let type_url = b"se.ingestion.v1.Ingest";
    let mut command = vec![0x0a, u8::try_from(type_url.len()).unwrap()];
    command.extend_from_slice(type_url);
    FlightDescriptor::new_cmd(command)
}

fn data_message(request_id: u64) -> FlightData {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![Some(1)]))],
    )
    .unwrap();
    let (_, encoded) = IpcDataGenerator::default()
        .encode(
            &batch,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
            &mut CompressionContext::default(),
        )
        .unwrap();
    let mut message: FlightData = encoded.into();
    message.app_metadata = serde_json::to_vec(&serde_json::json!({ "request_id": request_id }))
        .unwrap()
        .into();
    message
}

fn request_id(result: arrow_flight::PutResult) -> u64 {
    serde_json::from_slice::<AckMetadata>(&result.app_metadata)
        .unwrap()
        .request_id
}
