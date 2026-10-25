use std::sync::Arc;

use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, IpcMessage, SchemaAsIpc, Ticket,
    sql::{
        CommandGetDbSchemas, CommandGetTables, ProstMessageExt, TicketStatementQuery,
        client::FlightSqlServiceClient,
    },
};
use arrow_schema::{DataType, Field, Schema};
use axum::{
    Json, Router,
    body::Body,
    extract::Query,
    http::{HeaderMap, Method, Response, StatusCode},
    routing::get,
};
use datafusion::arrow::compute::concat_batches;
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::{FileIO, FileIOBuilder, MemoryStorageFactory},
    spec::{ManifestListWriter, ManifestWriterBuilder, PartitionSpec, Struct},
    writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder},
};
use object_store::{memory::InMemory, path::Path};
use prost::Message;
use se_common::clock::DefaultSystemClock;
use se_flight_server::service;
use se_ingestion::Ingestor;
use se_meta_store::catalog::{CatalogConfig, CatalogName, CatalogStore, RestCatalogConfig};
use se_scheduler::object_store::{ObjectStoreBroker, ObjectStoreSchedulerOptions};
use se_secret_store::memory::InMemorySecretStore;
use serde::Deserialize;
use serde_json::json;
use tokio_stream::StreamExt;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tonic::{service::Routes, transport::Channel};

#[derive(Default, Deserialize)]
struct NamespaceQuery {
    parent: Option<String>,
}

#[tokio::test]
async fn discovers_only_the_selected_catalog_through_flight_sql() {
    let store = CatalogStore::new(
        Arc::new(InMemorySecretStore::new()),
        Arc::new(InMemory::new()),
    );
    let mut fixtures = Vec::new();
    for (name, root) in [("first", "analytics"), ("second", "finance")] {
        let (files, metadata) = table_fixture(root, if name == "first" { 0 } else { 100 }).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = uri.clone();
        let router = Router::new()
            .route(
                "/warehouse/{*path}",
                get(move |axum::extract::Path(path): axum::extract::Path<String>, headers, method| {
                    read_object(files.clone(), path, headers, method)
                }),
            )
            .route(
                "/v1/config",
                get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
            )
            .route(
                "/v1/namespaces",
                get(move |Query(query): Query<NamespaceQuery>| async move {
                    let namespaces = match query.parent.as_deref() {
                        None => vec![vec![root]],
                        Some(parent) if parent == root => vec![vec![root, "events"]],
                        _ => vec![],
                    };
                    Json(json!({"namespaces": namespaces}))
                }),
            )
            .route(
                "/v1/namespaces/{namespace}/tables",
                get(
                    move |axum::extract::Path(namespace): axum::extract::Path<String>| async move {
                        let identifiers = if namespace == format!("{root}\u{1f}events") {
                            vec![json!({"namespace": [root, "events"], "name": "page_views"})]
                        } else {
                            vec![]
                        };
                        Json(json!({"identifiers": identifiers}))
                    },
                ),
            )
            .route(
                "/v1/namespaces/{namespace}/tables/{table}",
                get(move || {
                    let metadata = metadata.clone();
                    let endpoint = endpoint.clone();
                    async move {
                    Json(json!({
                        "metadata-location": format!("s3://warehouse/{root}/metadata.json"),
                        "metadata": metadata,
                        "config": {
                            "s3.endpoint": endpoint, "s3.region": "local",
                            "s3.access-key-id": "query-key", "s3.secret-access-key": "query-secret",
                            "s3.session-token": "query-token", "s3.path-style-access": "true"
                        }
                    }))
                    }
                }),
            );
        fixtures.push(AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        })));
        store
            .create(
                CatalogName::new(name).unwrap(),
                CatalogConfig::Rest(RestCatalogConfig {
                    uri,
                    warehouse: None,
                    properties: Default::default(),
                }),
            )
            .await
            .unwrap();
    }

    let clock = Arc::new(DefaultSystemClock::new());
    let (broker, scheduler) = ObjectStoreBroker::new(
        Arc::new(InMemory::new()),
        Path::from("queue.json"),
        "test".into(),
        ObjectStoreSchedulerOptions::default(),
        clock.clone(),
    )
    .await
    .unwrap()
    .run(CancellationToken::new());
    let _broker = AbortOnDropHandle::new(broker);
    let ingestor = Ingestor::new(store.clone(), clock, scheduler);
    let ct = CancellationToken::new();
    let router = Routes::new(service(store, ingestor.clone(), ct.clone())).into_axum_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let channel = Channel::from_shared(uri).unwrap().connect().await.unwrap();
    let mut client = FlightSqlServiceClient::new(channel);

    let error = client.get_catalogs().await.unwrap_err();
    assert_eq!(error_code(error), tonic::Code::InvalidArgument);
    assert_eq!(
        error_code(
            client
                .execute("SHOW TABLES".into(), None)
                .await
                .unwrap_err()
        ),
        tonic::Code::InvalidArgument
    );
    for value in ["not json", "{}", r#"{"catalog":"first"}"#] {
        client.set_header("query-metadata", value);
        assert_eq!(
            error_code(client.get_catalogs().await.unwrap_err()),
            tonic::Code::InvalidArgument
        );
    }
    client.set_header("query-metadata", r#"{"catalog":"catalogs/missing"}"#);
    assert_eq!(
        error_code(client.get_catalogs().await.unwrap_err()),
        tonic::Code::NotFound
    );

    for (name, root) in [("first", "analytics"), ("second", "finance")] {
        client.set_header(
            "query-metadata",
            format!(r#"{{"catalog":"catalogs/{name}"}}"#),
        );
        let info = client.get_catalogs().await.unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(strings(&batch, 0), ["default"]);

        let info = client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some("default".into()),
                db_schema_filter_pattern: Some(format!("{root}%")),
            })
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(strings(&batch, 0), ["default", "default"]);
        assert_eq!(
            strings(&batch, 1),
            [root.to_owned(), format!("{root}.events")]
        );

        let query = CommandGetTables {
            catalog: Some("default".into()),
            db_schema_filter_pattern: Some("%.events".into()),
            table_name_filter_pattern: Some("page_vie_s".into()),
            table_types: vec!["TABLE".into()],
            include_schema: true,
        };
        let info = client.get_tables(query.clone()).await.unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(strings(&batch, 0), ["default"]);
        assert_eq!(strings(&batch, 1), [format!("{root}.events")]);
        assert_eq!(strings(&batch, 2), ["page_views"]);
        assert_eq!(strings(&batch, 3), ["TABLE"]);
        let bytes = batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0);
        let schema = Schema::try_from(IpcMessage(bytes.to_vec().into())).unwrap();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());

        for query in [
            CommandGetTables {
                catalog: Some(name.into()),
                ..query.clone()
            },
            CommandGetTables {
                table_types: vec!["VIEW".into()],
                ..query.clone()
            },
            CommandGetTables {
                table_name_filter_pattern: Some("absent%".into()),
                ..query.clone()
            },
        ] {
            let info = client.get_tables(query).await.unwrap();
            assert_eq!(fetch(&mut client, info).await.num_rows(), 0);
        }
        let info = client
            .get_tables(CommandGetTables {
                include_schema: false,
                ..query
            })
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!((batch.num_rows(), batch.num_columns()), (1, 4));
        let info = client.get_table_types().await.unwrap();
        assert_eq!(strings(&fetch(&mut client, info).await, 0), ["TABLE"]);

        let info = client
            .execute(
                format!("DESCRIBE default.\"{root}.events\".page_views"),
                None,
            )
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(strings(&batch, 0), ["id", "value"]);
        assert_eq!(strings(&batch, 1), ["Int64", "Utf8"]);
        assert_eq!(strings(&batch, 2), ["NO", "YES"]);

        let info = client
            .execute(
                format!("SHOW COLUMNS FROM default.\"{root}.events\".page_views"),
                None,
            )
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(strings(&batch, 3), ["id", "value"]);
        assert_eq!(strings(&batch, 4), ["Int64", "Utf8"]);
        assert_eq!(strings(&batch, 5), ["NO", "YES"]);

        let offset = if name == "first" { 0 } else { 100 };
        let info = client
            .execute(
                format!("SELECT id, value FROM default.\"{root}.events\".page_views ORDER BY id"),
                None,
            )
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(
            longs(&batch, 0),
            [2 + offset, 5 + offset, 9 + offset, 12 + offset]
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("two"), None, Some("nine"), Some("twelve")]
        );

        let info = client
            .execute(
                format!("SELECT value FROM default.\"{root}.events\".page_views WHERE id > {} AND id < {} ORDER BY id", 4 + offset, 12 + offset),
                None,
            )
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [None, Some("nine")]
        );

        let info = client
            .execute(
                format!("SELECT count(*), sum(id) FROM default.\"{root}.events\".page_views"),
                None,
            )
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(longs(&batch, 0), [4]);
        assert_eq!(longs(&batch, 1), [28 + 4 * offset]);
        let info = client
            .execute(
                format!("SELECT count(*), sum(r.id - l.id) FROM default.\"{root}.events\".page_views l \
                         JOIN default.\"{root}.events\".page_views r ON l.id < r.id WHERE r.value IS NOT NULL"),
                None,
            )
            .await
            .unwrap();
        let batch = fetch(&mut client, info).await;
        assert_eq!(longs(&batch, 0), [5]);
        assert_eq!(longs(&batch, 1), [31]);
        let info = client
            .execute(
                format!(
                    "SELECT id FROM default.\"{root}.events\".page_views ORDER BY id DESC LIMIT 1"
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(longs(&fetch(&mut client, info).await, 0), [12 + offset]);
        let info = client
            .execute(
                format!("SELECT * FROM default.\"{root}.events\".page_views WHERE id < 0"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(fetch(&mut client, info).await.num_rows(), 0);
    }

    let info = client
        .execute("SELECT 7 * 6 AS answer".into(), None)
        .await
        .unwrap();
    let batch = fetch(&mut client, info).await;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    for (sql, code) in [
        ("SELECT FROM", tonic::Code::InvalidArgument),
        (
            "SELECT * FROM default.\"analytics.events\".page_views",
            tonic::Code::InvalidArgument,
        ),
        (
            "CREATE TABLE example (id INT)",
            tonic::Code::InvalidArgument,
        ),
        (
            "INSERT INTO default.\"finance.events\".page_views VALUES (200, 'new')",
            tonic::Code::InvalidArgument,
        ),
        (
            "SET datafusion.execution.batch_size = 1",
            tonic::Code::InvalidArgument,
        ),
    ] {
        assert_eq!(
            error_code(client.execute(sql.into(), None).await.unwrap_err()),
            code
        );
    }
    let info = client.execute("SELECT 1".into(), None).await.unwrap();
    client.set_header("query-metadata", "{}");
    assert_eq!(
        error_code(
            client
                .do_get(info.endpoint[0].ticket.clone().unwrap())
                .await
                .err()
                .unwrap()
        ),
        tonic::Code::InvalidArgument
    );
    client.set_header("query-metadata", r#"{"catalog":"catalogs/first"}"#);
    assert_eq!(
        error_code(
            client
                .do_get(info.endpoint[0].ticket.clone().unwrap())
                .await
                .err()
                .unwrap()
        ),
        tonic::Code::InvalidArgument
    );

    let ticket = TicketStatementQuery {
        statement_handle: b"invalid json".to_vec().into(),
    };
    assert_eq!(
        error_code(
            client
                .do_get(Ticket::new(ticket.as_any().encode_to_vec()))
                .await
                .err()
                .unwrap()
        ),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        error_code(
            client
                .execute("SELECT 1".into(), Some(vec![1].into()))
                .await
                .unwrap_err()
        ),
        tonic::Code::Unimplemented
    );

    let info = client.get_catalogs().await.unwrap();
    client.set_header("query-metadata", "{}");
    assert_eq!(
        error_code(
            client
                .do_get(info.endpoint[0].ticket.clone().unwrap())
                .await
                .err()
                .unwrap()
        ),
        tonic::Code::InvalidArgument
    );

    // Ingestion uses its existing app-metadata, not the query-metadata header.
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let mut message: FlightData = SchemaAsIpc::new(&schema, &Default::default()).into();
    let type_url = b"se.ingestion.v1.Ingest";
    let mut command = vec![0x0a, u8::try_from(type_url.len()).unwrap()];
    command.extend_from_slice(type_url);
    message.flight_descriptor = Some(FlightDescriptor::new_cmd(command));
    message.app_metadata = serde_json::to_vec(&json!({
        "request_id": 0, "catalog": "first",
        "namespace": ["analytics", "events"], "table_name": "page_views"
    }))
    .unwrap()
    .into();
    let mut responses = client.do_put(tokio_stream::iter([message])).await.unwrap();
    let ack = responses.message().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ack.app_metadata).unwrap(),
        json!({"request_id": 0})
    );
    assert!(responses.message().await.unwrap().is_none());
    ingestor.close().await.unwrap();

    ct.cancel();
    client.set_header("query-metadata", r#"{"catalog":"catalogs/first"}"#);
    assert_eq!(
        error_code(client.execute("SELECT 1".into(), None).await.unwrap_err()),
        tonic::Code::Cancelled
    );
}

async fn read_object(
    files: FileIO,
    path: String,
    headers: HeaderMap,
    method: Method,
) -> Response<Body> {
    assert!(
        headers["authorization"]
            .to_str()
            .unwrap()
            .contains("Credential=query-key/")
    );
    assert_eq!(headers["x-amz-security-token"], "query-token");
    let bytes = files
        .new_input(format!("s3://warehouse/{path}"))
        .unwrap()
        .read()
        .await
        .unwrap();
    let size = bytes.len();
    let (status, bytes, range) = if let Some(range) = headers.get("range") {
        let range = range.to_str().unwrap().strip_prefix("bytes=").unwrap();
        let (start, end) = range.split_once('-').unwrap();
        let start: usize = start.parse().unwrap();
        let end = if end.is_empty() {
            size - 1
        } else {
            end.parse::<usize>().unwrap().min(size - 1)
        };
        (
            StatusCode::PARTIAL_CONTENT,
            bytes.slice(start..=end),
            Some(format!("bytes {start}-{end}/{size}")),
        )
    } else {
        (StatusCode::OK, bytes, None)
    };
    let mut response = Response::builder()
        .status(status)
        .header("content-length", bytes.len())
        .header("accept-ranges", "bytes");
    if let Some(range) = range {
        response = response.header("content-range", range);
    }
    response
        .body(if method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(bytes)
        })
        .unwrap()
}

async fn table_fixture(root: &str, offset: i64) -> (FileIO, serde_json::Value) {
    let location = format!("s3://warehouse/{root}");
    let schema_json = json!({"type": "struct", "schema-id": 0, "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "value", "required": false, "type": "string"}
    ]});
    let schema: iceberg::spec::Schema = serde_json::from_value(schema_json.clone()).unwrap();
    let schema = Arc::new(schema);
    let batch = RecordBatch::try_new(
        Arc::new(schema_to_arrow_schema(&schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(vec![
                2 + offset,
                5 + offset,
                9 + offset,
                12 + offset,
            ])),
            Arc::new(StringArray::from(vec![
                Some("two"),
                None,
                Some("nine"),
                Some("twelve"),
            ])),
        ],
    )
    .unwrap();
    let files = FileIOBuilder::new(Arc::new(MemoryStorageFactory)).build();
    let mut manifest = ManifestWriterBuilder::new(
        files
            .new_output(format!("{location}/manifest.avro"))
            .unwrap(),
        Some(1),
        schema.clone(),
        PartitionSpec::unpartition_spec(),
    )
    .build_v1();
    for index in 0..2 {
        let mut writer = ParquetWriterBuilder::new(Default::default(), schema.clone())
            .build(
                files
                    .new_output(format!("{location}/{index}.parquet"))
                    .unwrap(),
            )
            .await
            .unwrap();
        writer.write(&batch.slice(index * 2, 2)).await.unwrap();
        for mut file in writer.close().await.unwrap() {
            manifest
                .add_file(
                    file.partition_spec_id(0)
                        .partition(Struct::empty())
                        .build()
                        .unwrap(),
                    0,
                )
                .unwrap();
        }
    }
    let manifest = manifest.write_manifest_file().await.unwrap();
    let list_path = format!("{location}/manifest-list.avro");
    let mut list = ManifestListWriter::v1(
        files
            .new_output(&list_path)
            .unwrap()
            .writer()
            .await
            .unwrap(),
        1,
        None,
    );
    list.add_manifests([manifest].into_iter()).unwrap();
    list.close().await.unwrap();
    (
        files,
        json!({
            "format-version": 1,
            "table-uuid": "00000000-0000-0000-0000-000000000007",
            "location": location,
            "last-updated-ms": 1, "last-column-id": 2,
            "schema": schema_json, "current-schema-id": 0,
            "partition-spec": [], "default-spec-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}], "default-sort-order-id": 0,
            "properties": {}, "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1, "timestamp-ms": 1, "schema-id": 0, "manifest-list": list_path,
                "summary": {"operation": "append"}}]
        }),
    )
}

async fn fetch(client: &mut FlightSqlServiceClient<Channel>, info: FlightInfo) -> RecordBatch {
    let expected_schema = Schema::try_from(info.clone()).unwrap();
    let mut stream = client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    assert_eq!(stream.schema().unwrap().as_ref(), &expected_schema);
    let batch = concat_batches(&Arc::new(expected_schema.clone()), &batches).unwrap();
    assert_eq!(batch.schema().as_ref(), &expected_schema);
    batch
}

fn longs(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn strings(batch: &RecordBatch, column: usize) -> Vec<&str> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(Option::unwrap)
        .collect()
}

fn error_code(error: arrow_flight::error::FlightError) -> tonic::Code {
    match error {
        arrow_flight::error::FlightError::Tonic(status) => status.code(),
        // PANIC: a non-gRPC error indicates an unexpected client or transport failure.
        other => panic!("unexpected error: {other}"),
    }
}
