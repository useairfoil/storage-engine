use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use se_metadata_client::{CatalogId, MetadataClient};
use se_wal::{BatchCommit, BatchMetadata, BatchRead, Error, InMemoryWal};
use serde_json::Value;
use tokio::{net::TcpListener, sync::Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type Wals = Arc<Mutex<HashMap<(String, String), InMemoryWal>>>;

#[tokio::test]
async fn should_scope_wal_actions_and_preserve_fencing_over_http() {
    let router = Router::new()
        .route("/api/catalogs/{id}/tables/{uuid}/wal", post(wal_action))
        .with_state(Wals::default())
        .fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future(),
    );
    let metadata = MetadataClient::new(format!("http://{address}/api/").parse().unwrap());
    let catalog = CatalogId::new("catalog with spaces").unwrap();
    let table = Uuid::from_u128(42);
    let factory: &dyn se_wal::WalClientFactory = &metadata;
    let wal = factory.wal_client(catalog.clone(), table);
    let batch: BatchMetadata = serde_json::from_value(serde_json::json!({
        "schema_id": -17,
        "location": "wal/first.batch",
        "timestamp": "2023-11-14T22:13:20.123Z"
    }))
    .unwrap();

    wal.commit(batch.clone()).await.unwrap();
    let mut second = batch.clone();
    second.location = "wal/second.batch".into();
    wal.commit(second.clone()).await.unwrap();

    let first_epoch = wal.claim_reader().await.unwrap();
    let epoch = wal.clone().claim_reader().await.unwrap();
    assert_eq!((first_epoch, epoch), (1, 2));
    let descriptors = wal.read_after(None, 1).await.unwrap();
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].sequence, 0);
    assert_eq!(descriptors[0].metadata, batch);
    let descriptors = wal.read_after(Some(0), 8).await.unwrap();
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].sequence, 1);
    assert_eq!(descriptors[0].metadata, second);

    insta::assert_compact_debug_snapshot!(wal.ack_through(first_epoch, 1).await.unwrap_err(), @"Fenced");
    assert_eq!(wal.read_after(None, 8).await.unwrap().len(), 2);
    for other in [
        metadata.wal_client(catalog, Uuid::from_u128(43)),
        metadata.wal_client(CatalogId::new("another-catalog").unwrap(), table),
    ] {
        assert_eq!(other.read_after(None, 8).await.unwrap().len(), 0);
        assert_eq!(other.claim_reader().await.unwrap(), 1);
    }

    wal.ack_through(epoch, 0).await.unwrap();
    assert_eq!(wal.read_after(None, 8).await.unwrap()[0].sequence, 1);
    wal.ack_through(epoch, 1).await.unwrap();
    assert_eq!(wal.read_after(None, 8).await.unwrap().len(), 0);

    let missing = MetadataClient::new(format!("http://{address}/missing/").parse().unwrap())
        .wal_client(CatalogId::new("catalog").unwrap(), table);
    assert!(matches!(
        missing.claim_reader().await,
        Err(Error::Commit(_))
    ));

    shutdown.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn commits_to_metadata_server_are_durable() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = slatedb::Db::open("metadata", store.clone()).await?;
    let metadata = se_metadata_client::CatalogId::new("catalog with spaces")?;
    let service = se_metadata::Metadata::from_db(
        db.clone(),
        Arc::new(se_secret_store::memory::InMemorySecretStore::new()),
        Arc::new(se_common::clock::DefaultSystemClock::new()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(
        axum::serve(listener, se_metadata_server::router(service.clone()))
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future(),
    );
    let client = MetadataClient::new(format!("http://{address}/").parse()?);
    let table = Uuid::from_u128(42);
    let wal = client.wal_client(metadata, table);
    let batch: BatchMetadata = serde_json::from_value(serde_json::json!({
        "schema_id": -17,
        "location": "wal/first.batch",
        "timestamp": "2023-11-14T22:13:20.123Z"
    }))?;

    wal.commit(batch.clone()).await?;
    let mut second = batch;
    second.location = "wal/second.batch".into();
    wal.commit(second).await?;

    let first = wal.read_after(None, 1).await?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].sequence, 0);
    assert_eq!(first[0].metadata.location, "wal/first.batch");
    let second = wal.read_after(Some(0), 8).await?;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].sequence, 1);
    assert_eq!(second[0].metadata.location, "wal/second.batch");
    assert_eq!(wal.read_after(None, 1).await?, first);
    insta::assert_debug_snapshot!(wal.read_after(Some(1), 8).await?, @"[]");
    insta::assert_debug_snapshot!(wal.read_after(None, 0).await?, @"[]");
    insta::assert_debug_snapshot!(wal.read_after(Some(u64::MAX), 8).await?, @"[]");
    let other = client.wal_client(CatalogId::new("another")?, table);
    insta::assert_debug_snapshot!(other.read_after(None, 8).await?, @"[]");
    let other = client.wal_client(CatalogId::new("catalog with spaces")?, Uuid::from_u128(43));
    insta::assert_debug_snapshot!(other.read_after(None, 8).await?, @"[]");

    let url = format!("http://{address}/catalogs/catalog%20with%20spaces/tables/{table}/wal");
    let http = reqwest::Client::new();
    let response = http
        .post(&url)
        .json(&serde_json::json!({"action": "claim_reader"}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<u64>().await?, 1);
    let response = http
        .post(&url)
        .json(&serde_json::json!({"action": "commit"}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response = http
        .post(url.replace(&table.to_string(), "not-a-uuid"))
        .json(&serde_json::json!({"action": "claim_reader"}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    service.close().await?;
    let response = http.post(&url).json(&serde_json::json!({
        "action": "commit",
        "batch": {"schema_id": 3, "location": "secret-location", "timestamp": "2023-11-14T22:13:20Z"}
    })).send().await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.json::<Value>().await?,
        serde_json::json!({"error": "internal server error"})
    );

    shutdown.cancel();
    server.await??;

    let db = slatedb::Db::open("metadata", store).await?;
    let state_key = [
        b"\x01\x02catalog with spaces\0".as_slice(),
        table.as_bytes(),
    ]
    .concat();
    assert_eq!(
        db.get(state_key)
            .await?
            .ok_or("missing WAL state")?
            .as_ref(),
        [2_u64.to_le_bytes(), 1_u64.to_le_bytes()].concat()
    );

    for (sequence, location) in [(0_u64, "wal/first.batch"), (1, "wal/second.batch")] {
        let key = [
            b"\x01\x03catalog with spaces\0".as_slice(),
            table.as_bytes(),
            &sequence.to_be_bytes(),
        ]
        .concat();
        let value = db.get(key).await?.ok_or("missing committed batch")?;
        let expected = [
            (-17_i32).to_le_bytes().as_slice(),
            &1_700_000_000_i64.to_le_bytes(),
            &123_000_000_u32.to_le_bytes(),
            location.as_bytes(),
            b"\0",
        ]
        .concat();
        assert_eq!(value.as_ref(), expected);
    }

    db.close().await?;

    Ok(())
}

#[tokio::test]
async fn metadata_acks_are_fenced_inclusive_idempotent_and_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let secrets = Arc::new(se_secret_store::memory::InMemorySecretStore::new());
    let clock = Arc::new(se_common::clock::DefaultSystemClock::new());
    let service =
        se_metadata::Metadata::new("metadata", store.clone(), secrets.clone(), clock.clone())
            .await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(
        axum::serve(listener, se_metadata_server::router(service.clone()))
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future(),
    );
    let client = MetadataClient::new(format!("http://{address}").parse()?);
    let id = CatalogId::new("catalog")?;
    let table = Uuid::from_u128(42);
    let wal = client.wal_client(id.clone(), table);
    let other = client.wal_client(id.clone(), Uuid::from_u128(43));
    insta::assert_compact_debug_snapshot!(wal.ack_through(0, 0).await.err(), @"Some(Fenced)");

    let (first, second) = tokio::try_join!(wal.claim_reader(), wal.claim_reader())?;
    let mut epochs = [first, second];
    epochs.sort();
    assert_eq!(epochs, [1, 2]);
    let epoch = epochs[1];
    let url = format!("http://{address}/catalogs/catalog/tables/{table}/wal");
    let response = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({
            "action": "ack_through", "epoch": epoch, "sequence": 0
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let batch: BatchMetadata = serde_json::from_value(serde_json::json!({
        "schema_id": 3, "location": "not-fetched", "timestamp": "2023-11-14T22:13:20Z"
    }))?;

    for _ in 0..3 {
        wal.commit(batch.clone()).await?;
    }
    other.commit(batch.clone()).await?;
    insta::assert_compact_debug_snapshot!(wal.ack_through(epochs[0], 1).await.err(), @"Some(Fenced)");
    let response = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({
            "action": "ack_through", "epoch": epoch, "sequence": 3
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    insta::assert_debug_snapshot!(wal.ack_through(epoch, u64::MAX).await.is_err(), @"true");
    assert_eq!(wal.read_after(None, 8).await?.len(), 3);

    wal.ack_through(epoch, 0).await?;
    wal.ack_through(epoch, 0).await?;
    let remaining = wal.read_after(None, 8).await?;
    assert_eq!(
        remaining
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(other.read_after(None, 8).await?.len(), 1);
    wal.ack_through(epoch, 2).await?;
    wal.ack_through(epoch, 2).await?;
    insta::assert_debug_snapshot!(wal.ack_through(epoch, 3).await.is_err(), @"true");
    insta::assert_debug_snapshot!(wal.read_after(None, 8).await?, @"[]");

    let next_epoch = wal.claim_reader().await?;
    assert_eq!(next_epoch, 3);
    insta::assert_compact_debug_snapshot!(wal.ack_through(epoch, 2).await.err(), @"Some(Fenced)");
    wal.commit(batch.clone()).await?;
    assert_eq!(wal.read_after(None, 8).await?[0].sequence, 3);
    wal.ack_through(next_epoch, 3).await?;

    shutdown.cancel();
    server.await??;
    service.close().await?;

    let reopened = se_metadata::Metadata::new("metadata", store, secrets, clock).await?;
    insta::assert_debug_snapshot!(reopened.wal_read_after(&id, table, None, 8).await?, @"[]");
    assert_eq!(reopened.wal_claim_reader(&id, table).await?, 4);
    reopened
        .wal_append(
            &id,
            table,
            batch.schema_id,
            &batch.location,
            batch.timestamp,
        )
        .await?;
    assert_eq!(
        reopened.wal_read_after(&id, table, None, 8).await?[0].sequence,
        4
    );
    reopened.close().await?;

    Ok(())
}

async fn wal_action(
    State(wals): State<Wals>,
    Path(key): Path<(String, String)>,
    Json(request): Json<Value>,
) -> Response {
    let wal = wals.lock().await.entry(key).or_default().clone();
    let result = match request["action"].as_str().unwrap() {
        "commit" => wal
            .commit(serde_json::from_value(request["batch"].clone()).unwrap())
            .await
            .map(|()| StatusCode::NO_CONTENT.into_response()),
        "claim_reader" => wal
            .claim_reader()
            .await
            .map(|epoch| Json(epoch).into_response()),
        "read_after" => {
            // Reads must not require an ownership token.
            assert_eq!(request.get("epoch"), None);
            wal.read_after(
                request["sequence"].as_u64(),
                request["max"].as_u64().unwrap() as usize,
            )
            .await
            .map(|batches| Json(batches).into_response())
        }
        "ack_through" => wal
            .ack_through(
                request["epoch"].as_u64().unwrap(),
                request["sequence"].as_u64().unwrap(),
            )
            .await
            .map(|()| StatusCode::NO_CONTENT.into_response()),
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };

    match result {
        Ok(response) => response,
        Err(Error::Fenced) => StatusCode::CONFLICT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
