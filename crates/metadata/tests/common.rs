use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use iceberg::{
    TableIdent,
    spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    },
};
use se_metadata::{
    CHECKPOINT_INTERVAL_PROPERTY, CatalogConfig, CatalogId, Metadata, RestCatalogConfig,
};
use serde_json::json;
use uuid::Uuid;

struct Server(tokio::task::JoinHandle<()>);

/// Resolves a test table through an ephemeral Iceberg REST catalog.
pub async fn resolve(
    metadata: &Metadata,
    catalog: &CatalogId,
    uuid: Uuid,
) -> Result<TableIdent, Box<dyn std::error::Error>> {
    resolve_with_properties(
        metadata,
        catalog,
        uuid,
        HashMap::from([(CHECKPOINT_INTERVAL_PROPERTY.into(), "15".into())]),
    )
    .await
}

/// Resolves a test table with the supplied Iceberg table properties.
pub async fn resolve_with_properties(
    metadata: &Metadata,
    catalog: &CatalogId,
    uuid: Uuid,
    properties: HashMap<String, String>,
) -> Result<TableIdent, Box<dyn std::error::Error>> {
    let router = Router::new()
        .route(
            "/v1/config",
            get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
        )
        .route("/v1/namespaces/{namespace}/tables/{table}", get(load_table))
        .with_state(properties);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = CatalogConfig::Rest(RestCatalogConfig {
        uri: format!("http://{}", listener.local_addr()?),
        warehouse: None,
        properties: HashMap::new(),
    });
    let _server = Server(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    if metadata.get_catalog(catalog).await?.is_some() {
        metadata.update_catalog(catalog, config).await?;
    } else {
        metadata.create_catalog(catalog.clone(), config).await?;
    }
    let ident = TableIdent::from_strs(["test", &uuid.to_string()])?;
    assert_eq!(metadata.resolve_table(catalog, &ident).await?.uuid(), uuid);
    Ok(ident)
}

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn load_table(
    Path((_, table)): Path<(String, String)>,
    State(properties): State<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "value",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
        .unwrap();
    let metadata = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "file:///tmp/table".into(),
        FormatVersion::V2,
        properties,
    )
    .unwrap()
    .assign_uuid(Uuid::parse_str(&table).unwrap())
    .build()
    .unwrap()
    .metadata;
    Json(
        json!({"metadata": metadata, "metadata-location": "file:///tmp/table/metadata.json", "config": {}}),
    )
}
