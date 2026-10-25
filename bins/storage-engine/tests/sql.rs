use std::sync::Arc;

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{Any, CommandStatementQuery, SqlInfo, server::FlightSqlService},
};
use datafusion::arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use tokio::process::Command;
use tokio_stream::StreamExt;
use tokio_util::task::AbortOnDropHandle;
use tonic::{Request, Response, Status, service::Routes};

struct QueryServer;

#[tokio::test]
async fn sql_command_sends_catalog_metadata_and_prints_all_results() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let router = Routes::new(FlightServiceServer::new(QueryServer)).into_axum_router();
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));

    let output = Command::new(env!("CARGO_BIN_EXE_se"))
        .args([
            "sql",
            "--catalog-id",
            "warehouse",
            "--server-address",
            &address,
            "SELECT id FROM events",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "+----+\n| id |\n+----+\n| 17 |\n| 42 |\n| 17 |\n| 42 |\n+----+\n"
    );

    let output = Command::new(env!("CARGO_BIN_EXE_se"))
        .args([
            "sql",
            "--catalog-id",
            "warehouse",
            "--server-address",
            &address,
            "unsupported",
        ])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("query execution is not implemented")
    );

    let output = Command::new(env!("CARGO_BIN_EXE_se"))
        .args([
            "sql",
            "--catalog-id",
            "warehouse",
            "--server-address",
            &address,
            "empty",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "+----+\n| id |\n+----+\n+----+\n"
    );
}

#[tokio::test]
async fn sql_command_requires_catalog_and_query_and_defaults_to_localhost() {
    for args in [
        vec!["sql", "SELECT 1"],
        vec!["sql", "--catalog-id", "warehouse"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_se"))
            .args(args)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("required")
        );
    }
    let output = Command::new(env!("CARGO_BIN_EXE_se"))
        .args(["sql", "--help"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("http://127.0.0.1:7777")
    );
    let output = Command::new(env!("CARGO_BIN_EXE_se"))
        .args(["sql", "--catalog-id", "invalid/id", "SELECT 1"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("InvalidCatalogId")
    );
}

#[tonic::async_trait]
impl FlightSqlService for QueryServer {
    type FlightService = Self;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        check_metadata(&request);
        if query.query == "unsupported" {
            return Err(Status::unimplemented("query execution is not implemented"));
        }
        let info = FlightInfo::new().try_with_schema(&schema()).unwrap();
        if query.query == "empty" {
            return Ok(Response::new(info));
        }
        assert_eq!(query.query, "SELECT id FROM events");
        let ticket = Ticket::new(request.into_inner().cmd);
        Ok(Response::new(info.with_endpoints(vec![
            FlightEndpoint::new().with_ticket(ticket.clone()),
            FlightEndpoint::new().with_ticket(ticket),
        ])))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        check_metadata(&request);
        let batches = [17, 42].map(|id| {
            Ok(RecordBatch::try_new(
                Arc::new(schema()),
                vec![Arc::new(Int64Array::from(vec![id]))],
            )
            .unwrap())
        });
        let output = FlightDataEncoderBuilder::new()
            .with_schema(Arc::new(schema()))
            .build(tokio_stream::iter(batches))
            .map(|result| result.map_err(Status::from));
        Ok(Response::new(Box::pin(output)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

fn schema() -> Schema {
    Schema::new(vec![Field::new("id", DataType::Int64, false)])
}

fn check_metadata<T>(request: &Request<T>) {
    let metadata = request.metadata().get("query-metadata").unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(metadata.as_encoded_bytes()).unwrap(),
        serde_json::json!({"catalog": "catalogs/warehouse"})
    );
}
