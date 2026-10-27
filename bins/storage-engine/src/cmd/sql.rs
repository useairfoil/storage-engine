use std::sync::Arc;

use arrow_flight::{error::FlightError, sql::client::FlightSqlServiceClient};
use clap::Args;
use datafusion::arrow::{
    datatypes::Schema, error::ArrowError, record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use se_metadata::CatalogId;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

#[derive(Debug, Args)]
pub struct SqlArgs {
    /// ID of the catalog to query.
    #[arg(long)]
    catalog_id: String,

    /// URL of the Flight SQL server.
    #[arg(long, default_value = "http://127.0.0.1:7777")]
    server_address: String,

    /// SQL query to execute.
    query: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid catalog id: {0}")]
    InvalidCatalogId(String),

    #[error("flight connection failed: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("flight SQL request failed: {0}")]
    Flight(#[from] FlightError),

    #[error("invalid query metadata: {0}")]
    Metadata(#[from] serde_json::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),

    #[error("server returned an endpoint without a ticket")]
    MissingTicket,
}

impl SqlArgs {
    /// Executes a Flight SQL query and prints its results as a table.
    pub async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        let query = async {
            let catalog = CatalogId::new(&self.catalog_id)
                .map_err(|_| Error::InvalidCatalogId(self.catalog_id.clone()))?;
            let metadata =
                serde_json::to_string(&serde_json::json!({ "catalog": catalog.as_str() }))?;

            let channel = Endpoint::from_shared(self.server_address)?
                .connect()
                .await?;
            let mut client = FlightSqlServiceClient::new(channel);
            client.set_header("query-metadata", metadata);

            let info = client.execute(self.query, None).await?;
            let schema = Arc::new(Schema::try_from(info.clone())?);
            let mut batches = Vec::new();

            for endpoint in info.endpoint {
                let ticket = endpoint.ticket.ok_or(Error::MissingTicket)?;
                let mut stream = client.do_get(ticket).await?;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
            }

            if batches.is_empty() {
                batches.push(RecordBatch::new_empty(schema));
            }

            println!("{}", pretty_format_batches(&batches)?);

            Ok(())
        };

        tokio::select! {
            _ = ct.cancelled() => Ok(()),
            result = query => result,
        }
    }
}
