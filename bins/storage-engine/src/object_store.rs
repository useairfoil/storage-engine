use std::sync::Arc;

use clap::{Args, ValueEnum};
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{
    Error, ErrorKind, Operator,
    layers::{RetryLayer, TimeoutLayer},
    services::S3,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ObjectStoreType {
    S3,
}

#[derive(Debug, Clone, Args)]
pub struct ObjectStoreArgs {
    /// Specifies the OpenDAL service to use. Service configuration is read from the environment.
    #[arg(
        long = "object-store.type",
        default_value = "s3",
        env = "SE_OBJECT_STORE_TYPE"
    )]
    pub opendal_service: ObjectStoreType,
}

impl ObjectStoreArgs {
    pub fn create_object_store(&self) -> Result<Arc<dyn ObjectStore>, opendal::Error> {
        let operator = match self.opendal_service {
            ObjectStoreType::S3 => Operator::from_iter::<S3>(collect_config_from_env("SE_S3_"))?,
        };

        let operator = operator
            .layer(TimeoutLayer::default())
            .layer(RetryLayer::default().with_jitter());

        check_capabilities(&operator)?;

        Ok(Arc::new(OpendalStore::new(operator)))
    }
}

fn check_capabilities(operator: &Operator) -> Result<(), Error> {
    let capabilities = operator.info().capability();
    let mut missing = Vec::new();

    for (supported, name) in [
        (capabilities.read, "read"),
        (capabilities.write, "write"),
        (capabilities.stat, "stat"),
        (
            capabilities.write_with_if_not_exists,
            "write_with_if_not_exists",
        ),
        (capabilities.write_with_if_match, "write_with_if_match"),
        (capabilities.list, "list"),
        (capabilities.list_with_recursive, "list_with_recursive"),
        (capabilities.delete, "delete"),
    ] {
        if !supported {
            missing.push(name);
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::Unsupported,
            format!(
                "object store is missing required capabilities: {}",
                missing.join(", ")
            ),
        )
        .with_operation("check_capabilities")
        .with_context("service", operator.info().scheme()))
    }
}

fn collect_config_from_env(prefix: &str) -> Vec<(String, String)> {
    std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix(prefix)
                .map(|field| (field.to_ascii_lowercase(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use opendal::services::Memory;

    use super::*;

    #[test]
    fn s3_advertises_required_capabilities() {
        let operator = Operator::new(
            S3::default()
                .bucket("test")
                .region("us-east-1")
                .disable_config_load(),
        )
        .unwrap();

        check_capabilities(&operator).unwrap();
    }

    #[test]
    fn rejects_missing_conditional_update() {
        let operator = Operator::new(Memory::default()).unwrap();

        let error = check_capabilities(&operator).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(error.to_string().contains("write_with_if_match"));
    }
}
