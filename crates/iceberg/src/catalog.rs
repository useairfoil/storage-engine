//! Catalog tracing shared by all callers.

use std::{collections::HashMap, future::Future, sync::Arc};

use async_trait::async_trait;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
    table::Table,
};

/// Adds one operation span to each call into an Iceberg catalog.
/// Arguments and results are passed through unchanged; errors are logged.
#[derive(Debug, Clone)]
pub struct TracedCatalog {
    inner: Arc<dyn Catalog>,
}

impl TracedCatalog {
    pub fn new(inner: Arc<dyn Catalog>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Catalog for TracedCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        trace_operation("list_namespaces", self.inner.list_namespaces(parent)).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        trace_operation(
            "create_namespace",
            self.inner.create_namespace(namespace, properties),
        )
        .await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        trace_operation("get_namespace", self.inner.get_namespace(namespace)).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        trace_operation("namespace_exists", self.inner.namespace_exists(namespace)).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        trace_operation(
            "update_namespace",
            self.inner.update_namespace(namespace, properties),
        )
        .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        trace_operation("drop_namespace", self.inner.drop_namespace(namespace)).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        trace_operation("list_tables", self.inner.list_tables(namespace)).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        trace_operation("create_table", self.inner.create_table(namespace, creation)).await
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        trace_operation("load_table", self.inner.load_table(table)).await
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        trace_operation("drop_table", self.inner.drop_table(table)).await
    }

    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        trace_operation("purge_table", self.inner.purge_table(table)).await
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        trace_operation("table_exists", self.inner.table_exists(table)).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        trace_operation("rename_table", self.inner.rename_table(src, dest)).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        trace_operation(
            "register_table",
            self.inner.register_table(table, metadata_location),
        )
        .await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        trace_operation("update_table", self.inner.update_table(commit)).await
    }
}

#[tracing::instrument(name = "catalog.operation", skip_all, fields(catalog.operation = operation), err)]
async fn trace_operation<T>(
    operation: &'static str,
    call: impl Future<Output = Result<T>>,
) -> Result<T> {
    call.await
}
