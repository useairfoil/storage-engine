use iceberg::spec::{Schema, SortOrder};
use slatedb::DbTransactionOps;
use uuid::Uuid;

use super::{
    Result,
    encoding::{
        CurrentSchemaIdKey, CurrentSchemaIdValue, DefaultSortOrderIdKey, DefaultSortOrderIdValue,
        SchemaKey, SchemaValue, SortOrderKey, SortOrderValue, TableUuidKey, TableUuidValue,
    },
};

/// Typed table mutations staged in a transaction.
pub trait TableWrite: DbTransactionOps {
    /// Stages a schema under the given ID until commit.
    fn put_schema(&self, id: i32, schema: &Schema) -> Result<()> {
        self.put(SchemaKey { id }.encode(), SchemaValue::encode(schema)?)?;
        Ok(())
    }

    /// Stages a sort order under the given ID until commit.
    fn put_sort_order(&self, id: i64, sort_order: &SortOrder) -> Result<()> {
        self.put(
            SortOrderKey { id }.encode(),
            SortOrderValue::encode(sort_order)?,
        )?;
        Ok(())
    }

    /// Stages the current schema ID until commit.
    fn put_current_schema_id(&self, id: i32) -> Result<()> {
        self.put(
            CurrentSchemaIdKey::encode(),
            CurrentSchemaIdValue { id }.encode(),
        )?;
        Ok(())
    }

    /// Stages the default sort order ID until commit.
    fn put_default_sort_order_id(&self, id: i64) -> Result<()> {
        self.put(
            DefaultSortOrderIdKey::encode(),
            DefaultSortOrderIdValue { id }.encode(),
        )?;
        Ok(())
    }

    /// Stages the table UUID. The caller must commit the transaction to persist it.
    fn put_table_uuid(&self, uuid: Uuid) -> Result<()> {
        self.put(TableUuidKey::encode(), TableUuidValue { uuid }.encode())?;
        Ok(())
    }
}

impl<T: DbTransactionOps + ?Sized> TableWrite for T {}
