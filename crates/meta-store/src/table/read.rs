use std::future::Future;

use iceberg::spec::{Schema, SortOrder};
use slatedb::DbReadOps;
use uuid::Uuid;

use super::{
    Result,
    encoding::{
        CurrentSchemaIdKey, CurrentSchemaIdValue, DefaultSortOrderIdKey, DefaultSortOrderIdValue,
        SchemaKey, SchemaValue, SortOrderKey, SortOrderValue, TableUuidKey, TableUuidValue,
    },
};

/// Typed table reads. Use a snapshot or transaction for consistent multi-read operations.
pub trait TableRead: DbReadOps + Sync {
    /// Returns the schema stored under the given ID, or `None` if absent.
    fn get_schema(&self, id: i32) -> impl Future<Output = Result<Option<Schema>>> + Send {
        async move {
            self.get(SchemaKey { id }.encode())
                .await?
                .map(|value| SchemaValue::decode(&value))
                .transpose()
        }
    }

    /// Returns the sort order stored under the given ID, or `None` if absent.
    fn get_sort_order(&self, id: i64) -> impl Future<Output = Result<Option<SortOrder>>> + Send {
        async move {
            self.get(SortOrderKey { id }.encode())
                .await?
                .map(|value| SortOrderValue::decode(&value))
                .transpose()
        }
    }

    /// Returns the current schema ID, or `None` if uninitialized.
    fn get_current_schema_id(&self) -> impl Future<Output = Result<Option<i32>>> + Send {
        async {
            self.get(CurrentSchemaIdKey::encode())
                .await?
                .map(|value| CurrentSchemaIdValue::decode(&value).map(|value| value.id))
                .transpose()
        }
    }

    /// Returns the default sort order ID, or `None` if uninitialized.
    fn get_default_sort_order_id(&self) -> impl Future<Output = Result<Option<i64>>> + Send {
        async {
            self.get(DefaultSortOrderIdKey::encode())
                .await?
                .map(|value| DefaultSortOrderIdValue::decode(&value).map(|value| value.id))
                .transpose()
        }
    }

    /// Returns the table UUID, or `None` if it has not been initialized.
    fn get_table_uuid(&self) -> impl Future<Output = Result<Option<Uuid>>> + Send {
        async {
            self.get(TableUuidKey::encode())
                .await?
                .map(|value| TableUuidValue::decode(&value).map(|value| value.uuid))
                .transpose()
        }
    }
}

impl<T: DbReadOps + Sync + ?Sized> TableRead for T {}
