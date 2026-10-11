use iceberg::spec::Schema;

/// The storage engine's write semantics for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMode {
    Append,
    Upsert,
}

/// Storage engine extensions for Iceberg schemas.
pub trait SchemaExt {
    /// Tables with identifier fields use upserts; other tables use appends.
    fn table_mode(&self) -> TableMode;
}

impl SchemaExt for Schema {
    fn table_mode(&self) -> TableMode {
        if self.identifier_field_ids().next().is_some() {
            TableMode::Upsert
        } else {
            TableMode::Append
        }
    }
}
