use ::iceberg::spec::Schema;

use crate::{Error, Result};

pub(crate) trait SchemaExt {
    /// Returns identifier field IDs and types sorted by field ID.
    fn stable_identifiers(&self) -> Result<Vec<(i32, String)>>;
}

impl SchemaExt for Schema {
    fn stable_identifiers(&self) -> Result<Vec<(i32, String)>> {
        let mut ids = self.identifier_field_ids().collect::<Vec<_>>();
        ids.sort_unstable();

        ids.into_iter()
            .map(|id| {
                let field = self.field_by_id(id).ok_or(Error::InvalidIndexState)?;
                Ok((id, field.field_type.to_string()))
            })
            .collect()
    }
}
