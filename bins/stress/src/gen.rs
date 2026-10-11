use std::{
    num::NonZeroU64,
    ops::RangeInclusive,
    sync::{Arc, Mutex},
};

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray, builder::LargeBinaryBuilder,
};
use arrow_schema::Schema as ArrowSchema;
use clap::ValueEnum;
use iceberg::{
    arrow::schema_to_arrow_schema,
    spec::{
        NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder,
        Transform, Type,
    },
};
use rand::{Rng, RngExt, rngs::StdRng};
use se_common::clock::SystemClock;
use se_iceberg::schema::TableMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SortColumn {
    Key,
    Ts,
}

pub struct DataGenerator {
    keys_cardinality: NonZeroU64,
    row_data_size_range: RangeInclusive<u64>,
    batch_size_range: RangeInclusive<u64>,
    schema: Schema,
    sort_order: SortOrder,
    arrow_schema: Arc<ArrowSchema>,
    clock: Arc<dyn SystemClock>,
    rng: Mutex<StdRng>,
}

impl DataGenerator {
    pub fn new(
        mode: TableMode,
        sort: Option<SortColumn>,
        keys_cardinality: NonZeroU64,
        row_data_size_range: RangeInclusive<u64>,
        batch_size_range: RangeInclusive<u64>,
        clock: Arc<dyn SystemClock>,
        rng: StdRng,
    ) -> iceberg::Result<Self> {
        let mut schema = Schema::builder().with_fields([
            NestedField::required(1, "key", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "data", Type::Primitive(PrimitiveType::Binary)).into(),
            NestedField::required(3, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
        ]);
        if mode == TableMode::Upsert {
            schema = schema.with_identifier_field_ids([1]);
        }
        let schema = schema.build()?;
        let sort_order = match sort {
            Some(column) => SortOrder::builder()
                .with_sort_field(SortField {
                    source_id: match column {
                        SortColumn::Key => 1,
                        SortColumn::Ts => 3,
                    },
                    transform: Transform::Identity,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::First,
                })
                .build(&schema)?,
            None => SortOrder::unsorted_order(),
        };
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);

        Ok(Self {
            keys_cardinality,
            row_data_size_range,
            batch_size_range,
            schema,
            sort_order,
            arrow_schema,
            clock,
            rng: Mutex::new(rng),
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn sort_order(&self) -> &SortOrder {
        &self.sort_order
    }

    pub fn arrow_schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }

    pub fn next_batch(&self) -> RecordBatch {
        // PANIC: The mutex is poisoned only if an earlier batch generation panicked.
        let mut rng = self.rng.lock().expect("data generator RNG mutex poisoned");
        // PANIC: CLI batch sizes are u64 and this process must support allocating that many rows.
        let row_count = usize::try_from(rng.random_range(self.batch_size_range.clone()))
            .expect("batch size exceeds addressable memory");
        let timestamp = self.clock.now().timestamp_micros();
        let mut keys = Vec::with_capacity(row_count);
        let mut data = LargeBinaryBuilder::new();

        for _ in 0..row_count {
            // Reinterpreting u64 as i64 preserves distinct key values across the full cardinality.
            keys.push(rng.random_range(0..self.keys_cardinality.get()) as i64);
            // PANIC: The configured data size must fit in the process address space.
            let size = usize::try_from(rng.random_range(self.row_data_size_range.clone()))
                .expect("row data size exceeds addressable memory");
            let mut bytes = vec![0; size];
            rng.fill_bytes(&mut bytes);
            data.append_value(bytes);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(data.finish()),
            Arc::new(
                TimestampMicrosecondArray::from(vec![timestamp; row_count]).with_timezone_utc(),
            ),
        ];
        // PANIC: All arrays have the same row count and match the converted Iceberg schema.
        RecordBatch::try_new(self.arrow_schema.clone(), columns)
            .expect("generated columns must match the Iceberg schema")
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use rand::SeedableRng;
    use se_common::clock::MockSystemClock;

    use super::*;

    fn generator(clock: Arc<MockSystemClock>, seed: u64) -> DataGenerator {
        DataGenerator::new(
            TableMode::Upsert,
            None,
            NonZeroU64::new(3).unwrap(),
            2..=5,
            4..=9,
            clock,
            StdRng::seed_from_u64(seed),
        )
        .unwrap()
    }

    #[test]
    fn schema_has_key_data_and_timestamp() {
        let generator = generator(Arc::new(MockSystemClock::new()), 42);

        let schema = generator.schema();
        insta::assert_debug_snapshot!(
            (
                schema.schema_id(),
                schema.as_struct().fields(),
                schema.identifier_field_ids().collect::<Vec<_>>(),
            ),
            @r#"
        (
            0,
            [
                NestedField {
                    id: 1,
                    name: "key",
                    required: true,
                    field_type: Primitive(
                        Long,
                    ),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                NestedField {
                    id: 2,
                    name: "data",
                    required: true,
                    field_type: Primitive(
                        Binary,
                    ),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                NestedField {
                    id: 3,
                    name: "ts",
                    required: true,
                    field_type: Primitive(
                        Timestamptz,
                    ),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
            [
                1,
            ],
        )
        "#
        );

        let append = DataGenerator::new(
            TableMode::Append,
            None,
            NonZeroU64::new(3).unwrap(),
            2..=5,
            4..=9,
            Arc::new(MockSystemClock::new()),
            StdRng::seed_from_u64(42),
        )
        .unwrap();
        assert_eq!(append.schema().identifier_field_ids().len(), 0);
        assert_eq!(append.schema().as_struct(), schema.as_struct());
        assert_eq!(append.arrow_schema(), generator.arrow_schema());
    }

    #[test]
    fn sort_order_matches_selected_column() {
        for mode in [TableMode::Append, TableMode::Upsert] {
            for (sort, field_id) in [
                (None, None),
                (Some(SortColumn::Key), Some(1)),
                (Some(SortColumn::Ts), Some(3)),
            ] {
                let generator = DataGenerator::new(
                    mode,
                    sort,
                    NonZeroU64::new(3).unwrap(),
                    2..=5,
                    4..=9,
                    Arc::new(MockSystemClock::new()),
                    StdRng::seed_from_u64(42),
                )
                .unwrap();
                let expected = match field_id {
                    Some(source_id) => SortOrder {
                        order_id: 1,
                        fields: vec![SortField {
                            source_id,
                            transform: Transform::Identity,
                            direction: SortDirection::Ascending,
                            null_order: NullOrder::First,
                        }],
                    },
                    None => SortOrder::unsorted_order(),
                };
                assert_eq!(generator.sort_order(), &expected);
            }
        }
    }

    #[test]
    fn batch_data_as_a_table() {
        let generator = generator(Arc::new(MockSystemClock::with_time(1_234)), 42);
        let batch = generator.next_batch();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(table, @"
        +-----+------------+--------------------------+
        | key | data       | ts                       |
        +-----+------------+--------------------------+
        | 1   | b23076fd   | 1970-01-01T00:00:01.234Z |
        | 2   | a39c3a6a   | 1970-01-01T00:00:01.234Z |
        | 2   | 5e831ca147 | 1970-01-01T00:00:01.234Z |
        | 0   | 09ac4e992e | 1970-01-01T00:00:01.234Z |
        | 1   | fcad       | 1970-01-01T00:00:01.234Z |
        | 2   | 7c4b3903   | 1970-01-01T00:00:01.234Z |
        | 1   | 1f55       | 1970-01-01T00:00:01.234Z |
        +-----+------------+--------------------------+
        ");
    }
}
