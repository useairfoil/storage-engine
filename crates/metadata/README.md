# Metadata

## Storage

### Key encoding

Every key starts with a version and scope. Global records use scope `0x00`:

```text
| version: u8 = 0x01 | scope: u8 = 0x00 | GlobalRecordType: u8 | fields |
```

Table records use scope `0x01`, with the table identity before the record type:

```text
| version: u8 = 0x01 | scope: u8 = 0x01 | catalog_id: TerminatedBytes | table_uuid: UUID | TableRecordType: u8 | suffix |
```

Fields are concatenated without padding. Numeric key fields use big-endian encoding so lexicographic ordering matches numeric ordering.

Variable-length catalog IDs and file paths use OpenData's `TerminatedBytes` encoding over their UTF-8 bytes:

| Raw byte | Encoded bytes |
| -------- | ------------- |
| `0x00`   | `0x01 0x01`   |
| `0x01`   | `0x01 0x02`   |
| rest     | unchanged     |

An unescaped `0x00` terminates the field. This preserves byte ordering and keeps neighboring catalog IDs and paths distinct.
Fixed-length bytes, such as UUIDs, occupy their raw bytes.

The table prefix ends at `table_uuid`, so all record families for a table are contiguous.
A single prefix scan reads all of a table's records; appending a `TableRecordType` restricts the scan to one family.
This prepares the layout for future table-level splitting, but does not create separate databases or SlateDB segments.

The following tables list fields **after** each layout's record type.
Here `+` means concatenation, and `Table` is shorthand for:

```text
Table = catalog_id: TerminatedBytes + table_uuid: UUID
```

`GlobalRecordType`:

| ID     | Record            | Key fields                                              | Value            |
| ------ | ----------------- | ------------------------------------------------------- | ---------------- |
| `0x01` | Catalog           | `catalog_id: TerminatedBytes`                           | `CatalogValue`   |
| `0x02` | Ready work        | `sortable_seconds: u64 + nanos: u32 + Table + kind: u8` | `Target`         |
| `0x03` | Worker assignment | `worker_id: UUID`                                       | `TaskAssignment` |

`TableRecordType`:

| ID     | Record                  | Key suffix                                           | Value                     |
| ------ | ----------------------- | ---------------------------------------------------- | ------------------------- |
| `0x01` | WAL state               | none                                                 | `WalStateValue`           |
| `0x02` | WAL batch               | `sequence: u64`                                      | `BatchMetadata`           |
| `0x03` | Table work              | none                                                 | `TableWork`               |
| `0x04` | Data file               | `path: TerminatedBytes`                              | `VersionedDataFile`       |
| `0x05` | Deletion-vector history | `target_file: TerminatedBytes + added_revision: u64` | `VersionedDeletionVector` |
| `0x06` | Current deletion vector | `target_file: TerminatedBytes`                       | `VersionedDeletionVector` |
| `0x07` | Prepared commit         | none                                                 | `PreparedCommit`          |
| `0x08` | Index delta             | `range_start: u64`                                   | `IndexDelta`              |
| `0x09` | Materialization receipt | `attempt_id: UUID`                                   | `MaterializationReceipt`  |

Ready-work timestamps use signed Unix seconds with the sign bit flipped, then encoded as big-endian u64, followed by big-endian nanoseconds.
This orders times before and after the Unix epoch correctly.
Task-kind tags are `0 = Materialize`, `1 = Reconcile`, and `2 = DeleteTable`.

### Value encoding

Values do not participate in key ordering and use little-endian integers.

| Type                             | Encoding                                                         |
| -------------------------------- | ---------------------------------------------------------------- |
| `u8`, `u32`, `i32`, `u64`, `i64` | Fixed-width little-endian integer                                |
| `UUID`, `Ulid`                   | Raw 16 bytes, not a textual identifier                           |
| `String`, `CatalogId`            | Byte length as u32, followed by UTF-8 bytes                      |
| `Vec<T>`                         | Element count as u32, followed by encoded elements               |
| `Vec<u8>`                        | Byte length as u32, followed by raw bytes                        |
| `BTreeMap<i32, T>`               | Entry count as u32, followed by i32 key/value pairs in key order |
| `Option<T>`                      | `0x00` for absent; `0x01` followed by the value for present      |
| `Timestamp`                      | Signed Unix seconds as i64, then nanoseconds as u32              |
| `TableIdent`                     | Namespace as `Vec<String>`, then table name as `String`          |

Lengths and collection counts must fit in u32.
Empty strings and collections are distinct from absent options.
Decoding rejects truncated values, trailing bytes, unknown tags, invalid UTF-8/timestamps/identifiers, and duplicate map keys.

