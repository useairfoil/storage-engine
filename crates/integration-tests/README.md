# Integration tests

This crate implements integration tests against real object stores and catalogs.

## Setup

`TestEnvironment::from_env()` reads three required environment variables:

- `SE_CI_CATALOG_CONFIG`: catalog configuration as a JSON string.
- `SE_CI_OBJECT_STORE_CONFIG`: object store configuration as a JSON string.

The object store configuration has this shape:

```json
{
  "scheme": "s3",
  "options": {
    "bucket": "integration-tests",
    "region": "eu-west-1"
  }
}
```

While the catalog configuration looks like this:

```json
{
  "rest": {
    "uri": "https://catalog.example.com",
    "warehouse": "test-warehouse",
    "properties": {}
  }
}
```
