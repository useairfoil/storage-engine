# Iceberg 0.10.1 patches

Source: the crates.io `iceberg` 0.10.1 release (Apache-2.0).

## Public table commit builder

Allows callers to construct a catalog commit with explicit validation requirements.
Direct callers are responsible for protecting updates against concurrent changes.

Files and APIs changed:

- `src/catalog/mod.rs`
  - **Changed struct:** `TableCommit` — changes its TypedBuilder build-method
    visibility from `pub(crate)` to `pub` and updates the struct documentation.
  - **Changed method:** the generated `TableCommitBuilder::build()` — now publicly
    accessible through `TableCommit::builder().build()`.

## Append planning without catalog submission

Allows callers to prepare and persist append updates against an exact table base
without invoking `Transaction::commit`, which reloads, rebases, and retries.

Files and APIs added:

- `src/transaction/append.rs`
  - **Added method:** `FastAppendAction::prepare(self, table: &Table) -> Result<ActionCommit>`
    — an async entry point that delegates to the existing `TransactionAction::commit`
    implementation. It writes append metadata but does not submit to a catalog.
