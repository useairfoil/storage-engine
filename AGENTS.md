# Agents

## Commands

If available, use `just`. If not, read the `justfile` to determine the equivalent command.
By default, commands run on all crates in the project. Pass the `-p <crate>` option to run on a specific crate.

The `format` command requires a nightly toolchain to run.

## Guidelines

### General

 - Keep docstrings short and update them when you update code.
   + Describe what functions do in one sentence.
   + No docstrings on structs and other types.
 - Use whitespace (blank lines) to improved code readability.
 - Never update README.md. If any information there becomes outdated, inform the user.
 - Ask the user before adding a new dependency to the workspace Cargo.toml. Adding already existing crates to local crates is fine.
 
 ### Error management 
 
 - Use `thiserror` for error management.
 - If creating a `Result` alias, use the `type Result<T, E = Error> = std::result::Result<T, E>;` pattern.
 - Document panics with a `// PANIC: <description>` comment.
 
 ### Airfoil-specific patterns
 
 - Use `CancellationToken` for task cancellation. `ct: CancellationToken` is _always_ the last argument to a function.
 - When interacting with time (`now`, `sleep`, `ticker` etc.) use the `SystemClock` trait from the `se_common` crate. `Arc<dyn SystemClock>` is the last argument before the cancellation token.
 - Use the `IdGenerator` trait from the `se_common` crate to generate new UUIDs and ULIDs.
 - Use `Arc<SeRand>` for randomness. This argument is the last argument before `Arc<dyn SystemClock>`.

### Testing

 - Always use `insta` snapshot testing over `assert!`. Use `assert` only when the equality is important, not the value.
 - Use unit tests only to test pure functions. Generally, if the test requires setup helpers, it should go in the crate's integration tests.
 - Small integration tests live in the `tests/` folder inside the crate.
 - If what you're testing is already covered by the integration tests inside `crates/integration-tests`, then don't repeat it.

### File structure

Within a file, write definitions in the following order:

 - Imports and submodules.
 - Constants. Public first, then private.
 - Public types, such as types, structs, enums.
 - Private helper types.
 - Public functions.
 - Impl blocks for public types, including trait implementations.
 - Private functions and impl blocks for private helper types.
