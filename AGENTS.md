# Agents

## Commands

If available, use `just`. If not, read the `justfile` to determine the equivalent command.
By default, commands run on all crates in the project. Pass the `-p <crate>` option to run on a specific crate.

The `format` command requires a nightly toolchain to run.

## Guidelines

 - Use `thiserror` for error management.
 - Ask the user before adding a new dependency.
 - When interacting with time (`now`, `sleep`, `ticker` etc.) use the `SystemClock` trait from the `se_common` crate.
 - Use the `IdGenerator` trait from the `se_common` crate to generate new UUIDs and ULIDs.
 - If creating a `Result` alias, use the `type Result<T, E = Error> = std::result::Result<T, E>;` pattern.
 - Use `insta` snapshot testing over `assert!`. Unless use `assert` when the equality is important, not the value.
 - Document panics with a `// PANIC: <description>` comment.

Within a file, write definitions in the following order:

 - Imports and submodules.
 - Constants. Public first, then private.
 - Public types, such as types, structs, enums.
 - Private helper types.
 - Public functions.
 - Impl blocks for public types, including trait implementations.
 - Private functions and impl blocks for private helper types.
