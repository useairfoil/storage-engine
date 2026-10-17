default:
    @just --list

check *ARGS="--all-targets":
    cargo check --workspace --all-features {{ ARGS }}

build *ARGS="--all-targets":
    cargo build --workspace --all-features {{ ARGS }}

test *ARGS="--all-targets":
    cargo nextest run --workspace --all-features {{ ARGS }}

test-integration *ARGS="":
    cargo nextest run -p se-integration-tests --all-features --profile integration {{ ARGS }}

test-doc *ARGS="":
    cargo test --workspace --all-features --doc {{ ARGS }}

clippy *ARGS="--all-targets":
    cargo clippy --workspace  --all-features {{ ARGS }}

format:
    nix develop .#nightly -c cargo fmt

dev:
    cargo run --bin se -- dev

stress *ARGS="":
    cargo run -p se-stress -- start {{ ARGS }}

alias fmt := format
