default:
    @just --list

check *ARGS="--all-targets":
    cargo check --workspace --all-features {{ ARGS }}

build *ARGS="--all-targets":
    cargo build --workspace --all-features {{ ARGS }}

test *ARGS="--all-targets":
    cargo test --workspace --all-features {{ ARGS }}

clippy *ARGS="--all-targets":
    cargo clippy --workspace  --all-features {{ ARGS }}

format:
    nix develop .#nightly -c cargo fmt

dev:
    cargo run --bin se -- dev

alias fmt := format
