use std::fmt::Write;

use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use se_scheduler::{Task, object_store::DEFAULT_LOCATION};
use serde_json::Value;

pub async fn document(store: &dyn ObjectStore) -> Value {
    let bytes = store
        .get(&Path::from(DEFAULT_LOCATION))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Records observable durable state without random IDs, wall-clock timestamps,
/// or jitter. Identity and timing guarantees are checked separately by the tests.
pub async fn record(trace: &mut String, step: &str, store: &dyn ObjectStore) {
    let state = document(store).await;
    writeln!(trace, "{step}").unwrap();
    writeln!(
        trace,
        "  broker: {}",
        state["broker"]["address"].as_str().unwrap()
    )
    .unwrap();
    let mut tasks = state["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            let status = if !entry["attempt"].is_null() {
                "running"
            } else if !entry["last_failure"].is_null() {
                "retry"
            } else {
                "queued"
            };
            let next_deadline = if entry["next_deadline"].is_null() {
                "-".into()
            } else {
                entry["next_deadline"]
                    .as_str()
                    .unwrap()
                    .parse::<DateTime<Utc>>()
                    .unwrap()
                    .timestamp()
                    .to_string()
            };
            let failure = if entry["last_failure"].is_null() {
                String::new()
            } else {
                format!(" failure={}", entry["last_failure"])
            };
            format!(
                "    {status}: {} attempts={} next_deadline={next_deadline}{failure}",
                task_label(&entry["task"]),
                entry["attempts"]
            )
        })
        .collect::<Vec<_>>();
    tasks.sort();
    writeln!(
        trace,
        "  tasks:{}",
        if tasks.is_empty() { " []" } else { "" }
    )
    .unwrap();
    for task in tasks {
        writeln!(trace, "{task}").unwrap();
    }
    let failures = state["failed_tasks"].as_array().unwrap();
    writeln!(
        trace,
        "  failures (newest first):{}",
        if failures.is_empty() { " []" } else { "" }
    )
    .unwrap();
    for failure in failures {
        writeln!(
            trace,
            "    {} attempts={} failure={}",
            task_label(&failure["task"]),
            failure["attempts"],
            failure["failure"]
        )
        .unwrap();
    }
    trace.push('\n');
}

fn task_label(value: &Value) -> String {
    match serde_json::from_value::<Task>(value.clone()).unwrap() {
        Task::Reconcile {
            catalog_name,
            table_ident,
            deadline,
        } => {
            format!(
                "reconcile {catalog_name}/{table_ident} deadline={}",
                deadline.timestamp()
            )
        }
    }
}
