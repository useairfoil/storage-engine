use std::{collections::HashSet, process::Stdio, sync::Arc, time::Duration};

use axum::{Json, Router, http::StatusCode, routing::post};
use se_common::clock::{DefaultSystemClock, SystemClock};
use serde_json::Value;
use tokio::{
    process::Command,
    sync::{Semaphore, mpsc},
};

#[tokio::test]
async fn worker_command_restarts_failed_workers_and_stops_on_shutdown() {
    let clock = DefaultSystemClock::default();

    // PANIC: Fail if worker startup, respawn, or shutdown stalls.
    tokio::select! {
        _ = clock.sleep(Duration::from_secs(30)) => panic!("worker command test timed out"),
        _ = async {
            for count in [1, 3] {
                let (send, mut claims) = mpsc::unbounded_channel();
                let gate = Arc::new(Semaphore::new(0));
                let router = Router::new().route("/tasks", post({
                    let gate = gate.clone();
                    move |Json(request): Json<Value>| {
                        let send = send.clone();
                        let gate = gate.clone();
                        async move {
                            assert_eq!(request["action"], "claim");
                            send.send(request["worker_id"].as_str().unwrap().to_owned()).unwrap();
                            let permit = gate.acquire().await.unwrap();
                            permit.forget();
                            (StatusCode::INTERNAL_SERVER_ERROR, Json(Value::Null))
                        }
                    }
                }));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
                let mut command = Command::new(env!("CARGO_BIN_EXE_se"));
                command.args(["worker", "--metadata.url", &format!("http://{address}")]);
                if count != 1 {
                    command.args(["--workers", &count.to_string()]);
                }
                let mut child = command
                    .env("OTEL_SDK_DISABLED", "true")
                    .env("SE_OBJECT_STORE_TYPE", "s3")
                    .env("SE_S3_BUCKET", "worker-cli-test")
                    .env("SE_S3_REGION", "us-east-1")
                    .env("SE_S3_ACCESS_KEY_ID", "test")
                    .env("SE_S3_SECRET_ACCESS_KEY", "test")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn().unwrap();

                let mut initial = HashSet::new();
                for _ in 0..count {
                    initial.insert(claims.recv().await.unwrap());
                }
                assert_eq!(initial.len(), count);
                gate.add_permits(count);

                let mut replacements = HashSet::new();
                for _ in 0..count {
                    replacements.insert(claims.recv().await.unwrap());
                }
                assert_eq!(replacements.len(), count);
                assert_eq!(initial.intersection(&replacements).count(), 0);
                assert_eq!(child.try_wait().unwrap(), None);

                let status = Command::new("kill")
                    .args(["-TERM", &child.id().unwrap().to_string()])
                    .status().await.unwrap();
                assert!(status.success());
                assert!(child.wait().await.unwrap().success());
                server.abort();
            }
        } => {}
    }
}
