pub mod cmd;
pub mod object_store;
pub mod secret_store;
mod server;

use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn handle_shutdown_signal(ct: CancellationToken) {
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to create terminate signal handler");

    let sig_type = tokio::select! {
        _ = sigterm.recv() => {
            "sigterm"
        }
        _ = tokio::signal::ctrl_c() => {
            "ctrl-c"
        }
    };

    ct.cancel();

    info!(signal = sig_type, "signal received. shutting down.")
}
