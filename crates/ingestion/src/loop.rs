use std::{
    collections::HashMap,
    future::poll_fn,
    sync::{Arc, Weak},
    time::Duration,
};

use se_meta_store::catalog::{CatalogName, TableIdent};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::time::{DelayQueue, delay_queue::Key};

use crate::{IngestMessage, TableState, WriteError};

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

pub(crate) enum Command {
    Lookup {
        key: (CatalogName, TableIdent),
        reply: oneshot::Sender<Lookup>,
    },
    Register {
        state: Arc<TableState>,
        reply: oneshot::Sender<Arc<TableState>>,
    },
    Write(IngestMessage),
}

pub(crate) enum Lookup {
    Active(Arc<TableState>),
    Cold(Arc<Mutex<()>>),
}

#[derive(Default)]
struct TableEntry {
    state: Weak<TableState>,
    gate: Arc<Mutex<()>>,
}

struct Pending {
    messages: Vec<IngestMessage>,
    bytes: usize,
    expiry: Key,
}

pub(crate) async fn ingestion_loop(mut receiver: mpsc::Receiver<Command>) {
    let mut tables: HashMap<(CatalogName, TableIdent), TableEntry> = HashMap::new();
    let mut pending: HashMap<(CatalogName, TableIdent), Pending> = HashMap::new();
    let mut expirations = DelayQueue::new();
    let mut uploads = JoinSet::new();

    loop {
        tokio::select! {
            message = receiver.recv() => match message {
                Some(Command::Lookup { key, reply }) => {
                    if !tables.contains_key(&key) {
                        tables.retain(|_, entry| {
                            entry.state.strong_count() > 0 || Arc::strong_count(&entry.gate) > 1
                        });
                    }
                    let entry = tables.entry(key).or_default();
                    let lookup = match entry.state.upgrade() {
                        Some(state) => Lookup::Active(state),
                        None => Lookup::Cold(Arc::clone(&entry.gate)),
                    };
                    let _ = reply.send(lookup);
                }
                Some(Command::Register { state, reply }) => {
                    let key = state.key.clone();
                    let entry = tables.entry(key).or_default();
                    let state = entry.state.upgrade().unwrap_or_else(|| {
                        entry.state = Arc::downgrade(&state);
                        state
                    });
                    let _ = reply.send(state);
                }
                Some(Command::Write(message)) => {
                    let key = message.state.key.clone();
                    let group = pending.entry(key.clone()).or_insert_with(|| Pending {
                        messages: Vec::new(),
                        bytes: 0,
                        expiry: expirations.insert(key.clone(), FLUSH_INTERVAL),
                    });
                    group.bytes = group.bytes.saturating_add(message.batch.get_array_memory_size());
                    group.messages.push(message);
                    if group.bytes >= MAX_BATCH_BYTES
                        && let Some(group) = pending.remove(&key) {
                            expirations.remove(&group.expiry);
                            uploads.spawn(flush(group.messages));
                        }
                }
                None => {
                    for (_, group) in pending.drain() {
                        uploads.spawn(flush(group.messages));
                    }
                    break;
                }
            },
            Some(expired) = poll_fn(|cx| expirations.poll_expired(cx)), if !expirations.is_empty() => {
                if let Some(group) = pending.remove(&expired.into_inner()) {
                    uploads.spawn(flush(group.messages));
                }
            },
            _ = uploads.join_next(), if !uploads.is_empty() => {},
        }
    }

    while uploads.join_next().await.is_some() {}
}

async fn flush(messages: Vec<IngestMessage>) {
    // WAL upload is not implemented yet. Never acknowledge uncommitted data.
    // PANIC: a group is created from its first message and is never empty.
    let _table = Arc::clone(&messages[0].state.table);
    for message in messages {
        // TODO: handle messages by creating parquet file and flushing.
        let _ = message.reply.send(Ok(()));
    }
}
