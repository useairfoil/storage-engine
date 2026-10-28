use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{BatchCommit, BatchDescriptor, BatchMetadata, BatchRead, Error, Result};

#[derive(Debug, Clone, Default)]
pub struct InMemoryWal {
    state: Arc<RwLock<State>>,
}

#[derive(Debug, Default)]
struct State {
    next_sequence: u64,
    epoch: u64,
    batches: Vec<BatchDescriptor>,
}

impl InMemoryWal {
    /// Returns committed batches in sequence order.
    pub async fn batches(&self) -> Vec<BatchDescriptor> {
        self.state.read().await.batches.clone()
    }
}

#[async_trait]
impl BatchCommit for InMemoryWal {
    async fn commit(&self, batch: BatchMetadata) -> Result<()> {
        let mut state = self.state.write().await;
        let sequence = state.next_sequence;

        state.next_sequence += 1;
        state.batches.push(BatchDescriptor {
            sequence,
            metadata: batch,
        });

        Ok(())
    }
}

#[async_trait]
impl BatchRead for InMemoryWal {
    async fn claim_reader(&self) -> Result<u64> {
        let mut state = self.state.write().await;
        state.epoch += 1;

        Ok(state.epoch)
    }

    async fn read_after(&self, sequence: Option<u64>, max: usize) -> Result<Vec<BatchDescriptor>> {
        let state = self.state.read().await;

        Ok(state
            .batches
            .iter()
            .filter(|entry| sequence.is_none_or(|sequence| entry.sequence > sequence))
            .take(max)
            .cloned()
            .collect())
    }

    async fn ack_through(&self, epoch: u64, sequence: u64) -> Result<()> {
        let mut state = self.state.write().await;
        if epoch == 0 || epoch != state.epoch {
            return Err(Error::Fenced);
        }

        if sequence >= state.next_sequence {
            return Err(Error::InvalidInput(format!(
                "cannot acknowledge sequence {sequence}: next WAL sequence is {}",
                state.next_sequence
            )));
        }

        state.batches.retain(|entry| entry.sequence > sequence);

        Ok(())
    }
}
