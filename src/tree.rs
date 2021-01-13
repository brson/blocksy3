use std::sync::Arc;
use crate::types::{View, Batch, BatchCommit};
use crate::command::Command;
use crate::log::Log;
use crate::batch_player::BatchPlayer;
use crate::index::Index;
use anyhow::Result;

pub struct Tree {
    log: Arc<Log>,
    batch_player: Arc<BatchPlayer>,
    index: Arc<Index>,
}

pub struct BatchWriter {
    batch: Batch,
    log: Arc<Log>,
    batch_player: Arc<BatchPlayer>,
    index: Arc<Index>,
}

pub struct ViewReader {
    view: View,
    log: Arc<Log>,
    index: Arc<Index>,
}

impl Tree {
    pub fn batch(&self, batch: Batch) -> BatchWriter {
        panic!()
    }

    pub fn view(&self, view: View) -> ViewReader {
        panic!()
    }
}

impl BatchWriter {
    pub async fn commit(&self, batch_commit: BatchCommit) -> Result<()> {
        let cmd = Command::ReadyCommit {
            batch: self.batch,
            batch_commit: batch_commit,
        };

        let address = self.log.append(&cmd).await?;
        self.batch_player.record(&cmd, address);
        let index_ops = self.batch_player.replay(self.batch, batch_commit);
        panic!()
    }
}
