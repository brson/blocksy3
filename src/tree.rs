use std::sync::Arc;
use crate::types::{View, Batch, BatchCommit, Commit};
use crate::command::Command;
use crate::log::Log;
use crate::batch_player::{BatchPlayer, IndexOp};
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
    pub async fn ready_commit(&self, batch_commit: BatchCommit) -> Result<()> {
        let cmd = Command::ReadyCommit {
            batch: self.batch,
            batch_commit: batch_commit,
        };

        let address = self.log.append(&cmd).await?;
        self.batch_player.record(&cmd, address);
        Ok(())
    }

    pub fn commit(&self, batch_commit: BatchCommit, commit: Commit) -> Result<()> {
        let index_ops = self.batch_player.replay(self.batch, batch_commit);
        let mut writer = self.index.writer(commit);
        for op in index_ops {
            match op {
                IndexOp::Write { key, address } => {
                    writer.write(key, address);
                },
                IndexOp::Delete { key, address } => {
                    writer.delete(key, address);
                },
                IndexOp::DeleteRange { start_key, end_key, address } => {
                    writer.delete_range(start_key..end_key, address);
                },
            }
        }
        Ok(())
    }
}
