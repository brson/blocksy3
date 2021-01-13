use std::sync::Arc;
use crate::command as cmd;
use crate::log::Log;
use crate::batch_player::{self as bp, BatchPlayer};
use crate::index::Index;
use anyhow::Result;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Copy, Clone)]
pub struct Batch(pub usize);

#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Copy, Clone)]
pub struct BatchCommit(pub usize);

#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Copy, Clone)]
pub struct View(pub usize);

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
        let cmd = cmd::Command::ReadyCommit {
            batch: cmd::Batch(self.batch.0),
            batch_commit: cmd::BatchCommit(batch_commit.0),
        };

        let address = self.log.append(&cmd).await?;
        self.batch_player.record(&cmd, bp::Address(address.0));
        let index_ops = self.batch_player.replay(cmd::Batch(self.batch.0), cmd::BatchCommit(batch_commit.0));
        panic!()
    }
}
