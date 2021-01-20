use serde::{Serialize, Deserialize};
use crate::log::Log;
use crate::types::{Commit, BatchCommit, Batch};
use futures::{stream, Stream, StreamExt};
use anyhow::Result;

pub struct CommitLog {
    log: Log<CommitCommand>,
}

#[derive(Serialize, Deserialize)]
pub enum CommitCommand {
    Commit {
        batch: Batch,
        batch_commit: BatchCommit,
        commit: Commit,
    }
}

impl CommitLog {
    pub fn new(log: Log<CommitCommand>) -> CommitLog {
        CommitLog { log }
    }

    //pub async fn replay(&self) -> impl Stream<Item = Result<(CommitCommand)> {
    //}

    pub async fn commit(&self, batch: Batch, batch_commit: BatchCommit, commit: Commit) -> Result<()> {
        self.log.append(CommitCommand::Commit {
            batch, batch_commit, commit
        }).await?;

        Ok(())
    }
}
