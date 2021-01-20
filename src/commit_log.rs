use serde::{Serialize, Deserialize};
use crate::log::Log;
use crate::types::{Commit, BatchCommit, Batch};
use futures::{Stream, StreamExt};
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

    pub fn replay(&self) -> impl Stream<Item = Result<CommitCommand>> + Unpin {
        self.log.replay().map(|r| r.map(|(cmd, _)| cmd))
    }

    pub async fn commit(&self, batch: Batch, batch_commit: BatchCommit, commit: Commit) -> Result<()> {
        self.log.append(CommitCommand::Commit {
            batch, batch_commit, commit
        }).await?;

        Ok(())
    }
}
