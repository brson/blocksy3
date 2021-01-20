use serde::{Serialize, Deserialize};
use crate::log::Log;
use crate::types::{Commit, BatchCommit, Batch};
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

    pub async fn commit(&self, batch: Batch, batch_commit: BatchCommit, commit: Commit) -> Result<()> {
        self.log.append(CommitCommand::Commit {
            batch, batch_commit, commit
        }).await?;

        Ok(())
    }
}
