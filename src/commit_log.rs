use serde::{Serialize, Deserialize};
use crate::log::Log;
use crate::types::{Commit, BatchCommit};
use anyhow::Result;

pub struct CommitLog {
    log: Log<CommitCommand>,
}

#[derive(Serialize, Deserialize)]
enum CommitCommand {
    Commit {
        commit: Commit,
        batch_commit: BatchCommit,
    }
}

impl CommitLog {
    pub fn new(log: Log<CommitCommand>) -> CommitLog {
        CommitLog { log }
    }

    pub async fn commit(&self, commit: Commit, batch_commit: BatchCommit) -> Result<()> {
        self.log.append(CommitCommand::Commit {
            commit, batch_commit,
        }).await?;

        Ok(())
    }
}
