use std::sync::Arc;
use crate::types::{View, Batch, BatchCommit, Commit, Key, Value};
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
        BatchWriter {
            batch,
            log: self.log.clone(),
            batch_player: self.batch_player.clone(),
            index: self.index.clone(),
        }
    }

    pub fn view(&self, view: View) -> ViewReader {
        panic!()
    }
}

impl BatchWriter {
    pub async fn open(&self) -> Result<()> {
        Ok(self.append_record(&Command::Open {
            batch: self.batch,
        }).await?)
    }

    pub async fn write(&self, key: Key, value: Value) -> Result<()> {
        Ok(self.append_record(&Command::Write {
            batch: self.batch,
            key,
            value,
        }).await?)
    }

    pub async fn delete(&self, key: Key) -> Result<()> {
        Ok(self.append_record(&Command::Delete {
            batch: self.batch,
            key,
        }).await?)
    }

    pub async fn delete_range(&self, start_key: Key, end_key: Key) -> Result<()> {
        Ok(self.append_record(&Command::DeleteRange {
            batch: self.batch,
            start_key,
            end_key,
        }).await?)
    }

    pub async fn push_save_point(&self) -> Result<()> {
        Ok(self.append_record(&Command::PushSavePoint {
            batch: self.batch,
        }).await?)
    }

    pub async fn pop_save_point(&self) -> Result<()> {
        Ok(self.append_record(&Command::PopSavePoint {
            batch: self.batch,
        }).await?)
    }

    pub async fn rollback_save_point(&self) -> Result<()> {
        Ok(self.append_record(&Command::RollbackSavePoint {
            batch: self.batch,
        }).await?)
    }

    pub async fn ready_commit(&self, batch_commit: BatchCommit) -> Result<()> {
        Ok(self.append_record(&Command::ReadyCommit {
            batch: self.batch,
            batch_commit,
        }).await?)
    }

    pub async fn abort_commit(&self, batch_commit: BatchCommit) -> Result<()> {
        Ok(self.append_record(&Command::AbortCommit {
            batch: self.batch,
            batch_commit,
        }).await?)
    }

    async fn append_record(&self, cmd: &Command) -> Result<()> {
        let address = self.log.append(&cmd).await?;
        self.batch_player.record(&cmd, address);
        Ok(())
    }

    pub fn commit(&self, batch_commit: BatchCommit, commit: Commit) {
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
    }

    pub async fn close(&self) -> Result<()> {
        Ok(self.append_record(&Command::Close {
            batch: self.batch,
        }).await?)
    }
}
