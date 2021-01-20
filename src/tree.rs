use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use crate::types::{Batch, BatchCommit, Commit, Key, Value};
use crate::command::Command;
use crate::log::Log;
use crate::batch_player::{BatchPlayer, IndexOp};
use crate::index::{self, Index};
use anyhow::{Result, anyhow};
use futures::{Stream, StreamExt};

pub struct Tree {
    initialized: AtomicBool,
    log: Arc<Log<Command>>,
    batch_player: Arc<BatchPlayer>,
    index: Arc<Index>,
}

pub struct BatchWriter {
    batch: Batch,
    log: Arc<Log<Command>>,
    batch_player: Arc<BatchPlayer>,
    index: Arc<Index>,
}

pub struct Cursor {
    log: Arc<Log<Command>>,
    index_cursor: index::Cursor,
    value: Option<Value>,
}

pub struct InitReplayer<'tree> {
    initialized: &'tree AtomicBool,
    log: &'tree Log<Command>,
    index: &'tree Index,
    batch_players: BTreeMap<Batch, BatchPlayer>,
    last_commit: Option<Commit>,
    init_success: bool,
}

impl<'tree> InitReplayer<'tree> {
    pub async fn replay_commit(&mut self,
                               batch: Batch,
                               batch_commit: BatchCommit,
                               commit: Commit) -> Result<()> {
        panic!()
    }

    pub fn init_success(mut self) {
        self.init_success = true
    }
}

impl<'tree> Drop for InitReplayer<'tree> {
    fn drop(&mut self) {
        self.initialized.store(self.init_success, Ordering::SeqCst);
    }
}

impl Tree {
    pub fn new(log: Log<Command>) -> Tree {
        Tree {
            initialized: AtomicBool::new(false),
            log: Arc::new(log),
            batch_player: Arc::new(BatchPlayer::new()),
            index: Arc::new(Index::new()),
        }
    }

    pub fn init_replayer(&self) -> InitReplayer {
        assert!(!self.initialized.load(Ordering::SeqCst));

        InitReplayer {
            initialized: &self.initialized,
            log: &*self.log,
            index: &*self.index,
            batch_players: BTreeMap::new(),
            last_commit: None,
            init_success: false,
        }
    }

    pub fn batch(&self, batch: Batch) -> BatchWriter {
        assert!(self.initialized.load(Ordering::SeqCst));

        BatchWriter {
            batch,
            log: self.log.clone(),
            batch_player: self.batch_player.clone(),
            index: self.index.clone(),
        }
    }

    pub async fn read(&self, commit_limit: Commit, key: &Key) -> Result<Option<Value>> {
        assert!(self.initialized.load(Ordering::SeqCst));

        let addr = self.index.read(commit_limit, key);

        if let Some(addr) = addr {
            let cmd = self.log.read_at(addr).await?;
            match cmd {
                Command::Write { key: log_key , value, .. } => {
                    assert_eq!(key, &log_key);
                    Ok(Some(value))
                }
                _ => {
                    Err(anyhow!(UNEXPECTED_LOG))
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn cursor(&self, commit_limit: Commit) -> Cursor {
        assert!(self.initialized.load(Ordering::SeqCst));

        Cursor {
            log: self.log.clone(),
            index_cursor: self.index.cursor(commit_limit),
            value: None,
        }
    }
}

impl BatchWriter {
    pub async fn open(&self) -> Result<()> {
        Ok(self.append_record(Command::Open {
            batch: self.batch,
        }).await?)
    }

    pub async fn write(&self, key: Key, value: Value) -> Result<()> {
        Ok(self.append_record(Command::Write {
            batch: self.batch,
            key,
            value,
        }).await?)
    }

    pub async fn delete(&self, key: Key) -> Result<()> {
        Ok(self.append_record(Command::Delete {
            batch: self.batch,
            key,
        }).await?)
    }

    pub async fn delete_range(&self, start_key: Key, end_key: Key) -> Result<()> {
        Ok(self.append_record(Command::DeleteRange {
            batch: self.batch,
            start_key,
            end_key,
        }).await?)
    }

    pub async fn push_save_point(&self) -> Result<()> {
        Ok(self.append_record(Command::PushSavePoint {
            batch: self.batch,
        }).await?)
    }

    pub async fn pop_save_point(&self) -> Result<()> {
        Ok(self.append_record(Command::PopSavePoint {
            batch: self.batch,
        }).await?)
    }

    pub async fn rollback_save_point(&self) -> Result<()> {
        Ok(self.append_record(Command::RollbackSavePoint {
            batch: self.batch,
        }).await?)
    }

    pub async fn ready_commit(&self, batch_commit: BatchCommit) -> Result<()> {
        Ok(self.append_record(Command::ReadyCommit {
            batch: self.batch,
            batch_commit,
        }).await?)
    }

    pub async fn abort_commit(&self, batch_commit: BatchCommit) -> Result<()> {
        Ok(self.append_record(Command::AbortCommit {
            batch: self.batch,
            batch_commit,
        }).await?)
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
        let res = self.append_record(Command::Close {
            batch: self.batch,
        }).await;

        if let Err(e) = res {
            self.batch_player.emergency_close(self.batch);
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn append_record(&self, cmd: Command) -> Result<()> {
        let address = self.log.append(cmd.clone()).await?;
        self.batch_player.record(&cmd, address);
        Ok(())
    }
}

impl Cursor {
    pub fn is_valid(&self) -> bool {
        self.index_cursor.is_valid()
    }

    pub fn key(&self) -> Key {
        self.index_cursor.key()
    }

    pub async fn value(&mut self) -> Result<Value> {
        assert!(self.is_valid());
        if let Some(value) = &self.value {
            Ok(value.clone())
        } else {
            let addr = self.index_cursor.address();
            let cmd = self.log.read_at(addr).await?;
            match cmd {
                Command::Write { key, value, .. } => {
                    assert_eq!(key, self.key());
                    Ok(value)
                },
                _ => {
                    Err(anyhow!(UNEXPECTED_LOG))
                }
            }
        }
    }

    pub fn next(&mut self) {
        self.value = None;
        self.index_cursor.next()
    }

    pub fn prev(&mut self) {
        self.value = None;
        self.index_cursor.prev()
    }

    pub fn seek_first(&mut self) {
        self.value = None;
        self.index_cursor.seek_first()
    }

    pub fn seek_last(&mut self) {
        self.value = None;
        self.index_cursor.seek_last()
    }

    pub fn seek_key(&mut self, key: Key) {
        self.value = None;
        self.index_cursor.seek_key(key)
    }

    pub fn seek_key_rev(&mut self, key: Key) {
        self.value = None;
        self.index_cursor.seek_key_rev(key)
    }
}

static UNEXPECTED_LOG: &'static str = "unexpected command in log";
