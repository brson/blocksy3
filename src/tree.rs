use std::pin::Pin;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use crate::types::{Batch, BatchCommit, Commit, Key, Value, Address};
use crate::command::Command;
use crate::log::Log;
use crate::batch_player::{BatchPlayer, IndexOp};
use crate::index::{self, Index};
use anyhow::{Result, anyhow, bail};
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
    cmd_stream: Pin<Box<dyn Stream<Item = Result<(Command, Address)>>>>,
    index: &'tree Index,
    batch_players: BTreeMap<Batch, BatchPlayer>,
    previous_commit: Option<Commit>,
    max_batch_seen: Option<Batch>,
    max_batch_commit_seen: Option<BatchCommit>,
    waiting_to_commit: BTreeSet<(Batch, BatchCommit)>,
    init_success: bool,
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
            cmd_stream: Box::pin(self.log.replay()),
            index: &*self.index,
            batch_players: BTreeMap::new(),
            previous_commit: None,
            max_batch_seen: None,
            max_batch_commit_seen: None,
            waiting_to_commit: BTreeSet::new(),
            init_success: false,
        }
    }

    pub fn skip_init(&self) {
        self.initialized.store(true, Ordering::SeqCst);
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

    pub async fn sync(&self) -> Result<()> {
        Ok(self.log.sync().await?)
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
        assert!(start_key <= end_key);
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

    pub fn commit_to_index(&self, batch_commit: BatchCommit, commit: Commit) {
        commit_to_index(&*self.batch_player,
                        &*self.index,
                        self.batch,
                        batch_commit,
                        commit)                        
    }

    /// NB: This must only be called after the batch is committed
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
    pub fn valid(&self) -> bool {
        self.index_cursor.valid()
    }

    pub fn key(&self) -> Key {
        self.index_cursor.key()
    }

    pub async fn value(&mut self) -> Result<Value> {
        assert!(self.valid());
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

impl<'tree> InitReplayer<'tree> {
    pub async fn replay_commit(&mut self,
                               batch: Batch,
                               batch_commit: BatchCommit,
                               commit: Commit) -> Result<()> {

        let target_batch = batch;
        let target_batch_commit = batch_commit;

        if self.waiting_to_commit.remove(&(target_batch, target_batch_commit)) {
            let batch_player = self.batch_players.get(&batch);
            if let Some(batch_player) = batch_player {
                commit_to_index(&batch_player, &self.index, batch, batch_commit, commit);
                return Ok(());
            } else {
                bail!("batch closed before commit during init replay");
            }
        }
        
        while let Some(next_cmd) = self.cmd_stream.next().await {
            let (next_cmd, addr) = next_cmd?;
            log::trace!("next cmd {:?}", next_cmd);

            let new_batch = Some(next_cmd.batch());
            let mut new_batch_commit = None;
            let mut done = false;

            match next_cmd {
                Command::Open { batch } => {
                    if self.batch_players.contains_key(&batch) {
                        bail!("redundant batch open during init-replay");
                    }
                    self.batch_players.insert(batch, BatchPlayer::new());
                    self.record_cmd(next_cmd, addr)?;
                },
                Command::Close { batch } => {
                    if !self.batch_players.contains_key(&batch) {
                        bail!("stray batch close during init-replay");
                    }
                    // NB: If close is never logged,
                    // which is possible but rare,
                    // then the only impact
                    // is that the temporary batch replayer here won't
                    // be deleted until the end of index reconstruction.
                    self.record_cmd(next_cmd, addr)?;
                    self.batch_players.remove(&batch);
                },
                Command::ReadyCommit { batch, batch_commit } => {
                    new_batch_commit = Some(batch_commit);
                    self.record_cmd(next_cmd, addr)?;

                    let is_target_batch = target_batch == batch;
                    let is_target_batch_commit = target_batch_commit == batch_commit;
                    let bad_batch_combo = is_target_batch ^ is_target_batch_commit;

                    if bad_batch_combo {
                        bail!(BATCH_MISMATCH);
                    }

                    let must_commit = is_target_batch && is_target_batch_commit;

                    if must_commit {
                        let batch_player = self.batch_players.get(&batch).expect("batch");
                        commit_to_index(&batch_player, &self.index, batch, batch_commit, commit);
                        done = true;
                    } else {
                        // This ready-commit log happend out-of-order
                        // of the final commit.
                        // It will be committed later,
                        // so have it to the side.
                        if self.waiting_to_commit.contains(&(batch, batch_commit)) {
                            bail!(DUPLICATE_BATCH_COMMIT);
                        } else {
                            self.waiting_to_commit.insert((batch, batch_commit));
                        }
                    }
                },
                Command::AbortCommit { batch, batch_commit } => {
                    new_batch_commit = Some(batch_commit);
                    self.record_cmd(next_cmd, addr)?;

                    let is_target_batch = target_batch == batch;
                    let is_target_batch_commit = target_batch_commit == batch_commit;
                    let bad_batch_combo = is_target_batch ^ is_target_batch_commit;

                    if bad_batch_combo {
                        bail!(BATCH_MISMATCH);
                    }

                    let must_commit = is_target_batch && is_target_batch_commit;

                    if must_commit {
                        // Nothing to commit on abort,
                        // but other trees may have committed.
                        done = true;
                    } else {
                        // This ready-commit log happend out-of-order
                        // of the final commit.
                        // It will be committed later,
                        // so have it to the side.
                        if self.waiting_to_commit.contains(&(batch, batch_commit)) {
                            bail!(DUPLICATE_BATCH_COMMIT);
                        } else {
                            self.waiting_to_commit.insert((batch, batch_commit));
                        }
                    }
                },
                _ => {
                    self.record_cmd(next_cmd, addr)?;
                },
            }

            self.update_max_batch_and_batch_commit(new_batch, new_batch_commit);

            if done {
                return Ok(());
            }
        }

        bail!("commit not found for batch {} / batch-commit {} / commit {}",
              target_batch.0, target_batch_commit.0, commit.0);
    }

    pub async fn replay_rest(&mut self) -> Result<(Option<Batch>, Option<BatchCommit>)> {
        while let Some(next_cmd) = self.cmd_stream.next().await {
            let (next_cmd, addr) = next_cmd?;

            let new_batch = Some(next_cmd.batch());
            let mut new_batch_commit = None;

            match next_cmd {
                Command::ReadyCommit { batch, batch_commit } => {
                    new_batch_commit = Some(batch_commit);
                },
                Command::AbortCommit { batch, batch_commit } => {
                    new_batch_commit = Some(batch_commit);
                },
                _ => { },
            }

            self.update_max_batch_and_batch_commit(new_batch, new_batch_commit);
        }

        Ok((self.max_batch_seen, self.max_batch_commit_seen))
    }

    fn record_cmd(&mut self, cmd: Command, addr: Address) -> Result<()> {
        log::trace!("record cmd {:?}", cmd);
        let batch = cmd.batch();
        let batch_player = self.batch_players.get(&batch)
            .ok_or_else(|| anyhow!("command replay before batch opened"))?;
        batch_player.record(&cmd, addr);
        Ok(())
    }

    fn update_max_batch_and_batch_commit(&mut self, new_batch: Option<Batch>, new_batch_commit: Option<BatchCommit>) {
        match (self.max_batch_seen, new_batch) {
            (None, new_batch) => {
                self.max_batch_seen = new_batch;
            },
            (Some(curr_max_batch), Some(new_batch)) => {
                self.max_batch_seen = Some(Batch(curr_max_batch.0.max(new_batch.0)));
            },
            (Some(_), None) => { }
        }

        match (self.max_batch_commit_seen, new_batch_commit) {
            (None, new_batch_commit) => {
                self.max_batch_commit_seen = new_batch_commit;
            },
            (Some(curr_max_batch_commit), Some(new_max_batch_commit)) => {
                self.max_batch_commit_seen = Some(BatchCommit(curr_max_batch_commit.0.max(new_max_batch_commit.0)));
            },
            (Some(_), None) => { }
        }
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

fn commit_to_index(batch_player: &BatchPlayer,
                   index: &Index,
                   batch: Batch,
                   batch_commit: BatchCommit,
                   commit: Commit) {
    let index_ops = batch_player.replay(batch, batch_commit);
    let mut writer = index.writer(commit);
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

static UNEXPECTED_LOG: &'static str = "unexpected command in log";
static BATCH_MISMATCH: &'static str = "mismatch in batch / batch_commit between commit log and tree log";
static DUPLICATE_BATCH_COMMIT: &'static str = "duplicate batch / batch_ commit during replay";
