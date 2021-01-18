use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::sync::Arc;
use std::collections::BTreeMap;
use std::path::PathBuf;
use crate::tree::{self, Tree};
use anyhow::{Result, Context, anyhow};
use crate::types::{Batch, BatchCommit, Commit, Key, Value};

pub struct Db {
    config: DbConfig,
    next_batch: AtomicU64,
    next_batch_commit: Arc<AtomicU64>,
    next_commit: Arc<AtomicU64>,
    view_commit_limit: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<()>>,
    trees: Arc<BTreeMap<String, Tree>>,
}

pub struct DbConfig {
    dir: PathBuf,
    trees: Vec<String>,
}

pub struct BatchWriter {
    batch: Batch,
    batch_writers: BTreeMap<String, tree::BatchWriter>,
    next_batch_commit: Arc<AtomicU64>,
    next_commit: Arc<AtomicU64>,
    view_commit_limit: Arc<AtomicU64>,
    commit_lock: Arc<Mutex<()>>,
}

pub struct ViewReader {
    commit_limit: Commit,
    trees: Arc<BTreeMap<String, Tree>>,
}

pub struct Cursor {
    tree_cursor: tree::Cursor,
}

impl Db {
    pub fn batch(&self) -> BatchWriter {
        let batch = Batch(self.next_batch.fetch_add(1, Ordering::SeqCst));
        assert_ne!(batch.0, u64::max_value());

        let batch_writers = self.trees.iter().map(|(name, tree)| {
            (name.clone(), tree.batch(batch))
        }).collect();

        BatchWriter {
            batch,
            batch_writers,
            next_batch_commit: self.next_batch_commit.clone(),
            next_commit: self.next_commit.clone(),
            view_commit_limit: self.view_commit_limit.clone(),
            commit_lock: self.commit_lock.clone(),
        }
    }

    pub fn view(&self) -> ViewReader {
        let commit_limit = Commit(self.view_commit_limit.load(Ordering::SeqCst));

        ViewReader {
            commit_limit,
            trees: self.trees.clone(),
        }
    }
}

impl BatchWriter {
    fn tree_writer(&self, tree: &str) -> &tree::BatchWriter {
        self.batch_writers.get(tree).expect("tree")
    }

    pub async fn open(&self, tree: &str) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.open().await?)
    }

    pub async fn write(&self, tree: &str, key: Key, value: Value) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.write(key, value).await?)
    }

    pub async fn delete(&self, tree: &str, key: Key) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.delete(key).await?)
    }

    pub async fn delete_range(&self, tree: &str, start_key: Key, end_key: Key) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.delete_range(start_key, end_key).await?)
    }

    pub async fn push_save_point(&self, tree: &str) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.push_save_point().await?)
    }

    pub async fn pop_save_point(&self, tree: &str) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.pop_save_point().await?)
    }

    pub async fn rollback_save_point(&self, tree: &str) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.rollback_save_point().await?)
    }

    pub fn new_batch_commit_number(&self) -> BatchCommit {
        // Take a new batch_commit number
        let batch_commit = BatchCommit(self.next_batch_commit.fetch_add(1, Ordering::SeqCst));
        assert_ne!(batch_commit.0, u64::max_value());
        batch_commit
    }

    pub async fn ready_commit(&self, tree: &str, batch_commit: BatchCommit) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.ready_commit(batch_commit).await?)
    }

    pub async fn commit(&self, batch_commit: BatchCommit) -> Result<()> {
        // Next steps are under the commit lock in order
        // to keep commits numbers stored monotonically
        let commit_lock = self.commit_lock.lock().expect("lock");

        // Take a new commit number
        let commit = Commit(self.next_commit.fetch_add(1, Ordering::SeqCst));
        assert_ne!(commit.0, u64::max_value());

        // Write the master commit.
        // If this fails then the commit is effectively aborted.
        self.write_master_commit(&commit_lock, commit, batch_commit).await?;

        // Infallably promote each tree's writes to its index.
        for (tree, writer) in self.batch_writers.iter() {
            writer.commit(batch_commit, commit)
        }

        // Bump the view commit limit
        let new_commit_limit = commit.0.checked_add(1).expect("overflow");
        let old_commit_limit = self.view_commit_limit.swap(new_commit_limit, Ordering::SeqCst);
        assert!(old_commit_limit < new_commit_limit);

        Ok(())
    }

    async fn write_master_commit(&self, _commit_lock: &MutexGuard<'_, ()>, commit: Commit, batch_commit: BatchCommit) -> Result<()> {
        panic!()
    }

    pub async fn close(self, tree: &str) -> Result<()> {
        let writer = self.tree_writer(tree);
        Ok(writer.close().await?)
    }
}

impl ViewReader {
    pub async fn read(&self, tree: &str, key: &Key) -> Result<Option<Value>> {
        panic!()
    }

    pub fn cursor(&self, tree: &str) -> Cursor {
        panic!()
    }
}

impl Cursor {
    pub fn is_valid(&self) -> bool {
        self.tree_cursor.is_valid()
    }

    pub fn key(&self) -> Key {
        self.tree_cursor.key()
    }

    pub async fn value(&mut self) -> Result<Value> {
        Ok(self.tree_cursor.value().await?)
    }

    pub fn next(&mut self) {
        self.tree_cursor.next()
    }

    pub fn prev(&mut self) {
        self.tree_cursor.prev()
    }

    pub fn seek_first(&mut self) {
        self.tree_cursor.seek_first()
    }

    pub fn seek_last(&mut self) {
        self.tree_cursor.seek_last()
    }

    pub fn seek_key(&mut self, key: Key) {
        self.tree_cursor.seek_key(key)
    }

    pub fn seek_key_rev(&mut self, key: Key) {
        self.tree_cursor.seek_key_rev(key)
    }
}
