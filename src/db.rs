use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::sync::Arc;
use std::collections::BTreeMap;
use std::path::PathBuf;
use crate::tree::{self, Tree};
use anyhow::{Result, Context};
use crate::types::{Batch, BatchCommit, Commit};

pub struct Db {
    config: DbConfig,
    next_batch: AtomicUsize,
    next_batch_commit: Arc<AtomicUsize>,
    next_commit: Arc<AtomicUsize>,
    next_view: AtomicUsize,
    view_commit_limit: Arc<AtomicUsize>,
    commit_lock: Arc<Mutex<()>>,
    trees: BTreeMap<String, Tree>,
}

pub struct DbConfig {
    dir: PathBuf,
    trees: Vec<String>,
}

pub struct BatchWriter {
    batch: Batch,
    batch_writers: BTreeMap<String, tree::BatchWriter>,
    next_batch_commit: Arc<AtomicUsize>,
    next_commit: Arc<AtomicUsize>,
    view_commit_limit: Arc<AtomicUsize>,
    commit_lock: Arc<Mutex<()>>,
}

impl BatchWriter {
    pub async fn commit(&self) -> Result<()> {
        // Take a new batch_commit number
        let batch_commit = BatchCommit(self.next_batch_commit.fetch_add(1, Ordering::SeqCst));
        assert_ne!(batch_commit.0, usize::max_value());

        // First, ready-commit all the trees
        {
            let mut any_error = None;
            for (tree, writer) in self.batch_writers.iter() {
                if any_error.is_some() {
                    if let Err(e) = writer.ready_commit(batch_commit).await {
                        any_error = Some((e, tree));
                    }
                } else {
                    writer.abort_commit(batch_commit).await;
                }
            }
            if let Some((any_error, tree)) = any_error {
                return Err(any_error)
                    .context(format!("error ready-committing {}", tree));
            }
        }

        // Next steps are under the commit lock in order
        // to keep commits numbers stored monotonically
        {
            let commit_lock = self.commit_lock.lock().expect("lock");

            // Take a new commit number
            let commit = Commit(self.next_commit.fetch_add(1, Ordering::SeqCst));
            assert_ne!(commit.0, usize::max_value());

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
        }

        Ok(())
    }

    async fn write_master_commit(&self, _commit_lock: &MutexGuard<'_, ()>, commit: Commit, batch_commit: BatchCommit) -> Result<()> {
        panic!()
    }

    pub async fn close(self) -> Result<()> {
        for (tree, writer) in self.batch_writers.into_iter() {
            // If close isn't written it only should affect memory
            // use during initial replay of logs.
            let _ = writer.close().await;
        }
        Ok(())
    }
}
