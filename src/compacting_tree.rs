use anyhow::Result;
use async_channel::{self, Sender, Receiver};
use std::sync::{RwLock, Mutex, Arc, RwLockWriteGuard};
use crate::tree::{self, Tree};
use crate::types::{Commit, Batch, BatchCommit};

/// Just one batch number in compacted logs
const COMPACTED_BATCH_NUM: Batch = Batch(0);
const COMPACTED_BATCH_COMMIT_NUM: BatchCommit = BatchCommit(0);

pub struct CompactingTree {
    trees: Arc<RwLock<Trees>>,
    compact_state: Arc<Mutex<CompactState>>,
}

struct Trees {
    /// The tree that future write batches will write to
    ///
    /// This is the first tree searched for reads.
    active: Tree,
    /// The tree that is being compacted.
    /// There may be outstanding write batches or read views
    /// attached to this at the time of compaction is requested.
    /// Compaction will not actually begin until all batches
    /// against it are closed, making the tree "done".
    ///
    /// This is the second tree searched for reads.
    compacting: Option<Tree>,
    /// This is the previously compacted state of the tree.
    /// It contains a single commit and is immutable.
    ///
    /// This is the third and last tree search for reads.
    compacted: Option<Tree>,
    /// This is the compacted log currently being produced
    /// from the `compacting` log.
    ///
    /// It is not searched for reads.
    compacted_wip: Option<Tree>,
    /// These are trees with outstanding read views at
    /// the time a compaction finished.
    ///
    /// They are waiting to be deleted.
    trash: Vec<Tree>,
}

enum CompactState {
    NotCompacting,
    Compacting,
}

pub struct BatchWriter {
}

pub struct Cursor {
    tree_cursors: Vec<tree::Cursor>,
}

impl CompactingTree {
    /// Compacts the tree, removing any stale data.
    ///
    /// Although this is async, it should probably be run in
    /// a dedicated thread, is it may take a long time to complete
    /// (and so probably should not be awaited),
    /// and it does significant CPU work between IO work.
    ///
    /// Returns `true` if a compaction was performed.
    /// Returns `false` if a compaction was already in progress.
    pub async fn compact(&self) -> Result<bool> {

        // Claim the compaction routine for this tree
        {
            let mut compact_state = self.compact_state.lock().expect("lock");

            match *compact_state {
                CompactState::NotCompacting => {
                    *compact_state = CompactState::Compacting;
                },
                CompactState::Compacting => {
                    return Ok(false);
                },
            }

            drop(compact_state);
        }

        let r = async {
            // Move trees around for future readers and writers
            {
                let mut trees = self.trees.write().expect("lock");

                assert!(trees.compacting.is_none());
                assert!(trees.compacted_wip.is_none());

                self.move_active_tree_to_compacting(&mut trees).await?;
                self.create_compacted_wip_tree(&mut trees).await?;

                drop(trees);
            }

            let last_commit = self.wait_for_all_writes_to_compacting_tree().await?;
            let commit_limit = Commit(last_commit.0.checked_add(1).expect("overflow"));

            // Open a cursor for the compacting tree,
            // and the compacted tree, and a writer
            // for the compacted_wip tree.
            let (cursor, writer) = {
                let trees = self.trees.read().expect("lock");
                let compacting_cursor = trees.compacting.as_ref().expect("tree").cursor(commit_limit);
                let compacted_cursor = trees.compacting.as_ref().expect("tree").cursor(commit_limit);
                let compacted_wip_writer = trees.compacted_wip.as_ref().expect("tree").batch(COMPACTED_BATCH_NUM);

                drop(trees);

                let cursor = Cursor {
                    tree_cursors: vec![compacting_cursor, compacted_cursor],
                };

                (cursor, compacted_wip_writer)
            };

            panic!()
        }.await;

        {
            let mut compact_state = self.compact_state.lock().expect("lock");
            *compact_state = CompactState::NotCompacting;
        }

        r
    }

    async fn move_active_tree_to_compacting(&self, trees: &mut RwLockWriteGuard<'_, Trees>) -> Result<()> {
        panic!()
    }

    async fn create_compacted_wip_tree(&self, trees: &mut RwLockWriteGuard<'_, Trees>) -> Result<()> {
        panic!()
    }

    async fn wait_for_all_writes_to_compacting_tree(&self) -> Result<Commit> {
        panic!()
    }
}
