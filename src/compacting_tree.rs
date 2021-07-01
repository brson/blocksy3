use anyhow::Result;
use async_channel::{self, Sender, Receiver};
use std::sync::{RwLock, Mutex, Arc, RwLockWriteGuard};
use crate::tree::{self, Tree};
use crate::types::{Commit, Batch, BatchCommit, Key, Value};

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
    active: Option<Tree>,
    /// The tree that is being compacted.
    /// There may be outstanding write batches or read views
    /// attached to this at the time compaction is requested.
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
    trees: Vec<tree::Cursor>,
    current: Option<usize>,
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

        let r: Result<_> = async {
            // Set up trees for compaction mode
            {
                let mut trees = self.trees.write().expect("lock");

                assert!(trees.compacting.is_none());
                assert!(trees.compacted_wip.is_none());

                // FIXME holding lock across await
                self.move_active_tree_to_compacting(&mut trees).await?;
                self.create_compacted_wip_tree(&mut trees).await?;

                drop(trees);
            }

            // Open a cursor for the compacting tree,
            // and the compacted tree, and a writer
            // for the compacted_wip tree.
            let (cursor, writer) = {
                let last_commit = self.wait_for_all_writes_to_compacting_tree().await?;
                let commit_limit = Commit(last_commit.0.checked_add(1).expect("overflow"));

                let trees = self.trees.read().expect("lock");
                let compacting_cursor = trees.compacting.as_ref().expect("tree").cursor(commit_limit);
                let compacted_cursor = trees.compacting.as_ref().expect("tree").cursor(commit_limit);
                let compacted_wip_writer = trees.compacted_wip.as_ref().expect("tree").batch(COMPACTED_BATCH_NUM);

                drop(trees);

                let cursor = Cursor {
                    trees: vec![compacting_cursor, compacted_cursor],
                    current: None,
                };

                (cursor, compacted_wip_writer)
            };

            {
                let mut cursor = cursor;
                let writer = writer;

                cursor.seek_first();
                writer.open().await?;

                while cursor.valid() {
                    let key = cursor.key();
                    let value = cursor.value().await?;
                    writer.write(key, value).await?;
                }

                writer.ready_commit(COMPACTED_BATCH_COMMIT_NUM).await?;
                writer.close().await?;
            }

            Ok(())
        }.await;

        // Move trees around to end compaction
        let r = async {
            match r {
                Ok(_) => {
                    let mut trees = self.trees.write().expect("lock");

                    assert!(trees.compacting.is_some());
                    assert!(trees.compacted_wip.is_some());

                    // FIXME holding lock across await
                    self.move_compacted_tree_to_trash(&mut trees).await?;
                    self.move_compacted_wip_tree_to_compacted(&mut trees).await?;
                }
                Err(e) => {
                    todo!()
                }
            }

            Ok(true)
        }.await;

        {
            let mut compact_state = self.compact_state.lock().expect("lock");
            *compact_state = CompactState::NotCompacting;
        }

        self.try_empty_trash().await?;

        r
    }

    async fn move_active_tree_to_compacting(&self, trees: &mut RwLockWriteGuard<'_, Trees>) -> Result<()> {
        todo!()
    }

    async fn create_compacted_wip_tree(&self, trees: &mut RwLockWriteGuard<'_, Trees>) -> Result<()> {
        todo!()
    }

    async fn move_compacted_tree_to_trash(&self, trees: &mut RwLockWriteGuard<'_, Trees>) -> Result<()> {
        todo!()
    }

    async fn move_compacted_wip_tree_to_compacted(&self, trees: &mut RwLockWriteGuard<'_, Trees>) -> Result<()> {
        todo!()
    }

    async fn wait_for_all_writes_to_compacting_tree(&self) -> Result<Commit> {
        todo!()
    }

    async fn try_empty_trash(&self) -> Result<()> {
        todo!()
    }
}

impl Cursor {
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn key(&self) -> Key {
        let idx = self.current.expect("invalid cursor");
        let tree = &self.trees[idx];
        tree.key()
    }

    pub async fn value(&mut self) -> Result<Value> {
        let idx = self.current.expect("invalid cursor");
        let tree = &mut self.trees[idx];
        tree.value().await
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        let current_idx = self.current.expect("valid");
        let current_key = self.trees[current_idx].key();
        panic!()
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        let current_idx = self.current.expect("valid");
        let current_key = self.trees[current_idx].key();
        panic!()
    }

    pub fn seek_first(&mut self) {
        let mut key_idx = None;
        for (new_idx, tree) in self.trees.iter_mut().enumerate() {
            tree.seek_first();
            if tree.valid() {
                let new_key = tree.key();
                if let Some((ref old_key, _)) = key_idx {
                    if new_key < *old_key {
                        key_idx = Some((new_key, new_idx));
                    } else {
                        /* pass */
                    }
                } else {
                    key_idx = Some((new_key, new_idx));
                }
            }
        }

        if let Some((_, idx)) = key_idx {
            self.current = Some(idx);
        }
    }

    pub fn seek_last(&mut self) {
        let mut key_idx = None;
        for (new_idx, tree) in self.trees.iter_mut().enumerate() {
            tree.seek_last();
            if tree.valid() {
                let new_key = tree.key();
                if let Some((ref old_key, _)) = key_idx {
                    if new_key > *old_key {
                        key_idx = Some((new_key, new_idx));
                    } else {
                        /* pass */
                    }
                } else {
                    key_idx = Some((new_key, new_idx));
                }
            }
        }

        if let Some((_, idx)) = key_idx {
            self.current = Some(idx);
        }
    }

    pub fn seek_key(&mut self, key: Key) {
        let mut key_idx = None;
        for (new_idx, tree) in self.trees.iter_mut().enumerate() {
            tree.seek_key(key.clone());
            if tree.valid() {
                let new_key = tree.key();
                if let Some((ref old_key, _)) = key_idx {
                    if new_key < *old_key {
                        key_idx = Some((new_key, new_idx));
                    } else {
                        /* pass */
                    }
                } else {
                    key_idx = Some((new_key, new_idx));
                }
            }
        }

        if let Some((_, idx)) = key_idx {
            self.current = Some(idx);
        }
    }

    pub fn seek_key_rev(&mut self, key: Key) {
        let mut key_idx = None;
        for (new_idx, tree) in self.trees.iter_mut().enumerate() {
            tree.seek_key_rev(key.clone());
            if tree.valid() {
                let new_key = tree.key();
                if let Some((ref old_key, _)) = key_idx {
                    if new_key > *old_key {
                        key_idx = Some((new_key, new_idx));
                    } else {
                        /* pass */
                    }
                } else {
                    key_idx = Some((new_key, new_idx));
                }
            }
        }

        if let Some((_, idx)) = key_idx {
            self.current = Some(idx);
        }
    }
}
