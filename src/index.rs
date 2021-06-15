use log::error;
use std::sync::Arc;
use std::sync::{RwLock, RwLockWriteGuard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::btree_map::{BTreeMap, Entry};
use std::ops::Range;
use crate::types::{Key, Address, Commit};

#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Debug)]
#[derive(Copy, Clone)]
pub struct BatchIdx(pub u32);

pub struct Index {
    maybe_next_commit: AtomicU64,
    state: Arc<RwLock<IndexState>>,
}

struct IndexState {
    keymap: BTreeMap<Key, Arc<Node>>,
    range_deletes: Vec<(Commit, Range<Key>, BatchIdx)>,
}

#[derive(Debug)]
struct Node {
    key: Key,
    prev: RwLock<Option<Arc<Node>>>,
    next: RwLock<Option<Arc<Node>>>,
    history: RwLock<Vec<(Commit, ReadValue, BatchIdx)>>,
}

pub struct Cursor {
    commit_limit: Commit,
    current: Option<(Arc<Node>, Address)>,
    state: Arc<RwLock<IndexState>>,
}

pub struct Writer<'index> {
    commit: Commit,
    maybe_next_commit: &'index AtomicU64,
    state: RwLockWriteGuard<'index, IndexState>,
}

#[derive(Copy, Clone)]
#[derive(Debug)]
pub enum ReadValue {
    Written(Address),
    Deleted(Address),
}

impl Index {
    pub fn new() -> Index {
        Index {
            maybe_next_commit: AtomicU64::new(0),
            state: Arc::new(RwLock::new(IndexState {
                keymap: BTreeMap::new(),
                range_deletes: Vec::new(),
            })),
        }
    }

    pub fn read(&self, commit_limit: Commit, key: &Key) -> Option<Address> {
        assert!(commit_limit <= Commit(self.maybe_next_commit.load(Ordering::SeqCst)));
        let state = self.state.read().expect("lock");
        state.key_true_value(commit_limit, key)
    }

    pub fn cursor(&self, commit_limit: Commit) -> Cursor {
        assert!(commit_limit <= Commit(self.maybe_next_commit.load(Ordering::SeqCst)));
        Cursor {
            commit_limit,
            current: None,
            state: self.state.clone(),
        }
    }

    pub fn writer(&self, commit: Commit) -> Writer {
        assert!(commit >= Commit(self.maybe_next_commit.load(Ordering::SeqCst)));
        Writer {
            commit: commit,
            maybe_next_commit: &self.maybe_next_commit,
            state: self.state.write().expect("lock"),
        }
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        // Don't panic on drop
        if let Ok(mut state) = self.state.write() {
            let mut keymap = &mut state.keymap;
            for (_, mut node) in keymap.iter_mut() {
                let err = || error!("failed index unlink due to lock poison");
                if let Ok(mut prev) = node.prev.write() {
                    *prev = None;
                } else {
                    err();
                }
                if let Ok(mut next) = node.next.write() {
                    *next = None;
                } else {
                    err();
                }
            }
        } else {
            error!("massive index leak due to lock poison");
        }
    }
}

impl IndexState {
    fn point_query(&self, commit_limit: Commit, key: &Key) -> Option<(Commit, ReadValue, BatchIdx)> {
        if let Some(node) = self.keymap.get(key) {
            self.node_value_within_commit_limit(commit_limit, node)
        } else {
            None
        }
    }

    fn node_value_within_commit_limit(&self, commit_limit: Commit, node: &Node) -> Option<(Commit, ReadValue, BatchIdx)> {
        let history = node.history.read().expect("lock");
        let mut rev_iter = history.iter().rev();
        let most_recent = rev_iter.find(|(commit, _, _)| *commit < commit_limit);
        most_recent.cloned()
    }

    fn range_delete_query(&self, commit_limit: Commit, key: &Key) -> Option<(Commit, BatchIdx)> {
        let mut rev_iter = self.range_deletes.iter().rev();
        let match_ = rev_iter.find(|(commit, range, _)| {
            *commit < commit_limit && range.contains(key)
        });
        match_.map(|(commit, _, batch_idx)| (*commit, *batch_idx))
    }

    fn key_true_value(&self, commit_limit: Commit, key: &Key) -> Option<Address> {
        let point_result = self.point_query(commit_limit, key);
        let range_delete_result = self.range_delete_query(commit_limit, key);
        self.inner_true_value(point_result, range_delete_result)
    }

    fn node_true_value(&self, commit_limit: Commit, node: &Node) -> Option<Address> {
        let point_result = self.node_value_within_commit_limit(commit_limit, node);
        let range_delete_result = self.range_delete_query(commit_limit, &node.key);
        self.inner_true_value(point_result, range_delete_result)
    }

    fn inner_true_value(&self,
                        point_result: Option<(Commit, ReadValue, BatchIdx)>,
                        range_delete_result: Option<(Commit, BatchIdx)>) -> Option<Address> {
        match (point_result, range_delete_result) {
            (None, Some(_)) => {
                None
            },
            (Some((p_commit, ReadValue::Written(addr), p_batch_idx)), Some((rd_commit, rd_batch_idx))) => {
                if p_commit > rd_commit {
                    Some(addr)
                } else if p_commit == rd_commit {
                    if p_batch_idx > rd_batch_idx {
                        Some(addr)
                    } else {
                        None
                    }
                } else {
                    None
                }
            },
            (Some((_, ReadValue::Deleted(addr), _)), Some(_)) => {
                None
            },
            (Some((_, ReadValue::Written(addr), _)), None) => {
                Some(addr)
            },
            (Some((_, ReadValue::Deleted(addr), _)), None) => {
                None
            },
            (None, None) => {
                None
            },
        }
    }
}

impl Cursor {
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn key(&self) -> Key {
        assert!(self.valid());
        self.current.as_ref().expect("valid").0.key.clone()
    }

    pub fn address(&self) -> Address {
        assert!(self.valid());
        self.current.as_ref().expect("valid").1
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        let mut candidate_node = {
            self.current.as_ref().expect("valid").0.next.read().expect("lock").clone()
        };
        while let Some(node) = candidate_node {
            if let Some(addr) = self.value_within_commit_limit(&node) {
                self.current = Some((node, addr));
                return;
            }
            candidate_node = node.next.read().expect("lock").clone();
        }
        self.current = None;
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        let mut candidate_node = {
            self.current.as_ref().expect("valid").0.prev.read().expect("lock").clone()
        };
        while let Some(node) = candidate_node {
            if let Some(addr) = self.value_within_commit_limit(&node) {
                self.current = Some((node, addr));
                return;
            }
            candidate_node = node.prev.read().expect("lock").clone();
        }
        self.current = None;
    }

    pub fn seek_first(&mut self) {
        let state = self.state.read().expect("lock");
        let iter = state.keymap.iter();
        self.current = self.first_within_commit_limit(iter);
    }

    pub fn seek_last(&mut self) {
        let state = self.state.read().expect("lock");
        let iter = state.keymap.iter().rev();
        self.current = self.first_within_commit_limit(iter);
    }

    pub fn seek_key(&mut self, key: Key) {
        let state = self.state.read().expect("lock");
        let iter = state.keymap.range(key..);
        self.current = self.first_within_commit_limit(iter);
    }

    pub fn seek_key_rev(&mut self, key: Key) {
        let state = self.state.read().expect("lock");
        let iter = state.keymap.range(..=key).rev();
        self.current = self.first_within_commit_limit(iter);
    }

    fn value_within_commit_limit(&self, node: &Node) -> Option<Address> {
        let state = self.state.read().expect("state");
        state.node_true_value(self.commit_limit, node)
    }

    fn first_within_commit_limit<'a>(&self, iter: impl Iterator<Item = (&'a Key, &'a Arc<Node>)>) -> Option<(Arc<Node>, Address)> {
        iter.map(|(_, node)| node)
            .filter_map(|node| {
                self.value_within_commit_limit(node).map(|addr| (node.clone(), addr))
            })
            .next()
    }
}

impl<'index> Writer<'index> {
    pub fn write(&mut self, key: Key, addr: Address, batch_idx: BatchIdx) {
        self.update_value(key, ReadValue::Written(addr), batch_idx)
    }

    pub fn delete(&mut self, key: Key, addr: Address, batch_idx: BatchIdx) {
        self.update_value(key, ReadValue::Deleted(addr), batch_idx)
    }

    pub fn delete_range(&mut self, range: Range<Key>, addr: Address, batch_idx: BatchIdx)
    {
        assert!(range.start <= range.end);
        self.state.range_deletes.push((self.commit, range, batch_idx));
    }

    fn update_value(&mut  self, key: Key, value: ReadValue, batch_idx: BatchIdx) {
        let new_node;
        if let Some(node) = self.state.keymap.get(&key) {
            // key already exists
            let mut history = node.history.write().expect("lock");
            history.push((self.commit, value, batch_idx));
            new_node = None;
        } else if let Some((_, next)) = self.state.keymap.range(key.clone()..).next() {
            // next key exists
            let mut next_prev = next.prev.write().expect("lock");
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(next_prev.clone()),
                next: RwLock::new(Some(next.clone())),
                history: RwLock::new(vec![(self.commit, value, batch_idx)]),
            });
            if let Some(next_prev) = next_prev.as_ref() {
                let mut next_prev_next = next_prev.next.write().expect("lock");
                *next_prev_next = Some(new.clone());
            }
            *next_prev = Some(new.clone());
            new_node = Some(new);
        } else if let Some((_, prev)) = self.state.keymap.range(..=key.clone()).rev().next() {
            // prev key exists
            let mut prev_next = prev.next.write().expect("lock");
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(Some(prev.clone())),
                next: RwLock::new(prev_next.clone()),
                history: RwLock::new(vec![(self.commit, value, batch_idx)]),
            });
            if let Some(prev_next) = prev_next.as_ref() {
                let mut prev_next_prev = prev_next.prev.write().expect("lock");
                *prev_next_prev = Some(new.clone());
            }
            *prev_next = Some(new.clone());
            new_node = Some(new);
        } else {
            // no key exists
            assert!(self.state.keymap.is_empty());
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(None),
                next: RwLock::new(None),
                history: RwLock::new(vec![(self.commit, value, batch_idx)]),
            });
            new_node = Some(new);
        }
        if let Some(new_node) = new_node {
            self.state.keymap.insert(key, new_node);
        }
    }
}

impl<'index> Drop for Writer<'index> {
    fn drop(&mut self) {
        let next_commit = self.commit.0.checked_add(1).expect("overflow");
        self.maybe_next_commit.store(next_commit, Ordering::SeqCst);
    }
}
