pub use log::error;
pub use std::sync::Arc;
pub use std::sync::{RwLock, RwLockWriteGuard};
pub use std::sync::atomic::{AtomicU64, Ordering};
pub use std::collections::btree_map::{BTreeMap, Entry};
pub use std::ops::RangeBounds;
use crate::types::{Key, Address, Commit};

pub struct Index {
    maybe_next_commit: AtomicU64,
    keymap: Arc<RwLock<BTreeMap<Key, Arc<Node>>>>,
}

#[derive(Debug)]
struct Node {
    key: Key,
    prev: RwLock<Option<Arc<Node>>>,
    next: RwLock<Option<Arc<Node>>>,
    history: RwLock<Vec<(Commit, ReadValue)>>,
}

pub struct Cursor {
    commit_limit: Commit,
    current: Option<(Arc<Node>, Address)>,
    keymap: Arc<RwLock<BTreeMap<Key, Arc<Node>>>>,
}

pub struct Writer<'index> {
    commit: Commit,
    maybe_next_commit: &'index AtomicU64,
    keymap: RwLockWriteGuard<'index, BTreeMap<Key, Arc<Node>>>,
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
            keymap: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn read(&self, commit_limit: Commit, key: &Key) -> Option<Address> {
        assert!(commit_limit <= Commit(self.maybe_next_commit.load(Ordering::SeqCst)));
        let map = self.keymap.read().expect("lock");
        if let Some(node) = map.get(key) {
            let history = node.history.read().expect("lock");
            for (h_commit, value) in history.iter().rev() {
                if *h_commit < commit_limit {
                    match value {
                        ReadValue::Written(addr) => {
                            return Some(*addr);
                        },
                        ReadValue::Deleted(addr) => {
                            return None;
                        }
                    }
                }
            }
            None
        } else {
            None
        }
    }

    pub fn cursor(&self, commit_limit: Commit) -> Cursor {
        assert!(commit_limit <= Commit(self.maybe_next_commit.load(Ordering::SeqCst)));
        Cursor {
            commit_limit,
            current: None,
            keymap: self.keymap.clone(),
        }
    }

    pub fn writer(&self, commit: Commit) -> Writer {
        assert!(commit >= Commit(self.maybe_next_commit.load(Ordering::SeqCst)));
        Writer {
            commit: commit,
            maybe_next_commit: &self.maybe_next_commit,
            keymap: self.keymap.write().expect("lock"),
        }
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        // Don't panic on drop
        if let Ok(mut keymap) = self.keymap.write() {
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
        let keymap = self.keymap.read().expect("lock");
        let iter = keymap.iter();
        self.current = self.first_within_commit_limit(iter);
    }

    pub fn seek_last(&mut self) {
        let keymap = self.keymap.read().expect("lock");
        let iter = keymap.iter().rev();
        self.current = self.first_within_commit_limit(iter);
    }

    pub fn seek_key(&mut self, key: Key) {
        let keymap = self.keymap.read().expect("lock");
        let iter = keymap.range(key..);
        self.current = self.first_within_commit_limit(iter);
    }

    pub fn seek_key_rev(&mut self, key: Key) {
        let keymap = self.keymap.read().expect("lock");
        let iter = keymap.range(..=key).rev();
        self.current = self.first_within_commit_limit(iter);
    }

    fn value_within_commit_limit(&self, node: &Node) -> Option<Address> {
        let history = node.history.read().expect("lock");
        for (commit, value) in history.iter().rev() {
            if *commit < self.commit_limit {
                match value {
                    ReadValue::Written(addr) => {
                        return Some(*addr);
                    },
                    ReadValue::Deleted(_) => {
                        return None;
                    },
                }
            }
        }
        None
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
    pub fn write(&mut self, key: Key, addr: Address) {
        self.update_value(key, ReadValue::Written(addr))
    }

    pub fn delete(&mut self, key: Key, addr: Address) {
        self.update_value(key, ReadValue::Deleted(addr))
    }

    pub fn delete_range<R>(&mut self, range: R, addr: Address)
    where R: RangeBounds<Key>
    {
        for (_, node) in self.keymap.range_mut(range) {
            let mut history = node.history.write().expect("lock");
            history.push((self.commit, ReadValue::Deleted(addr)));
        }
    }

    fn update_value(&mut  self, key: Key, value: ReadValue) {
        let new_node;
        if let Some(node) = self.keymap.get(&key) {
            // key already exists
            let mut history = node.history.write().expect("lock");
            history.push((self.commit, value));
            new_node = None;
        } else if let Some((_, next)) = self.keymap.range(key.clone()..).next() {
            // next key exists
            let mut next_prev = next.prev.write().expect("lock");
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(next_prev.clone()),
                next: RwLock::new(Some(next.clone())),
                history: RwLock::new(vec![(self.commit, value)]),
            });
            if let Some(next_prev) = next_prev.as_ref() {
                let mut next_prev_next = next_prev.next.write().expect("lock");
                *next_prev_next = Some(new.clone());
            }
            *next_prev = Some(new.clone());
            new_node = Some(new);
        } else if let Some((_, prev)) = self.keymap.range(..=key.clone()).rev().next() {
            // prev key exists
            let mut prev_next = prev.next.write().expect("lock");
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(Some(prev.clone())),
                next: RwLock::new(prev_next.clone()),
                history: RwLock::new(vec![(self.commit, value)]),
            });
            if let Some(prev_next) = prev_next.as_ref() {
                let mut prev_next_prev = prev_next.prev.write().expect("lock");
                *prev_next_prev = Some(new.clone());
            }
            *prev_next = Some(new.clone());
            new_node = Some(new);
        } else {
            // no key exists
            assert!(self.keymap.is_empty());
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(None),
                next: RwLock::new(None),
                history: RwLock::new(vec![(self.commit, value)]),
            });
            new_node = Some(new);
        }
        if let Some(new_node) = new_node {
            self.keymap.insert(key, new_node);
        }
    }
}

impl<'index> Drop for Writer<'index> {
    fn drop(&mut self) {
        let next_commit = self.commit.0.checked_add(1).expect("overflow");
        self.maybe_next_commit.store(next_commit, Ordering::SeqCst);
    }
}
