pub use std::sync::Arc;
pub use std::sync::{RwLock, RwLockWriteGuard};
pub use std::sync::atomic::{AtomicUsize, Ordering};
pub use std::collections::btree_map::{BTreeMap, Entry};
pub use std::ops::RangeBounds;
use crate::types::{Key, Address, Commit};

#[derive(Copy, Clone)]
pub enum Value {
    Written(Address),
    Deleted(Address),
}

pub struct Index {
    commit: AtomicUsize,
    keymap: Arc<RwLock<BTreeMap<Key, Arc<Node>>>>,
}

struct Node {
    key: Key,
    prev: RwLock<Option<Arc<Node>>>,
    next: RwLock<Option<Arc<Node>>>,
    history: RwLock<Vec<(Commit, Value)>>,
}

pub struct Cursor {
    commit: Commit,
    current: Option<Arc<Node>>,
    keymap: Arc<RwLock<BTreeMap<Key, Arc<Node>>>>,
}

pub struct Writer<'index> {
    next_commit: Commit,
    current_commit: &'index AtomicUsize,
    keymap: RwLockWriteGuard<'index, BTreeMap<Key, Arc<Node>>>,
}

impl Index {
    pub fn new() -> Index {
        Index {
            commit: AtomicUsize::new(0),
            keymap: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn read(&self, commit: Commit, key: &Key) -> Option<Value> {
        assert!(commit <= Commit(self.commit.load(Ordering::SeqCst)));
        let map = self.keymap.read().expect("lock");
        if let Some(node) = map.get(key) {
            let history = node.history.read().expect("lock");
            for (h_commit, value) in history.iter().rev() {
                if *h_commit < commit {
                    return Some(*value);
                }
            }
            None
        } else {
            None
        }
    }

    pub fn cursor(&self, commit: Commit) -> Cursor {
        assert!(commit <= Commit(self.commit.load(Ordering::SeqCst)));
        Cursor {
            commit,
            current: None,
            keymap: self.keymap.clone(),
        }
    }

    pub fn writer(&self, commit: Commit) -> Writer {
        assert!(commit >= Commit(self.commit.load(Ordering::SeqCst)));
        Writer {
            next_commit: commit,
            current_commit: &self.commit,
            keymap: self.keymap.write().expect("lock"),
        }
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        let mut keymap = self.keymap.write().expect("lock");
        for (_, mut node) in keymap.iter_mut() {
            let mut prev = node.prev.write().expect("lock");
            *prev = None;
            let mut next = node.next.write().expect("lock");
            *next = None;
        }
    }
}

impl Cursor {
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        let mut candidate_node = {
            self.current.as_ref().expect("valid").next.read().expect("lock").clone()
        };
        while let Some(node) = candidate_node {
            let history = node.history.read().expect("lock");
            for (commit, _) in history.iter().rev() {
                if *commit < self.commit {
                    self.current = Some(node.clone());
                    return;
                }
            }
            candidate_node = node.next.read().expect("lock").clone();
        }
        self.current = None;
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        let mut candidate_node = {
            self.current.as_ref().expect("valid").prev.read().expect("lock").clone()
        };
        while let Some(node) = candidate_node {
            let history = node.history.read().expect("lock");
            for (commit, _) in history.iter().rev() {
                if *commit < self.commit {
                    self.current = Some(node.clone());
                    return;
                }
            }
            candidate_node = node.prev.read().expect("lock").clone();
        }
        self.current = None;
    }

    pub fn key(&self) -> Key {
        assert!(self.valid());
        self.current.as_ref().expect("valid").key.clone()
    }

    pub fn seek_first(&mut self) {
        let keymap = self.keymap.read().expect("lock");
        self.current = keymap.iter().map(|(_, node)| node.clone()).next();
    }

    pub fn seek_last(&mut self) {
        let keymap = self.keymap.read().expect("lock");
        self.current = keymap.iter().rev().map(|(_, node)| node.clone()).next();
    }

    pub fn seek_key(&mut self, key: Key) {
        let keymap = self.keymap.read().expect("lock");
        self.current = keymap.range(key..).map(|(_, node)| node.clone()).next();
    }

    pub fn seek_key_rev(&mut self, key: Key) {
        let keymap = self.keymap.read().expect("lock");
        self.current = keymap.range(..=key).rev().map(|(_, node)| node.clone()).next();
    }
}

impl<'index> Writer<'index> {
    pub fn write(&mut self, key: Key, addr: Address) {
        self.update_value(key, Value::Written(addr))
    }

    pub fn delete(&mut self, key: Key, addr: Address) {
        self.update_value(key, Value::Deleted(addr))
    }

    pub fn delete_range<R>(&mut self, range: R, addr: Address)
    where R: RangeBounds<Key>
    {
        for (_, node) in self.keymap.range_mut(range) {
            let mut history = node.history.write().expect("lock");
            history.push((self.next_commit, Value::Deleted(addr)));
        }
    }

    fn update_value(&mut  self, key: Key, value: Value) {
        let new_node;
        if let Some(node) = self.keymap.get_mut(&key) {
            // key already exists
            let mut history = node.history.write().expect("lock");
            history.push((self.next_commit, value));
            new_node = None;
        } else if let Some((_, next)) = self.keymap.range(key.clone()..).next() {
            // next key exists
            let mut next_prev = next.prev.write().expect("lock");
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(next_prev.clone()),
                next: RwLock::new(Some(next.clone())),
                history: RwLock::new(vec![(self.next_commit, value)]),
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
                history: RwLock::new(vec![(self.next_commit, value)]),
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
                history: RwLock::new(vec![(self.next_commit, value)]),
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
        let next_commit = self.next_commit.0.checked_add(1).expect("overflow");
        self.current_commit.store(next_commit, Ordering::SeqCst);
    }
}
