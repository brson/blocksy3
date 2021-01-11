pub use std::sync::Arc;
pub use std::sync::{RwLock, RwLockWriteGuard};
pub use std::sync::atomic::{AtomicUsize, Ordering};
pub use std::collections::btree_map::{BTreeMap, Entry};
pub use std::ops::RangeBounds;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Clone)]
pub struct Key(pub Arc<Vec<u8>>);

#[derive(Copy, Clone)]
pub enum Value {
    Written(Address),
    Deleted(Address),
}

#[derive(Copy, Clone)]
#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct Generation(pub usize);

#[derive(Copy, Clone)]
pub struct Address(pub usize);

pub struct Index {
    gen: AtomicUsize,
    keymap: Arc<RwLock<BTreeMap<Key, Arc<Node>>>>,
}

struct Node {
    key: Key,
    prev: RwLock<Option<Arc<Node>>>,
    next: RwLock<Option<Arc<Node>>>,
    history: RwLock<Vec<(Generation, Value)>>,
}

pub struct Cursor {
    gen: Generation,
    current: Option<Arc<Node>>,
    keymap: Arc<RwLock<BTreeMap<Key, Arc<Node>>>>,
}

pub struct Writer<'index> {
    next_gen: Generation,
    current_gen: &'index AtomicUsize,
    keymap: RwLockWriteGuard<'index, BTreeMap<Key, Arc<Node>>>,
}

impl Index {
    pub fn new() -> Index {
        Index {
            gen: AtomicUsize::new(0),
            keymap: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn read(&self, gen: Generation, key: &Key) -> Option<Value> {
        assert!(gen <= Generation(self.gen.load(Ordering::SeqCst)));
        let map = self.keymap.read().expect("lock");
        if let Some(node) = map.get(key) {
            let history = node.history.read().expect("lock");
            for (h_gen, value) in history.iter().rev() {
                if *h_gen < gen {
                    return Some(*value);
                }
            }
            None
        } else {
            None
        }
    }

    pub fn cursor(&self, gen: Generation) -> Cursor {
        assert!(gen <= Generation(self.gen.load(Ordering::SeqCst)));
        Cursor {
            gen,
            current: None,
            keymap: self.keymap.clone(),
        }
    }

    pub fn writer(&self, gen: Generation) -> Writer {
        assert!(gen >= Generation(self.gen.load(Ordering::SeqCst)));
        Writer {
            next_gen: gen,
            current_gen: &self.gen,
            keymap: self.keymap.write().expect("lock"),
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
            for (gen, _) in history.iter().rev() {
                if *gen < self.gen {
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
            for (gen, _) in history.iter().rev() {
                if *gen < self.gen {
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
        panic!()
    }

    pub fn seek_last(&mut self) {
        panic!()
    }

    pub fn seek_key(&mut self, key: &[u8]) {
        panic!()
    }

    pub fn seek_key_rev(&mut self, key: &[u8]) {
        panic!()
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
            history.push((self.next_gen, Value::Deleted(addr)));
        }
    }

    fn update_value(&mut  self, key: Key, value: Value) {
        let new_node;
        if let Some(node) = self.keymap.get_mut(&key) {
            // key already exists
            let mut history = node.history.write().expect("lock");
            history.push((self.next_gen, value));
            new_node = None;
        } else if let Some((_, next)) = self.keymap.range(key.clone()..).next() {
            // next key exists
            let mut next_prev = next.prev.write().expect("lock");
            let new = Arc::new(Node {
                key: key.clone(),
                prev: RwLock::new(next_prev.clone()),
                next: RwLock::new(Some(next.clone())),
                history: RwLock::new(vec![(self.next_gen, value)]),
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
                history: RwLock::new(vec![(self.next_gen, value)]),
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
                history: RwLock::new(vec![(self.next_gen, value)]),
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
        let next_gen = self.next_gen.0.checked_add(1).expect("overflow");
        self.current_gen.store(next_gen, Ordering::SeqCst);
    }
}
