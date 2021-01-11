pub use std::sync::Arc;
pub use std::sync::{RwLock, RwLockWriteGuard};
pub use std::sync::atomic::{AtomicUsize, Ordering};
pub use std::collections::btree_map::{BTreeMap, Entry};

#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Clone)]
pub struct Key(pub Vec<u8>);

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

pub struct Node {
    prev_next: RwLock<(Option<Arc<Node>>, Option<Arc<Node>>)>,
    history: RwLock<Vec<(Generation, Value)>>,
}

pub struct Cursor {
    gen: Generation,
    current: Option<Node>,
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

impl<'index> Writer<'index> {
    pub fn write(&mut self, key: Key, addr: Address) {
        if let Some(node) = self.keymap.get_mut(&key) {
            // key already exists
            let mut history = node.history.write().expect("lock");
            history.push((self.next_gen, Value::Written(addr)));
        } if let Some(next) = self.keymap.range(key.clone()..).next() {
            // next key exists
            panic!()
        } else if let Some(prev) = self.keymap.range(..=key.clone()).next() {
            // prev key exists
            panic!()
        } else {
            // no key exists
            assert!(self.keymap.is_empty());
            panic!()
        }
    }

    pub fn delete(&mut self, key: Key, addr: Address) {
    }

    pub fn delete_range(&mut self, start: Key, end: Key, addr: Address) {
    }
}

impl<'index> Drop for Writer<'index> {
    fn drop(&mut self) {
        let next_gen = self.next_gen.0.checked_add(1).expect("overflow");
        self.current_gen.store(next_gen, Ordering::SeqCst);
    }
}
