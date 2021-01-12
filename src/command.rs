use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Clone)]
pub struct Key(pub Arc<Vec<u8>>);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Ord, PartialOrd)]
#[derive(Clone)]
pub struct Value(pub Arc<Vec<u8>>);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Copy, Clone)]
pub struct Batch(usize);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Copy, Clone)]
pub struct BatchCommit(usize);

#[derive(Serialize, Deserialize)]
pub enum Command {
    Write {
        batch: Batch,
        key: Key,
        value: Value,
    },
    Delete {
        batch: Batch,
        key: Key,
    },
    DeleteRange {
        batch: Batch,
        start_key: Key,
        end_key: Key,
    },
    PushSavePoint {
        batch: Batch,
    },
    PopSavePoint {
        batch: Batch,
    },
    RollbackSavePoint {
        batch: Batch,
    },
    ReadyCommit {
        batch: Batch,
        batch_commit: BatchCommit,
    },
    Close {
        batch: Batch,
    },
}

