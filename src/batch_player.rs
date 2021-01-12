use std::collections::BTreeMap;
use std::sync::Mutex;
use crate::command::{Command, Batch, BatchCommit, Key};
use crate::log::Address;

pub struct BatchPlayer {
    batches: Mutex<BTreeMap<Batch, BatchData>>,
}

struct BatchData {
    commands: Vec<SimpleCommand>,
}

enum SimpleCommand {
    Write {
        key: Key,
        address: Address,
    },
    Delete {
        key: Key,
    },
    DeleteRange {
        start_key: Key,
        end_key: Key,
    },
    PushSavePoint,
    PopSavePoint,
    RollbackSavePoint,
    ReadyCommit {
        batch_commit: BatchCommit,
    },
    AbortCommit {
        batch_commit: BatchCommit,
    },
}

pub enum IndexOp {
    Write(Key, Address),
    Delete(Key, Address),
    DeleteRange { start: Key, end: Key },
}

impl BatchPlayer {
    fn new() -> BatchPlayer {
        BatchPlayer {
            batches: Mutex::new(BTreeMap::new()),
        }
    }

    fn record(&self, cmd: &Command, address: Address) {
        let mut batches = self.batches.lock().expect("lock");
        match cmd {
            Command::Open { batch } => {
                assert!(!batches.contains_key(batch));
                batches.insert(*batch, BatchData {
                    commands: vec![],
                });
            },
            Command::Write { batch, key, .. } => {
                let mut batch_data = batches.get(batch).expect("batch");
            },
            Command::Delete { batch, key } => {
                panic!()
            },
            Command::DeleteRange { batch, start_key, end_key } => {
                panic!()
            },
            Command::PushSavePoint { batch } => {
                panic!()
            },
            Command::PopSavePoint { batch } => {
                panic!()
            },
            Command::RollbackSavePoint { batch } => {
                panic!()
            },
            Command::ReadyCommit { batch, batch_commit } => {
                panic!()
            },
            Command::AbortCommit { batch, batch_commit } => {
                panic!()
            },
            Command::Close { batch } => {
                assert!(batches.contains_key(batch));
                batches.remove(batch);
            },
        }
    }

    fn replay(&self, batch: Batch, batch_commit: BatchCommit) -> impl Iterator<Item = IndexOp> {
        std::iter::empty()
    }
}
