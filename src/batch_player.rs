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
        address: Address,
    },
    DeleteRange {
        start_key: Key,
        end_key: Key,
        address: Address,
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
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::Write {
                    key: key.clone(),
                    address,
                });
            },
            Command::Delete { batch, key } => {
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::Delete {
                    key: key.clone(),
                    address,
                });
            },
            Command::DeleteRange { batch, start_key, end_key } => {
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::DeleteRange {
                    start_key: start_key.clone(),
                    end_key: end_key.clone(),
                    address,
                });
            },
            Command::PushSavePoint { batch } => {
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::PushSavePoint);
            },
            Command::PopSavePoint { batch } => {
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::PopSavePoint);
            },
            Command::RollbackSavePoint { batch } => {
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::RollbackSavePoint);
            },
            Command::ReadyCommit { batch, batch_commit } => {
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::ReadyCommit {
                    batch_commit: *batch_commit,
                });
            },
            Command::AbortCommit { batch, batch_commit } => {
                let mut batch_data = batches.get_mut(batch).expect("batch");
                batch_data.commands.push(SimpleCommand::AbortCommit {
                    batch_commit: *batch_commit,
                });
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
