use std::collections::BTreeMap;
use std::sync::Mutex;
use crate::command::Command;
use crate::types::{Address, Key, Batch, BatchCommit};

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
    Write {
        key: Key,
        address: Address
    },
    Delete {
        key: Key,
        address: Address
    },
    DeleteRange {
        start_key: Key,
        end_key: Key,
        address: Address
    },
}

impl BatchPlayer {
    fn new() -> BatchPlayer {
        BatchPlayer {
            batches: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn record(&self, cmd: &Command, address: Address) {
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

    pub fn replay(&self, batch: Batch, batch_commit: BatchCommit) -> impl Iterator<Item = IndexOp> {
        let mut batches = self.batches.lock().expect("lock");
        let batch_data = batches.get(&batch).expect("batch");
        let mut ops = vec![];
        let mut save_point_indexes = vec![];
        for cmd in &batch_data.commands {
            match cmd {
                SimpleCommand::Write { key, address } => {
                    ops.push(IndexOp::Write {
                        key: key.clone(),
                        address: *address,
                    });
                },
                SimpleCommand::Delete { key, address } => {
                    ops.push(IndexOp::Delete {
                        key: key.clone(),
                        address: *address,
                    });
                },
                SimpleCommand::DeleteRange { start_key, end_key, address } => {
                    ops.push(IndexOp::DeleteRange {
                        start_key: start_key.clone(),
                        end_key: end_key.clone(),
                        address: *address,
                    });
                },
                SimpleCommand::PushSavePoint => {
                    save_point_indexes.push(ops.len());
                },
                SimpleCommand::PopSavePoint => {
                    save_point_indexes.pop();
                },
                SimpleCommand::RollbackSavePoint => {
                    if let Some(save_point) = save_point_indexes.pop() {
                        assert!(save_point <= ops.len());
                        ops.truncate(save_point);
                    } else {
                        panic!("rollback without save point");
                    }
                },
                SimpleCommand::ReadyCommit { batch_commit: bc } => {
                    if batch_commit == *bc {
                        return ops.into_iter();
                    }
                },
                SimpleCommand::AbortCommit { batch_commit: bc }=> {
                    if batch_commit == *bc {
                        ops.clear();
                        return ops.into_iter();
                    }
                },                    
            }
        }
        panic!("uncommitted/unaborted batch replay");
        ops.clear();
        ops.into_iter()
    }
}
