use serde::{Serialize, Deserialize};
use std::sync::Arc;
use crate::types::{Key, Value, Batch, BatchCommit};

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
#[derive(Debug)]
#[serde(tag = "type")]
pub enum Command {
    Open {
        batch: Batch,
    },
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
    AbortCommit {
        batch: Batch,
        batch_commit: BatchCommit,
    },
    Close {
        batch: Batch,
    },
}

impl Command {
    pub fn batch(&self) -> Batch {
        use Command::*;
        match self {
            Open { batch }
            | Write { batch, .. }
            | Delete { batch, .. }
            | DeleteRange { batch, .. }
            | PushSavePoint { batch, .. }
            | PopSavePoint { batch, .. }
            | RollbackSavePoint { batch, .. }
            | ReadyCommit { batch, .. }
            | AbortCommit { batch, .. }
            | Close { batch } => {
                *batch
            }
        }
    }
}
