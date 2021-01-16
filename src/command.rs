use serde::{Serialize, Deserialize};
use std::sync::Arc;
use crate::types::{Key, Value, Batch, BatchCommit};

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
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

