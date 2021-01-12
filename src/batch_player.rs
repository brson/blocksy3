use crate::command::{Command, Batch, BatchCommit};

pub struct BatchPlayer {
}

impl BatchPlayer {
    fn record(&self, cmd: &Command) {
        panic!()
    }

    fn replay(&self, batch: Batch, batch_commit: BatchCommit) -> ! {
        panic!()
    }
}
