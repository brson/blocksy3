use anyhow::Result;
use futures::Stream;

use crate::log_file::LogFile;
use crate::types::Address;
use crate::command::Command;

pub struct Log {
    log_file: LogFile<Command>,
}

impl Log {
    pub fn new(log_file: LogFile<Command>) -> Log {
        panic!()
    }

    pub async fn replay(&self) -> impl Stream<Item = Result<Command>> {
        futures::stream::empty()
    }

    pub async fn append(&self, cmd: &Command) -> Result<Address> {
        panic!()
    }

    pub async fn read_at(&self, address: Address) -> Result<Command> {
        panic!()
    }
}
