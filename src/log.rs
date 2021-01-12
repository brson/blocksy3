use serde::{Serialize, Deserialize};
use anyhow::Result;
use std::future::Future;
use futures::Stream;

use crate::command::Command;

#[derive(Eq, PartialEq)]
#[derive(Copy, Clone)]
pub struct Address(usize);

pub struct LogFile<Cmd> where Cmd: Serialize + for <'de> Deserialize<'de> {
    append: Box<dyn Fn(&Cmd) -> Box<dyn Future<Output = Result<Address>>> + Send + Sync>,
    read_at: Box<dyn Fn(Address) -> Box<dyn Future<Output = Result<(Cmd, Option<Address>)>>> + Send + Sync>,
}

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
