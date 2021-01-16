use serde::{Serialize, Deserialize};
use std::future::Future;
use crate::types::Address;
use anyhow::Result;

pub struct LogFile<Cmd> where Cmd: Serialize + for <'de> Deserialize<'de> {
    append: Box<dyn Fn(&Cmd) -> Box<dyn Future<Output = Result<Address>>> + Send + Sync>,
    read_at: Box<dyn Fn(Address) -> Box<dyn Future<Output = Result<(Cmd, Option<Address>)>>> + Send + Sync>,
}

