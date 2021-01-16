use serde::{Serialize, Deserialize};
use std::future::Future;
use crate::types::Address;
use anyhow::Result;

pub struct LogFile<Cmd> where Cmd: Serialize + for <'de> Deserialize<'de> {
    pub append: Box<dyn Fn(Cmd) -> Box<dyn Future<Output = Result<Address>>> + Send + Sync>,
    pub read_at: Box<dyn Fn(Address) -> Box<dyn Future<Output = Result<(Cmd, Option<Address>)>>> + Send + Sync>,
}

impl<Cmd> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    pub async fn append(&self, cmd: Cmd) -> Result<Address> {
        Ok(panic!())
    }

    pub async fn read_at(&self, addr: Address) -> Result<(Cmd, Option<Address>)> {
        Ok(panic!())
    }
}

