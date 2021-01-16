use std::sync::Arc;
use serde::{Serialize, Deserialize};
use std::future::Future;
use crate::types::Address;
use anyhow::Result;

pub struct LogFile<Cmd> where Cmd: Serialize + for <'de> Deserialize<'de> {
    pub append: Arc<Box<dyn Fn(Cmd) -> Box<dyn Future<Output = Result<Address>> + Unpin + 'static> + Send + Sync>>,
    pub read_at: Arc<Box<dyn Fn(Address) -> Box<dyn Future<Output = Result<(Cmd, Option<Address>)>>> + Send + Sync>>,
}

impl<Cmd> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    pub async fn append(&self, cmd: Cmd) -> Result<Address> {
        let append_fn = self.append.clone();
        let future = append_fn(cmd);
        let res = future.await;
        res
    }

    pub async fn read_at(&self, addr: Address) -> Result<(Cmd, Option<Address>)> {
        Ok(panic!())
    }
}

