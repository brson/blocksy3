use std::sync::Arc;
use serde::{Serialize, Deserialize};
use std::future::Future;
use crate::types::Address;
use anyhow::Result;

pub struct LogFile<Cmd> where Cmd: Serialize + for <'de> Deserialize<'de> {
    pub append: Arc<Box<dyn Fn(Cmd) -> Box<dyn Future<Output = Result<Address>> + Unpin + 'static> + Send + Sync>>,
    pub read_at: Arc<Box<dyn Fn(Address) -> Box<dyn Future<Output = Result<(Cmd, Option<Address>)>> + Unpin + 'static> + Send + Sync>>,
}

impl<Cmd> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    pub async fn append(&self, cmd: Cmd) -> Result<Address> {
        (self.append)(cmd).await
    }

    pub async fn read_at(&self, addr: Address) -> Result<(Cmd, Option<Address>)> {
        (self.read_at)(addr).await
    }
}

