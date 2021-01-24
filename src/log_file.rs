use std::sync::Arc;
use serde::{Serialize, Deserialize};
use std::future::Future;
use crate::types::Address;
use anyhow::Result;
use futures::future::BoxFuture;

pub struct LogFile<Cmd> where Cmd: Serialize + for <'de> Deserialize<'de> {
    pub append: Box<dyn Fn(Cmd) -> BoxFuture<'static, Result<Address>> + Send + Sync>,
    pub read_at: Box<dyn Fn(Address) -> BoxFuture<'static, Result<(Cmd, Option<Address>)>> + Send + Sync>,
    pub sync: Box<dyn Fn() -> BoxFuture<'static, Result<()>> + Send + Sync>
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

    pub async fn sync(&self) -> Result<()> {
        (self.sync)().await
    }
}

