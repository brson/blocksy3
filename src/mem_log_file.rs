use crate::types::Address;
use anyhow::Result;
use std::future::Future;
use std::sync::{Arc, RwLock};
use crate::log_file::LogFile;
use crate::fs_thread::FsThread;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use futures::future::BoxFuture;
use std::io::{Seek, SeekFrom, BufReader};
use crate::frame;

pub fn create<Cmd>() -> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let buffers = RwLock::new(vec![]);
    let state1 = Arc::new(State { buffers });
    let state2 = state1.clone();
    let state3 = state1.clone();
    let state4 = state1.clone();

    let is_empty_impl: Box<dyn Fn() -> BoxFuture<'static, Result<bool>> + Send + Sync> = {
        Box::new(move || {
            Box::pin(is_empty(state1.clone()))
        })
    };

    let append_impl: Box<dyn Fn(Cmd) -> BoxFuture<'static, Result<Address>> + Send + Sync> = {
        Box::new(move |cmd| {
            Box::pin(append(state2.clone(), cmd))
        })
    };
    let read_at_impl: Box<dyn Fn(Address) -> BoxFuture<'static, Result<(Cmd, Option<Address>)>> + Send + Sync> = {
        Box::new(move |addr| {
            Box::pin(read_at(state3.clone(), addr))
        })
    };
    let sync_impl: Box<dyn Fn() -> BoxFuture<'static, Result<()>> + Send + Sync> = {
        Box::new(move || {
            Box::pin(sync(state4.clone()))
        })
    };

    LogFile {
        is_empty: is_empty_impl,
        append: append_impl,
        read_at: read_at_impl,
        sync: sync_impl,
    }
}

struct State {
    buffers: RwLock<Vec<Buffer>>,
}

type Buffer = Vec<u8>;

async fn is_empty(state: Arc<State>) -> Result<bool> {
    panic!()
}

async fn append<Cmd>(state: Arc<State>, cmd: Cmd) -> Result<Address>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    panic!()
}

async fn read_at<Cmd>(state: Arc<State>, addr: Address) -> Result<(Cmd, Option<Address>)>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    panic!()
}

async fn sync(state: Arc<State>) -> Result<()> {
    Ok(( /* nop */ ))
}
