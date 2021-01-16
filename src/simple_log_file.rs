use crate::types::Address;
use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use crate::log_file::LogFile;
use crate::fs_thread::FsThread;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use futures::future::BoxFuture;

fn create<Cmd>(path: PathBuf, fs_thread: Arc<FsThread>) -> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let state1 = Arc::new(State { path, fs_thread });
    let state2 = state1.clone();

    let append_impl: Box<dyn Fn(Cmd) -> BoxFuture<'static, Result<Address>> + Send + Sync> = {
        Box::new(move |cmd| {
            Box::pin(append(state1.clone(), cmd))
        })
    };
    let read_at_impl: Box<dyn Fn(Address) -> BoxFuture<'static, Result<(Cmd, Option<Address>)>> + Send + Sync> = {
        Box::new(move |addr| {
            Box::pin(read_at(state2.clone(), addr))
        })
    };

    LogFile {
        append: append_impl,
        read_at: read_at_impl,
    }
}

struct State {
    path: PathBuf,
    fs_thread: Arc<FsThread>
}

async fn append<Cmd>(state: Arc<State>, cmd: Cmd) -> Result<Address>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    Ok(panic!())
}

async fn read_at<Cmd>(state: Arc<State>, addr: Address) -> Result<(Cmd, Option<Address>)>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    Ok(panic!())
}
