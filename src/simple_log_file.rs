use crate::types::Address;
use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use crate::log_file::LogFile;
use crate::fs_thread::FsThread;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;

fn create<Cmd>(path: PathBuf, fs_thread: Arc<FsThread>) -> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de> + 'static
{
    let state1 = Arc::new(State { path, fs_thread });
    let state2 = state1.clone();

    let append_impl: Box<dyn Fn(Cmd) -> Box<dyn Future<Output = Result<Address>> + Unpin + 'static> + Send + Sync> = {
        Box::new(move |cmd| {
            Box::new(append(state1.clone(), cmd))
        })
    };
    let read_at_impl: Box<dyn Fn(Address) -> Box<dyn Future<Output = Result<(Cmd, Option<Address>)>> + Unpin + 'static> + Send + Sync> = {
        Box::new(move |addr| {
            Box::new(read_at(state2.clone(), addr))
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

fn append<Cmd>(state: Arc<State>, cmd: Cmd) -> impl Future<Output = Result<Address>> + Unpin
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    std::future::pending()
}

fn read_at<Cmd>(state: Arc<State>, addr: Address) -> impl Future<Output = Result<(Cmd, Option<Address>)>> + Unpin
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    std::future::pending()
}
