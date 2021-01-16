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
        Box::new(|cmd| {
            Box::new(append(state1.clone(), cmd))
        })
    };
    panic!();
}

struct State {
    path: PathBuf,
    fs_thread: Arc<FsThread>
}

fn append<Cmd>(state: Arc<State>, cmd: Cmd) -> impl Future<Output = Result<Address>> + Unpin
where Cmd: Serialize + for <'de> Deserialize<'de> + 'static
{
    std::future::pending()
}

async fn read_at<Cmd>(state: &State, addr: Address) -> Result<(Cmd, Option<Address>)>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    Ok(panic!())
}
