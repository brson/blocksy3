use crate::types::Address;
use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use crate::log_file::LogFile;
use crate::fs_thread::FsThread;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use futures::future::BoxFuture;
use std::io::{Seek, SeekFrom};

fn create<Cmd>(path: PathBuf, fs_thread: Arc<FsThread>) -> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let path = Arc::new(path);
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
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>
}

async fn append<Cmd>(state: Arc<State>, cmd: Cmd) -> Result<Address>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let path = state.path.clone();
    let future = state.fs_thread.run(move |ctx| -> Result<_> {
        let file = ctx.open_append(&path)?;
        Ok(Address(0))
    });
    Ok(future.await?)
}

async fn read_at<Cmd>(state: Arc<State>, addr: Address) -> Result<(Cmd, Option<Address>)>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let path = state.path.clone();
    let future = state.fs_thread.run(move |ctx| -> Result<_> {
        let mut file = ctx.open_read(&path)?;
        file.seek(SeekFrom::Start(addr.0))?;
        panic!()
    });
    Ok(future.await?)
}
