use crate::types::Address;
use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use crate::log_file::LogFile;
use crate::fs_thread::FsThread;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use futures::future::BoxFuture;
use std::io::{Seek, SeekFrom, BufReader};
use crate::frame;

pub fn create<Cmd>(path: PathBuf, fs_thread: Arc<FsThread>) -> LogFile<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let path = Arc::new(path);
    let state1 = Arc::new(State { path, fs_thread });
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
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>
}

async fn is_empty(state: Arc<State>) -> Result<bool> {
    let path = state.path.clone();
    let future = state.fs_thread.run(move |ctx| -> Result<_> {
        let mut file = ctx.open_read(&path)?;
        let pos = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;
        if pos == 0 {
            Ok(true)
        } else {
            Ok(false)
        }
    });
    Ok(future.await?)
}

async fn append<Cmd>(state: Arc<State>, cmd: Cmd) -> Result<Address>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let path = state.path.clone();
    let future = state.fs_thread.run(move |ctx| -> Result<_> {
        let mut file = ctx.open_append(&path)?;
        frame::write(file, &cmd)?;
        let pos = file.seek(SeekFrom::Current(0))?;
        let addr = Address(pos);
        Ok(addr)
    });
    Ok(future.await?)
}

async fn read_at<Cmd>(state: Arc<State>, addr: Address) -> Result<(Cmd, Option<Address>)>
where Cmd: Serialize + for <'de> Deserialize<'de> + Send + 'static
{
    let path = state.path.clone();
    let future = state.fs_thread.run(move |ctx| -> Result<_> {
        let mut file = ctx.open_read(&path)?;
        let mut file = BufReader::new(file);
        file.seek(SeekFrom::Start(addr.0))?;
        let cmd = frame::read(&mut file)?;
        let pos = file.seek(SeekFrom::Current(0))?;
        let eof = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(pos))?;
        let next_addr = if pos != eof {
            Some(Address(pos))
        } else {
            None
        };
        Ok((cmd, next_addr))
    });
    Ok(future.await?)
}

async fn sync(state: Arc<State>) -> Result<()> {
    let path = state.path.clone();
    let future = state.fs_thread.run(move |ctx| -> Result<_> {
        let file = ctx.open_append(&path)?;
        file.sync_all()?;
        Ok(())
    });
    Ok(future.await?)
}
