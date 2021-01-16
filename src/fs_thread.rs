use std::collections::btree_map::Entry;
use log::error;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use anyhow::Result;
use std::thread::{self, JoinHandle};
use async_channel::{self, Sender, Receiver, TrySendError};
use futures::executor::{LocalPool, block_on};
use std::sync::mpsc;

#[derive(Debug)]
pub struct FsThread {
    handle: JoinHandle<()>,
    tx: Sender<Message>,
}

pub struct FsThreadContext {
    append_handles: BTreeMap<PathBuf, File>,
    read_handles: BTreeMap<PathBuf, File>,
}

enum Message {
    Run(Box<dyn FnOnce(&mut FsThreadContext) + Send>),
    Shutdown(mpsc::Sender<()>),
}

impl Drop for FsThread {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl FsThread {
    pub fn start() -> Result<FsThread> {
        let (tx, rx) = async_channel::unbounded();
        let handle = thread::spawn(move || {
            let mut context = FsThreadContext::new();
            loop {
                let msg = block_on(rx.recv()).expect("recv");
                match msg {
                    Message::Run(f) => {
                        f(&mut context);
                    },
                    Message::Shutdown(rsp_tx) => {
                        context.shutdown();
                        rsp_tx.send(()).expect("send");
                        break;
                    }
                }
            }
        });

        Ok(FsThread {
            handle, tx
        })
    }

    pub fn run<F, R>(&self, f: F) -> impl Future<Output = R>
    where F: FnOnce(&mut FsThreadContext) -> R + Send + 'static,
          R: Send + 'static,
    {
        let (rsp_tx, rsp_rx) = async_channel::bounded(1);

        let simple_f = move |ctx: &mut FsThreadContext| {
            let r = f(ctx);
            let _r = rsp_tx.try_send(r);
        };

        self.tx.try_send(Message::Run(Box::new(simple_f))).expect("send");

        async {
            let rsp_rx = rsp_rx;
            rsp_rx.recv().await.expect("recv")
        }
    }
}

impl FsThread {
    fn shutdown(&mut self) {
        let (mut rsp_tx, rsp_rx) = mpsc::channel();
        loop {
            let r = self.tx.try_send(Message::Shutdown(rsp_tx));
            if let Err(e) = r {
                match e {
                    TrySendError::Full(Message::Shutdown(tx)) => {
                        rsp_tx = tx;
                    },
                    TrySendError::Full(_) => {
                        unreachable!();
                    }
                    TrySendError::Closed(_) => {
                        panic!("shutdown channel closed");
                        break;
                    }
                }
            } else {
                break;
            }
            thread::yield_now();
        }
        rsp_rx.recv().expect("recv");
    }
}

impl FsThreadContext {
    pub fn open_append(&mut self, path: &Path) -> Result<&mut File> {
        let mut entry = self.append_handles.entry(path.to_owned());
        match entry {
            Entry::Vacant(mut entry) => {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                Ok(entry.insert(file))
            }
            Entry::Occupied(entry) => {
                Ok(entry.into_mut())
            }
        }
    }

    pub fn open_read(&mut self, path: &Path) -> Result<&mut File> {
        let mut entry = self.read_handles.entry(path.to_owned());
        match entry {
            Entry::Vacant(mut entry) => {
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(path)?;
                Ok(entry.insert(file))
            }
            Entry::Occupied(entry) => {
                Ok(entry.into_mut())
            }
        }
    }

    pub fn close(&mut self, path: &Path) {
        sync_close(path, self.append_handles.remove(path).as_mut());
        sync_close(path, self.read_handles.remove(path).as_mut());
    }
}

impl FsThreadContext {
    fn new() -> FsThreadContext {
        FsThreadContext {
            append_handles: BTreeMap::new(),
            read_handles: BTreeMap::new(),
        }
    }

    fn shutdown(&mut self) {
        let files = self.append_handles.iter_mut()
            .chain(self.read_handles.iter_mut());
        for (path, file) in files {
            sync_close(path, Some(file));
        }
    }
}

fn sync_close(path: &Path, file: Option<&mut File>) {
    if let Some(file) = file {
        if let Err(e) = file.sync_all() {
            error!("error closing file {:?}: {}",
                   path.display(), e);
        }
    }
}
