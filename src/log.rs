use anyhow::Result;
use futures::{stream, Stream, StreamExt};
use std::sync::Arc;

use crate::log_file::LogFile;
use crate::types::Address;
use crate::command::Command;

pub struct Log {
    log_file: Arc<LogFile<Command>>,
}

impl Log {
    pub fn new(log_file: LogFile<Command>) -> Log {
        Log {
            log_file: Arc::new(log_file),
        }
    }

    pub async fn replay(&self) -> impl Stream<Item = Result<(Command, Address)>> {
        let addr = Address(0);
        let state = Some((self.log_file.clone(), addr));
        stream::unfold(state, |state| async {
            match state {
                Some((log_file, addr)) => {
                    let cmd = (log_file.read_at)(addr).await;
                    match cmd {
                        Err(e) => {
                            Some((Err(e), None))
                        },
                        Ok((cmd, Some(next_addr))) => {
                            let next_state = Some((log_file, next_addr));
                            Some((Ok((cmd, addr)), next_state))
                        },
                        Ok((cmd, None)) => {
                            Some((Ok((cmd, addr)), None))
                        }
                    }
                },
                None => {
                    None
                }
            }
        })
    }

    pub async fn append(&self, cmd: &Command) -> Result<Address> {
        Ok((self.log_file.append)(cmd).await?)
    }

    pub async fn read_at(&self, address: Address) -> Result<Command> {
        Ok((self.log_file.read_at)(address).await
           .map(|(cmd, _)| cmd)?)
    }
}
