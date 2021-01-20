use serde::{Serialize, Deserialize};
use anyhow::Result;
use futures::{stream, Stream, StreamExt};
use std::sync::Arc;

use crate::log_file::LogFile;
use crate::types::Address;

pub struct Log<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    log_file: Arc<LogFile<Cmd>>,
}

impl<Cmd> Log<Cmd>
where Cmd: Serialize + for <'de> Deserialize<'de>
{
    pub fn new(log_file: LogFile<Cmd>) -> Log<Cmd> {
        Log {
            log_file: Arc::new(log_file),
        }
    }

    pub async fn replay(&self) -> impl Stream<Item = Result<(Cmd, Address)>> {
        let addr = Address(0);
        let state = Some((self.log_file.clone(), addr));
        stream::unfold(state, |state| async {
            match state {
                Some((log_file, addr)) => {
                    let cmd = log_file.read_at(addr).await;
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

    pub async fn append(&self, cmd: Cmd) -> Result<Address> {
        Ok(self.log_file.append(cmd).await?)
    }

    pub async fn read_at(&self, address: Address) -> Result<Cmd> {
        Ok(self.log_file.read_at(address).await
           .map(|(cmd, _)| cmd)?)
    }
}
