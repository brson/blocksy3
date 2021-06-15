#![allow(unused)]

use std::env;
use anyhow::{Result, bail, anyhow};
use std::path::PathBuf;
use std::collections::HashMap;

use blocksy3 as db;
use futures::executor::block_on;

use db::cmdscript::Command;

fn main() -> Result<()> {
    Ok(block_on(run())?)
}

async fn run() -> Result<()> {
    env_logger::init();

    let args = env::args();
    let (path, commands) = db::cmdscript::parse_commands(args)?;

    db::cmdscript::exec(path, commands).await?;

    Ok(())
}
