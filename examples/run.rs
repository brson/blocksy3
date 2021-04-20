#![allow(unused)]

use std::env;
use anyhow::{Result, bail, anyhow};
use std::path::PathBuf;

use blocksy3::facade as db;
use futures::executor::block_on;

#[derive(Debug)]
enum Command {
    Write {
        tree: String,
        key: String,
        value: String,
    },
    Delete {
        tree: String,
        key: String,
    },
    DeleteRange {
        tree: String,
        start: String,
        end: String,
    },
    Read {
        tree: String,
        key: String,
    },
    Iterate {
        tree: String,
    },
}

fn main() -> Result<()> {
    Ok(block_on(run())?)
}

async fn run() -> Result<()> {
    env_logger::init();

    let commands = parse_commands()?;

    let config = db::DbConfig {
        dir: PathBuf::from("testdb"),
        trees: vec!["a".to_string(), "b".to_string()],
    };

    let db = db::Db::open(config).await?;

    for command in commands {
        match command {
            Command::Write { tree, key, value } => {
                let batch = db.write_batch().await?;
                let tree = batch.tree(&tree);
                tree.write(key.as_bytes(), value.as_bytes()).await?;
                drop(tree);
                batch.commit().await?;
                batch.close().await;
            },
            Command::Delete { tree, key } => {
                let batch = db.write_batch().await?;
                let tree = batch.tree(&tree);
                tree.delete(key.as_bytes()).await?;
                drop(tree);
                batch.commit().await?;
                batch.close().await;
            }
            Command::Iterate { tree } => {
                let view = db.read_view();
                let tree = view.tree(&tree);
                let mut cursor = tree.cursor();
                cursor.seek_first();
                while cursor.is_valid() {
                    let key = String::from_utf8(cursor.key()).expect("utf8");
                    let value = String::from_utf8(cursor.value().await?).expect("utf8");
                    println!("{}: {}", key, value);
                    cursor.next();
                }
            },
            _ => panic!(),
        }
    }

    Ok(())
}

fn parse_commands() -> Result<Vec<Command>> {
    let args = &mut env::args();
    let mut commands = vec![];

    args.next();

    loop {
        if let Some(next_command) = args.next() {
            let cmd = match next_command.as_ref() {
                "write" => {
                    let tree = parse_tree(args)?;
                    let key = parse_key(args)?;
                    let value = parse_value(args)?;
                    Command::Write { tree, key, value }
                },
                "delete" => {
                    let tree = parse_tree(args)?;
                    let key = parse_key(args)?;
                    Command::Delete { tree, key }
                },
                "delete-range" => {
                    let tree = parse_tree(args)?;
                    let start = parse_key(args)?;
                    let end = parse_key(args)?;
                    Command::DeleteRange { tree, start, end }
                },
                "read" => {
                    let tree = parse_tree(args)?;
                    let key = parse_key(args)?;
                    Command::Read { tree, key }
                },
                "iterate" => {
                    let tree = parse_tree(args)?;
                    Command::Iterate { tree }
                },
                _ => {
                    bail!("unknown command '{}'", next_command);
                }
            };
            commands.push(cmd);
        } else {
            return Ok(commands);
        }
    }

    fn parse_tree(iter: &mut impl Iterator<Item = String>) -> Result<String> {
        iter.next().ok_or_else(|| anyhow!("expected tree name"))
    }

    fn parse_key(iter: &mut impl Iterator<Item = String>) -> Result<String> {
        iter.next().ok_or_else(|| anyhow!("expected key name"))
    }

    fn parse_value(iter: &mut impl Iterator<Item = String>) -> Result<String> {
        iter.next().ok_or_else(|| anyhow!("expected key value"))
    }
}
