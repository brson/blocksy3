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
    env_logger::init();

    let commands = parse_commands()?;

    let config = db::DbConfig {
        dir: PathBuf::from("testdb"),
        trees: vec!["a".to_string(), "b".to_string(), "c".to_string()],
    };

    let db = db::Db::open(config);
    let db = block_on(db)?;

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
