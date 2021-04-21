#![allow(unused)]

use std::env;
use anyhow::{Result, bail, anyhow};
use std::path::PathBuf;
use std::collections::HashMap;

use blocksy3 as db;
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

    BatchOpen {
        batch: String,
    },
    BatchWrite {
        batch: String,
        tree: String,
        key: String,
        value: String,
    },
    BatchDelete {
        batch: String,
        tree: String,
        key: String,
    },
    BatchDeleteRange {
        batch: String,
        tree: String,
        start: String,
        end: String,
    },
    BatchPushSavePoint {
        batch: String,
    },
    BatchPopSavePoint {
        batch: String,
    },
    BatchRollbackSavePoint {
        batch: String,
    },
    BatchCommit {
        batch: String,
    },
    BatchAbort {
        batch: String,
    },
    BatchClose {
        batch: String,
    },

    ViewOpen {
        view: String,
    },
    ViewRead {
        view: String,
        tree: String,
        key: String,
    },
    ViewIterate {
        view: String,
        tree: String,
    },
    ViewClose {
        view: String,
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
        trees: vec!["t1".to_string(), "t2".to_string()],
    };

    let db = db::Db::open(config).await?;

    let mut batches: HashMap<String, db::WriteBatch> = HashMap::new();
    let mut views: HashMap<String, db::ReadView> = HashMap::new();

    for command in commands {
        println!("executing {:?}", command);
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
            },
            Command::DeleteRange { tree, start, end } => {
                let batch = db.write_batch().await?;
                let tree = batch.tree(&tree);
                tree.delete_range(start.as_bytes(), end.as_bytes()).await?;
                drop(tree);
                batch.commit().await?;
                batch.close().await;
            },

            Command::Read { tree, key } => {
                let view = db.read_view();
                let tree = view.tree(&tree);
                let value = tree.read(key.as_bytes()).await?;
                if let Some(value) = value {
                    let value = String::from_utf8(value).expect("utf8");
                    println!("{}: {}", key, value);
                } else {
                    println!("{}: <none>", key);
                }
            },
            Command::Iterate { tree } => {
                let view = db.read_view();
                let tree = view.tree(&tree);
                let mut cursor = tree.cursor();
                cursor.seek_first();
                while cursor.valid() {
                    let key = String::from_utf8(cursor.key()).expect("utf8");
                    let value = String::from_utf8(cursor.value().await?).expect("utf8");
                    println!("{}: {}", key, value);
                    cursor.next();
                }
            },

            Command::BatchOpen { batch } => {
                let batch_ = db.write_batch().await?;
                batches.insert(batch, batch_);
            },
            Command::BatchWrite { batch, tree, key, value } => {
                let batch = batches.get(&batch).expect("batch");
                let tree = batch.tree(&tree);
                tree.write(key.as_bytes(), value.as_bytes()).await?;
            },
            Command::BatchDelete { batch, tree, key } => {
                let batch = batches.get(&batch).expect("batch");
                let tree = batch.tree(&tree);
                tree.delete(key.as_bytes()).await?;
            },
            Command::BatchDeleteRange { batch, tree, start, end } => {
                let batch = batches.get(&batch).expect("batch");
                let tree = batch.tree(&tree);
                tree.delete_range(start.as_bytes(), end.as_bytes()).await?;
            },
            Command::BatchPushSavePoint { batch } => {
                let batch = batches.get(&batch).expect("batch");
                batch.push_save_point().await?;
            },
            Command::BatchPopSavePoint { batch } => {
                let batch = batches.get(&batch).expect("batch");
                batch.pop_save_point().await?;
            },
            Command::BatchRollbackSavePoint { batch } => {
                let batch = batches.get(&batch).expect("batch");
                batch.rollback_save_point().await?;
            },
            Command::BatchCommit { batch } => {
                let batch = batches.get(&batch).expect("batch");
                batch.commit().await?;
            },
            Command::BatchAbort { batch } => {
                let batch = batches.get(&batch).expect("batch");
                batch.abort().await;
            },
            Command::BatchClose { batch } => {
                let batch = batches.remove(&batch).expect("batch");
                batch.close().await;
            },

            Command::ViewOpen { view } => {
                let view_ = db.read_view();
                views.insert(view, view_);
            },
            Command::ViewRead { view, tree, key } => {
                let view = views.get(&view).expect("view");
                let tree = view.tree(&tree);
                let value = tree.read(key.as_bytes()).await?;
                if let Some(value) = value {
                    let value = String::from_utf8(value).expect("utf8");
                    println!("{}: {}", key, value);
                } else {
                    println!("{}: <none>", key);
                }
            },
            Command::ViewIterate { view, tree } => {
                let view = views.get(&view).expect("view");
                let tree = view.tree(&tree);
                let mut cursor = tree.cursor();
                cursor.seek_first();
                while cursor.valid() {
                    let key = String::from_utf8(cursor.key()).expect("utf8");
                    let value = String::from_utf8(cursor.value().await?).expect("utf8");
                    println!("{}: {}", key, value);
                    cursor.next();
                }
            },
            Command::ViewClose { view } => {
                views.remove(&view);
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
                "/" => {
                    /* command separator */
                    continue;
                },

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

                "batch-open" => {
                    let batch = parse_batch(args)?;
                    Command::BatchOpen { batch }
                },
                "batch-write" => {
                    let batch = parse_batch(args)?;
                    let tree = parse_tree(args)?;
                    let key = parse_key(args)?;
                    let value = parse_value(args)?;
                    Command::BatchWrite { batch, tree, key, value }
                },
                "batch-delete" => {
                    let batch = parse_batch(args)?;
                    let tree = parse_tree(args)?;
                    let key = parse_key(args)?;
                    Command::BatchDelete { batch, tree, key }
                },
                "batch-delete-range" => {
                    let batch = parse_batch(args)?;
                    let tree = parse_tree(args)?;
                    let start = parse_key(args)?;
                    let end = parse_key(args)?;
                    Command::BatchDeleteRange { batch, tree, start, end }
                },
                "batch-push-save-point" => {
                    let batch = parse_batch(args)?;
                    Command::BatchPushSavePoint { batch }
                },
                "batch-pop-save-point" => {
                    let batch = parse_batch(args)?;
                    Command::BatchPopSavePoint { batch }
                },
                "batch-rollback-save-point" => {
                    let batch = parse_batch(args)?;
                    Command::BatchRollbackSavePoint { batch }
                },
                "batch-commit" => {
                    let batch = parse_batch(args)?;
                    Command::BatchCommit { batch }
                },
                "batch-abort" => {
                    let batch = parse_batch(args)?;
                    Command::BatchAbort { batch }
                },
                "batch-close" => {
                    let batch = parse_batch(args)?;
                    Command::BatchClose { batch }
                },

                "view-open" => {
                    let view = parse_view(args)?;
                    Command::ViewOpen { view }
                },
                "view-read" => {
                    let view = parse_view(args)?;
                    let tree = parse_tree(args)?;
                    let key = parse_key(args)?;
                    Command::ViewRead { view, tree, key }
                },
                "view-iterate" => {
                    let view = parse_view(args)?;
                    let tree = parse_tree(args)?;
                    Command::ViewIterate { view, tree }
                },
                "view-close" => {
                    let view = parse_view(args)?;
                    Command::ViewClose { view }
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

    fn parse_batch(iter: &mut impl Iterator<Item = String>) -> Result<String> {
        iter.next().ok_or_else(|| anyhow!("expected batch name"))
    }

    fn parse_view(iter: &mut impl Iterator<Item = String>) -> Result<String> {
        iter.next().ok_or_else(|| anyhow!("expected view name"))
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
