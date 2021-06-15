use crate as db;

use anyhow::{Result, anyhow, bail};
use std::path::PathBuf;
use std::collections::HashMap;

#[derive(Debug)]
pub enum Command {
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
    ReadAssert {
        tree: String,
        key: String,
        expected_value: Option<String>,
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

pub async fn exec(path: Option<PathBuf>, commands: Vec<Command>) -> Result<()> {
    let config = db::DbConfig {
        dir: path,
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
            Command::ReadAssert { tree, key, expected_value } => {
                let view = db.read_view();
                let tree = view.tree(&tree);
                let value = tree.read(key.as_bytes()).await?;
                if let Some(value) = value {
                    let value = String::from_utf8(value).expect("utf8");
                    println!("{}: {}", key, value);
                    assert_eq!(expected_value, Some(value));
                } else {
                    println!("{}: <none>", key);
                    assert_eq!(expected_value, None);
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

pub fn parse_commands(iter: impl Iterator<Item = String>) -> Result<(Option<PathBuf>, Vec<Command>)> {
    let mut commands = vec![];

    let mut iter = iter
        .filter(|s| !s.is_empty());
    let iter = &mut iter;

    let path = iter.next().ok_or_else(|| anyhow!("expected path or mem"))?;
    let path = if path == "path" {
        let path = iter.next().ok_or_else(|| anyhow!("expected path or mem"))?;
        Some(PathBuf::from(path))
    } else if path == "mem" {
        None
    } else {
        bail!("expected path or mem");
    };

    loop {
        if let Some(next_command) = iter.next() {
            let cmd = match next_command.as_ref() {
                "/" => {
                    /* command separator */
                    continue;
                },

                "write" => {
                    let tree = parse_tree(iter)?;
                    let key = parse_key(iter)?;
                    let value = parse_value(iter)?;
                    Command::Write { tree, key, value }
                },
                "delete" => {
                    let tree = parse_tree(iter)?;
                    let key = parse_key(iter)?;
                    Command::Delete { tree, key }
                },
                "delete-range" => {
                    let tree = parse_tree(iter)?;
                    let start = parse_key(iter)?;
                    let end = parse_key(iter)?;
                    Command::DeleteRange { tree, start, end }
                },

                "read" => {
                    let tree = parse_tree(iter)?;
                    let key = parse_key(iter)?;
                    Command::Read { tree, key }
                },
                "read-assert" => {
                    let tree = parse_tree(iter)?;
                    let key = parse_key(iter)?;
                    let expected_value = parse_expected_value(iter)?;
                    Command::ReadAssert { tree, key, expected_value }
                },
                "iterate" => {
                    let tree = parse_tree(iter)?;
                    Command::Iterate { tree }
                },

                "batch-open" => {
                    let batch = parse_batch(iter)?;
                    Command::BatchOpen { batch }
                },
                "batch-write" => {
                    let batch = parse_batch(iter)?;
                    let tree = parse_tree(iter)?;
                    let key = parse_key(iter)?;
                    let value = parse_value(iter)?;
                    Command::BatchWrite { batch, tree, key, value }
                },
                "batch-delete" => {
                    let batch = parse_batch(iter)?;
                    let tree = parse_tree(iter)?;
                    let key = parse_key(iter)?;
                    Command::BatchDelete { batch, tree, key }
                },
                "batch-delete-range" => {
                    let batch = parse_batch(iter)?;
                    let tree = parse_tree(iter)?;
                    let start = parse_key(iter)?;
                    let end = parse_key(iter)?;
                    Command::BatchDeleteRange { batch, tree, start, end }
                },
                "batch-push-save-point" => {
                    let batch = parse_batch(iter)?;
                    Command::BatchPushSavePoint { batch }
                },
                "batch-pop-save-point" => {
                    let batch = parse_batch(iter)?;
                    Command::BatchPopSavePoint { batch }
                },
                "batch-rollback-save-point" => {
                    let batch = parse_batch(iter)?;
                    Command::BatchRollbackSavePoint { batch }
                },
                "batch-commit" => {
                    let batch = parse_batch(iter)?;
                    Command::BatchCommit { batch }
                },
                "batch-abort" => {
                    let batch = parse_batch(iter)?;
                    Command::BatchAbort { batch }
                },
                "batch-close" => {
                    let batch = parse_batch(iter)?;
                    Command::BatchClose { batch }
                },

                "view-open" => {
                    let view = parse_view(iter)?;
                    Command::ViewOpen { view }
                },
                "view-read" => {
                    let view = parse_view(iter)?;
                    let tree = parse_tree(iter)?;
                    let key = parse_key(iter)?;
                    Command::ViewRead { view, tree, key }
                },
                "view-iterate" => {
                    let view = parse_view(iter)?;
                    let tree = parse_tree(iter)?;
                    Command::ViewIterate { view, tree }
                },
                "view-close" => {
                    let view = parse_view(iter)?;
                    Command::ViewClose { view }
                },

                _ => {
                    bail!("unknown command '{}'", next_command);
                }
            };
            commands.push(cmd);
        } else {
            return Ok((path, commands));
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

    fn parse_expected_value(iter: &mut impl Iterator<Item = String>) -> Result<Option<String>> {
        iter.next().ok_or_else(|| anyhow!("expected key value"))
            .map(|s| {
                if s == "<none>" {
                    None
                } else {
                    Some(s)
                }
            })
    }
}
