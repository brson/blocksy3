use anyhow::{Result, bail};
use std::collections::BTreeMap;
use crate::commit_log::{CommitLog, CommitCommand};
use crate::tree::Tree;
use futures::stream::StreamExt;
use crate::types::{Batch, BatchCommit, Commit};

pub async fn load(commit_log: &CommitLog, trees: &BTreeMap<String, Tree>) -> Result<DbInitState> {
    let mut commit_replay_stream = commit_log.replay();

    let mut tree_players: BTreeMap<_, _> = trees.iter().map(|(tree_name, tree)| {
        (tree_name, tree.init_replayer())
    }).collect();

    let mut max_commit = None;

    while let Some(next_commit) = commit_replay_stream.next().await {
        let next_commit = next_commit?;

        // FIXME: If a tree doesn't participate in a batch,
        // then this will not work as expected and eat
        // a bunch of memory.
        // Fix for this is to do ready-commit under its
        // own lock so that it is serialized.
        panic!("fixme");
        for (_, player) in tree_players.iter_mut() {
            player.replay_commit(next_commit.batch,
                                 next_commit.batch_commit,
                                 next_commit.commit).await?;
        }

        if let Some(max_commit) = max_commit {
            if !(max_commit < next_commit.commit) {
                bail!("non-monotonic commit number");
            }
        }

        max_commit = Some(next_commit.commit);
    }

    let mut max_batch = None;
    let mut max_batch_commit = None;

    for (_, player) in tree_players.iter_mut() {
        let (tree_max_batch, tree_max_batch_commit)
            = player.replay_rest().await?;

        match (max_batch, tree_max_batch) {
            (None, tree_max_batch) => {
                max_batch = tree_max_batch;
            },
            (Some(curr_max_batch), Some(tree_max_batch)) => {
                max_batch = Some(Batch(curr_max_batch.0.max(tree_max_batch.0)));
            },
            (Some(_), None) => { }
        }

        match (max_batch_commit, tree_max_batch_commit) {
            (None, tree_max_batch_commit) => {
                max_batch_commit = tree_max_batch_commit;
            },
            (Some(curr_max_batch_commit), Some(tree_max_batch_commit)) => {
                max_batch_commit = Some(BatchCommit(curr_max_batch_commit.0.max(tree_max_batch_commit.0)));
            },
            (Some(_), None) => { }
        }
    }

    for (_, player) in tree_players.into_iter() {
        player.init_success();
    }

    let next_batch = Batch(max_batch.map(|b| b.0.checked_add(1).expect("overflow")).unwrap_or(0));
    let next_batch_commit = BatchCommit(max_batch_commit.map(|b| b.0.checked_add(1).expect("overflow")).unwrap_or(0));
    let next_commit = Commit(max_commit.map(|b| b.0.checked_add(1).expect("overflow")).unwrap_or(0));

    Ok(DbInitState {
        next_batch,
        next_batch_commit,
        next_commit,
    })
}

pub struct DbInitState {
    pub next_batch: Batch,
    pub next_batch_commit: BatchCommit,
    pub next_commit: Commit,
}
