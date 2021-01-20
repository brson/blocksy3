use anyhow::Result;
use std::collections::BTreeMap;
use crate::commit_log::{CommitLog, CommitCommand};
use crate::tree::Tree;
use futures::stream::StreamExt;

pub async fn load(commit_log: &CommitLog, trees: &BTreeMap<String, Tree>) -> Result<DbInitState> {
    let mut commit_replay_stream = commit_log.replay();

    let mut tree_players: BTreeMap<_, _> = trees.iter().map(|(tree_name, tree)| {
        (tree_name, tree.init_replayer())
    }).collect();

    while let Some(next_commit) = commit_replay_stream.next().await {
        let next_commit = next_commit?;

        for (_, player) in tree_players.iter_mut() {
            player.replay_commit(next_commit.batch,
                                 next_commit.batch_commit,
                                 next_commit.commit).await?;
        }
    }

    for (_, player) in tree_players.into_iter() {
        player.init_success();
    }

    panic!()
}

pub struct DbInitState {
    pub next_batch: u64,
    pub next_batch_commit: u64,
    pub next_commit: u64,
    pub view_commit_limit: u64,
}
