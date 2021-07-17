#![allow(unused)]

// The public API of this crate is reexported here
pub use doc::*;

/// The documented public API of the crate.
mod doc;

/// The same public API, but with no docs, for easy skimming.
mod pretty;

/// The same public API, implemented on top of [`basic_db`].
mod imp;

/// A multi-tree database with atomic commits.
mod basic_db;

/// A single key/value tree, composed of a log and index.
mod tree;
/// A sequential log of tree commands.
mod log;
/// An in-memory index of the log.
mod index;
/// Adds committed batches from the log to the index.
mod batch_player;

/// Commands in a tree's log.
mod command;
/// Basic key, value, batch, commit definitions.
mod types;

/// A logging interface.
mod log_file;
/// An in-memory log.
mod mem_log_file;
/// A simple on-disk, human-readable, log.
mod simple_log_file;

/// The master commit log.
mod commit_log;
/// A section of a log file.
mod frame;
/// Off-thread async file I/O.
mod fs_thread;
/// Loads a set of trees from logs and commit log.
mod loader;

/// A tree that compacts other trees.
mod compacting_tree;

/// A testing script language
pub mod cmdscript;

/// Public access to building blocks
pub mod raw {
    pub mod fs_thread {
        pub use crate::fs_thread::*;
    }
    pub mod log {
        pub use crate::log::*;
    }
    pub mod simple_log_file {
        pub use crate::simple_log_file::*;
    }
    pub mod tree {
        pub use crate::tree::*;
    }
    pub mod types {
        pub use crate::types::*;
    }
}
