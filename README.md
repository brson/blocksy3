# A simple key/value store

This is an in-process key/value store,
ala [RocksDB].

It is intended to be as simple as possible
while implementing the features required
by a [TiKV] storage engine.
In particular,
it is meant to validate that TiKV's
test suite can be implemented on top of engines other than RocksDB,
while also informing the implementation of its storage engine test suite.

It is not fast,
it is not memory-efficient,
and it is not for production use.

With its simplicity though,
it may eventually serve as a useful learning resource
for demonstrating concurrent and parallel Rust programming.

[RocksDB]: https://github.com/facebook/rocksdb
[TiKV]: https://github.com/tikv/tikv


## Features

- Tiny and readable
- Multiple key/value collections ("column families")
- Point reads and deletes
- Range deletes
- Consistent read views and cursors ("snapshots")
- Atomically-committed write batches
  - With save points, rollbacks, and multiple commits
- On-disk on in-memory storage

## WIP

- Compaction
