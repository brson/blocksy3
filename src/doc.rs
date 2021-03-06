use crate::pretty as imp;

pub use anyhow::{self, Result};

/// Configuration for a database.
pub type DbConfig = imp::DbConfig;

/// A key-value data store with
/// multiple trees,
/// batch commits,
/// and consistent snapshots.
#[derive(Clone, Debug)]
pub struct Db(imp::Db);

/// An atomically-committed series of write commands.
pub struct WriteBatch(imp::WriteBatch);

/// A write handle to a single tree in a `WriteBatch`.
pub struct WriteTree<'batch>(imp::WriteTree<'batch>);

/// A consistent view of the database.
#[derive(Clone, Debug)]
pub struct ReadView(imp::ReadView);

/// A read handle to a single tree in a `ReadView`.
pub struct ReadTree<'view>(imp::ReadTree<'view>);

/// A cursor over the keys and values of a `ReadTree`.
pub struct Cursor(imp::Cursor);

impl Db {
    /// Open a new or existing database.
    pub async fn open(config: DbConfig) -> Result<Db> { imp::Db::open(config).await.map(Db) }

    /// Create a write batch ([`WriteBatch`]).
    pub async fn write_batch(&self) -> Result<WriteBatch> { Ok(WriteBatch(self.0.write_batch().await?)) }

    /// Create a read view ([`ReadView`]).
    pub fn read_view(&self) -> ReadView { ReadView(self.0.read_view()) }

    /// Sync file system to disk.
    pub async fn sync(&self) -> Result<()> { self.0.sync().await }
}

impl WriteBatch {
    /// Get a write handle to a single tree ([`WriteTree`]).
    pub fn tree<'batch>(&'batch self, tree: &str) -> WriteTree<'batch> { WriteTree(self.0.tree(tree)) }

    pub async fn push_save_point(&self) -> Result<()> { self.0.push_save_point().await }
    pub async fn pop_save_point(&self) -> Result<()> { self.0.pop_save_point().await }
    pub async fn rollback_save_point(&self) -> Result<()> { self.0.rollback_save_point().await }
    pub async fn commit(&self) -> Result<()> { self.0.commit().await }
    pub async fn abort(&self) { self.0.abort().await }
    pub async fn close(self) { self.0.close().await }
}

impl ReadView {
    /// Get a read handle to a single tree ([`ReadTree`]).
    pub fn tree<'view>(&'view self, tree: &str) -> ReadTree<'view> { ReadTree(self.0.tree(tree)) }
}

impl<'batch> WriteTree<'batch> {
    pub async fn write(&self, key: &[u8], value: &[u8]) -> Result<()> { self.0.write(key, value).await }
    pub async fn delete(&self, key: &[u8]) -> Result<()> { self.0.delete(key).await }
    pub async fn delete_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> { self.0.delete_range(start_key, end_key).await }
}

impl<'view> ReadTree<'view> {
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { self.0.read(key).await }
    pub fn cursor(&self) -> Cursor { Cursor(self.0.cursor()) }
}

impl Cursor {
    pub fn valid(&self) -> bool { self.0.valid() }
    pub fn key(&self) -> Vec<u8> { self.0.key() }
    pub async fn value(&mut self) -> Result<Vec<u8>> { self.0.value().await }
    pub fn next(&mut self) { self.0.next() }
    pub fn prev(&mut self) { self.0.prev() }
    pub fn seek_first(&mut self) { self.0.seek_first() }
    pub fn seek_last(&mut self) { self.0.seek_last() }
    pub fn seek_key(&mut self, key: &[u8]) { self.0.seek_key(key) }
    pub fn seek_key_rev(&mut self, key: &[u8]) { self.0.seek_key_rev(key) }
}
