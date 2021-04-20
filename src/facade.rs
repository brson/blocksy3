pub use anyhow::Result;

use crate::imp;

pub type DbConfig = imp::DbConfig;

#[derive(Clone)]
pub struct Db(imp::Db);

pub struct WriteBatch(imp::WriteBatch);
pub struct ReadView(imp::ReadView);
pub struct WriteTree<'batch>(imp::WriteTree<'batch>);
pub struct ReadTree<'view>(imp::ReadTree<'view>);

pub struct Cursor(imp::Cursor);

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> { imp::Db::open(config).await.map(Db) }
    pub async fn write_batch(&self) -> Result<WriteBatch> { Ok(WriteBatch(self.0.write_batch().await?)) }
    pub fn read_view(&self) -> ReadView { ReadView(self.0.read_view()) }
    pub async fn sync(&self) -> Result<()> { self.0.sync().await }
}

impl WriteBatch {
    pub fn tree<'batch>(&'batch self, tree: &str) -> WriteTree<'batch> { WriteTree(self.0.tree(tree)) }
    pub async fn commit(&self) -> Result<()> { self.0.commit().await }
    pub async fn abort(&self) { self.0.abort().await }
    pub async fn close(self) { self.0.close().await }
}

impl ReadView {
    pub fn tree<'view>(&'view self, tree: &str) -> ReadTree<'view> { ReadTree(self.0.tree(tree)) }
}

impl<'batch> WriteTree<'batch> {
    pub async fn write(&self, key: &[u8], value: &[u8]) -> Result<()> { Ok(self.0.write(key, value).await?) }
    pub async fn delete(&self, key: &[u8]) -> Result<()> { Ok(self.0.delete(key).await?) }
    pub async fn delete_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> { Ok(self.0.delete_range(start_key, end_key).await?) }
}

impl<'view> ReadTree<'view> {
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { self.0.read(key).await }
    pub fn cursor(&self) -> Cursor { Cursor(self.0.cursor()) }
}

impl Cursor {
    pub fn is_valid(&self) -> bool { self.0.is_valid() }
    pub fn key(&self) -> Vec<u8> { self.0.key() }
    pub async fn value(&mut self) -> Result<Vec<u8>> { Ok(self.0.value().await?) }
    pub fn next(&mut self) { self.0.next() }
    pub fn prev(&mut self) { self.0.prev() }
    pub fn seek_first(&mut self) { self.0.seek_first() }
    pub fn seek_last(&mut self) { self.0.seek_last() }
    pub fn seek_key(&mut self, key: &[u8]) { self.0.seek_key(key) }
    pub fn seek_key_rev(&mut self, key: &[u8]) { self.0.seek_key_rev(key) }
}
