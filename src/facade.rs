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
    pub fn write_batch(&self) -> WriteBatch { WriteBatch(self.0.write_batch()) }
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
    pub fn write(&self, key: &[u8], value: &[u8]) { self.0.write(key, value) }
    pub fn delete(&self, key: &[u8]) { self.0.delete(key) }
}

impl<'view> ReadTree<'view> {
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { self.0.read(key).await }
    pub fn cursor(&self) -> Cursor { Cursor(self.0.cursor()) }
}

impl Cursor {
    pub fn valid(&self) -> bool { self.0.valid() }
    pub async fn next(&mut self) -> Result<()> { Ok(self.0.next().await?) }
    pub async fn prev(&mut self) -> Result<()> { Ok(self.0.prev().await?) }
    pub fn key_value(&self) -> (&[u8], &[u8]) { self.0.key_value() }
    pub async fn seek_first(&mut self) -> Result<()> { Ok(self.0.seek_first().await?) }
    pub async fn seek_last(&mut self) -> Result<()> { Ok(self.0.seek_last().await?) }
    pub async fn seek_key(&mut self, key: &[u8]) -> Result<()> { Ok(self.0.seek_key(key).await?) }
    pub async fn seek_key_rev(&mut self, key: &[u8]) -> Result<()> { Ok(self.0.seek_key_rev(key).await?) }
}
