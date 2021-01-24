use anyhow::Result;

pub struct DbConfig;

#[derive(Clone, Debug)]
pub struct Db;

pub struct WriteBatch;

#[derive(Clone, Debug)]
pub struct ReadView;

pub struct WriteTree<'batch>(std::marker::PhantomData<&'batch ()>);

pub struct ReadTree<'view>(std::marker::PhantomData<&'view ()>);

pub struct Cursor;

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> {
        panic!()
    }

    pub fn write_batch(&self) -> WriteBatch {
        panic!()
    }

    pub fn read_view(&self) -> ReadView {
        panic!()
    }

    pub async fn sync(&self) -> Result<()> {
        panic!()
    }
}

impl WriteBatch {
    pub fn tree<'batch>(&'batch self, tree: &str) -> WriteTree<'batch> {
        panic!()
    }

    pub async fn commit(self) -> Result<()> {
        panic!()
    }

    pub fn abort(self) {
        panic!()
    }
}

impl ReadView {
    pub fn tree<'view>(&'view self, tree: &str) -> ReadTree<'view> {
        panic!()
    }
}

impl<'batch> WriteTree<'batch> {
    pub fn write(&self, key: &[u8], value: &[u8]) {
        panic!()
    }

    pub fn delete(&self, key: &[u8]) {
        panic!()
    }
}

impl<'view> ReadTree<'view> {
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        panic!()
    }

    pub fn cursor(&self) -> Cursor {
        panic!()
    }
}

impl Cursor {
    pub fn valid(&self) -> bool {
        panic!()
    }

    pub async fn next(&mut self) -> Result<()> {
        panic!()
    }

    pub async fn prev(&mut self) -> Result<()> {
        panic!()
    }

    pub fn key_value(&self) -> (&[u8], &[u8]) {
        panic!()
    }

    pub async fn seek_first(&mut self) -> Result<()> {
        panic!()
    }

    pub async fn seek_last(&mut self) -> Result<()> {
        panic!()
    }

    pub async fn seek_key(&mut self, key: &[u8]) -> Result<()> {
        panic!()
    }

    pub async fn seek_key_rev(&mut self, key: &[u8]) -> Result<()> {
        panic!()
    }
}
