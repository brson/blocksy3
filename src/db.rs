use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
use std::sync::Arc;


pub struct Db {
    next_batch: AtomicUsize,
    next_view: AtomicUsize,
    next_commit: AtomicUsize,
}
