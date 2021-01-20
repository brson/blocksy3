use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Eq, PartialEq)]
#[derive(Copy, Clone)]
pub struct Address(pub u64);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Ord, PartialOrd)]
#[derive(Clone)]
#[derive(Debug)]
pub struct Key(pub Arc<Vec<u8>>);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Ord, PartialOrd)]
#[derive(Clone)]
pub struct Value(pub Arc<Vec<u8>>);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Ord, PartialOrd)]
#[derive(Copy, Clone)]
#[derive(Debug)]
pub struct Batch(pub u64);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Ord, PartialOrd)]
#[derive(Copy, Clone)]
#[derive(Debug)]
pub struct BatchCommit(pub u64);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Ord, PartialOrd)]
#[derive(Copy, Clone)]
#[derive(Debug)]
pub struct Commit(pub u64);
