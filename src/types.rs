use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Eq, PartialEq)]
#[derive(Copy, Clone)]
#[derive(Debug)]
pub struct Address(pub u64);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Ord, PartialOrd)]
#[derive(Clone)]
#[derive(Debug)]
pub struct Key(pub Vec<u8>);

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq)]
#[derive(Ord, PartialOrd)]
#[derive(Clone)]
#[derive(Debug)]
pub struct Value(pub Vec<u8>);

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

impl Key {
    pub fn from_slice(other: &[u8]) -> Key {
        Key(other.to_vec())
    }
}

impl Value {
    pub fn from_slice(other: &[u8]) -> Value {
        Value(other.to_vec())
    }
}
