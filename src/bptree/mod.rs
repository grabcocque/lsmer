//! B+ Tree implementation for LSM storage

mod node;
mod tree;

pub use node::{BPTreeNode, IndexEntry, NodeType};
pub use tree::BPlusTree;

use std::fmt::Debug;

/// Error types for the B+ tree operations
#[derive(Debug)]
pub enum IndexError {
    /// Key not found in the tree
    KeyNotFound,
    /// Invalid operation
    InvalidOperation,
}

/// A key-value pair with an optional storage reference
#[derive(Debug, Clone)]
pub struct IndexKeyValue<K, V> {
    /// The key
    pub key: K,
    /// The value
    pub value: Option<V>,
    /// A reference to where this entry is stored (for SSTable integration)
    pub storage_ref: Option<StorageReference>,
}

/// A reference to data stored in an SSTable
#[derive(Debug, Clone, PartialEq)]
pub struct StorageReference {
    /// The file path where the data is stored
    pub file_path: String,
    /// The offset in the file
    pub offset: usize,
    /// Whether this is a tombstone entry
    pub is_tombstone: bool,
}

/// Operations for a B+ tree
pub trait TreeOps<K, V> {
    /// Find a key in the tree
    fn find(&self, key: &K) -> Result<Option<IndexKeyValue<K, V>>, IndexError>;

    /// Insert a key-value pair into the tree
    fn insert(
        &mut self,
        key: K,
        value: V,
        storage_ref: Option<StorageReference>,
    ) -> Result<(), IndexError>;

    /// Delete a key from the tree
    fn delete(&mut self, key: &K) -> Result<(), IndexError>;

    /// Get a range of key-value pairs from the tree
    fn range<R: std::ops::RangeBounds<K> + Clone>(
        &self,
        range: R,
    ) -> Result<Vec<IndexKeyValue<K, V>>, IndexError>;

    /// Get the number of keys in the tree
    fn len(&self) -> usize;

    /// Check if the tree is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all entries from the tree
    fn clear(&mut self);
}
