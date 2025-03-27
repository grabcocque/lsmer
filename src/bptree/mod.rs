//! B+ Tree implementation for LSM storage
//!
//! This module provides a B+ tree implementation optimized for disk-based storage systems.
//! It includes support for storage references, making it suitable for LSM tree implementations
//! where values might be stored on disk.
//!
//! # Features
//!
//! - Balanced tree structure maintaining O(log n) height
//! - Efficient range queries with leaf node linking
//! - Support for storage references to handle disk-based values
//! - Thread-safe operations
//!
//! # Examples
//!
//! ```
//! use lsmer::bptree::{BPlusTree, TreeOps, IndexKeyValue};
//!
//! // Create a new B+ tree with order 4
//! let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
//!
//! // Insert some key-value pairs
//! tree.insert(1, "one".to_string(), None)?;
//! tree.insert(2, "two".to_string(), None)?;
//!
//! // Find a value
//! if let Some(entry) = tree.find(&1)? {
//!     assert_eq!(entry.value.unwrap(), "one");
//! }
//!
//! // Perform a range query
//! let range_result = tree.range(1..=2)?;
//! assert_eq!(range_result.len(), 2);
//! # Ok::<(), lsmer::bptree::IndexError>(())
//! ```

mod node;
mod tree;

pub use node::{BPTreeNode, IndexEntry, NodeType};
pub use tree::BPlusTree;

use std::fmt::Debug;

/// Error types for the B+ tree operations
///
/// This enum represents the various errors that can occur during B+ tree operations.
///
/// # Examples
///
/// ```
/// use lsmer::bptree::{BPlusTree, TreeOps, IndexError};
///
/// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
///
/// // Trying to delete a non-existent key results in KeyNotFound error
/// match tree.delete(&1) {
///     Err(IndexError::KeyNotFound) => println!("Key not found, as expected"),
///     _ => panic!("Expected KeyNotFound error"),
/// }
/// ```
#[derive(Debug)]
pub enum IndexError {
    /// Key not found in the tree
    KeyNotFound,
    /// Invalid operation attempted on the tree
    InvalidOperation,
}

/// A key-value pair with an optional storage reference
///
/// This structure represents an entry in the B+ tree, containing a key, an optional value,
/// and an optional reference to where the value is stored on disk.
///
/// # Examples
///
/// ```
/// use lsmer::bptree::{IndexKeyValue, StorageReference};
///
/// let storage_ref = StorageReference {
///     file_path: "data.sst".to_string(),
///     offset: 1234,
///     is_tombstone: false,
/// };
///
/// let entry = IndexKeyValue {
///     key: "example_key",
///     value: Some("example_value"),
///     storage_ref: Some(storage_ref),
/// };
///
/// assert_eq!(entry.key, "example_key");
/// assert_eq!(entry.value.unwrap(), "example_value");
/// assert!(entry.storage_ref.is_some());
/// ```
#[derive(Debug, Clone)]
pub struct IndexKeyValue<K, V> {
    /// The key
    pub key: K,
    /// The value, if present in memory
    pub value: Option<V>,
    /// A reference to where this entry is stored (for SSTable integration)
    pub storage_ref: Option<StorageReference>,
}

/// A reference to data stored in an SSTable
///
/// This structure provides information about where a value is stored on disk,
/// including the file path, offset, and whether it represents a tombstone entry.
///
/// # Examples
///
/// ```
/// use lsmer::bptree::StorageReference;
///
/// let storage_ref = StorageReference {
///     file_path: "data.sst".to_string(),
///     offset: 1234,
///     is_tombstone: false,
/// };
///
/// assert_eq!(storage_ref.file_path, "data.sst");
/// assert_eq!(storage_ref.offset, 1234);
/// assert!(!storage_ref.is_tombstone);
/// ```
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
///
/// This trait defines the standard operations that can be performed on a B+ tree.
/// It provides methods for finding, inserting, deleting, and range querying key-value pairs.
///
/// # Examples
///
/// ```
/// use lsmer::bptree::{BPlusTree, TreeOps};
///
/// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
///
/// // Use TreeOps methods
/// tree.insert(1, "one".to_string(), None)?;
/// tree.insert(2, "two".to_string(), None)?;
///
/// assert_eq!(tree.len(), 2);
/// assert!(!tree.is_empty());
///
/// let range_result = tree.range(1..=2)?;
/// assert_eq!(range_result.len(), 2);
///
/// tree.clear();
/// assert!(tree.is_empty());
/// # Ok::<(), lsmer::bptree::IndexError>(())
/// ```
pub trait TreeOps<K, V> {
    /// Find a key in the tree
    ///
    /// Returns `Ok(Some(IndexKeyValue))` if the key is found, `Ok(None)` if not found,
    /// or an error if the operation fails.
    fn find(&self, key: &K) -> Result<Option<IndexKeyValue<K, V>>, IndexError>;

    /// Insert a key-value pair into the tree
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to insert
    /// * `storage_ref` - Optional reference to where the value is stored on disk
    fn insert(
        &mut self,
        key: K,
        value: V,
        storage_ref: Option<StorageReference>,
    ) -> Result<(), IndexError>;

    /// Delete a key from the tree
    ///
    /// Returns `Ok(())` if the key was found and deleted, or `Err(IndexError::KeyNotFound)`
    /// if the key was not found.
    fn delete(&mut self, key: &K) -> Result<(), IndexError>;

    /// Get a range of key-value pairs from the tree
    ///
    /// Returns all key-value pairs within the specified range, inclusive of the bounds.
    fn range<R: std::ops::RangeBounds<K> + Clone>(
        &self,
        range: R,
    ) -> Result<Vec<IndexKeyValue<K, V>>, IndexError>;

    /// Get the number of keys in the tree
    fn len(&self) -> usize;

    /// Check if the tree is empty
    ///
    /// Returns `true` if the tree contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all entries from the tree
    fn clear(&mut self);
}
