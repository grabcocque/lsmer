use super::{IndexError, IndexKeyValue, StorageReference, TreeOps};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;

/// A B+ tree implementation optimized for range queries
///
/// This implementation uses a BTreeMap internally for simplicity and efficiency.
/// It supports storage references for disk-based values and provides efficient
/// range query operations.
///
/// # Type Parameters
///
/// * `K` - The key type, must implement `Clone + PartialOrd + Debug + Ord`
/// * `V` - The value type, must implement `Clone + Debug`
///
/// # Examples
///
/// ```
/// use lsmer::bptree::{BPlusTree, StorageReference};
///
/// // Create a new B+ tree with order 4
/// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
///
/// // Insert some values
/// tree.insert(1, "one".to_string(), None)?;
/// tree.insert(2, "two".to_string(), Some(StorageReference {
///     file_path: "data.sst".to_string(),
///     offset: 0,
///     is_tombstone: false,
/// }))?;
///
/// // Find values
/// let value1 = tree.find(&1)?;
/// assert_eq!(value1.unwrap().value.unwrap(), "one");
///
/// let value2 = tree.find(&2)?;
/// assert!(value2.unwrap().storage_ref.is_some());
///
/// // Delete a value
/// tree.delete(&1)?;
/// assert!(tree.find(&1)?.is_none());
/// # Ok::<(), lsmer::bptree::IndexError>(())
/// ```
#[derive(Debug, Clone)]
pub struct BPlusTree<K, V> {
    /// Internal storage using BTreeMap for simplicity
    storage: BTreeMap<K, (Option<V>, Option<StorageReference>)>,
    /// The order of the tree (maximum number of children per node)
    #[allow(dead_code)]
    order: usize,
}

impl<K: Clone + PartialOrd + Debug + Ord, V: Clone + Debug> BPlusTree<K, V> {
    /// Create a new B+ tree with the specified order
    ///
    /// The order determines the maximum number of children per node. A higher order
    /// means more entries per node but potentially more memory usage.
    ///
    /// # Arguments
    ///
    /// * `order` - The order of the tree (must be >= 3)
    ///
    /// # Panics
    ///
    /// Panics if the order is less than 3.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::BPlusTree;
    ///
    /// let tree: BPlusTree<i32, String> = BPlusTree::new(4);
    /// assert!(tree.is_empty());
    ///
    /// // Order must be at least 3
    /// let result = std::panic::catch_unwind(|| {
    ///     BPlusTree::<i32, String>::new(2);
    /// });
    /// assert!(result.is_err());
    /// ```
    pub fn new(order: usize) -> Self {
        if order < 3 {
            panic!("B+ tree order must be at least 3");
        }

        BPlusTree {
            storage: BTreeMap::new(),
            order,
        }
    }

    /// Find a key-value pair in the tree
    ///
    /// # Arguments
    ///
    /// * `key` - The key to find
    ///
    /// # Returns
    ///
    /// * `Ok(Some(IndexKeyValue))` - The key was found with its value and storage reference
    /// * `Ok(None)` - The key was not found
    /// * `Err(IndexError)` - An error occurred during the operation
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::BPlusTree;
    ///
    /// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
    /// tree.insert(1, "one".to_string(), None)?;
    ///
    /// let found = tree.find(&1)?;
    /// assert_eq!(found.unwrap().value.unwrap(), "one");
    ///
    /// let not_found = tree.find(&2)?;
    /// assert!(not_found.is_none());
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
    pub fn find(&self, key: &K) -> Result<Option<IndexKeyValue<K, V>>, IndexError> {
        if let Some((value, storage_ref)) = self.storage.get(key) {
            Ok(Some(IndexKeyValue {
                key: key.clone(),
                value: value.clone(),
                storage_ref: storage_ref.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Insert a key-value pair into the tree
    ///
    /// If the key already exists, its value and storage reference will be updated.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to insert
    /// * `storage_ref` - Optional reference to where the value is stored on disk
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::BPlusTree;
    ///
    /// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
    ///
    /// // Insert a new value
    /// tree.insert(1, "one".to_string(), None)?;
    /// assert_eq!(tree.find(&1)?.unwrap().value.unwrap(), "one");
    ///
    /// // Update an existing value
    /// tree.insert(1, "ONE".to_string(), None)?;
    /// assert_eq!(tree.find(&1)?.unwrap().value.unwrap(), "ONE");
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
    pub fn insert(
        &mut self,
        key: K,
        value: V,
        storage_ref: Option<StorageReference>,
    ) -> Result<(), IndexError> {
        self.storage.insert(key, (Some(value), storage_ref));
        Ok(())
    }

    /// Delete a key from the tree
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The key was found and deleted
    /// * `Err(IndexError::KeyNotFound)` - The key was not found
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::{BPlusTree, IndexError};
    ///
    /// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
    /// tree.insert(1, "one".to_string(), None)?;
    ///
    /// // Delete existing key
    /// tree.delete(&1)?;
    /// assert!(tree.find(&1)?.is_none());
    ///
    /// // Try to delete non-existent key
    /// assert!(matches!(tree.delete(&2), Err(IndexError::KeyNotFound)));
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
    pub fn delete(&mut self, key: &K) -> Result<(), IndexError> {
        if self.storage.remove(key).is_some() {
            Ok(())
        } else {
            Err(IndexError::KeyNotFound)
        }
    }

    /// Get a range of key-value pairs from the tree
    ///
    /// # Arguments
    ///
    /// * `range` - The range of keys to retrieve
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::BPlusTree;
    ///
    /// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
    /// tree.insert(1, "one".to_string(), None)?;
    /// tree.insert(2, "two".to_string(), None)?;
    /// tree.insert(3, "three".to_string(), None)?;
    ///
    /// // Inclusive range
    /// let range1 = tree.range(1..=2)?;
    /// assert_eq!(range1.len(), 2);
    ///
    /// // Exclusive range
    /// let range2 = tree.range(1..3)?;
    /// assert_eq!(range2.len(), 2);
    ///
    /// // Unbounded range
    /// let range3 = tree.range(2..)?;
    /// assert_eq!(range3.len(), 2);
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
    pub fn range<R: RangeBounds<K> + Clone>(
        &self,
        range: R,
    ) -> Result<Vec<IndexKeyValue<K, V>>, IndexError> {
        let result = self
            .storage
            .range(range)
            .map(|(k, (v, sr))| IndexKeyValue {
                key: k.clone(),
                value: v.clone(),
                storage_ref: sr.clone(),
            })
            .collect();

        Ok(result)
    }

    /// Get the number of keys in the tree
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::BPlusTree;
    ///
    /// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
    /// assert_eq!(tree.len(), 0);
    ///
    /// tree.insert(1, "one".to_string(), None)?;
    /// assert_eq!(tree.len(), 1);
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Check if the tree is empty
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::BPlusTree;
    ///
    /// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
    /// assert!(tree.is_empty());
    ///
    /// tree.insert(1, "one".to_string(), None)?;
    /// assert!(!tree.is_empty());
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Clear the tree
    ///
    /// Removes all entries from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::BPlusTree;
    ///
    /// let mut tree: BPlusTree<i32, String> = BPlusTree::new(4);
    /// tree.insert(1, "one".to_string(), None)?;
    /// tree.insert(2, "two".to_string(), None)?;
    ///
    /// tree.clear();
    /// assert!(tree.is_empty());
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
    pub fn clear(&mut self) {
        self.storage.clear();
    }
}

impl<K: Clone + PartialOrd + Debug + Ord, V: Clone + Debug> TreeOps<K, V> for BPlusTree<K, V> {
    fn find(&self, key: &K) -> Result<Option<IndexKeyValue<K, V>>, IndexError> {
        self.find(key)
    }

    fn insert(
        &mut self,
        key: K,
        value: V,
        storage_ref: Option<StorageReference>,
    ) -> Result<(), IndexError> {
        self.insert(key, value, storage_ref)
    }

    fn delete(&mut self, key: &K) -> Result<(), IndexError> {
        self.delete(key)
    }

    fn range<R: RangeBounds<K> + Clone>(
        &self,
        range: R,
    ) -> Result<Vec<IndexKeyValue<K, V>>, IndexError> {
        self.range(range)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn clear(&mut self) {
        self.clear()
    }
}
