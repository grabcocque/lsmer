use super::{IndexError, IndexKeyValue, StorageReference, TreeOps};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;

/// A B+ tree implementation optimized for range queries
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
    pub fn delete(&mut self, key: &K) -> Result<(), IndexError> {
        if self.storage.remove(key).is_some() {
            Ok(())
        } else {
            Err(IndexError::KeyNotFound)
        }
    }

    /// Get a range of key-value pairs from the tree
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
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Check if the tree is empty
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Clear the tree
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
