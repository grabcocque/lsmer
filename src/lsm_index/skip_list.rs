use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

// A simple wrapper around a BTreeMap to provide an API compatible with what we need
// In a production setting, this would be replaced with a real concurrent skip list
pub struct ConcurrentSkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    // Use RwLock for thread safety
    data: Arc<RwLock<BTreeMap<K, V>>>,
}

impl<K, V> ConcurrentSkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Create a new empty skip list
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Create a new empty skip list with the specified maximum height
    pub fn with_max_height(_max_height: usize) -> Self {
        Self::new()
    }

    /// Get the number of elements in the skip list
    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    /// Check if the skip list is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert a key-value pair into the skip list
    /// Returns the previous value if the key already existed
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let mut map = self.data.write().unwrap();
        map.insert(key, value)
    }

    /// Get the value associated with a key
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let map = self.data.read().unwrap();
        map.get(key).cloned()
    }

    /// Remove a key from the skip list
    /// Returns the removed value if found
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut map = self.data.write().unwrap();
        map.remove(key)
    }
}
