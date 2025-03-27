use crossbeam_skiplist::SkipMap;
use std::io;
use std::sync::Arc;

/// A simple index based on crossbeam's SkipMap
pub struct SkipListIndex<K, V>
where
    K: Ord + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    /// The underlying skip map
    map: Arc<SkipMap<K, V>>,
}

impl<K, V> SkipListIndex<K, V>
where
    K: Ord + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    /// Create a new skip list index
    pub fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
        }
    }

    /// Insert a key-value pair
    pub fn insert(&self, key: K, value: V) -> Result<(), io::Error> {
        self.map.insert(key, value);
        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &K) -> Result<Option<V>, io::Error> {
        if let Some(entry) = self.map.get(key) {
            Ok(Some(entry.value().clone()))
        } else {
            Ok(None)
        }
    }

    /// Remove a key
    pub fn remove(&self, key: &K) -> Result<(), io::Error> {
        self.map.remove(key);
        Ok(())
    }

    /// Check if a key exists
    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<K, V> Default for SkipListIndex<K, V>
where
    K: Ord + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
