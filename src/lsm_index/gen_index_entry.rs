use crate::bptree::StorageReference;
use crate::lsm_index::gen_ref::{make_gen_ref, GenRefHandle};

/// A generationally reference-counted index entry
///
/// This structure represents a key-value pair in the LSM index with
/// generational reference counting for safe concurrent access.
#[derive(Debug, Clone)]
pub struct GenIndexEntry {
    /// The value for this entry, if stored in memory
    /// Using a generational reference to ensure safe concurrent access
    value: Option<GenRefHandle<Vec<u8>>>,
    /// Reference to storage on disk (SSTables), if applicable
    storage_ref: Option<StorageReference>,
}

impl GenIndexEntry {
    /// Create a new `GenIndexEntry` with the given value and storage reference
    pub fn new(value: Option<Vec<u8>>, storage_ref: Option<StorageReference>) -> Self {
        // Convert value to a generationally reference-counted value if present
        let gen_value = value.map(make_gen_ref);

        GenIndexEntry {
            value: gen_value,
            storage_ref,
        }
    }

    /// Get a clone of the value, if present
    pub fn value(&self) -> Option<Vec<u8>> {
        self.value.as_ref().map(|handle| handle.clone_data())
    }

    /// Get a reference to the storage reference, if present
    pub fn storage_ref(&self) -> Option<&StorageReference> {
        self.storage_ref.as_ref()
    }

    /// Update the value, returning a new entry
    pub fn with_value(self, value: Vec<u8>) -> Self {
        GenIndexEntry {
            value: Some(make_gen_ref(value)),
            storage_ref: self.storage_ref,
        }
    }

    /// Update the storage reference, returning a new entry
    pub fn with_storage_ref(self, storage_ref: StorageReference) -> Self {
        GenIndexEntry {
            value: self.value,
            storage_ref: Some(storage_ref),
        }
    }

    /// Check if the value is a tombstone
    pub fn is_tombstone(&self) -> bool {
        if let Some(ref_storage) = &self.storage_ref {
            ref_storage.is_tombstone
        } else {
            // If no storage reference and no value, it's a tombstone
            self.value.is_none()
        }
    }

    /// Check if this entry's value handle is stale (the data has been updated)
    pub fn is_value_stale(&self) -> bool {
        self.value.as_ref().is_some_and(|handle| handle.is_stale())
    }
}

/// Convert from the legacy IndexEntry to a GenIndexEntry
#[cfg(test)]
impl From<crate::lsm_index::IndexEntry> for GenIndexEntry {
    fn from(entry: crate::lsm_index::IndexEntry) -> Self {
        GenIndexEntry::new(entry.value, entry.storage_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_index_entry_basic() {
        // Create a new entry with a value
        let entry = GenIndexEntry::new(Some(vec![1, 2, 3]), None);

        // Check value
        assert_eq!(entry.value(), Some(vec![1, 2, 3]));
        assert_eq!(entry.storage_ref(), None);
        assert!(!entry.is_tombstone());
    }

    #[test]
    fn test_gen_index_entry_with_storage_ref() {
        // Create storage reference
        let storage_ref = StorageReference {
            file_path: "test.sst".to_string(),
            offset: 123,
            is_tombstone: false,
        };

        // Create a new entry with a storage reference
        let entry = GenIndexEntry::new(None, Some(storage_ref.clone()));

        // Check storage reference
        assert_eq!(entry.value(), None);
        assert_eq!(entry.storage_ref().unwrap().file_path, "test.sst");
        assert_eq!(entry.storage_ref().unwrap().offset, 123);
        assert!(!entry.is_tombstone());
    }

    #[test]
    fn test_gen_index_entry_tombstone() {
        // Create storage reference with tombstone
        let storage_ref = StorageReference {
            file_path: "test.sst".to_string(),
            offset: 123,
            is_tombstone: true,
        };

        // Create a new entry with a tombstone storage reference
        let entry = GenIndexEntry::new(None, Some(storage_ref));

        // Check tombstone
        assert!(entry.is_tombstone());
    }

    #[test]
    fn test_gen_index_entry_update() {
        // Create a new entry with a value
        let entry = GenIndexEntry::new(Some(vec![1, 2, 3]), None);

        // Update value
        let updated = entry.with_value(vec![4, 5, 6]);

        // Check updated value
        assert_eq!(updated.value(), Some(vec![4, 5, 6]));
    }

    #[test]
    fn test_gen_index_entry_clone() {
        // Create a new entry with a value
        let entry = GenIndexEntry::new(Some(vec![1, 2, 3]), None);

        // Clone the entry
        let clone = entry.clone();

        // Check that clone has the same value
        assert_eq!(clone.value(), Some(vec![1, 2, 3]));
    }
}
