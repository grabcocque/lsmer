use lsmer::bptree::{BPlusTree, IndexError, IndexKeyValue, StorageReference, TreeOps};

struct TestTree {
    count: usize,
}

impl TreeOps<String, Vec<u8>> for TestTree {
    fn find(&self, _key: &String) -> Result<Option<IndexKeyValue<String, Vec<u8>>>, IndexError> {
        Err(IndexError::KeyNotFound)
    }

    fn insert(
        &mut self,
        _key: String,
        _value: Vec<u8>,
        _storage_ref: Option<StorageReference>,
    ) -> Result<(), IndexError> {
        Err(IndexError::InvalidOperation)
    }

    fn delete(&mut self, _key: &String) -> Result<(), IndexError> {
        Err(IndexError::InvalidOperation)
    }

    fn range<R: std::ops::RangeBounds<String> + Clone>(
        &self,
        _range: R,
    ) -> Result<Vec<IndexKeyValue<String, Vec<u8>>>, IndexError> {
        Ok(Vec::new())
    }

    fn len(&self) -> usize {
        self.count
    }

    fn clear(&mut self) {
        self.count = 0;
    }
}

#[test]
fn test_tree_ops_trait() {
    // Test the default implementation of is_empty in the TreeOps trait
    let mut test_tree = TestTree { count: 0 };

    // Should be empty initially
    assert!(test_tree.is_empty());

    // Set count to simulate insertion
    test_tree.count = 5;
    assert!(!test_tree.is_empty());

    // Clear and check empty again
    test_tree.clear();
    assert!(test_tree.is_empty());
}

#[test]
fn test_bptree_module_imports() {
    // Testing the public imports from the module
    let mut tree: BPlusTree<String, Vec<u8>> = BPlusTree::new(10);

    // Insert test data
    let key = "test_key".to_string();
    let value = vec![1, 2, 3];
    tree.insert(key.clone(), value.clone(), None).unwrap();

    // Test that find works
    let found = tree.find(&key).unwrap().unwrap();
    assert_eq!(found.key, key);
    assert_eq!(found.value.unwrap(), value);

    // Create a storage reference
    let storage_ref = StorageReference {
        file_path: "test.sst".to_string(),
        offset: 123,
        is_tombstone: false,
    };

    // Insert with storage reference
    let key2 = "test_key2".to_string();
    let value2 = vec![4, 5, 6];
    tree.insert(key2.clone(), value2.clone(), Some(storage_ref.clone()))
        .unwrap();

    // Find with storage reference
    let found2 = tree.find(&key2).unwrap().unwrap();
    assert_eq!(found2.key, key2);
    assert_eq!(found2.value.unwrap(), value2);
    assert!(found2.storage_ref.is_some());
    assert_eq!(found2.storage_ref.unwrap(), storage_ref);

    // Test range queries
    let range_result = tree
        .range("test_key".to_string().."test_key3".to_string())
        .unwrap();
    assert_eq!(range_result.len(), 2);

    // Test is_empty and len
    assert!(!tree.is_empty());
    assert_eq!(tree.len(), 2);

    // Test clear
    tree.clear();
    assert!(tree.is_empty());
    assert_eq!(tree.len(), 0);

    // Test error handling
    let not_found = tree.find(&"nonexistent".to_string()).unwrap();
    assert!(not_found.is_none());

    let delete_result = tree.delete(&"nonexistent".to_string());
    assert!(matches!(delete_result, Err(IndexError::KeyNotFound)));
}

#[test]
fn test_storage_reference() {
    // Test creating and comparing storage references
    let ref1 = StorageReference {
        file_path: "test.sst".to_string(),
        offset: 123,
        is_tombstone: false,
    };

    let ref2 = StorageReference {
        file_path: "test.sst".to_string(),
        offset: 123,
        is_tombstone: false,
    };

    let ref3 = StorageReference {
        file_path: "other.sst".to_string(),
        offset: 456,
        is_tombstone: true,
    };

    // Test equality
    assert_eq!(ref1, ref2);
    assert_ne!(ref1, ref3);

    // Test debug output
    let debug_str = format!("{:?}", ref1);
    assert!(debug_str.contains("test.sst"));
    assert!(debug_str.contains("123"));
}

#[test]
fn test_index_key_value() {
    // Test creating and using IndexKeyValue
    let key = "test_key".to_string();
    let value = vec![1, 2, 3];
    let storage_ref = StorageReference {
        file_path: "test.sst".to_string(),
        offset: 123,
        is_tombstone: false,
    };

    // With value and storage ref
    let kv1 = IndexKeyValue {
        key: key.clone(),
        value: Some(value.clone()),
        storage_ref: Some(storage_ref.clone()),
    };

    // With value but no storage ref
    let _kv2 = IndexKeyValue {
        key: key.clone(),
        value: Some(value.clone()),
        storage_ref: None,
    };

    // With storage ref but no value (like a reference to an SSTable)
    let _kv3: IndexKeyValue<String, Vec<u8>> = IndexKeyValue {
        key: key.clone(),
        value: None,
        storage_ref: Some(storage_ref.clone()),
    };

    // Test debug output
    let debug_str1 = format!("{:?}", kv1);
    assert!(debug_str1.contains("test_key"));

    // Test cloning
    let kv1_clone = kv1.clone();
    assert_eq!(kv1_clone.key, key);
    assert_eq!(kv1_clone.value, Some(value));
    assert_eq!(kv1_clone.storage_ref, Some(storage_ref));
}

#[test]
fn test_index_error() {
    // Test the IndexError enum
    let err1 = IndexError::KeyNotFound;
    let err2 = IndexError::InvalidOperation;

    // Test debug output
    let err1_str = format!("{:?}", err1);
    let err2_str = format!("{:?}", err2);

    assert_eq!(err1_str, "KeyNotFound");
    assert_eq!(err2_str, "InvalidOperation");
}
