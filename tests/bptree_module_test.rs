use lsmer::bptree::{BPlusTree, StorageReference};
use std::time::Duration;
use tokio::time::timeout;

// Test the BPlusTree module functionality
#[tokio::test]
async fn test_bptree_mod_functions() {
    let test_future = async {
        // Create a B+Tree
        let mut tree = BPlusTree::<String, Vec<u8>>::new(4); // Order must be >= 3

        // Test empty tree
        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());

        // Test insert and get
        let key = "test_key".to_string();
        let value = vec![1, 2, 3, 4, 5];

        tree.insert(key.clone(), value.clone(), None).unwrap(); // Add None for storage_ref

        // Verify insertion
        assert_eq!(tree.len(), 1);
        assert!(!tree.is_empty());

        // Test find
        let retrieved = tree.find(&key).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.key, key);
        assert_eq!(retrieved.value, Some(value.clone()));

        // Test non-existent key
        let non_existent = tree.find(&"nonexistent".to_string()).unwrap();
        assert!(non_existent.is_none());

        // Test clear
        tree.clear();
        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());

        // Test delete
        tree.insert(key.clone(), value.clone(), None).unwrap();
        assert_eq!(tree.len(), 1);

        tree.delete(&key).unwrap();
        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());

        // Test range queries
        tree.insert("a".to_string(), vec![1], None).unwrap();
        tree.insert("b".to_string(), vec![2], None).unwrap();
        tree.insert("c".to_string(), vec![3], None).unwrap();

        let range_results = tree.range("a".to_string().."c".to_string()).unwrap();
        assert_eq!(range_results.len(), 2); // a and b, but not c (exclusive range)
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

// Test StorageReference
#[tokio::test]
async fn test_storage_reference() {
    let test_future = async {
        // Create a new StorageReference
        let sr = StorageReference {
            file_path: "test.db".to_string(),
            offset: 100,
            is_tombstone: false,
        };

        // Test properties
        assert_eq!(sr.file_path, "test.db");
        assert_eq!(sr.offset, 100);
        assert!(!sr.is_tombstone);

        // Test equality
        let sr2 = StorageReference {
            file_path: "test.db".to_string(),
            offset: 100,
            is_tombstone: false,
        };
        assert_eq!(sr, sr2);

        let sr3 = StorageReference {
            file_path: "test.db".to_string(),
            offset: 101,
            is_tombstone: false,
        };
        assert_ne!(sr, sr3);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
