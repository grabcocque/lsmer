use lsmer::bptree::{BPlusTree, StorageReference};
use std::ops::Bound;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_bptree_basic_operations() {
    let test_future = async {
        let mut tree = BPlusTree::<String, String>::new(4);

        // Test empty tree
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);

        // Test insertion
        tree.insert("key1".to_string(), "value1".to_string(), None)
            .unwrap();
        tree.insert("key2".to_string(), "value2".to_string(), None)
            .unwrap();
        tree.insert("key3".to_string(), "value3".to_string(), None)
            .unwrap();

        // Test len and is_empty
        assert!(!tree.is_empty());
        assert_eq!(tree.len(), 3);

        // Test find
        let result = tree.find(&"key1".to_string()).unwrap();
        assert!(result.is_some());
        let kv = result.unwrap();
        assert_eq!(kv.key, "key1");
        assert_eq!(kv.value, Some("value1".to_string()));
        assert!(kv.storage_ref.is_none());

        // Test find for non-existent key
        let result = tree.find(&"non-existent".to_string()).unwrap();
        assert!(result.is_none());

        // Test range queries
        let range_results = tree.range(..).unwrap();
        assert_eq!(range_results.len(), 3);
        assert_eq!(range_results[0].key, "key1");
        assert_eq!(range_results[1].key, "key2");
        assert_eq!(range_results[2].key, "key3");

        // Test bounded range queries
        let bounded_range = tree.range("key1".to_string()..="key2".to_string()).unwrap();
        assert_eq!(bounded_range.len(), 2);

        // Test delete
        tree.delete(&"key2".to_string()).unwrap();
        assert_eq!(tree.len(), 2);
        assert!(tree.find(&"key2".to_string()).unwrap().is_none());

        // Test clear
        tree.clear();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bptree_with_storage_references() {
    let test_future = async {
        let mut tree = BPlusTree::<String, String>::new(4);

        // Create storage references
        let storage_ref1 = StorageReference {
            file_path: "file1.sst".to_string(),
            offset: 100,
            is_tombstone: false,
        };

        let storage_ref2 = StorageReference {
            file_path: "file2.sst".to_string(),
            offset: 200,
            is_tombstone: false,
        };

        // Insert with storage references
        tree.insert("key1".to_string(), "value1".to_string(), Some(storage_ref1))
            .unwrap();
        tree.insert("key2".to_string(), "value2".to_string(), Some(storage_ref2))
            .unwrap();

        // Verify storage references
        let result1 = tree.find(&"key1".to_string()).unwrap().unwrap();
        assert!(result1.storage_ref.is_some());
        let sr1 = result1.storage_ref.unwrap();
        assert_eq!(sr1.file_path, "file1.sst");
        assert_eq!(sr1.offset, 100);

        let result2 = tree.find(&"key2".to_string()).unwrap().unwrap();
        assert!(result2.storage_ref.is_some());
        let sr2 = result2.storage_ref.unwrap();
        assert_eq!(sr2.file_path, "file2.sst");
        assert_eq!(sr2.offset, 200);

        // Test range with storage references
        let range_results = tree.range(..).unwrap();
        assert_eq!(range_results.len(), 2);
        assert!(range_results[0].storage_ref.is_some());
        assert!(range_results[1].storage_ref.is_some());
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bptree_complex_range_queries() {
    let test_future = async {
        let mut tree = BPlusTree::<i32, String>::new(4);

        // Insert 10 items
        for i in 0..10 {
            tree.insert(i, format!("value{}", i), None).unwrap();
        }

        // Test inclusive range
        let range1 = tree.range(3..=7).unwrap();
        assert_eq!(range1.len(), 5);
        for (idx, item) in range1.iter().enumerate() {
            assert_eq!(item.key, idx as i32 + 3);
        }

        // Test exclusive range
        let range2 = tree.range(3..7).unwrap();
        assert_eq!(range2.len(), 4);
        for (idx, item) in range2.iter().enumerate() {
            assert_eq!(item.key, idx as i32 + 3);
        }

        // Test with lower bound only
        let range3 = tree.range(5..).unwrap();
        assert_eq!(range3.len(), 5);
        for (idx, item) in range3.iter().enumerate() {
            assert_eq!(item.key, idx as i32 + 5);
        }

        // Test with upper bound only
        let range4 = tree.range(..5).unwrap();
        assert_eq!(range4.len(), 5);
        for (idx, item) in range4.iter().enumerate() {
            assert_eq!(item.key, idx as i32);
        }

        // Test with custom bounds
        let range5 = tree
            .range((Bound::Excluded(2), Bound::Included(8)))
            .unwrap();
        assert_eq!(range5.len(), 6);
        for (idx, item) in range5.iter().enumerate() {
            assert_eq!(item.key, idx as i32 + 3);
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
