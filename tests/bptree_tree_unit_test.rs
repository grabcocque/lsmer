use std::time::Duration;
use tokio::time::timeout;
use lsmer::bptree::{BPlusTree, StorageReference, TreeOps};
use std::ops::Bound;

#[tokio::test]
#[should_panic(expected = "B+ tree order must be at least 3")]
async fn test_bplustree_constructor_validation() {
    let test_future = async {
        // This should panic with order < 3
        let _tree: BPlusTree<i32, String> = BPlusTree::new(2);
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bplustree_basic_operations() {
    let test_future = async {
        // Create a new tree with order 4
        let mut tree = BPlusTree::new(4);
    
        // Initial state
        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());
    
        // Test find on empty tree
        let result = tree.find(&10);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    
        // Test insert
        let insert_result = tree.insert(10, "ten".to_string(), None);
        assert!(insert_result.is_ok());
    
        // Verify length increased
        assert_eq!(tree.len(), 1);
        assert!(!tree.is_empty());
    
        // Test find after insert
        let find_result = tree.find(&10);
        assert!(find_result.is_ok());
        let entry = find_result.unwrap();
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.key, 10);
        assert_eq!(entry.value.unwrap(), "ten");
        assert!(entry.storage_ref.is_none());
    
        // Test insert with storage reference
        let storage_ref = StorageReference {
            file_path: "test.sst".to_string(),
            offset: 100,
            is_tombstone: false,
        };
    
        let insert_result = tree.insert(20, "twenty".to_string(), Some(storage_ref.clone()));
        assert!(insert_result.is_ok());
    
        // Test find with storage reference
        let find_result = tree.find(&20);
        assert!(find_result.is_ok());
        let entry = find_result.unwrap().unwrap();
        assert_eq!(entry.key, 20);
        assert_eq!(entry.value.unwrap(), "twenty");
        assert_eq!(entry.storage_ref.unwrap(), storage_ref);
    
        // Test update existing key
        let update_result = tree.insert(10, "TEN".to_string(), None);
        assert!(update_result.is_ok());
    
        // Verify update worked
        let find_result = tree.find(&10);
        assert!(find_result.is_ok());
        let entry = find_result.unwrap().unwrap();
        assert_eq!(entry.value.unwrap(), "TEN");
    
        // Test delete
        let delete_result = tree.delete(&10);
        assert!(delete_result.is_ok());
    
        // Verify deletion
        let find_result = tree.find(&10);
        assert!(find_result.is_ok());
        assert!(find_result.unwrap().is_none());
    
        // Test delete non-existent key
        let delete_result = tree.delete(&30);
        assert!(delete_result.is_err());
        match delete_result {
            Err(lsmer::bptree::IndexError::KeyNotFound) => {}
            _ => panic!("Expected KeyNotFound error"),
        }
    
        // Test clear
        tree.clear();
        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bplustree_range_queries() {
    let test_future = async {
        let mut tree = BPlusTree::new(4);
    
        // Insert some data
        for i in 0..10 {
            tree.insert(i, format!("value_{}", i), None).unwrap();
        }
    
        // Test inclusive range
        let range_result = tree.range(3..=7);
        assert!(range_result.is_ok());
        let entries = range_result.unwrap();
        assert_eq!(entries.len(), 5);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, i as i32 + 3);
            assert_eq!(entry.value.as_ref().unwrap(), &format!("value_{}", i + 3));
        }
    
        // Test exclusive range
        let range_result = tree.range(3..7);
        assert!(range_result.is_ok());
        let entries = range_result.unwrap();
        assert_eq!(entries.len(), 4);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, i as i32 + 3);
        }
    
        // Test from bound
        let range_result = tree.range(8..);
        assert!(range_result.is_ok());
        let entries = range_result.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, 8);
        assert_eq!(entries[1].key, 9);
    
        // Test to bound
        let range_result = tree.range(..3);
        assert!(range_result.is_ok());
        let entries = range_result.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, 0);
        assert_eq!(entries[1].key, 1);
        assert_eq!(entries[2].key, 2);
    
        // Test full range
        let range_result = tree.range(..);
        assert!(range_result.is_ok());
        let entries = range_result.unwrap();
        assert_eq!(entries.len(), 10);
    
        // Test empty range
        let range_result = tree.range(20..30);
        assert!(range_result.is_ok());
        let entries = range_result.unwrap();
        assert_eq!(entries.len(), 0);
    
        // Test with explicit bounds
        let range_result = tree.range((Bound::Excluded(3), Bound::Excluded(7)));
        assert!(range_result.is_ok());
        let entries = range_result.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, 4);
        assert_eq!(entries[1].key, 5);
        assert_eq!(entries[2].key, 6);
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bplustree_trait_implementation() {
    let test_future = async {
        // Test the TreeOps trait implementation directly
        let mut tree = BPlusTree::new(4);
    
        // Test trait methods directly
        assert!(TreeOps::is_empty(&tree));
    
        TreeOps::insert(&mut tree, 1, "one".to_string(), None).unwrap();
        TreeOps::insert(&mut tree, 2, "two".to_string(), None).unwrap();
    
        assert_eq!(TreeOps::len(&tree), 2);
    
        // Test find
        let result = TreeOps::find(&tree, &1).unwrap().unwrap();
        assert_eq!(result.key, 1);
        assert_eq!(result.value.unwrap(), "one");
    
        // Test range
        let range_result = TreeOps::range(&tree, 1..=2).unwrap();
        assert_eq!(range_result.len(), 2);
    
        // Test delete
        TreeOps::delete(&mut tree, &1).unwrap();
        assert_eq!(TreeOps::len(&tree), 1);
    
        // Test clear
        TreeOps::clear(&mut tree);
        assert_eq!(TreeOps::len(&tree), 0);
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bplustree_with_storage_references() {
    let test_future = async {
        let mut tree = BPlusTree::new(4);
    
        // Insert entries with storage references
        for i in 0..5 {
            let storage_ref = StorageReference {
                file_path: format!("file_{}.sst", i),
                offset: i * 100,
                is_tombstone: i % 2 == 0,
            };
    
            tree.insert(i, format!("value_{}", i), Some(storage_ref))
                .unwrap();
        }
    
        // Verify entries and storage references
        for i in 0..5 {
            let result = tree.find(&i).unwrap().unwrap();
            assert_eq!(result.key, i);
            assert_eq!(result.value.unwrap(), format!("value_{}", i));
    
            let storage_ref = result.storage_ref.unwrap();
            assert_eq!(storage_ref.file_path, format!("file_{}.sst", i));
            assert_eq!(storage_ref.offset, i * 100);
            assert_eq!(storage_ref.is_tombstone, i % 2 == 0);
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
