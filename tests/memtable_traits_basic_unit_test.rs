use lsmer::memtable::{Memtable, StringMemtable};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_memtable_iterator() {
    let test_future = async {
        let memtable = StringMemtable::new(1024);

        // Test empty iterator
        let entries = memtable.iter().unwrap();
        assert_eq!(entries.len(), 0);

        // Add some entries
        memtable.insert("key1".to_string(), vec![1, 2, 3]).unwrap();
        memtable.insert("key2".to_string(), vec![4, 5, 6]).unwrap();
        memtable.insert("key3".to_string(), vec![7, 8, 9]).unwrap();

        // Test iterator with entries
        let entries = memtable.iter().unwrap();
        assert_eq!(entries.len(), 3);

        // Verify entries are in correct order (keys should be sorted)
        assert_eq!(entries[0].0, "key1");
        assert_eq!(entries[0].1, vec![1, 2, 3]);
        assert_eq!(entries[1].0, "key2");
        assert_eq!(entries[1].1, vec![4, 5, 6]);
        assert_eq!(entries[2].0, "key3");
        assert_eq!(entries[2].1, vec![7, 8, 9]);

        // Remove an entry
        memtable.remove(&"key2".to_string()).unwrap();

        // Test iterator after removal
        let entries = memtable.iter().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "key1");
        assert_eq!(entries[1].0, "key3");
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_range() {
    let test_future = async {
        let memtable = StringMemtable::new(1024);

        // Insert ordered keys
        memtable.insert("a".to_string(), vec![1]).unwrap();
        memtable.insert("b".to_string(), vec![2]).unwrap();
        memtable.insert("c".to_string(), vec![3]).unwrap();
        memtable.insert("d".to_string(), vec![4]).unwrap();
        memtable.insert("e".to_string(), vec![5]).unwrap();

        // Test complete range
        let range_results = memtable.range("a".to_string().."z".to_string()).unwrap();
        assert_eq!(range_results.len(), 5);

        // Test partial range
        let range_results = memtable.range("b".to_string().."e".to_string()).unwrap();
        assert_eq!(range_results.len(), 3);
        assert_eq!(range_results[0].0, "b");
        assert_eq!(range_results[1].0, "c");
        assert_eq!(range_results[2].0, "d");

        // Test range with non-existent start
        let range_results = memtable.range("0".to_string().."c".to_string()).unwrap();
        assert_eq!(range_results.len(), 2); // a, b

        // Test range with non-existent end
        let range_results = memtable.range("c".to_string().."z".to_string()).unwrap();
        assert_eq!(range_results.len(), 3); // c, d, e

        // Test empty range
        let range_results = memtable.range("f".to_string().."z".to_string()).unwrap();
        assert_eq!(range_results.len(), 0);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_len_and_is_empty() {
    let test_future = async {
        let memtable = StringMemtable::new(1024);

        // Initially empty
        assert_eq!(memtable.len().unwrap(), 0);
        assert!(memtable.is_empty().unwrap());

        // Add an entry
        memtable.insert("key1".to_string(), vec![1, 2, 3]).unwrap();
        assert_eq!(memtable.len().unwrap(), 1);
        assert!(!memtable.is_empty().unwrap());

        // Add more entries
        memtable.insert("key2".to_string(), vec![4, 5, 6]).unwrap();
        memtable.insert("key3".to_string(), vec![7, 8, 9]).unwrap();
        assert_eq!(memtable.len().unwrap(), 3);

        // Remove an entry
        memtable.remove(&"key2".to_string()).unwrap();
        assert_eq!(memtable.len().unwrap(), 2);

        // Clear all entries
        memtable.clear().unwrap();
        assert_eq!(memtable.len().unwrap(), 0);
        assert!(memtable.is_empty().unwrap());
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_size_bytes() {
    let test_future = async {
        let memtable = StringMemtable::new(1024);

        // Initial size should be very small
        let initial_size = memtable.size_bytes().unwrap();

        // Add some data
        memtable
            .insert("key1".to_string(), vec![1, 2, 3, 4, 5])
            .unwrap();

        // Size should increase
        let size_after_insert = memtable.size_bytes().unwrap();
        assert!(size_after_insert > initial_size);

        // Add more data
        memtable
            .insert("key2".to_string(), vec![6, 7, 8, 9, 10])
            .unwrap();
        memtable
            .insert("key3".to_string(), vec![11, 12, 13, 14, 15])
            .unwrap();

        // Size should increase further
        let size_after_more_inserts = memtable.size_bytes().unwrap();
        assert!(size_after_more_inserts > size_after_insert);

        // Remove data
        memtable.remove(&"key2".to_string()).unwrap();

        // Size should decrease
        let size_after_remove = memtable.size_bytes().unwrap();
        assert!(size_after_remove < size_after_more_inserts);

        // Clear all data
        memtable.clear().unwrap();

        // Size should be close to initial
        let size_after_clear = memtable.size_bytes().unwrap();
        assert!(size_after_clear <= initial_size + 10); // Allow small overhead
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
