use lsmer::memtable::AsyncStringMemtable;
use lsmer::memtable::{ByteSize, Memtable, StringMemtable, ToBytes};
use std::io;
use std::time::Duration;
use tokio::time::timeout;

// A simple struct to test ByteSize and ToBytes traits
struct TestValue {
    data: Vec<u8>,
}

impl ByteSize for TestValue {
    fn byte_size(&self) -> usize {
        self.data.len()
    }
}

impl ToBytes for TestValue {
    fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }

    fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        Ok(TestValue {
            data: bytes.to_vec(),
        })
    }
}

#[tokio::test]
async fn test_memtable_value_traits() {
    let test_future = async {
        // Test the ByteSize and ToBytes trait implementations
        let original_data = vec![1, 2, 3, 4, 5];
        let value = TestValue {
            data: original_data.clone(),
        };

        // Test byte_size method (ByteSize trait)
        assert_eq!(value.byte_size(), 5);

        // Test to_bytes method (ToBytes trait)
        let bytes = value.to_bytes();
        assert_eq!(bytes, original_data);

        // Test from_bytes method (ToBytes trait)
        let reconstructed = TestValue::from_bytes(&bytes).unwrap();
        assert_eq!(reconstructed.data, original_data);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

// Simple test to ensure the Memtable trait methods work as expected
#[tokio::test]
async fn test_memtable_trait_with_string_memtable() {
    let test_future = async {
        let memtable = StringMemtable::new(1024);

        // Test insert and get
        let key = "test_key".to_string();
        let value = vec![1, 2, 3, 4, 5];

        // Insert value
        memtable.insert(key.clone(), value.clone()).unwrap();

        // Get value
        let retrieved = memtable.get(&key).unwrap();
        assert_eq!(retrieved, Some(value.clone()));

        // Test range method
        let start_key = "test_j".to_string();
        let end_key = "test_l".to_string();
        let range_results = memtable.range(start_key..end_key).unwrap();
        assert_eq!(range_results.len(), 1);
        assert_eq!(range_results[0].0, key);
        assert_eq!(range_results[0].1, value);

        // Test remove
        memtable.remove(&key).unwrap();
        let after_remove = memtable.get(&key).unwrap();
        assert_eq!(after_remove, None);

        // Test len and is_empty
        assert_eq!(memtable.len().unwrap(), 0);
        assert!(memtable.is_empty().unwrap());

        // Test size_bytes
        let size = memtable.size_bytes().unwrap();
        // Just print the size for debugging, as the exact value isn't important
        println!("Size bytes after removal: {}", size);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

// Test AsyncStringMemtable
#[tokio::test]
async fn test_async_memtable() {
    let test_future = async {
        // Create an AsyncStringMemtable with required parameters
        let memtable = AsyncStringMemtable::new(
            1024,                 // max_size_bytes
            "./test_data".into(), // base_path
            60,                   // compaction_interval_secs
        )
        .await
        .unwrap();

        // Test insert and get
        let key = "async_key".to_string();
        let value = vec![5, 4, 3, 2, 1];

        // Insert asynchronously
        memtable.insert(key.clone(), value.clone()).await.unwrap();

        // Get asynchronously
        let retrieved = memtable.get(&key).await.unwrap();
        assert_eq!(retrieved, Some(value.clone()));

        // Test remove
        memtable.remove(&key).await.unwrap();
        let after_remove = memtable.get(&key).await.unwrap();
        assert_eq!(after_remove, None);

        // Test len and is_empty
        assert_eq!(memtable.len().await.unwrap(), 0);
        assert!(memtable.is_empty().await.unwrap());

        // Test clear
        memtable.insert("key1".to_string(), vec![1]).await.unwrap();
        memtable.insert("key2".to_string(), vec![2]).await.unwrap();
        assert_eq!(memtable.len().await.unwrap(), 2);

        memtable.clear().await.unwrap();
        assert_eq!(memtable.len().await.unwrap(), 0);

        // Shutdown the memtable
        memtable.shutdown().await.unwrap();
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
