use lsmer::memtable::{ByteSize, Memtable, MemtableError, StringMemtable, ToBytes};
use std::io;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_traits_simple_types() {
    let test_future = async {
        // Test u8 ByteSize
        let byte: u8 = 42;
        assert_eq!(byte.byte_size(), 1);

        // Test Vec<u8> ToBytes
        let bytes = vec![1, 2, 3, 4, 5];
        let serialized = bytes.to_bytes();
        assert_eq!(serialized, bytes);

        let deserialized = Vec::<u8>::from_bytes(&serialized).unwrap();
        assert_eq!(deserialized, bytes);
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_string_bytes_conversion() {
    let test_future = async {
        // Test valid string conversion
        let original = "Test string".to_string();
        let bytes = original.to_bytes();
        let reconstructed = String::from_bytes(&bytes).unwrap();
        assert_eq!(original, reconstructed);

        // Test invalid UTF-8 handling
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let result = String::from_bytes(&invalid_utf8);
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        }
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_memtable_capacity_errors() {
    let test_future = async {
        // Create a tiny memtable that will quickly exceed capacity
        let memtable = StringMemtable::new(10);

        // Insert data
        let first_result = memtable.insert("key1".to_string(), vec![1, 2, 3]);

        // Check if first insert succeeds or fails
        // Both outcomes are acceptable - some implementations have overhead,
        // so even the first small insert might fail on a tiny memtable
        if first_result.is_err() {
            // If first insert fails, test that it's a capacity error
            match first_result.unwrap_err() {
                MemtableError::CapacityExceeded => {
                    // Test passed - we got the expected error type
                }
                e => panic!("Expected CapacityExceeded error, got {:?}", e),
            }
        } else {
            // First insert succeeded, now try a second one that should definitely fail
            let result = memtable.insert("key2".to_string(), vec![4, 5, 6, 7, 8, 9, 10]);
            assert!(result.is_err());

            // Check if it's a capacity exceeded error
            match result {
                Err(MemtableError::CapacityExceeded) => {
                    // Test passed - we got the expected error type
                }
                Err(e) => panic!("Expected CapacityExceeded error, got {:?}", e),
                Ok(_) => panic!("Expected error, got Ok"),
            }
        }
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_key_not_found_error() {
    let test_future = async {
        let memtable = StringMemtable::new(1024);

        // Try to get a non-existent key
        let result = memtable.get(&"nonexistent".to_string());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Try to remove a non-existent key
        let result = memtable.remove(&"nonexistent".to_string());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}
