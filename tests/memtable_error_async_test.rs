use lsmer::memtable::{Memtable, MemtableError, StringMemtable};
use lsmer::wal::WalError;
use std::io;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_memtable_error_display() {
    let test_future = async {
        // Test the Display implementation for MemtableError
        let errors = vec![
            MemtableError::CapacityExceeded,
            MemtableError::KeyNotFound,
            MemtableError::WalError(io::Error::new(io::ErrorKind::Other, "WAL error")),
            MemtableError::IoError(io::Error::new(io::ErrorKind::NotFound, "IO error")),
            MemtableError::LockError,
        ];

        // Verify each error has a sensible display representation
        for error in errors {
            let display_str = format!("{}", error);
            assert!(!display_str.is_empty(), "Error display should not be empty");

            // Verify specific error messages
            match error {
                MemtableError::CapacityExceeded => {
                    assert_eq!(display_str, "Memtable capacity exceeded");
                }
                MemtableError::KeyNotFound => {
                    assert_eq!(display_str, "Key not found in memtable");
                }
                MemtableError::WalError(_) => {
                    assert!(display_str.contains("WAL error"));
                }
                MemtableError::IoError(_) => {
                    assert!(display_str.contains("I/O error"));
                }
                MemtableError::LockError => {
                    assert_eq!(display_str, "Failed to acquire lock");
                }
            }
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_error_conversions() {
    let test_future = async {
        // Test From<WalError> for MemtableError
        let wal_io_error = WalError::IoError(io::Error::new(io::ErrorKind::Other, "WAL IO error"));
        let wal_invalid_record = WalError::InvalidRecord;
        let wal_checkpoint_not_found = WalError::CheckpointNotFound;

        let memtable_error_from_wal_io = MemtableError::from(wal_io_error);
        let memtable_error_from_invalid = MemtableError::from(wal_invalid_record);
        let memtable_error_from_checkpoint = MemtableError::from(wal_checkpoint_not_found);

        if let MemtableError::WalError(e) = memtable_error_from_wal_io {
            assert_eq!(e.kind(), io::ErrorKind::Other);
        } else {
            panic!("Expected WalError variant");
        }

        if let MemtableError::WalError(_) = memtable_error_from_invalid {
            // Converted to IoError with Other kind
        } else {
            panic!("Expected WalError variant");
        }

        if let MemtableError::WalError(_) = memtable_error_from_checkpoint {
            // Converted to IoError with Other kind
        } else {
            panic!("Expected WalError variant");
        }

        // Test From<io::Error> for MemtableError
        let io_error = io::Error::new(io::ErrorKind::NotFound, "IO error message");
        let memtable_error_from_io = MemtableError::from(io_error);

        if let MemtableError::IoError(e) = memtable_error_from_io {
            assert_eq!(e.kind(), io::ErrorKind::NotFound);
        } else {
            panic!("Expected IoError variant");
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_capacity_exceeded_error() {
    let test_future = async {
        // Create a small memtable
        let memtable = StringMemtable::new(10); // Very small capacity

        // Insert data until capacity is exceeded
        let key = "test_key".to_string();
        let value = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // Large enough to exceed capacity

        // This should fail with CapacityExceeded
        let result = memtable.insert(key, value);

        assert!(result.is_err());
        match result {
            Err(MemtableError::CapacityExceeded) => {
                // Expected error
            }
            _ => panic!("Expected CapacityExceeded error"),
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_key_not_found_error() {
    let test_future = async {
        // This test cannot be easily created with the current API
        // since the Memtable::get() returns Option<Vec<u8>> for missing keys,
        // not a KeyNotFound error. However, we can test internal methods
        // that might return this error.

        // Create a memtable
        let memtable = StringMemtable::new(1024);

        // Try to remove a non-existent key
        let result = memtable.remove(&"nonexistent".to_string());

        // This should succeed with None, not fail with KeyNotFound
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Note: Since KeyNotFound is not currently used in the public API,
        // we've verified the error exists and can be displayed, but cannot
        // trigger it without modifying the implementation.
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
