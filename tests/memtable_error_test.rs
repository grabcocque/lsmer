use lsmer::memtable::{Memtable, MemtableError, StringMemtable};
use std::error::Error;
use std::io;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_memtable_error_display() {
    let test_future = async {
        // Test CapacityExceeded error display
        let capacity_error = MemtableError::CapacityExceeded;
        assert_eq!(capacity_error.to_string(), "Memtable capacity exceeded");

        // Test KeyNotFound error display
        let key_error = MemtableError::KeyNotFound;
        assert_eq!(key_error.to_string(), "Key not found in memtable");

        // Test WalError display
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let wal_error = MemtableError::WalError(io_err);
        assert!(wal_error.to_string().contains("WAL error: file not found"));

        // Test IoError display
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
        let io_error = MemtableError::IoError(io_err);
        assert!(
            io_error
                .to_string()
                .contains("I/O error: permission denied")
        );

        // Test LockError display
        let lock_error = MemtableError::LockError;
        assert_eq!(lock_error.to_string(), "Failed to acquire lock");
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_error_source() {
    let test_future = async {
        // Test that IoError has a source
        let io_err = io::Error::new(io::ErrorKind::Other, "test error");
        let error = MemtableError::IoError(io_err);
        assert!(error.source().is_some());

        // Test that WalError has a source
        let io_err = io::Error::new(io::ErrorKind::Other, "test error");
        let error = MemtableError::WalError(io_err);
        assert!(error.source().is_some());

        // Test that other errors don't have a source
        assert!(MemtableError::CapacityExceeded.source().is_none());
        assert!(MemtableError::KeyNotFound.source().is_none());
        assert!(MemtableError::LockError.source().is_none());
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_error_from_io_error() {
    let test_future = async {
        // Create an IO error
        let io_err = io::Error::new(io::ErrorKind::NotFound, "test error");

        // Convert to MemtableError
        let memtable_err: MemtableError = io_err.into();

        // Check that it's the correct variant
        match memtable_err {
            MemtableError::IoError(e) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
                assert_eq!(e.to_string(), "test error");
            }
            _ => panic!("Expected IoError variant"),
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_capacity_exceeded() {
    let test_future = async {
        // Create a memtable with a very small capacity
        let memtable = StringMemtable::new(10);

        // Insert a value that will exceed the capacity
        let result = memtable.insert("key".to_string(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

        // Should return a CapacityExceeded error
        assert!(result.is_err());

        match result {
            Err(MemtableError::CapacityExceeded) => (),
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
async fn test_memtable_key_not_found() {
    let test_future = async {
        let memtable = StringMemtable::new(1024);

        // Try to get a key that doesn't exist
        let key = "nonexistent".to_string();
        let result = memtable.get(&key).unwrap();

        // Should return None, not an error
        assert!(result.is_none());

        // Try to remove a key that doesn't exist
        let result = memtable.remove(&key);

        // Should be Ok(None), not an error
        assert!(result.is_ok());
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
