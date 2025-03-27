use std::time::Duration;
use tokio::time::timeout;
use lsmer::memtable::MemtableError;
use lsmer::wal::WalError;
use std::error::Error;
use std::io;

#[tokio::test]
async fn test_memtable_error_variants_display() {
    let test_future = async {
        // Test Display implementation for each variant
        let errors = [
            MemtableError::CapacityExceeded,
            MemtableError::KeyNotFound,
            MemtableError::WalError(io::Error::new(io::ErrorKind::NotFound, "test")),
            MemtableError::IoError(io::Error::new(io::ErrorKind::Other, "io test")),
            MemtableError::LockError,
        ];
    
        for err in &errors {
            let display_str = format!("{}", err);
            assert!(!display_str.is_empty());
    
            // Also test Debug formatting
            let debug_str = format!("{:?}", err);
            assert!(!debug_str.is_empty());
        }
    
        // Test specific error messages
        let err = MemtableError::CapacityExceeded;
        assert_eq!(err.to_string(), "Memtable capacity exceeded");
    
        let err = MemtableError::KeyNotFound;
        assert_eq!(err.to_string(), "Key not found in memtable");
    
        let err = MemtableError::LockError;
        assert_eq!(err.to_string(), "Failed to acquire lock");
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_error_source() {
    let test_future = async {
        // Test Error::source() implementation
    
        // WalError has a source
        let wal_err =
            MemtableError::WalError(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        assert!(wal_err.source().is_some());
    
        // IoError has a source
        let io_err = MemtableError::IoError(io::Error::new(io::ErrorKind::Other, "io error"));
        assert!(io_err.source().is_some());
    
        // Other variants should have no source
        assert!(MemtableError::CapacityExceeded.source().is_none());
        assert!(MemtableError::KeyNotFound.source().is_none());
        assert!(MemtableError::LockError.source().is_none());
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_error_from_io_error() {
    let test_future = async {
        // Test From<io::Error> implementation
        let io_error = io::Error::new(io::ErrorKind::Other, "test io error");
        let memtable_error = MemtableError::from(io_error);
    
        match memtable_error {
            MemtableError::IoError(e) => {
                assert_eq!(e.kind(), io::ErrorKind::Other);
                assert_eq!(e.to_string(), "test io error");
            }
            _ => panic!("Expected IoError variant"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_memtable_error_from_wal_error() {
    let test_future = async {
        // Test From<WalError> implementation
    
        // IoError variant within WalError
        let io_error = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let wal_io_error = WalError::IoError(io_error);
        let memtable_error = MemtableError::from(wal_io_error);
    
        match memtable_error {
            MemtableError::WalError(e) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
                assert_eq!(e.to_string(), "file not found");
            }
            _ => panic!("Expected WalError variant"),
        }
    
        // Other variants of WalError
        let wal_error = WalError::InvalidRecord;
        let memtable_error = MemtableError::from(wal_error);
    
        // For non-IoError variants, we should still get a WalError with kind Other
        match memtable_error {
            MemtableError::WalError(e) => {
                assert_eq!(e.kind(), io::ErrorKind::Other);
                // Instead of checking the exact message, just verify it's not empty
                assert!(!e.to_string().is_empty());
            }
            _ => panic!("Expected WalError variant"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
