use lsmer::wal::{RecordType, WalError, WalRecord, WriteAheadLog};
use std::io;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_wal_error_conversions() {
    let test_future = async {
        let test_future = async {
            // Test IoError conversion
            let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
            let wal_err = WalError::from(io_err);

            match wal_err {
                WalError::IoError(_) => (), // Expected
                _ => panic!("Expected IoError variant"),
            }

            // Test converting WalError to String
            let wal_err = WalError::InvalidRecord;
            let err_string = format!("{}", wal_err);
            assert!(!err_string.is_empty());
        };

        match timeout(Duration::from_secs(5), test_future).await {
            Ok(_) => (),
            Err(_) => panic!("Test timed out"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_record_type_conversions() {
    let test_future = async {
        let test_future = async {
            // Test RecordType from_u8
            assert_eq!(RecordType::from_u8(1), RecordType::Insert);
            assert_eq!(RecordType::from_u8(2), RecordType::Remove);
            assert_eq!(RecordType::from_u8(3), RecordType::Clear);
            assert_eq!(RecordType::from_u8(4), RecordType::CheckpointStart);
            assert_eq!(RecordType::from_u8(5), RecordType::CheckpointEnd);
            assert_eq!(RecordType::from_u8(6), RecordType::TransactionBegin);
            assert_eq!(RecordType::from_u8(7), RecordType::TransactionPrepare);
            assert_eq!(RecordType::from_u8(8), RecordType::TransactionCommit);
            assert_eq!(RecordType::from_u8(9), RecordType::TransactionAbort);
            assert_eq!(RecordType::from_u8(0), RecordType::Unknown);
            assert_eq!(RecordType::from_u8(255), RecordType::Unknown);
        };

        match timeout(Duration::from_secs(5), test_future).await {
            Ok(_) => (),
            Err(_) => panic!("Test timed out"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_wal_record_operations() {
    let test_future = async {
        let test_future = async {
            // Create a new record
            let record_data = vec![1, 2, 3, 4];
            let record = WalRecord::new(RecordType::Insert, record_data.clone());

            // Verify record properties
            assert_eq!(record.record_type, RecordType::Insert);
            assert_eq!(record.data, record_data);
            assert_eq!(record.transaction_id, 0);

            // Test serialization and deserialization
            let serialized = record.serialize().unwrap();
            let deserialized = WalRecord::deserialize(&serialized).unwrap();

            // Verify deserialization worked correctly
            assert_eq!(deserialized.record_type, record.record_type);
            assert_eq!(deserialized.data, record.data);
        };

        match timeout(Duration::from_secs(5), test_future).await {
            Ok(_) => (),
            Err(_) => panic!("Test timed out"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_write_ahead_log_basic() {
    let test_future = async {
        let test_future = async {
            // Create a temporary directory
            let temp_dir = tempdir().unwrap();
            let wal_path = temp_dir.path().join("test_wal.log");
            let wal_path_str = wal_path.to_str().unwrap();

            // Create WAL
            let mut wal = WriteAheadLog::new(wal_path_str).unwrap();

            // Test write one record
            let record = WalRecord::new(RecordType::Insert, vec![1, 2, 3]);
            wal.append_and_sync(record).unwrap();

            // Simple test path accessor
            assert_eq!(wal.path, wal_path_str);
        };

        match timeout(Duration::from_secs(5), test_future).await {
            Ok(_) => (),
            Err(_) => panic!("Test timed out"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_wal_error_display() {
    let test_future = async {
        let test_future = async {
            // Test display implementation for WalError variants
            let variants = vec![
                WalError::IoError(io::Error::new(io::ErrorKind::NotFound, "file not found")),
                WalError::InvalidRecord,
                WalError::CheckpointNotFound,
            ];

            for err in variants {
                let display_str = format!("{}", err);
                assert!(!display_str.is_empty());

                // Debug formatting
                let debug_str = format!("{:?}", err);
                assert!(!debug_str.is_empty());
            }
        };

        match timeout(Duration::from_secs(5), test_future).await {
            Ok(_) => (),
            Err(_) => panic!("Test timed out"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
