use lsmer::bptree::{IndexError, StorageReference};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_storage_reference_creation_and_equality() {
    let test_future = async {
        // Create a storage reference
        let sr1 = StorageReference {
            file_path: "test_file.db".to_string(),
            offset: 1024,
            is_tombstone: false,
        };

        // Create an identical storage reference
        let sr2 = StorageReference {
            file_path: "test_file.db".to_string(),
            offset: 1024,
            is_tombstone: false,
        };

        // Create a different storage reference
        let sr3 = StorageReference {
            file_path: "test_file.db".to_string(),
            offset: 2048,
            is_tombstone: false,
        };

        // Test equality
        assert_eq!(sr1, sr2);
        assert_ne!(sr1, sr3);

        // Test clone
        let sr4 = sr1.clone();
        assert_eq!(sr1, sr4);

        // Test debug trait
        let debug_str = format!("{:?}", sr1);
        assert!(debug_str.contains("file_path"));
        assert!(debug_str.contains("offset"));
        assert!(debug_str.contains("is_tombstone"));
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_index_error_debug() {
    let test_future = async {
        // Test KeyNotFound debug representation
        let error = IndexError::KeyNotFound;
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("KeyNotFound"));

        // Test InvalidOperation debug representation
        let error = IndexError::InvalidOperation;
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("InvalidOperation"));
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_index_key_value_debug_and_clone() {
    let test_future = async {
        use lsmer::bptree::IndexKeyValue;

        // Create with no storage reference or value
        let ikv1: IndexKeyValue<String, Vec<u8>> = IndexKeyValue {
            key: "test_key".to_string(),
            value: None,
            storage_ref: None,
        };

        // Test debug
        let debug_str = format!("{:?}", ikv1);
        assert!(debug_str.contains("test_key"));
        assert!(debug_str.contains("None"));

        // Create with value and storage reference
        let ikv2: IndexKeyValue<String, Vec<u8>> = IndexKeyValue {
            key: "test_key".to_string(),
            value: Some(vec![1, 2, 3]),
            storage_ref: Some(StorageReference {
                file_path: "test_file.db".to_string(),
                offset: 1024,
                is_tombstone: false,
            }),
        };

        // Test debug with values
        let debug_str = format!("{:?}", ikv2);
        assert!(debug_str.contains("test_key"));
        assert!(debug_str.contains("Some"));
        assert!(debug_str.contains("storage_ref"));

        // Test clone
        let ikv3 = ikv2.clone();
        assert_eq!(ikv3.key, ikv2.key);
        assert_eq!(ikv3.value, ikv2.value);
        assert_eq!(ikv3.storage_ref, ikv2.storage_ref);
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
