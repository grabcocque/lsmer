use lsmer::wal::durability::{DurabilityError, DurabilityManager};
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_durability_manager_create() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Verify WAL file was created
        let wal_log_file = Path::new(&wal_path);
        assert!(wal_log_file.exists());

        // Test basic operation
        durability_manager
            .insert("test_key".to_string(), vec![1, 2, 3])
            .unwrap();

        // Test transaction operations
        let tx_id = durability_manager.begin_transaction().unwrap();
        durability_manager.commit_transaction(tx_id).unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_transaction_operations() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Begin transaction
        let tx_id = durability_manager.begin_transaction().unwrap();

        // Add operations to transaction
        durability_manager
            .insert("key1".to_string(), vec![1, 2, 3])
            .unwrap();
        durability_manager
            .insert("key2".to_string(), vec![4, 5, 6])
            .unwrap();

        // Prepare transaction
        durability_manager.prepare_transaction(tx_id).unwrap();

        // Commit transaction
        durability_manager.commit_transaction(tx_id).unwrap();

        // Begin another transaction that will be aborted
        let tx_id2 = durability_manager.begin_transaction().unwrap();
        durability_manager
            .insert("key_to_abort".to_string(), vec![7, 8, 9])
            .unwrap();
        durability_manager.abort_transaction(tx_id2).unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_batch_operations() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Insert multiple key-value pairs
        for i in 0..5 {
            let key = format!("batch_key{}", i);
            let value = vec![i as u8];
            durability_manager.insert(key, value).unwrap();
        }

        // Remove some keys
        durability_manager.remove("batch_key1").unwrap();
        durability_manager.remove("batch_key3").unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_durability_error_handling() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Test with invalid path
        let invalid_path = Path::new(&temp_path).join("nonexistent");
        let nonexistent_path = invalid_path.to_str().unwrap();
        let result = DurabilityManager::new(nonexistent_path, nonexistent_path);
        assert!(result.is_err());

        // Test error debug output instead of display
        let error = DurabilityError::TransactionNotFound(12345);
        let error_string = format!("{:?}", error);
        assert!(error_string.contains("TransactionNotFound"));

        // Test transaction errors
        let tx_id = durability_manager.begin_transaction().unwrap();

        // Abort the transaction
        durability_manager.abort_transaction(tx_id).unwrap();

        // Trying to commit after abort should fail
        let result = durability_manager.commit_transaction(tx_id);
        assert!(result.is_err());

        // Trying to use non-existent transaction should fail
        let result = durability_manager.abort_transaction(999999);
        assert!(result.is_err());
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_clear_operation() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Insert some data
        durability_manager
            .insert("key1".to_string(), vec![1, 2, 3])
            .unwrap();
        durability_manager
            .insert("key2".to_string(), vec![4, 5, 6])
            .unwrap();

        // Clear all data
        durability_manager.clear().unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}
