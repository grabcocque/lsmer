use lsmer::wal::durability::{DurabilityError, DurabilityManager};
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_wal_basic_operations() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Test basic operations
        durability_manager
            .insert("key1".to_string(), vec![1, 2, 3])
            .unwrap();
        durability_manager
            .insert("key2".to_string(), vec![4, 5, 6])
            .unwrap();

        // Verify WAL directory exists
        let wal_dir = Path::new(&wal_path);
        assert!(wal_dir.exists());

        // Test transaction operations
        let tx_id = durability_manager.begin_transaction().unwrap();
        durability_manager.prepare_transaction(tx_id).unwrap();
        durability_manager.commit_transaction(tx_id).unwrap();

        // Test error handling
        let result = durability_manager.commit_transaction(999999);
        assert!(matches!(
            result,
            Err(DurabilityError::TransactionNotFound(_))
        ));
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
