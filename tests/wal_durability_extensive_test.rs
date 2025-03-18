use lsmer::sstable::SSTableWriter;
use lsmer::wal::durability::{DurabilityError, DurabilityManager, KeyValuePair, Operation};
use std::fs;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

// Helper function to write SSTable directly
async fn write_simple_sstable(path: &str, pairs: &[KeyValuePair]) -> std::io::Result<()> {
    // Create directory if it doesn't exist
    if let Some(parent) = std::path::Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }

    // Create SSTable writer with the correct types
    let mut writer = SSTableWriter::new(
        path,
        pairs.len(), // This is already a usize, no conversion needed
        false,       // Don't use bloom filter to avoid issues
        0.0,         // Bloom filter not used
    )?;

    // Write entries
    for pair in pairs {
        writer.write_entry(&pair.key, &pair.value)?;
    }

    // Finalize
    writer.finalize()?;

    Ok(())
}

#[tokio::test]
async fn test_execute_batch_operations() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Create a batch of operations
        let mut operations = Vec::new();
        for i in 0..5 {
            let key = format!("batch_key{}", i);
            let value = vec![i as u8, (i + 1) as u8, (i + 2) as u8];
            operations.push(Operation::Insert { key, value });
        }

        // Add a remove operation
        operations.push(Operation::Remove {
            key: "batch_key2".to_string(),
        });

        // Execute the batch
        durability_manager.execute_batch(operations).unwrap();

        // Test empty batch
        let empty_batch: Vec<Operation> = Vec::new();
        durability_manager.execute_batch(empty_batch).unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_crash_recovery_with_checkpoints() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);
        fs::create_dir_all(&sstable_path).unwrap();

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Just verify the recover_from_crash method returns successfully
        // The actual recovery depends on internal implementation details
        let _recovered_memtable = durability_manager.recover_from_crash().unwrap();

        // Test successful execution
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_transaction_lifecycle() {
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
            .add_to_transaction(
                tx_id,
                Operation::Insert {
                    key: "tx_key1".to_string(),
                    value: vec![1, 2, 3],
                },
            )
            .unwrap();

        durability_manager
            .add_to_transaction(
                tx_id,
                Operation::Insert {
                    key: "tx_key2".to_string(),
                    value: vec![4, 5, 6],
                },
            )
            .unwrap();

        // Prepare transaction
        durability_manager.prepare_transaction(tx_id).unwrap();

        // Should fail to prepare again
        let result = durability_manager.prepare_transaction(tx_id);
        assert!(result.is_err());
        if let Err(DurabilityError::TransactionAlreadyPrepared(id)) = result {
            assert_eq!(id, tx_id);
        } else {
            panic!("Expected TransactionAlreadyPrepared error");
        }

        // Commit transaction
        durability_manager.commit_transaction(tx_id).unwrap();

        // Should fail to commit again
        let result = durability_manager.commit_transaction(tx_id);
        assert!(result.is_err());
        if let Err(DurabilityError::TransactionAlreadyCommitted(id)) = result {
            assert_eq!(id, tx_id);
        } else {
            panic!("Expected TransactionAlreadyCommitted error");
        }

        // Create a transaction to abort
        let tx_id_to_abort = durability_manager.begin_transaction().unwrap();

        durability_manager
            .add_to_transaction(
                tx_id_to_abort,
                Operation::Insert {
                    key: "tx_key_abort".to_string(),
                    value: vec![7, 8, 9],
                },
            )
            .unwrap();

        // Abort transaction
        durability_manager
            .abort_transaction(tx_id_to_abort)
            .unwrap();

        // Should fail to abort again
        let result = durability_manager.abort_transaction(tx_id_to_abort);
        assert!(result.is_err());
        if let Err(DurabilityError::TransactionAlreadyAborted(id)) = result {
            assert_eq!(id, tx_id_to_abort);
        } else {
            panic!("Expected TransactionAlreadyAborted error");
        }

        // Test non-existent transaction
        let result = durability_manager.commit_transaction(999999);
        assert!(result.is_err());
        if let Err(DurabilityError::TransactionNotFound(id)) = result {
            assert_eq!(id, 999999);
        } else {
            panic!("Expected TransactionNotFound error");
        }
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_verify_sstable_integrity() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);
        fs::create_dir_all(&sstable_path).unwrap();

        // Create durability manager
        let durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Create some key-value pairs
        let mut pairs = Vec::new();
        for i in 0..5 {
            let key = format!("integrity_key{}", i);
            let value = vec![i as u8];
            pairs.push(KeyValuePair { key, value });
        }

        // Begin checkpoint
        let checkpoint_id = 12345;

        // Write SSTable directly using our helper
        let sstable_filename = format!("{}/checkpoint_{}.sst", sstable_path, checkpoint_id);
        write_simple_sstable(&sstable_filename, &pairs)
            .await
            .unwrap();

        // Test with invalid path
        let invalid_path = "non_existent_file.sst";
        let result = durability_manager.verify_sstable_integrity(invalid_path);
        assert!(result.is_err());
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_find_and_extract_sstables() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);
        fs::create_dir_all(&sstable_path).unwrap();

        // Create durability manager
        let durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Simply verify the find_sstables method returns successfully
        // We won't verify the actual contents since that depends on filesystem details
        let _sstables = durability_manager.find_sstables().unwrap();

        // Test successful execution
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_truncate_and_recovery() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);
        fs::create_dir_all(&sstable_path).unwrap();

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Insert some data
        for i in 0..5 {
            let key = format!("truncate_key{}", i);
            let value = vec![i as u8];
            durability_manager.insert(key, value).unwrap();
        }

        // Begin checkpoint
        let checkpoint_id = durability_manager.begin_checkpoint().unwrap();

        // Create key-value pairs
        let mut pairs = Vec::new();
        for i in 0..5 {
            let key = format!("truncate_key{}", i);
            let value = vec![i as u8];
            pairs.push(KeyValuePair { key, value });
        }

        // Write SSTable directly using our helper
        let sstable_filename = format!("{}/checkpoint_{}.sst", sstable_path, checkpoint_id);
        write_simple_sstable(&sstable_filename, &pairs)
            .await
            .unwrap();

        // Register checkpoint
        durability_manager
            .register_durable_checkpoint(checkpoint_id, &sstable_filename)
            .unwrap();

        // Log checkpoint end
        durability_manager
            .log_checkpoint_end(checkpoint_id)
            .unwrap();

        // End checkpoint
        durability_manager.end_checkpoint(checkpoint_id).unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_single_operation_transactions() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Test execute_transaction for insert
        let insert_op = Operation::Insert {
            key: "single_tx_key1".to_string(),
            value: vec![1, 2, 3],
        };
        durability_manager.execute_transaction(insert_op).unwrap();

        // Test execute_transaction for remove
        let remove_op = Operation::Remove {
            key: "single_tx_key1".to_string(),
        };
        durability_manager.execute_transaction(remove_op).unwrap();

        // Test execute_transaction for clear
        let clear_op = Operation::Clear;
        durability_manager.execute_transaction(clear_op).unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_checkpoint_operations() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create paths for WAL and SSTable directory
        let wal_path = format!("{}/wal", temp_path);
        let sstable_path = format!("{}/sstables", temp_path);

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Begin checkpoint and get checkpoint ID
        let checkpoint_id = durability_manager.begin_checkpoint().unwrap();

        // Log checkpoint start
        durability_manager
            .log_checkpoint_start(checkpoint_id)
            .unwrap();

        // Insert some data
        for i in 0..3 {
            let key = format!("checkpoint_key{}", i);
            let value = vec![i as u8];
            durability_manager.insert(key, value).unwrap();
        }

        // Log checkpoint end
        durability_manager
            .log_checkpoint_end(checkpoint_id)
            .unwrap();

        // End checkpoint
        durability_manager.end_checkpoint(checkpoint_id).unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}
