#[cfg(test)]
mod tests {
    use lsmer::wal::durability::{DurabilityManager, Operation};
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_transaction() {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir
            .path()
            .join("test_wal.log")
            .to_str()
            .unwrap()
            .to_string();
        let sstable_dir = temp_dir
            .path()
            .join("sstables")
            .to_str()
            .unwrap()
            .to_string();

        // Create directories
        fs::create_dir_all(&sstable_dir).unwrap();

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_dir).unwrap();

        // Begin transaction
        let tx_id = durability_manager.begin_transaction().unwrap();

        // Add operations to transaction
        durability_manager
            .add_to_transaction(
                tx_id,
                Operation::Insert {
                    key: "key1".to_string(),
                    value: "value1".as_bytes().to_vec(),
                },
            )
            .unwrap();

        durability_manager
            .add_to_transaction(
                tx_id,
                Operation::Insert {
                    key: "key2".to_string(),
                    value: "value2".as_bytes().to_vec(),
                },
            )
            .unwrap();

        // Commit transaction
        durability_manager.commit_transaction(tx_id).unwrap();

        // Apply operations from durability manager
        durability_manager
            .insert("key3".to_string(), "value3".as_bytes().to_vec())
            .unwrap();
    }

    #[tokio::test]
    async fn test_transaction_batch() {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir
            .path()
            .join("test_batch_wal.log")
            .to_str()
            .unwrap()
            .to_string();
        let sstable_dir = temp_dir
            .path()
            .join("batch_sstables")
            .to_str()
            .unwrap()
            .to_string();

        // Create directories
        fs::create_dir_all(&sstable_dir).unwrap();

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_dir).unwrap();

        // Create a batch of operations
        let operations = vec![
            Operation::Insert {
                key: "batch1".to_string(),
                value: "value1".as_bytes().to_vec(),
            },
            Operation::Insert {
                key: "batch2".to_string(),
                value: "value2".as_bytes().to_vec(),
            },
            Operation::Insert {
                key: "batch3".to_string(),
                value: "value3".as_bytes().to_vec(),
            },
        ];

        // Execute batch as a single transaction
        durability_manager.execute_batch(operations).unwrap();

        // Verify WAL contains all operations in a single transaction
        // This would require recovery testing, but we're just verifying it doesn't crash
    }

    #[tokio::test]
    async fn test_transaction_abort() {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir
            .path()
            .join("test_abort_wal.log")
            .to_str()
            .unwrap()
            .to_string();
        let sstable_dir = temp_dir
            .path()
            .join("abort_sstables")
            .to_str()
            .unwrap()
            .to_string();

        // Create directories
        fs::create_dir_all(&sstable_dir).unwrap();

        // Create durability manager
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_dir).unwrap();

        // Begin transaction
        let tx_id = durability_manager.begin_transaction().unwrap();

        // Add operations to transaction
        durability_manager
            .add_to_transaction(
                tx_id,
                Operation::Insert {
                    key: "abort_key1".to_string(),
                    value: "value1".as_bytes().to_vec(),
                },
            )
            .unwrap();

        // Abort transaction
        durability_manager.abort_transaction(tx_id).unwrap();

        // Try to add to aborted transaction (should fail)
        let result = durability_manager.add_to_transaction(
            tx_id,
            Operation::Insert {
                key: "abort_key2".to_string(),
                value: "value2".as_bytes().to_vec(),
            },
        );

        assert!(result.is_err());
    }
}
