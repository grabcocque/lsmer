mod helpers {
    include!("helpers/mod.rs");
}

use helpers::TestDir;
use lsmer::wal::durability::{DurabilityManager, Operation};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_parallel_safe_example_1() {
    let test_future = async {
        // Create a unique test directory using our helper
        let test_dir = TestDir::new("parallel_test_1");

        // Create base directory
        let base_dir = test_dir.as_str();

        // Define file paths for DurabilityManager
        let wal_path = format!("{}/wal.log", base_dir);
        let sstable_path = format!("{}/sstables", base_dir);

        // Create the sstables directory
        std::fs::create_dir_all(&sstable_path).unwrap();

        // Now we can create our test instance with isolated paths
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Perform test operations
        for i in 0..5 {
            let key = format!("parallel_key{}", i);
            let value = vec![i as u8];
            durability_manager.insert(key, value).unwrap();
        }

        // Everything is isolated and won't conflict with other tests
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }

    // The TestDir will automatically clean up when it goes out of scope
}

#[tokio::test]
async fn test_parallel_safe_example_2() {
    let test_future = async {
        // Create a different unique test directory
        let test_dir = TestDir::new("parallel_test_2");

        // Create base directory
        let base_dir = test_dir.as_str();

        // Define file paths for DurabilityManager
        let wal_path = format!("{}/wal.log", base_dir);
        let sstable_path = format!("{}/sstables", base_dir);

        // Create the sstables directory
        std::fs::create_dir_all(&sstable_path).unwrap();

        // This test can safely run in parallel with the other one
        let mut durability_manager = DurabilityManager::new(&wal_path, &sstable_path).unwrap();

        // Add a transaction
        let tx_id = durability_manager.begin_transaction().unwrap();

        // Add an operation to the transaction
        durability_manager
            .add_to_transaction(
                tx_id,
                Operation::Insert {
                    key: "parallel_tx_key".to_string(),
                    value: vec![1, 2, 3],
                },
            )
            .unwrap();

        // Complete the transaction
        durability_manager.prepare_transaction(tx_id).unwrap();
        durability_manager.commit_transaction(tx_id).unwrap();
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}
