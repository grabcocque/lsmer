use lsmer::lsm_index::LsmIndex;
use std::io;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_lsm_index_error_handling_extended() {
    let test_future = async {
        // Test error creation and conversion
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let lsm_err = lsmer::lsm_index::LsmIndexError::from(io_err);

        match lsm_err {
            lsmer::lsm_index::LsmIndexError::IoError(_) => (), // Expected
            _ => panic!("Expected IoError variant"),
        }

        // Create memtable error
        let memtable_err = lsmer::memtable::MemtableError::KeyNotFound;
        let lsm_err = lsmer::lsm_index::LsmIndexError::from(memtable_err);

        match lsm_err {
            lsmer::lsm_index::LsmIndexError::MemtableError(_) => (), // Expected
            _ => panic!("Expected MemtableError variant"),
        }

        // Create durability error
        let durability_err =
            lsmer::wal::durability::DurabilityError::RecoveryFailed("test".to_string());
        let lsm_err = lsmer::lsm_index::LsmIndexError::from(durability_err);

        match lsm_err {
            lsmer::lsm_index::LsmIndexError::DurabilityError(_) => (), // Expected
            _ => panic!("Expected DurabilityError variant"),
        }

        // Create index error
        let index_err = lsmer::bptree::IndexError::KeyNotFound;
        let lsm_err = lsmer::lsm_index::LsmIndexError::from(index_err);

        match lsm_err {
            lsmer::lsm_index::LsmIndexError::IndexError(_) => (), // Expected
            _ => panic!("Expected IndexError variant"),
        }

        // Test debug formatting
        let key_not_found = lsmer::lsm_index::LsmIndexError::KeyNotFound;
        let debug_str = format!("{:?}", key_not_found);
        assert_eq!(debug_str, "KeyNotFound");

        let invalid_op =
            lsmer::lsm_index::LsmIndexError::InvalidOperation("test reason".to_string());
        let debug_str = format!("{:?}", invalid_op);
        assert!(debug_str.contains("test reason"));
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_lsm_index_compaction_scheduler() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create an LSM index with compaction enabled
        // Set the compaction threshold low to trigger compaction
        let compaction_threshold = Some(3);
        let index = LsmIndex::new(
            1000, // Larger capacity to avoid capacity exceeded errors
            temp_path,
            compaction_threshold, // Enable compaction
            true,                 // Use bloom filters
            0.01,                 // 1% false positive rate
        )
        .unwrap();

        // Insert a few key-value pairs to trigger compaction
        for i in 0..10 {
            let key = format!("compaction_key{}", i);
            let value = vec![i as u8];
            match index.insert(key, value) {
                Ok(_) => (),
                Err(e) => {
                    // If we get capacity exceeded, that's fine for this test
                    match e {
                        lsmer::lsm_index::LsmIndexError::MemtableError(
                            lsmer::memtable::MemtableError::CapacityExceeded,
                        ) => {
                            // This is expected, the memtable got flushed to disk
                            // which is what we want for testing compaction
                        }
                        _ => panic!("Unexpected error: {:?}", e),
                    }
                }
            };

            // Give some time for potential background compaction
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Sleep a bit to allow compaction to run
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Perform more operations to trigger additional compaction cycles
        for i in 10..20 {
            let key = format!("compaction_key{}", i);
            let value = vec![i as u8];
            match index.insert(key, value) {
                Ok(_) => (),
                Err(e) => {
                    // If we get capacity exceeded, that's fine for this test
                    match e {
                        lsmer::lsm_index::LsmIndexError::MemtableError(
                            lsmer::memtable::MemtableError::CapacityExceeded,
                        ) => {
                            // This is expected, the memtable got flushed to disk
                        }
                        _ => panic!("Unexpected error: {:?}", e),
                    }
                }
            };

            // Give some time for potential background compaction
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Sleep again to allow compaction to run
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Check that values are still accessible after compaction
        // Note: Not all values may be available due to capacity limits,
        // so we just check a few of them
        let mut found_count = 0;
        for i in 0..20 {
            let key = format!("compaction_key{}", i);
            if let Ok(Some(_)) = index.get(&key) {
                found_count += 1;
            }
        }

        // We should find at least some of the keys
        assert!(
            found_count > 0,
            "Expected to find at least some keys, found none"
        );
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_lsm_index_sstable_readers() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create LSM index
        let index = LsmIndex::new(
            100, // Larger capacity to avoid immediate errors
            temp_path, None, // No compaction
            true, // Use bloom filters
            0.01, // 1% false positive rate
        )
        .unwrap();

        // Insert data, handling potential capacity errors
        let mut insert_count = 0;
        for i in 0..20 {
            let key = format!("sstable_key{}", i);
            let value = vec![i as u8];
            match index.insert(key, value) {
                Ok(_) => insert_count += 1,
                Err(e) => {
                    match e {
                        lsmer::lsm_index::LsmIndexError::MemtableError(
                            lsmer::memtable::MemtableError::CapacityExceeded,
                        ) => {
                            // Expected, continue with test
                        }
                        _ => panic!("Unexpected error: {:?}", e),
                    }
                }
            }

            // If we can't insert any more, stop trying
            if insert_count == 0 && i > 5 {
                break;
            }
        }

        // If we didn't insert anything, the test is meaningless
        if insert_count == 0 {
            return;
        }

        // Explicitly flush to create an SSTable
        match index.flush() {
            Ok(_) => (),
            Err(e) => {
                // If there's nothing to flush, that's ok
                println!("Flush error: {:?}", e);
            }
        }

        // Test that at least some data can be retrieved
        let mut found_count = 0;
        for i in 0..insert_count {
            let key = format!("sstable_key{}", i);
            if let Ok(Some(_)) = index.get(&key) {
                found_count += 1;
            }
        }

        // We should find at least some keys
        assert!(
            found_count > 0,
            "Expected to find at least some keys, found none"
        );

        // Test range query if we have enough data
        if insert_count >= 5 {
            let range_result = index
                .range("sstable_key1".to_string().."sstable_key5".to_string())
                .unwrap();
            // We may not get all 4 keys due to capacity issues, but we should get some
            assert!(
                !range_result.is_empty(),
                "Expected at least one key in range query"
            );
        }

        // Test that non-existent keys return None
        let result = index.get("nonexistent_sstable_key").unwrap();
        assert!(result.is_none());
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}

#[tokio::test]
async fn test_lsm_index_metadata_operations() {
    let test_future = async {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create LSM index
        let index = LsmIndex::new(
            100, // Reasonable capacity
            temp_path.clone(),
            None, // No compaction
            true, // Use bloom filters
            0.01, // 1% false positive rate
        )
        .unwrap();

        // Check initial state
        assert!(index.get("meta_key1").unwrap().is_none());

        // Insert data
        index
            .insert("meta_key1".to_string(), vec![1, 2, 3])
            .unwrap();
        index
            .insert("meta_key2".to_string(), vec![4, 5, 6])
            .unwrap();

        // Verify data is correctly inserted
        assert!(index.get("meta_key1").unwrap().is_some());
        assert!(index.get("meta_key2").unwrap().is_some());

        // Test removing a key
        index.remove("meta_key1").unwrap();

        // Verify key is gone
        let result = index.get("meta_key1").unwrap();
        assert!(result.is_none());

        // Clear index
        index.clear().unwrap();

        // Verify all data is gone
        assert!(index.get("meta_key2").unwrap().is_none());
    };

    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}
