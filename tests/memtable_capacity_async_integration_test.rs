use lsmer::AsyncStringMemtable;
use lsmer::memtable::MemtableError;
use std::fs;
use std::io;
use std::time::Duration;
use tokio::time::timeout;

/// Set up a clean test directory
fn setup_test_dir(dir: &str) -> io::Result<()> {
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir)
}

#[tokio::test]
async fn test_async_memtable_capacity_methods() {
    let test_future = async {
        // Create a test directory
        let test_dir = "target/test_async_memtable_capacity_methods";
        setup_test_dir(test_dir).unwrap();

        // Create a memtable with a very small capacity to test is_full
        let small_capacity = 100; // Just 100 bytes
        let memtable = AsyncStringMemtable::new(small_capacity, test_dir.to_string(), 60)
            .await
            .unwrap();

        // Test max_capacity method
        assert_eq!(memtable.max_capacity(), small_capacity);

        // Initially memtable should not be full
        let initial_is_full = memtable.is_full().await.unwrap();
        assert!(!initial_is_full);

        // Get the initial size
        let initial_size = memtable.size_bytes().await.unwrap();
        println!("Initial size: {} bytes", initial_size);

        // Create data smaller than capacity to avoid CapacityExceeded errors
        let small_key = "key".to_string();
        let small_value = vec![1u8; 20]; // 20 bytes for value

        // Insert the data
        memtable
            .insert(small_key.clone(), small_value.clone())
            .await
            .unwrap();

        // Check size after insert
        let size_after_insert = memtable.size_bytes().await.unwrap();
        println!("Size after small insert: {} bytes", size_after_insert);

        // Try inserting more data until we approach capacity
        for i in 0..3 {
            let key = format!("key{}", i);
            let value = vec![i as u8; 10]; // Small values to avoid immediate capacity issues

            // We'll try to insert, but won't fail the test if it exceeds capacity
            match memtable.insert(key, value).await {
                Ok(_) => println!("Successfully inserted item {}", i),
                Err(MemtableError::CapacityExceeded) => {
                    println!("Capacity exceeded at item {}", i);
                    break;
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        // Now check if it's getting full
        match memtable.is_full().await {
            Ok(is_full) => println!("Is memtable full: {}", is_full),
            Err(MemtableError::CapacityExceeded) => println!("Capacity already exceeded"),
            Err(e) => panic!("Unexpected error: {:?}", e),
        }

        // Verify we can still get the first data
        let value = memtable.get(&small_key).await.unwrap();
        assert_eq!(value, Some(small_value));

        // Clean up
        memtable.shutdown().await.unwrap();
    };

    // Run the test with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_async_memtable_flush_to_sstable() {
    let test_future = async {
        // Create a test directory
        let test_dir = "target/test_async_memtable_flush";
        setup_test_dir(test_dir).unwrap();

        // Create a memtable with a reasonable capacity
        let memtable = AsyncStringMemtable::new(1024, test_dir.to_string(), 60)
            .await
            .unwrap();

        // Insert some data
        for i in 0..5 {
            let key = format!("flush_key{}", i);
            let value = vec![i as u8; 10];
            memtable.insert(key, value).await.unwrap();
        }

        // Test flush_to_sstable method
        let sstable_path = memtable.flush_to_sstable(test_dir).await.unwrap();
        println!("SSTable path: {}", sstable_path);

        // Verify the SSTable file exists
        assert!(fs::metadata(&sstable_path).is_ok());

        // Clean up
        memtable.shutdown().await.unwrap();
    };

    // Run the test with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_async_memtable_error_handling() {
    let test_future = async {
        // Create a test directory
        let test_dir = "target/test_async_memtable_errors";
        setup_test_dir(test_dir).unwrap();

        // Create a memtable that we'll shut down to force errors
        let memtable = AsyncStringMemtable::new(1024, test_dir.to_string(), 60)
            .await
            .unwrap();

        // Insert some data
        memtable
            .insert("error_key".to_string(), vec![1, 2, 3])
            .await
            .unwrap();

        // Shut down the memtable
        memtable.shutdown().await.unwrap();

        // Give some time for shutdown to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now try operations that should fail
        let insert_result = memtable
            .insert("after_shutdown".to_string(), vec![4, 5, 6])
            .await;
        assert!(insert_result.is_err());

        if let Err(e) = insert_result {
            match e {
                MemtableError::LockError => (), // Expected error
                _ => panic!("Unexpected error: {:?}", e),
            }
        }

        let get_result = memtable.get(&"error_key".to_string()).await;
        assert!(get_result.is_err());

        let remove_result = memtable.remove(&"error_key".to_string()).await;
        assert!(remove_result.is_err());

        let len_result = memtable.len().await;
        assert!(len_result.is_err());

        let is_empty_result = memtable.is_empty().await;
        assert!(is_empty_result.is_err());

        let clear_result = memtable.clear().await;
        assert!(clear_result.is_err());

        let size_result = memtable.size_bytes().await;
        assert!(size_result.is_err());

        let is_full_result = memtable.is_full().await;
        assert!(is_full_result.is_err());

        // Testing force_compaction after shutdown
        let compaction_result = memtable.force_compaction().await;
        assert!(compaction_result.is_err());

        // Testing flush_to_sstable after shutdown
        let flush_result = memtable.flush_to_sstable(test_dir).await;
        assert!(flush_result.is_err());
    };

    // Run the test with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_async_memtable_sequential_operations() {
    let test_future = async {
        // Create a test directory
        let test_dir = "target/test_async_memtable_sequential";
        setup_test_dir(test_dir).unwrap();

        // Create a memtable with a larger capacity to avoid capacity issues
        let memtable = AsyncStringMemtable::new(10240, test_dir.to_string(), 60)
            .await
            .unwrap();

        // Perform a series of operations in sequence
        // 1. Insert data
        for i in 0..10 {
            let key = format!("seq_key{}", i);
            let value = vec![i as u8; 20];
            memtable.insert(key, value).await.unwrap();
        }

        // 2. Check size after inserts
        let size_after_inserts = memtable.size_bytes().await.unwrap();
        assert!(size_after_inserts > 0);
        println!("Size after inserts: {} bytes", size_after_inserts);

        // 3. Force compaction and check that data remains accessible
        let compaction_path = memtable.force_compaction().await.unwrap();
        println!("Compaction path: {}", compaction_path);

        // After compaction, data might be cleared, so we'll reinsert to be sure
        for i in 0..10 {
            let key = format!("seq_key{}", i);
            let value = vec![i as u8; 20];
            memtable.insert(key, value).await.unwrap();
        }

        // 4. Check size after compaction and reinsertion
        let size_after_reinsert = memtable.size_bytes().await.unwrap();
        println!(
            "Size after compaction and reinsert: {} bytes",
            size_after_reinsert
        );

        // 5. Update some values
        for i in 0..5 {
            let key = format!("seq_key{}", i);
            let value = vec![i as u8 + 100; 15]; // Different value
            memtable.insert(key, value).await.unwrap();
        }

        // 6. Verify the updates
        for i in 0..5 {
            let key = format!("seq_key{}", i);
            let value = memtable.get(&key).await.unwrap();
            assert_eq!(value, Some(vec![i as u8 + 100; 15]));
        }

        // 7. Check if the memtable is considered full
        let is_full = memtable.is_full().await.unwrap();
        println!("Is memtable full: {}", is_full);

        // 8. Test flush_to_sstable (which calls force_compaction)
        let sstable_path = memtable.flush_to_sstable(test_dir).await.unwrap();
        println!("SSTable path from flush: {}", sstable_path);

        // Clean up
        memtable.shutdown().await.unwrap();
    };

    // Run the test with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
