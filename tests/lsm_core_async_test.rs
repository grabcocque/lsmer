use lsmer::{AsyncStringMemtable, MemtableError};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

async fn setup_test_dir(dir: &str) -> std::io::Result<()> {
    let path = Path::new(dir);
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

#[tokio::test]
async fn test_async_memtable_simple() {
    let test_future = async {
        let test_dir = "target/test_async_memtable_simple";
        setup_test_dir(test_dir).await.unwrap();

        // Create a new async memtable
        let memtable_future = AsyncStringMemtable::new(
            1000, // capacity
            test_dir.to_string(),
            60, // compaction interval
        );

        // Await the future to get the memtable
        let memtable = memtable_future.await.unwrap();

        // Insert a key-value pair
        let insert_result = memtable
            .insert("key1".to_string(), vec![1, 2, 3])
            .await
            .unwrap();
        assert_eq!(insert_result, None);

        // Get the value
        let get_result = memtable.get(&"key1".to_string()).await.unwrap();
        assert_eq!(get_result, Some(vec![1, 2, 3]));

        // Try to get a non-existent key
        let missing_result = memtable.get(&"nonexistent".to_string()).await.unwrap();
        assert_eq!(missing_result, None);
    };

    // Run the test with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_async_memtable_concurrent() {
    let test_future = async {
        let test_dir = "target/test_async_memtable_concurrent";
        setup_test_dir(test_dir).await.unwrap();

        // Create a new memtable
        let memtable_future = AsyncStringMemtable::new(
            1000, // capacity
            test_dir.to_string(),
            60, // compaction interval
        );

        // Await the future to get the memtable and wrap in Arc for sharing
        let memtable = Arc::new(memtable_future.await.unwrap());

        // Spawn a few tasks that use the memtable concurrently
        let mut handles = Vec::new();

        for i in 0..5 {
            let memtable_clone = Arc::clone(&memtable);

            handles.push(tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = vec![i as u8, i as u8 + 1];

                // Insert and then get the value
                memtable_clone
                    .insert(key.clone(), value.clone())
                    .await
                    .unwrap();
                let result = memtable_clone.get(&key).await.unwrap();

                assert_eq!(result, Some(value));
            }));
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
    };

    // Run the test with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_async_memtable_compaction() {
    // Create the memtable outside the test_future so we can clean it up
    let test_dir = "target/test_async_memtable_compaction";
    println!("Setting up test directory at {}", test_dir);
    let _ = setup_test_dir(test_dir).await;

    // Create a new memtable
    println!("Creating memtable...");
    let memtable_future = AsyncStringMemtable::new(
        1000, // capacity
        test_dir.to_string(),
        60, // compaction interval
    );

    // Await the future to get the memtable
    println!("Awaiting memtable future...");
    let memtable = memtable_future.await.unwrap();
    println!("Memtable created successfully");

    let test_future = async {
        // Insert some key-value pairs
        println!("Inserting 10 key-value pairs...");
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = vec![i as u8; 5];
            memtable.insert(key, value).await.unwrap();
        }

        // Check that memtable has 10 items
        let len = memtable.len().await.unwrap();
        println!("Memtable length: {}", len);
        assert_eq!(len, 10);

        // Force compaction
        println!("Forcing compaction...");
        let sstable_path = memtable.force_compaction().await.unwrap();
        println!("Compaction completed, SSTable path: {}", sstable_path);

        // Verify the sstable path exists
        let path_exists = Path::new(&sstable_path).exists();
        println!("SSTable path exists: {}", path_exists);
        assert!(path_exists);

        // Verify the memtable is now empty
        let new_len = memtable.len().await.unwrap();
        println!("Memtable length after compaction: {}", new_len);
        assert_eq!(new_len, 0);

        // But we can still insert new items
        println!("Inserting new item after compaction...");
        memtable
            .insert("new_key".to_string(), vec![99])
            .await
            .unwrap();
        let final_len = memtable.len().await.unwrap();
        println!("Final memtable length: {}", final_len);
        assert_eq!(final_len, 1);
    };

    // Run the test with a 10-second timeout
    let result = timeout(Duration::from_secs(10), test_future).await;

    // Ensure we always shutdown the memtable properly, even on timeout
    println!("Test completed, shutting down memtable...");
    if let Err(e) = memtable.shutdown().await {
        println!("Error shutting down memtable: {:?}", e);
    }

    // Now check the result
    match result {
        Ok(_) => println!("Test completed successfully"),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_async_memtable_capacity_limit() {
    // Create the memtable outside the test_future so we can clean it up
    let test_dir = "target/test_async_memtable_capacity";
    let _ = setup_test_dir(test_dir).await;

    // Create a very small memtable (only 100 bytes)
    let memtable_future = AsyncStringMemtable::new(
        100, // very small capacity
        test_dir.to_string(),
        60, // compaction interval
    );

    // Await the future to get the memtable
    let memtable = memtable_future.await.unwrap();

    let test_future = async {
        // Insert keys until we hit the capacity limit
        let mut count = 0;
        let mut capacity_exceeded = false;

        for i in 0..1000 {
            // Try to insert up to 1000 items, but we'll hit capacity way before that
            let key = format!("key{}", i);
            let value = vec![i as u8; 10]; // Each value is 10 bytes

            match memtable.insert(key, value).await {
                Ok(_) => count += 1,
                Err(MemtableError::CapacityExceeded) => {
                    capacity_exceeded = true;
                    break;
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        // We should have inserted some items but hit capacity
        assert!(count > 0, "Should have inserted at least one item");
        assert!(capacity_exceeded, "Should have exceeded capacity");

        // After compaction, we should be able to insert more
        println!("Forcing compaction after capacity reached...");
        memtable.force_compaction().await.unwrap();
        assert_eq!(memtable.len().await.unwrap(), 0);

        // Now we can insert more
        assert!(
            memtable
                .insert("new_key".to_string(), vec![99; 10])
                .await
                .is_ok()
        );
    };

    // Run the test with a 10-second timeout
    let result = timeout(Duration::from_secs(10), test_future).await;

    // Ensure we always shutdown the memtable properly, even on timeout
    println!("Test completed, shutting down memtable...");
    if let Err(e) = memtable.shutdown().await {
        println!("Error shutting down memtable: {:?}", e);
    }

    // Now check the result
    match result {
        Ok(_) => println!("Test completed successfully"),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
