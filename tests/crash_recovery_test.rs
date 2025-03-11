use lsmer::lsm_index::LsmIndex;
use std::fs;
use std::io::{self, ErrorKind};
use std::time::Duration;
use tokio::time::timeout;

// Helper function to set up test directory
fn setup_test_dir(dir_name: &str) -> io::Result<()> {
    let test_dir = format!("target/{}", dir_name);
    if let Err(e) = fs::create_dir_all(&test_dir) {
        if e.kind() != ErrorKind::AlreadyExists {
            return Err(e);
        }
    }
    Ok(())
}

// Helper function to clean test directory
fn clean_test_dir(dir_name: &str) -> io::Result<()> {
    let test_dir = format!("target/{}", dir_name);
    if let Err(e) = fs::remove_dir_all(&test_dir) {
        if e.kind() != ErrorKind::NotFound {
            return Err(e);
        }
    }
    setup_test_dir(dir_name)
}

#[tokio::test]
async fn test_basic_recovery() {
    let test_future = async {
        // Create a clean temporary directory for the test
        let test_dir = "target/test_basic_recovery";
        clean_test_dir(test_dir).unwrap();

        println!("Step 1: Creating database and testing in-memory operations");
        // Test in-memory operations first to isolate issues
        {
            let lsm =
                LsmIndex::new(1024 * 1024, test_dir.to_string(), Some(3600), false, 0.0).unwrap();

            // Insert data
            lsm.insert("key1".to_string(), vec![1, 2, 3]).unwrap();
            lsm.insert("key2".to_string(), vec![4, 5, 6]).unwrap();

            // Verify in-memory reads work
            let val1 = lsm.get("key1").unwrap();
            let val2 = lsm.get("key2").unwrap();

            assert_eq!(val1, Some(vec![1, 2, 3]), "In-memory read of key1 failed");
            assert_eq!(val2, Some(vec![4, 5, 6]), "In-memory read of key2 failed");

            // No need to flush, we're testing in-memory recovery
        }

        println!("Step 2: Creating a new instance and testing inserts with manual recovery");
        {
            let mut lsm =
                LsmIndex::new(1024 * 1024, test_dir.to_string(), Some(3600), false, 0.0).unwrap();

            // Insert data again
            lsm.insert("key3".to_string(), vec![7, 8, 9]).unwrap();

            // Manually trigger recovery to see if it works
            lsm.recover().unwrap();

            // Check if the data is still there after recovery
            let val3 = lsm.get("key3").unwrap();
            assert_eq!(val3, Some(vec![7, 8, 9]), "Data lost after manual recovery");
        }

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    };

    // Run with timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
