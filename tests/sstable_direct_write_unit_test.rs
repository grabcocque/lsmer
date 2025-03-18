use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_basic_sstable_write_read() {
    let test_future = async {
        // Create a temporary directory for the SSTable
        let temp_dir = tempdir().unwrap();
        let sstable_path = temp_dir.path().join("basic.sst");
        let sstable_path_str = sstable_path.to_str().unwrap();

        // Step 1: Write a simple SSTable with exactly the same parameters as working tests
        {
            let mut writer = SSTableWriter::new(
                sstable_path_str,
                1,     // Just one entry for simplicity
                false, // No bloom filter (known to work)
                0.0,   // No false positive rate needed
            )
            .unwrap();

            // Write a single entry
            writer.write_entry("test_key", &[1, 2, 3]).unwrap();

            // Finalize the SSTable
            writer.finalize().unwrap();
        }

        // Step 2: Verify the file exists
        assert!(sstable_path.exists(), "SSTable file was not created");

        // Step 3: Read from the SSTable
        {
            let mut reader = SSTableReader::open(sstable_path_str).unwrap();

            // Check entry count
            assert_eq!(reader.entry_count(), 1, "Expected 1 entry in SSTable");

            // Get the entry
            let value = reader.get("test_key").unwrap();
            assert_eq!(
                value,
                Some(vec![1, 2, 3]),
                "Retrieved value doesn't match expected"
            );

            // Check non-existent key
            let missing = reader.get("missing_key").unwrap();
            assert_eq!(missing, None, "Non-existent key should return None");
        }
    };

    // Run with a short timeout to avoid hanging tests
    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}
