use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_sstable_writer_trait() {
    let test_future = async {
        // Create a temporary directory for the SSTable
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        // Create an SSTable writer with minimal entries
        {
            let mut writer = SSTableWriter::new(
                &format!("{}/test.sst", temp_path),
                2,     // Expected number of entries
                false, // Don't use bloom filter
                0.0,   // False positive rate doesn't matter when bloom filter is disabled
            )
            .unwrap();

            // Write small entries
            writer.write_entry("a", &[1]).unwrap();
            writer.write_entry("b", &[2]).unwrap();

            // Finalize the SSTable
            writer.finalize().unwrap();
        }

        // Read back the SSTable and verify content
        {
            let mut reader = SSTableReader::open(&format!("{}/test.sst", temp_path)).unwrap();
            assert_eq!(reader.entry_count(), 2);

            let entry1 = reader.get("a").unwrap().unwrap();
            assert_eq!(entry1, vec![1]);

            let entry2 = reader.get("b").unwrap().unwrap();
            assert_eq!(entry2, vec![2]);

            // Test non-existent key
            assert!(reader.get("c").unwrap().is_none());
        }
    };

    // Run with timeout
    match timeout(Duration::from_secs(5), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out"),
    }
}
