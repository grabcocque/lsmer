use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_basic_sstable_write_read() {
    let test_future = async {
        // Create a temporary directory
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Write to SSTable
        let mut writer = SSTableWriter::new(
            path.to_str().unwrap(),
            10,    // Expected entries
            false, // No bloom filter
            0.01,  // False positive rate (not used when bloom filter is disabled)
        )
        .unwrap();

        // Add entries
        writer.write_entry("key1", "value1".as_bytes()).unwrap();
        writer.write_entry("key2", "value2".as_bytes()).unwrap();
        writer.write_entry("key3", "value3".as_bytes()).unwrap();

        // Finalize the SSTable
        writer.finalize().unwrap();

        // Create a reader
        let mut reader = SSTableReader::open(path.to_str().unwrap()).unwrap();

        // Verify entries
        let entry1 = reader.get("key1").unwrap();
        assert_eq!(entry1.unwrap(), "value1".as_bytes());

        let entry2 = reader.get("key2").unwrap();
        assert_eq!(entry2.unwrap(), "value2".as_bytes());

        let entry3 = reader.get("key3").unwrap();
        assert_eq!(entry3.unwrap(), "value3".as_bytes());

        // Verify non-existent key returns None
        let non_existent = reader.get("non_existent").unwrap();
        assert!(non_existent.is_none());
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
