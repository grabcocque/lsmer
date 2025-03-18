use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

#[tokio::test]
async fn test_sstable_writer_trait() {
    let test_future = async {
        // Create a temporary directory
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_writer.sst");

        // Create an SSTable writer with no Bloom filter
        let mut writer = SSTableWriter::new(
            path.to_str().unwrap(),
            5,     // Expected entries
            false, // No bloom filter
            0.01,  // False positive rate (not used)
        )
        .unwrap();

        // Add some entries
        writer.write_entry("key1", "value1".as_bytes()).unwrap();
        writer.write_entry("key2", "value2".as_bytes()).unwrap();
        writer.write_entry("key3", "value3".as_bytes()).unwrap();

        // Finalize the writer
        writer.finalize().unwrap();

        // Verify the file was created and can be read
        let mut reader = SSTableReader::open(path.to_str().unwrap()).unwrap();

        // Verify the contents
        assert_eq!(reader.entry_count(), 3);
        assert!(!reader.has_bloom_filter());

        // Check that entries can be retrieved
        assert_eq!(
            reader.get("key1").unwrap(),
            Some("value1".as_bytes().to_vec())
        );
        assert_eq!(
            reader.get("key2").unwrap(),
            Some("value2".as_bytes().to_vec())
        );
        assert_eq!(
            reader.get("key3").unwrap(),
            Some("value3".as_bytes().to_vec())
        );
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
