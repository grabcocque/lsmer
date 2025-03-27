use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::fs;
use std::io::ErrorKind;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

// A single basic test that doesn't corrupt anything
#[tokio::test]
async fn test_basic_read_write() {
    let test_future = async {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir
            .path()
            .join("simple.sst")
            .to_str()
            .unwrap()
            .to_string();

        // Create a minimal SSTable
        {
            let mut writer = SSTableWriter::new(&path, 1, false, 0.0).unwrap();
            writer.write_entry("test", b"value").unwrap();
            writer.finalize().unwrap();
        }

        // Just verify we can read it back
        let mut reader = SSTableReader::open(&path).unwrap();
        let value = reader.get("test").unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

// Test corruption of the magic number (safest corruption test)
#[tokio::test]
async fn test_magic_number_corruption() {
    let test_future = async {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir
            .path()
            .join("magic.sst")
            .to_str()
            .unwrap()
            .to_string();

        // Create a basic SSTable
        {
            let mut writer = SSTableWriter::new(&path, 1, false, 0.0).unwrap();
            writer.write_entry("test", b"value").unwrap();
            writer.finalize().unwrap();
        }

        // Corrupt just the first byte of the magic number
        let mut data = fs::read(&path).unwrap();
        if !data.is_empty() {
            data[0] = 0xAA; // Change just first byte
            fs::write(&path, &data).unwrap();
        }

        // Should fail with clear error
        let result = SSTableReader::open(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

// Test that version validation works
#[tokio::test]
async fn test_version_validation() {
    let test_future = async {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir
            .path()
            .join("version.sst")
            .to_str()
            .unwrap()
            .to_string();

        // Create a basic SSTable
        {
            let mut writer = SSTableWriter::new(&path, 1, false, 0.0).unwrap();
            writer.write_entry("test", b"value").unwrap();
            writer.finalize().unwrap();
        }

        // Set version to an impossibly high number
        let mut data = fs::read(&path).unwrap();
        if data.len() >= 12 {
            // Version is at bytes 8-11
            data[8] = 0xFF;
            data[9] = 0xFF;
            data[10] = 0xFF;
            data[11] = 0xFF;
            fs::write(&path, &data).unwrap();
        }

        // Should fail with clear error about version
        let result = SSTableReader::open(&path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(format!("{}", err).contains("version"));
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

// Test that a corrupted key length is caught safely
#[tokio::test]
async fn test_key_length_validation() {
    let test_future = async {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir
            .path()
            .join("key_length.sst")
            .to_str()
            .unwrap()
            .to_string();

        // Create a valid SSTable first
        {
            let mut writer = SSTableWriter::new(&path, 1, false, 0.0).unwrap();
            writer.write_entry("test", b"value").unwrap();
            writer.finalize().unwrap();
        }

        // Now modify the key length at the offset where first key would be
        // The offset is header size + some additional bytes - this test might need adjustment
        // based on actual file layout
        let mut data = fs::read(&path).unwrap();
        // Just change one byte past the header - this will likely corrupt key length
        if data.len() >= 50 {
            data[45] = 0xFF; // Set an impossibly large key length
            fs::write(&path, &data).unwrap();
        }

        // Try to read the file - may fail with different errors, but shouldn't crash
        let mut reader = match SSTableReader::open(&path) {
            Ok(r) => r,
            Err(_) => return, // If it fails to open, that's fine too
        };

        // If we got here, the reader opened successfully, but get() should fail safely
        let result = reader.get("test");
        // Should either return None or an error, but not crash
        match result {
            Ok(None) => (), // Key not found is a valid result
            Err(_) => (),   // Error is also a valid result
            Ok(Some(_)) => panic!("Unexpected successful read with corrupted key length!"),
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
