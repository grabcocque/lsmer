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

        // Show file size to help with debugging
        let file_size = fs::metadata(&path).unwrap().len();
        println!("File size: {} bytes", file_size);

        // Now modify the key length at the exact offset where first key would be
        // SSTable header size is defined in src/sstable/mod.rs as HEADER_SIZE
        let mut data = fs::read(&path).unwrap();

        // The header is followed immediately by entries
        // Each entry has: key_len (4 bytes) + key + value_len (4 bytes) + value + checksum (4 bytes)
        // So we need to modify the first 4 bytes after the header
        let header_size = 49; // Based on constants in src/sstable/mod.rs

        if data.len() >= header_size + 4 {
            // Corrupt the key length field - set it to a impossibly large value
            println!(
                "Setting key length bytes to FF FF FF FF at offset {}",
                header_size
            );
            data[header_size] = 0xFF;
            data[header_size + 1] = 0xFF;
            data[header_size + 2] = 0xFF;
            data[header_size + 3] = 0xFF;
            fs::write(&path, &data).unwrap();
        } else {
            println!("File too small to modify key length: {} bytes", data.len());
        }

        // Try to read the file - may fail with different errors, but shouldn't crash
        let mut reader = match SSTableReader::open(&path) {
            Ok(r) => r,
            Err(e) => {
                println!("Failed to open SSTable: {}", e);
                return; // If it fails to open, that's fine too
            }
        };

        // If we got here, the reader opened successfully, but get() should fail safely
        let result = reader.get("test");
        println!("get() result: {:?}", result);

        // Should either return None or an error, but not crash
        match result {
            Ok(None) => {
                println!("Key not found - test passed");
            }
            Err(e) => {
                println!("Error detected (expected): {}", e);
            }
            Ok(Some(_)) => {
                panic!("Unexpected successful read with corrupted key length!");
            }
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
