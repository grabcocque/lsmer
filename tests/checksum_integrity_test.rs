use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::fs;
use std::io::ErrorKind;
use tempfile::tempdir;

// Basic test with minimal configuration
#[test]
fn test_basic_integrity() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir
        .path()
        .join("basic.sst")
        .to_str()
        .unwrap()
        .to_string();

    // Create a minimal SSTable with a single entry
    // Using the same parameters as the working test
    {
        let mut writer = SSTableWriter::new(&file_path, 1, false, 0.0).unwrap();
        writer.write_entry("key", b"value").unwrap();
        writer.finalize().unwrap();
    }

    // Read it back
    let mut reader = SSTableReader::open(&file_path).unwrap();
    let value = reader.get("key").unwrap();
    assert_eq!(value, Some(b"value".to_vec()));
}

// Test that completely invalid data is properly rejected
#[test]
fn test_completely_invalid_data() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir
        .path()
        .join("invalid.sst")
        .to_str()
        .unwrap()
        .to_string();

    // Write some garbage data
    fs::write(&file_path, b"THIS_IS_NOT_AN_SSTABLE").unwrap();

    // Attempt to open it
    let result = SSTableReader::open(&file_path);
    assert!(result.is_err());
}

// Test that a corrupted magic number is detected
#[test]
fn test_corrupted_magic_number() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir
        .path()
        .join("corrupt_magic.sst")
        .to_str()
        .unwrap()
        .to_string();

    // Create a valid SSTable first
    {
        let mut writer = SSTableWriter::new(&file_path, 1, false, 0.0).unwrap();
        writer.write_entry("key", b"value").unwrap();
        writer.finalize().unwrap();
    }

    // Now corrupt just the first byte of the magic number
    let mut data = fs::read(&file_path).unwrap();
    if !data.is_empty() {
        data[0] = 0xFF;
        fs::write(&file_path, &data).unwrap();
    }

    // Try to open it
    let result = SSTableReader::open(&file_path);
    assert!(result.is_err());

    // Check that the error is the expected kind
    if let Err(e) = result {
        assert_eq!(e.kind(), ErrorKind::InvalidData);
    }
}
