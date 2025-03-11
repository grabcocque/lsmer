use crc32fast::hash;
use std::fs::{self, File};
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_crc32fast_implementation() {
    // Test vectors from the CRC32 spec
    let test_data = [
        (b"".as_ref(), 0x0),
        (b"a".as_ref(), 0xe8b7be43),
        (b"abc".as_ref(), 0x352441c2),
        (b"message digest".as_ref(), 0x20159d7f),
        (b"abcdefghijklmnopqrstuvwxyz".as_ref(), 0x4c2750bd),
    ];

    for (data, expected) in test_data.iter() {
        let actual = hash(data);
        assert_eq!(actual, *expected, "CRC32 hash mismatch for {:?}", data);
    }
}

#[test]
fn test_crc32_file_integrity() {
    // Create a temporary file
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.dat");

    // Write some test data to the file
    let test_data = b"This is a test file for CRC32 calculation";
    let mut file = File::create(&file_path).unwrap();
    file.write_all(test_data).unwrap();
    file.flush().unwrap();

    // Calculate CRC32 of the file content
    let file_data = fs::read(&file_path).unwrap();
    let crc1 = hash(&file_data);

    // Modify the file
    let mut file = fs::OpenOptions::new().write(true).open(&file_path).unwrap();
    file.write_all(b" with some additional data").unwrap();
    file.flush().unwrap();

    // Calculate CRC32 again
    let file_data2 = fs::read(&file_path).unwrap();
    let crc2 = hash(&file_data2);

    // The checksums should be different
    assert_ne!(crc1, crc2, "CRC32 should detect file changes");

    // Clean up
    temp_dir.close().unwrap();
}

#[test]
fn test_crc32_incremental_calculation() {
    // Test data
    let data1 = b"Part one of the data";
    let data2 = b"Part two of the data";

    // Calculate checksum for concatenated data
    let mut combined = Vec::new();
    combined.extend_from_slice(data1);
    combined.extend_from_slice(data2);
    let full_checksum = hash(&combined);

    // Calculate checksum incrementally
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data1);
    hasher.update(data2);
    let incremental_checksum = hasher.finalize();

    // They should match
    assert_eq!(
        full_checksum, incremental_checksum,
        "Incremental CRC32 calculation should match single pass"
    );
}
