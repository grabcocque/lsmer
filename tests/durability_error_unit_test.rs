use lsmer::memtable::MemtableError;
use lsmer::{DurabilityError, WalError};
use std::io;

#[test]
fn test_durability_error_from_wal_error() {
    // Test conversion from WalError to DurabilityError
    let wal_error = WalError::IoError(io::Error::new(io::ErrorKind::NotFound, "file not found"));
    let durability_error = DurabilityError::from(wal_error);

    match durability_error {
        DurabilityError::WalError(WalError::IoError(_)) => (),
        _ => panic!("Expected WalError variant"),
    }
}

#[test]
fn test_durability_error_from_io_error() {
    // Test conversion from io::Error to DurabilityError
    let io_error = io::Error::new(io::ErrorKind::Other, "test io error");
    let durability_error = DurabilityError::from(io_error);

    match durability_error {
        DurabilityError::IoError(_) => (),
        _ => panic!("Expected IoError variant"),
    }
}

#[test]
fn test_durability_error_from_memtable_error() {
    // Test conversion from MemtableError to DurabilityError

    // Test CapacityExceeded
    let memtable_error = MemtableError::CapacityExceeded;
    let durability_error = DurabilityError::from(memtable_error);

    match durability_error {
        DurabilityError::MemtableError(MemtableError::CapacityExceeded) => (),
        _ => panic!("Expected MemtableError::CapacityExceeded variant"),
    }

    // Test KeyNotFound
    let memtable_error = MemtableError::KeyNotFound;
    let durability_error = DurabilityError::from(memtable_error);

    match durability_error {
        DurabilityError::MemtableError(MemtableError::KeyNotFound) => (),
        _ => panic!("Expected MemtableError::KeyNotFound variant"),
    }

    // Test WalError within MemtableError
    let memtable_error =
        MemtableError::WalError(io::Error::new(io::ErrorKind::NotFound, "file not found"));
    let durability_error = DurabilityError::from(memtable_error);

    match durability_error {
        DurabilityError::MemtableError(MemtableError::WalError(_)) => (),
        _ => panic!("Expected MemtableError::WalError variant"),
    }

    // Test IoError within MemtableError
    let memtable_error =
        MemtableError::IoError(io::Error::new(io::ErrorKind::NotFound, "file not found"));
    let durability_error = DurabilityError::from(memtable_error);

    match durability_error {
        DurabilityError::MemtableError(MemtableError::IoError(_)) => (),
        _ => panic!("Expected MemtableError::IoError variant"),
    }
}

#[test]
fn test_durability_error_variants() {
    // Test creation of different DurabilityError variants
    let variants = [
        DurabilityError::WalError(WalError::InvalidRecord),
        DurabilityError::IoError(io::Error::new(io::ErrorKind::Other, "test error")),
        DurabilityError::MemtableError(MemtableError::KeyNotFound),
        DurabilityError::CheckpointNotFound(123),
        DurabilityError::SsTableIntegrityCheckFailed,
        DurabilityError::RecoveryFailed("Test recovery failed".to_string()),
        DurabilityError::DataCorruption("Data corrupt".to_string()),
        DurabilityError::TransactionAlreadyExists(456),
        DurabilityError::TransactionNotFound(789),
        DurabilityError::TransactionWrongState(101, "Wrong state".to_string()),
        DurabilityError::TransactionNotPrepared(102),
        DurabilityError::TransactionAlreadyPrepared(103),
        DurabilityError::TransactionAlreadyCommitted(104),
        DurabilityError::TransactionAlreadyAborted(105),
    ];

    // Make sure debug formatting works for all variants
    for variant in &variants {
        let debug_str = format!("{:?}", variant);
        assert!(!debug_str.is_empty());
    }
}
