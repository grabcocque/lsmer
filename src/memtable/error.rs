use std::error::Error;
use std::fmt::{self, Debug};
use std::io;

use crate::wal::WalError;

/// Errors that can occur during memtable operations
#[derive(Debug)]
pub enum MemtableError {
    /// The memtable has reached its capacity limit
    CapacityExceeded,
    /// The requested key was not found in the memtable
    KeyNotFound,
    /// An error occurred while operating on the WAL
    WalError(io::Error),
    /// An error occurred during I/O operations
    IoError(io::Error),
    /// An error occurred while acquiring a lock
    LockError,
}

impl fmt::Display for MemtableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemtableError::CapacityExceeded => write!(f, "Memtable capacity exceeded"),
            MemtableError::KeyNotFound => write!(f, "Key not found in memtable"),
            MemtableError::WalError(e) => write!(f, "WAL error: {}", e),
            MemtableError::IoError(e) => write!(f, "I/O error: {}", e),
            MemtableError::LockError => write!(f, "Failed to acquire lock"),
        }
    }
}

impl Error for MemtableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            MemtableError::WalError(e) => Some(e),
            MemtableError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<WalError> for MemtableError {
    fn from(error: WalError) -> Self {
        match error {
            WalError::IoError(e) => MemtableError::WalError(e),
            e => MemtableError::WalError(io::Error::new(io::ErrorKind::Other, e.to_string())),
        }
    }
}

impl From<io::Error> for MemtableError {
    fn from(error: io::Error) -> Self {
        MemtableError::IoError(error)
    }
}
