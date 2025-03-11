mod async_memtable;
mod error;
mod string_memtable;
mod traits;

use std::io::{self};
use std::sync::mpsc;

pub use async_memtable::AsyncStringMemtable;
pub use error::MemtableError;
pub use string_memtable::StringMemtable;
pub use traits::{ByteSize, Memtable, SSTableWriter, ToBytes};

// Messages that can be sent to the background thread
#[allow(dead_code)]
enum MemtableMessage<K, V> {
    Insert(K, V, mpsc::Sender<Result<Option<V>, MemtableError>>),
    Get(K, mpsc::Sender<Result<Option<V>, MemtableError>>),
    Remove(K, mpsc::Sender<Result<Option<V>, MemtableError>>),
    Len(mpsc::Sender<Result<usize, MemtableError>>),
    IsEmpty(mpsc::Sender<Result<bool, MemtableError>>),
    Clear(mpsc::Sender<Result<(), MemtableError>>),
    SizeBytes(mpsc::Sender<Result<usize, MemtableError>>),
    ForceCompaction(mpsc::Sender<io::Result<()>>),
    Shutdown,
}
