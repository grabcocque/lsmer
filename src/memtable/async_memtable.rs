use crate::memtable::error::MemtableError;
use crate::memtable::string_memtable::StringMemtable;
use crate::memtable::traits::{ByteSize, Memtable, SSTableWriter, ToBytes};
use std::fs;
use std::io;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;

// Messages that can be sent to the background task
enum MemtableMessage<K, V> {
    Insert(K, V, oneshot::Sender<Result<Option<V>, MemtableError>>),
    Get(K, oneshot::Sender<Result<Option<V>, MemtableError>>),
    Remove(K, oneshot::Sender<Result<Option<V>, MemtableError>>),
    Len(oneshot::Sender<Result<usize, MemtableError>>),
    IsEmpty(oneshot::Sender<Result<bool, MemtableError>>),
    Clear(oneshot::Sender<Result<(), MemtableError>>),
    SizeBytes(oneshot::Sender<Result<usize, MemtableError>>),
    ForceCompaction(oneshot::Sender<io::Result<String>>),
    Shutdown,
}

/// An async memtable implementation using Tokio's async/await architecture.
/// This implementation is thread-safe and non-blocking, using Tokio tasks
/// instead of OS threads.
pub struct AsyncMemtable<K, V> {
    sender: mpsc::Sender<MemtableMessage<K, V>>,
    worker_task: Option<JoinHandle<()>>,
    _base_path: String, // Prefix with underscore to indicate it's intentionally unused
    max_size_bytes: usize,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K, V> AsyncMemtable<K, V>
where
    K: ByteSize + ToBytes + Clone + Ord + Send + Sync + 'static + ToString,
    V: ByteSize
        + ToBytes
        + Clone
        + Send
        + Sync
        + 'static
        + Into<Vec<u8>>
        + From<Vec<u8>>
        + Into<Vec<u8>>
        + From<Vec<u8>>,
{
    /// Create a new async memtable with the specified maximum size and base path
    pub async fn new(
        max_size_bytes: usize,
        base_path: String,
        compaction_interval_secs: u64,
    ) -> io::Result<Self> {
        // Create the base directory if it doesn't exist
        fs::create_dir_all(&base_path)?;

        // Create a channel for communication with the worker task
        #[allow(clippy::type_complexity)]
        let (sender, mut receiver): (
            mpsc::Sender<MemtableMessage<K, V>>,
            mpsc::Receiver<MemtableMessage<K, V>>,
        ) = mpsc::channel(100); // Buffer size of 100 should be sufficient

        // Clone the base path for the worker task
        let worker_base_path = base_path.clone();

        // Start the worker task
        let worker_task = tokio::spawn(async move {
            // Create the StringMemtable that will store the actual data
            let memtable = StringMemtable::new(max_size_bytes);
            let compaction_interval = Duration::from_secs(compaction_interval_secs);
            let mut compaction_ticker = interval(compaction_interval);

            loop {
                tokio::select! {
                    // Check for messages
                    Some(message) = receiver.recv() => {
                        match message {
                            MemtableMessage::Insert(key, value, result_sender) => {
                                // Convert to StringMemtable compatible types
                                let key_str = key.to_string();
                                let value_bytes: Vec<u8> = value.into();

                                // Do the insert operation
                                let result = memtable.insert(key_str, value_bytes);

                                // Map the result back to our type
                                let mapped_result = result.map(|old_val| old_val.map(|v| V::from(v)));

                                // Send the result back
                                let _ = result_sender.send(mapped_result);

                                // Check if we need to compact after insert
                                if memtable.is_full().unwrap_or(false) {
                                    let _ = Self::do_compaction(&memtable, &worker_base_path).await;
                                }
                            }
                            MemtableMessage::Get(key, result_sender) => {
                                // Convert to StringMemtable compatible type
                                let key_str = key.to_string();

                                // Do the get operation
                                let result = memtable.get(&key_str);

                                // Map the result back to our type
                                let mapped_result = result.map(|maybe_val| maybe_val.map(|v| V::from(v)));

                                // Send the result back
                                let _ = result_sender.send(mapped_result);
                            }
                            MemtableMessage::Remove(key, result_sender) => {
                                // Convert to StringMemtable compatible type
                                let key_str = key.to_string();

                                // Do the remove operation
                                let result = memtable.remove(&key_str);

                                // Map the result back to our type
                                let mapped_result = result.map(|maybe_val| maybe_val.map(|v| V::from(v)));

                                // Send the result back
                                let _ = result_sender.send(mapped_result);
                            }
                            MemtableMessage::Len(result_sender) => {
                                let _ = result_sender.send(memtable.len());
                            }
                            MemtableMessage::IsEmpty(result_sender) => {
                                let _ = result_sender.send(memtable.is_empty());
                            }
                            MemtableMessage::Clear(result_sender) => {
                                let _ = result_sender.send(memtable.clear());
                            }
                            MemtableMessage::SizeBytes(result_sender) => {
                                let _ = result_sender.send(memtable.size_bytes());
                            }
                            MemtableMessage::ForceCompaction(result_sender) => {
                                println!("Received ForceCompaction message");
                                let result = Self::do_compaction(&memtable, &worker_base_path).await;
                                println!("do_compaction finished with result: {:?}", result);

                                // If compaction was successful, clear the memtable
                                if result.is_ok() {
                                    println!("Clearing memtable after successful compaction");
                                    if let Err(e) = memtable.clear() {
                                        println!("Error clearing memtable: {:?}", e);
                                    } else {
                                        println!("Memtable successfully cleared");
                                    }
                                }

                                match result_sender.send(result) {
                                    Ok(_) => println!("Sent compaction result back to caller"),
                                    Err(e) => println!("Failed to send compaction result: {:?}", e),
                                }
                            }
                            MemtableMessage::Shutdown => {
                                break;
                            }
                        }
                    }

                    // Check for compaction interval
                    _ = compaction_ticker.tick() => {
                        if !memtable.is_empty().unwrap_or(true) {
                            let _ = Self::do_compaction(&memtable, &worker_base_path).await;
                        }
                    }

                    // Exit if all senders are dropped and channel is closed
                    else => {
                        break;
                    }
                }
            }
        });

        Ok(AsyncMemtable {
            sender,
            worker_task: Some(worker_task),
            _base_path: base_path,
            max_size_bytes,
            _key_type: PhantomData,
            _value_type: PhantomData,
        })
    }

    /// Perform a compaction operation to flush the memtable to disk
    async fn do_compaction(memtable: &StringMemtable, base_path: &str) -> io::Result<String> {
        println!("do_compaction: Starting compaction");
        // Clone the data needed for the blocking task
        println!("do_compaction: Cloning memtable data");
        let memtable_data: Vec<(String, Vec<u8>)> = memtable.iter().unwrap_or_default();
        println!("do_compaction: Cloned {} items", memtable_data.len());
        let base_path = base_path.to_string();

        // Use tokio's spawn_blocking for CPU-bound flush_to_sstable operation
        println!("do_compaction: Spawning blocking task");
        let result = tokio::task::spawn_blocking(move || {
            println!("  blocking task: Creating temporary memtable");

            // Make sure the output directory exists
            if let Err(e) = std::fs::create_dir_all(&base_path) {
                println!(
                    "  blocking task: Failed to create directory {}: {}",
                    base_path, e
                );
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to create directory: {}", e),
                ));
            }

            // Create a new memtable with the cloned data
            let temp_memtable = StringMemtable::new(1024 * 1024);

            println!("  blocking task: Inserting {} items", memtable_data.len());
            // Insert the data into the temporary memtable
            for (key, value) in memtable_data {
                let _ = temp_memtable.insert(key, value);
            }

            println!("  blocking task: Flushing to SSTable at {}", base_path);
            // Flush the memtable to the SSTable
            let result = temp_memtable.flush_to_sstable(&base_path);
            println!("  blocking task: Flush result: {:?}", result);
            result
        })
        .await;

        println!(
            "do_compaction: Blocking task completed with result: {:?}",
            result
        );
        result?
    }

    /// Check if the memtable is full
    pub async fn is_full(&self) -> Result<bool, MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::SizeBytes(sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        let size = receiver.await.map_err(|_| MemtableError::LockError)??;
        Ok(size >= self.max_capacity())
    }

    /// Get the maximum capacity of the memtable
    pub fn max_capacity(&self) -> usize {
        self.max_size_bytes
    }

    /// Force compaction of the memtable to disk
    pub async fn force_compaction(&self) -> io::Result<String> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::ForceCompaction(sender))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "channel closed"))?;

        receiver
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Worker thread did not respond"))?
    }

    /// Shut down the memtable worker task
    pub async fn shutdown(&self) -> io::Result<()> {
        // Send shutdown message
        let _ = self.sender.send(MemtableMessage::Shutdown).await;
        Ok(())
    }

    /// Insert a key-value pair into the memtable
    pub async fn insert(&self, key: K, value: V) -> Result<Option<V>, MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::Insert(key, value, sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        receiver.await.map_err(|_| MemtableError::LockError)?
    }

    /// Get a value from the memtable by key
    pub async fn get(&self, key: &K) -> Result<Option<V>, MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::Get(key.clone(), sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        receiver.await.map_err(|_| MemtableError::LockError)?
    }

    /// Remove a key-value pair from the memtable
    pub async fn remove(&self, key: &K) -> Result<Option<V>, MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::Remove(key.clone(), sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        receiver.await.map_err(|_| MemtableError::LockError)?
    }

    /// Get the number of entries in the memtable
    pub async fn len(&self) -> Result<usize, MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::Len(sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        receiver.await.map_err(|_| MemtableError::LockError)?
    }

    /// Check if the memtable is empty
    pub async fn is_empty(&self) -> Result<bool, MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::IsEmpty(sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        receiver.await.map_err(|_| MemtableError::LockError)?
    }

    /// Clear all entries from the memtable
    pub async fn clear(&self) -> Result<(), MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::Clear(sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        receiver.await.map_err(|_| MemtableError::LockError)?
    }

    /// Get the current size of the memtable in bytes
    pub async fn size_bytes(&self) -> Result<usize, MemtableError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(MemtableMessage::SizeBytes(sender))
            .await
            .map_err(|_| MemtableError::LockError)?;

        receiver.await.map_err(|_| MemtableError::LockError)?
    }

    /// Flush the memtable to an SSTable
    pub async fn flush_to_sstable(&self, _base_path: &str) -> io::Result<String> {
        // Force a compaction, which will create an SSTable
        self.force_compaction().await
    }
}

impl<K, V> Drop for AsyncMemtable<K, V> {
    fn drop(&mut self) {
        // Take the join handle to ensure the task is dropped when this struct is dropped
        if let Some(handle) = self.worker_task.take() {
            // Since we can't use .await in drop, we need to abort the task
            handle.abort();
        }
    }
}

// Define a type alias for an async string memtable for convenience
pub type AsyncStringMemtable = AsyncMemtable<String, Vec<u8>>;
