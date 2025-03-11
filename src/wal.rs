use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const WAL_MAGIC: u64 = 0x4C534D_57414C30; // "LSM-WAL0" in hex
const WAL_VERSION: u32 = 1;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    InvalidMagic,
    InvalidVersion,
    InvalidRecord,
    Corrupted,
}

impl From<io::Error> for WalError {
    fn from(error: io::Error) -> Self {
        WalError::Io(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum RecordType {
    Insert = 1,
    Delete = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RecordType::Insert),
            2 => Ok(RecordType::Delete),
            _ => Err(WalError::InvalidRecord),
        }
    }
}

/// A record in the Write-Ahead Log
#[derive(Debug)]
pub struct WalRecord {
    pub record_type: RecordType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // None for Delete records
}

/// Write-Ahead Log implementation
pub struct WriteAheadLog {
    file: BufWriter<File>,
    path: String,
}

impl WriteAheadLog {
    /// Creates a new WAL file or opens an existing one
    pub fn new(path: &str) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Preserve existing content
            .open(path)?;

        let mut wal = WriteAheadLog {
            file: BufWriter::new(file),
            path: path.to_string(),
        };

        // If the file is empty, write the header
        if Path::new(path).metadata()?.len() == 0 {
            wal.write_header()?;
        } else {
            // Verify the header of existing file
            wal.verify_header()?;
            // Seek to end for appending
            wal.file.seek(SeekFrom::End(0))?;
        }

        Ok(wal)
    }

    /// Writes the WAL file header
    fn write_header(&mut self) -> Result<(), WalError> {
        self.file.write_all(&WAL_MAGIC.to_le_bytes())?;
        self.file.write_all(&WAL_VERSION.to_le_bytes())?;
        self.file.flush()?;
        Ok(())
    }

    /// Verifies the WAL file header
    fn verify_header(&mut self) -> Result<(), WalError> {
        let mut reader = BufReader::new(self.file.get_ref());

        let mut magic_bytes = [0u8; 8];
        reader.read_exact(&mut magic_bytes)?;
        let magic = u64::from_le_bytes(magic_bytes);

        if magic != WAL_MAGIC {
            return Err(WalError::InvalidMagic);
        }

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);

        if version != WAL_VERSION {
            return Err(WalError::InvalidVersion);
        }

        Ok(())
    }

    /// Appends a record to the WAL
    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        // Write record type
        self.file.write_all(&[record.record_type as u8])?;

        // Write key length and key
        let key_len = record.key.len() as u32;
        self.file.write_all(&key_len.to_le_bytes())?;
        self.file.write_all(&record.key)?;

        // Write value for Insert records
        match record.record_type {
            RecordType::Insert => {
                let value = record.value.as_ref().ok_or(WalError::InvalidRecord)?;
                let value_len = value.len() as u32;
                self.file.write_all(&value_len.to_le_bytes())?;
                self.file.write_all(value)?;
            }
            RecordType::Delete => {
                // No value to write for delete records
            }
        }

        // Ensure the record is written to disk
        self.file.flush()?;

        Ok(())
    }

    /// Reads all records from the WAL
    pub fn read_all(&self) -> Result<Vec<WalRecord>, WalError> {
        let mut reader = BufReader::new(File::open(&self.path)?);
        let mut records = Vec::new();

        // Skip header
        reader.seek(SeekFrom::Start(12))?; // 8 bytes magic + 4 bytes version

        while let Ok(record_type_byte) = reader.read_u8() {
            let record_type = RecordType::try_from(record_type_byte)?;

            // Read key
            let mut key_len_bytes = [0u8; 4];
            reader.read_exact(&mut key_len_bytes)?;
            let key_len = u32::from_le_bytes(key_len_bytes) as usize;

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            // Read value for Insert records
            let value = match record_type {
                RecordType::Insert => {
                    let mut value_len_bytes = [0u8; 4];
                    reader.read_exact(&mut value_len_bytes)?;
                    let value_len = u32::from_le_bytes(value_len_bytes) as usize;

                    let mut value = vec![0u8; value_len];
                    reader.read_exact(&mut value)?;
                    Some(value)
                }
                RecordType::Delete => None,
            };

            records.push(WalRecord {
                record_type,
                key,
                value,
            });
        }

        Ok(records)
    }

    /// Truncates the WAL file, keeping only the header
    pub fn truncate(&mut self) -> Result<(), WalError> {
        self.file.seek(SeekFrom::Start(12))?; // 8 bytes magic + 4 bytes version
        self.file.get_mut().set_len(12)?;
        self.file.flush()?;
        Ok(())
    }
}

// Helper trait for reading primitive types
trait ReadExt: Read {
    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}

impl<R: Read> ReadExt for R {}
