use crate::serialization::codec;
use crate::table::RowId;
use crate::types::Value;
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};

pub type TxnId = u64;

/// WAL record types for transactional logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    Begin {
        txn_id: TxnId,
    },
    Commit {
        txn_id: TxnId,
    },
    Rollback {
        txn_id: TxnId,
    },
    Insert {
        txn_id: TxnId,
        table: String,
        row_id: RowId,
        values: Vec<Value>,
    },
    Update {
        txn_id: TxnId,
        table: String,
        row_id: RowId,
        before: Vec<Value>,
        after: Vec<Value>,
    },
    Delete {
        txn_id: TxnId,
        table: String,
        row_id: RowId,
        values: Vec<Value>,
    },
}

#[derive(Debug)]
pub enum WalError {
    IoError(io::Error),
    InvalidRecordTag(u8),
    InvalidValueTag(u8),
}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        WalError::IoError(err)
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::IoError(e) => write!(f, "I/O error: {}", e),
            WalError::InvalidRecordTag(tag) => write!(f, "Invalid WAL record tag: {}", tag),
            WalError::InvalidValueTag(tag) => write!(f, "Invalid WAL value tag: {}", tag),
        }
    }
}

impl std::error::Error for WalError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum RecordTag {
    Begin = 0,
    Commit = 1,
    Rollback = 2,
    Insert = 3,
    Update = 4,
    Delete = 5,
}

impl RecordTag {
    fn from_u8(value: u8) -> Result<Self, WalError> {
        match value {
            0 => Ok(RecordTag::Begin),
            1 => Ok(RecordTag::Commit),
            2 => Ok(RecordTag::Rollback),
            3 => Ok(RecordTag::Insert),
            4 => Ok(RecordTag::Update),
            5 => Ok(RecordTag::Delete),
            _ => Err(WalError::InvalidRecordTag(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ValueTag {
    Integer = 0,
    Unsigned = 1,
    Float = 2,
    Boolean = 3,
    String = 4,
    Null = 5,
    Date = 6,
    Timestamp = 7,
    Decimal = 8,
}

impl ValueTag {
    fn from_u8(value: u8) -> Result<Self, WalError> {
        match value {
            0 => Ok(ValueTag::Integer),
            1 => Ok(ValueTag::Unsigned),
            2 => Ok(ValueTag::Float),
            3 => Ok(ValueTag::Boolean),
            4 => Ok(ValueTag::String),
            5 => Ok(ValueTag::Null),
            6 => Ok(ValueTag::Date),
            7 => Ok(ValueTag::Timestamp),
            8 => Ok(ValueTag::Decimal),
            _ => Err(WalError::InvalidValueTag(value)),
        }
    }

    fn from_value(value: &Value) -> Self {
        match value {
            Value::Integer(_) => ValueTag::Integer,
            Value::Unsigned(_) => ValueTag::Unsigned,
            Value::Float(_) => ValueTag::Float,
            Value::Boolean(_) => ValueTag::Boolean,
            Value::String(_) => ValueTag::String,
            Value::Null => ValueTag::Null,
            Value::Date(_) => ValueTag::Date,
            Value::Timestamp(_) => ValueTag::Timestamp,
            Value::Decimal(_) => ValueTag::Decimal,
        }
    }
}

impl WalRecord {
    /// Serialize a WAL record into bytes (length prefixing happens at the file layer).
    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        match self {
            WalRecord::Begin { txn_id } => {
                codec::write_u8(&mut buf, RecordTag::Begin as u8)?;
                codec::write_u64(&mut buf, *txn_id)?;
            }
            WalRecord::Commit { txn_id } => {
                codec::write_u8(&mut buf, RecordTag::Commit as u8)?;
                codec::write_u64(&mut buf, *txn_id)?;
            }
            WalRecord::Rollback { txn_id } => {
                codec::write_u8(&mut buf, RecordTag::Rollback as u8)?;
                codec::write_u64(&mut buf, *txn_id)?;
            }
            WalRecord::Insert {
                txn_id,
                table,
                row_id,
                values,
            } => {
                codec::write_u8(&mut buf, RecordTag::Insert as u8)?;
                codec::write_u64(&mut buf, *txn_id)?;
                write_table(&mut buf, table)?;
                write_row_id(&mut buf, row_id)?;
                write_values(&mut buf, values)?;
            }
            WalRecord::Update {
                txn_id,
                table,
                row_id,
                before,
                after,
            } => {
                codec::write_u8(&mut buf, RecordTag::Update as u8)?;
                codec::write_u64(&mut buf, *txn_id)?;
                write_table(&mut buf, table)?;
                write_row_id(&mut buf, row_id)?;
                write_values(&mut buf, before)?;
                write_values(&mut buf, after)?;
            }
            WalRecord::Delete {
                txn_id,
                table,
                row_id,
                values,
            } => {
                codec::write_u8(&mut buf, RecordTag::Delete as u8)?;
                codec::write_u64(&mut buf, *txn_id)?;
                write_table(&mut buf, table)?;
                write_row_id(&mut buf, row_id)?;
                write_values(&mut buf, values)?;
            }
        }

        Ok(buf)
    }

    /// Deserialize a WAL record from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, WalError> {
        let mut cursor = Cursor::new(bytes);
        let tag = RecordTag::from_u8(codec::read_u8(&mut cursor)?)?;
        let txn_id = codec::read_u64(&mut cursor)?;

        let record = match tag {
            RecordTag::Begin => WalRecord::Begin { txn_id },
            RecordTag::Commit => WalRecord::Commit { txn_id },
            RecordTag::Rollback => WalRecord::Rollback { txn_id },
            RecordTag::Insert => {
                let table = read_table(&mut cursor)?;
                let row_id = read_row_id(&mut cursor)?;
                let values = read_values(&mut cursor)?;
                WalRecord::Insert {
                    txn_id,
                    table,
                    row_id,
                    values,
                }
            }
            RecordTag::Update => {
                let table = read_table(&mut cursor)?;
                let row_id = read_row_id(&mut cursor)?;
                let before = read_values(&mut cursor)?;
                let after = read_values(&mut cursor)?;
                WalRecord::Update {
                    txn_id,
                    table,
                    row_id,
                    before,
                    after,
                }
            }
            RecordTag::Delete => {
                let table = read_table(&mut cursor)?;
                let row_id = read_row_id(&mut cursor)?;
                let values = read_values(&mut cursor)?;
                WalRecord::Delete {
                    txn_id,
                    table,
                    row_id,
                    values,
                }
            }
        };

        Ok(record)
    }
}

pub struct WalFile {
    path: PathBuf,
}

impl WalFile {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn append(&self, record: &WalRecord) -> io::Result<()> {
        let data = record.serialize()?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        codec::write_u32(&mut file, data.len() as u32)?;
        file.write_all(&data)?;
        file.flush()
    }

    pub fn read_all(&self) -> io::Result<Vec<WalRecord>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let mut file = std::fs::File::open(&self.path)?;
        let mut records = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            }

            let len = u32::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            if let Err(err) = file.read_exact(&mut data) {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(err);
            }
            let record = WalRecord::deserialize(&data).map_err(to_io_error)?;
            records.push(record);
        }

        Ok(records)
    }

    pub fn truncate(&self) -> io::Result<()> {
        if !self.path.exists() {
            return Ok(());
        }

        let file = std::fs::OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(0)
    }
}

fn to_io_error(err: WalError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn write_table(buf: &mut Vec<u8>, table: &str) -> io::Result<()> {
    codec::write_string(buf, table)
}

fn read_table(cursor: &mut Cursor<&[u8]>) -> Result<String, WalError> {
    Ok(codec::read_string(cursor)?)
}

fn write_row_id(buf: &mut Vec<u8>, row_id: &RowId) -> io::Result<()> {
    codec::write_u32(buf, row_id.page_id())?;
    codec::write_u16(buf, row_id.slot_id())
}

fn read_row_id(cursor: &mut Cursor<&[u8]>) -> Result<RowId, WalError> {
    let page_id = codec::read_u32(cursor)?;
    let slot_id = codec::read_u16(cursor)?;
    Ok(RowId::new(page_id, slot_id))
}

fn write_values(buf: &mut Vec<u8>, values: &[Value]) -> io::Result<()> {
    codec::write_u32(buf, values.len() as u32)?;
    for value in values {
        write_value(buf, value)?;
    }
    Ok(())
}

fn read_values(cursor: &mut Cursor<&[u8]>) -> Result<Vec<Value>, WalError> {
    let count = codec::read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_value(cursor)?);
    }
    Ok(values)
}

fn write_value(buf: &mut Vec<u8>, value: &Value) -> io::Result<()> {
    let tag = ValueTag::from_value(value) as u8;
    codec::write_u8(buf, tag)?;
    match value {
        Value::Integer(i) => codec::write_i64(buf, *i),
        Value::Unsigned(u) => codec::write_u64(buf, *u),
        Value::Float(fv) => codec::write_f64(buf, *fv),
        Value::Boolean(b) => codec::write_u8(buf, *b as u8),
        Value::String(s) => codec::write_string(buf, s),
        Value::Null => Ok(()),
        Value::Date(d) => {
            codec::write_i32(buf, d.year)?;
            codec::write_u8(buf, d.month)?;
            codec::write_u8(buf, d.day)
        }
        Value::Timestamp(t) => {
            codec::write_i32(buf, t.year)?;
            codec::write_u8(buf, t.month)?;
            codec::write_u8(buf, t.day)?;
            codec::write_u8(buf, t.hour)?;
            codec::write_u8(buf, t.minute)?;
            codec::write_u8(buf, t.second)
        }
        Value::Decimal(d) => {
            codec::write_i128(buf, d.value)?;
            codec::write_u32(buf, d.scale)
        }
    }
}

fn read_value(cursor: &mut Cursor<&[u8]>) -> Result<Value, WalError> {
    let tag = ValueTag::from_u8(codec::read_u8(cursor)?)?;
    let value = match tag {
        ValueTag::Integer => Value::Integer(codec::read_i64(cursor)?),
        ValueTag::Unsigned => Value::Unsigned(codec::read_u64(cursor)?),
        ValueTag::Float => Value::Float(codec::read_f64(cursor)?),
        ValueTag::Boolean => Value::Boolean(codec::read_u8(cursor)? != 0),
        ValueTag::String => Value::String(codec::read_string(cursor)?),
        ValueTag::Null => Value::Null,
        ValueTag::Date => {
            let year = codec::read_i32(cursor)?;
            let month = codec::read_u8(cursor)?;
            let day = codec::read_u8(cursor)?;
            Value::Date(crate::types::Date { year, month, day })
        }
        ValueTag::Timestamp => {
            let year = codec::read_i32(cursor)?;
            let month = codec::read_u8(cursor)?;
            let day = codec::read_u8(cursor)?;
            let hour = codec::read_u8(cursor)?;
            let minute = codec::read_u8(cursor)?;
            let second = codec::read_u8(cursor)?;
            Value::Timestamp(crate::types::Timestamp {
                year,
                month,
                day,
                hour,
                minute,
                second,
            })
        }
        ValueTag::Decimal => {
            let value = codec::read_i128(cursor)?;
            let scale = codec::read_u32(cursor)?;
            Value::Decimal(crate::types::Decimal { value, scale })
        }
    };
    Ok(value)
}
