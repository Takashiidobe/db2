use std::io::{self, Read, Write};

/// Write a u8 (1 byte)
pub fn write_u8<W: Write>(writer: &mut W, value: u8) -> io::Result<()> {
    writer.write_all(&[value])
}

/// Read a u8 (1 byte)
pub fn read_u8<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

/// Write a u16 (2 bytes, little-endian)
pub fn write_u16<W: Write>(writer: &mut W, value: u16) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

/// Read a u16 (2 bytes, little-endian)
pub fn read_u16<R: Read>(reader: &mut R) -> io::Result<u16> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

/// Write a u32 (4 bytes, little-endian)
pub fn write_u32<W: Write>(writer: &mut W, value: u32) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

/// Read a u32 (4 bytes, little-endian)
pub fn read_u32<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Write an i32 (4 bytes, little-endian)
pub fn write_i32<W: Write>(writer: &mut W, value: i32) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

/// Read an i32 (4 bytes, little-endian)
pub fn read_i32<R: Read>(reader: &mut R) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(i32::from_le_bytes(buf))
}

/// Write an i64 (8 bytes, little-endian)
pub fn write_i64<W: Write>(writer: &mut W, value: i64) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

/// Read an i64 (8 bytes, little-endian)
pub fn read_i64<R: Read>(reader: &mut R) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

/// Write a u64 (8 bytes, little-endian)
pub fn write_u64<W: Write>(writer: &mut W, value: u64) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

/// Read a u64 (8 bytes, little-endian)
pub fn read_u64<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

/// Write an i128 (16 bytes, little-endian)
pub fn write_i128<W: Write>(writer: &mut W, value: i128) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

/// Read an i128 (16 bytes, little-endian)
pub fn read_i128<R: Read>(reader: &mut R) -> io::Result<i128> {
    let mut buf = [0u8; 16];
    reader.read_exact(&mut buf)?;
    Ok(i128::from_le_bytes(buf))
}

/// Write an f64 (8 bytes, little-endian)
pub fn write_f64<W: Write>(writer: &mut W, value: f64) -> io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

/// Read an f64 (8 bytes, little-endian)
pub fn read_f64<R: Read>(reader: &mut R) -> io::Result<f64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(f64::from_le_bytes(buf))
}

/// Write a string (4 bytes length + UTF-8 data)
pub fn write_string<W: Write>(writer: &mut W, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();
    let len = bytes.len() as u32;
    write_u32(writer, len)?;
    writer.write_all(bytes)
}

/// Read a string (4 bytes length + UTF-8 data)
pub fn read_string<R: Read>(reader: &mut R) -> io::Result<String> {
    let len = read_u32(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
