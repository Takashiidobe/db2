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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_u8_round_trip() {
        let mut buf = Vec::new();
        write_u8(&mut buf, 42).unwrap();
        assert_eq!(buf, vec![42]);

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_u8(&mut cursor).unwrap(), 42);
    }

    #[test]
    fn test_u16_round_trip() {
        let mut buf = Vec::new();
        write_u16(&mut buf, 1000).unwrap();
        assert_eq!(buf.len(), 2);

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_u16(&mut cursor).unwrap(), 1000);
    }

    #[test]
    fn test_u32_round_trip() {
        let mut buf = Vec::new();
        write_u32(&mut buf, 100000).unwrap();
        assert_eq!(buf.len(), 4);

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_u32(&mut cursor).unwrap(), 100000);
    }

    #[test]
    fn test_i64_round_trip() {
        let mut buf = Vec::new();
        write_i64(&mut buf, -12345678901234).unwrap();
        assert_eq!(buf.len(), 8);

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_i64(&mut cursor).unwrap(), -12345678901234);
    }

    #[test]
    fn test_string_round_trip() {
        let mut buf = Vec::new();
        write_string(&mut buf, "hello world").unwrap();
        // 4 bytes for length + 11 bytes for "hello world"
        assert_eq!(buf.len(), 15);

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_string(&mut cursor).unwrap(), "hello world");
    }

    #[test]
    fn test_empty_string() {
        let mut buf = Vec::new();
        write_string(&mut buf, "").unwrap();
        assert_eq!(buf.len(), 4); // Just the length field

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_string(&mut cursor).unwrap(), "");
    }

    #[test]
    fn test_utf8_string() {
        let mut buf = Vec::new();
        let test_str = "Hello 世界 🌍";
        write_string(&mut buf, test_str).unwrap();

        let mut cursor = Cursor::new(buf);
        assert_eq!(read_string(&mut cursor).unwrap(), test_str);
    }

    #[test]
    fn test_read_truncated_data() {
        // Try to read u32 from incomplete data
        let buf = vec![1, 2]; // Only 2 bytes instead of 4
        let mut cursor = Cursor::new(buf);
        assert!(read_u32(&mut cursor).is_err());
    }

    #[test]
    fn test_read_truncated_string() {
        let mut buf = Vec::new();
        write_u32(&mut buf, 10).unwrap(); // Claim 10 bytes
        buf.extend_from_slice(b"short"); // But only provide 5 bytes

        let mut cursor = Cursor::new(buf);
        assert!(read_string(&mut cursor).is_err());
    }
}
