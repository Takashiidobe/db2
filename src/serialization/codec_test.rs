#[cfg(test)]
mod tests {
    use crate::serialization::codec;
    use std::io::Cursor;

    #[test]
    fn test_u8_round_trip() {
        let mut buf = Vec::new();
        codec::write_u8(&mut buf, 42).unwrap();
        assert_eq!(buf, vec![42]);

        let mut cursor = Cursor::new(buf);
        assert_eq!(codec::read_u8(&mut cursor).unwrap(), 42);
    }

    #[test]
    fn test_u16_round_trip() {
        let mut buf = Vec::new();
        codec::write_u16(&mut buf, 1000).unwrap();
        assert_eq!(buf.len(), 2);

        let mut cursor = Cursor::new(buf);
        assert_eq!(codec::read_u16(&mut cursor).unwrap(), 1000);
    }

    #[test]
    fn test_u32_round_trip() {
        let mut buf = Vec::new();
        codec::write_u32(&mut buf, 100000).unwrap();
        assert_eq!(buf.len(), 4);

        let mut cursor = Cursor::new(buf);
        assert_eq!(codec::read_u32(&mut cursor).unwrap(), 100000);
    }

    #[test]
    fn test_i64_round_trip() {
        let mut buf = Vec::new();
        codec::write_i64(&mut buf, -12345678901234).unwrap();
        assert_eq!(buf.len(), 8);

        let mut cursor = Cursor::new(buf);
        assert_eq!(codec::read_i64(&mut cursor).unwrap(), -12345678901234);
    }

    #[test]
    fn test_string_round_trip() {
        let mut buf = Vec::new();
        codec::write_string(&mut buf, "hello world").unwrap();
        // 4 bytes for length + 11 bytes for "hello world"
        assert_eq!(buf.len(), 15);

        let mut cursor = Cursor::new(buf);
        assert_eq!(codec::read_string(&mut cursor).unwrap(), "hello world");
    }

    #[test]
    fn test_empty_string() {
        let mut buf = Vec::new();
        codec::write_string(&mut buf, "").unwrap();
        assert_eq!(buf.len(), 4); // Just the length field

        let mut cursor = Cursor::new(buf);
        assert_eq!(codec::read_string(&mut cursor).unwrap(), "");
    }

    #[test]
    fn test_utf8_string() {
        let mut buf = Vec::new();
        let test_str = "Hello 世界 🌍";
        codec::write_string(&mut buf, test_str).unwrap();

        let mut cursor = Cursor::new(buf);
        assert_eq!(codec::read_string(&mut cursor).unwrap(), test_str);
    }

    #[test]
    fn test_read_truncated_data() {
        // Try to read u32 from incomplete data
        let buf = vec![1, 2]; // Only 2 bytes instead of 4
        let mut cursor = Cursor::new(buf);
        assert!(codec::read_u32(&mut cursor).is_err());
    }

    #[test]
    fn test_read_truncated_string() {
        let mut buf = Vec::new();
        codec::write_u32(&mut buf, 10).unwrap(); // Claim 10 bytes
        buf.extend_from_slice(b"short"); // But only provide 5 bytes

        let mut cursor = Cursor::new(buf);
        assert!(codec::read_string(&mut cursor).is_err());
    }
}
