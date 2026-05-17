//! Write cursor for binary data output.

use byteorder::{ByteOrder, LE, WriteBytesExt};
use std::io::{self, Write};

/// A cursor for writing binary data with little-endian encoding.
///
/// This is the write counterpart to the read `Cursor`.
pub struct WriteCursor<W: Write> {
    writer: W,
    pos: u64,
}

impl<W: Write> WriteCursor<W> {
    /// Create a new WriteCursor wrapping the given writer.
    pub fn new(writer: W) -> Self {
        Self { writer, pos: 0 }
    }

    /// Get the current position (number of bytes written).
    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Get the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// Get a mutable reference to the inner writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Write raw bytes.
    pub fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)?;
        self.pos += data.len() as u64;
        Ok(())
    }

    /// Write a little-endian u32.
    pub fn write_u32(&mut self, val: u32) -> io::Result<()> {
        self.writer.write_u32::<LE>(val)?;
        self.pos += 4;
        Ok(())
    }

    /// Write a little-endian u64.
    pub fn write_u64(&mut self, val: u64) -> io::Result<()> {
        self.writer.write_u64::<LE>(val)?;
        self.pos += 8;
        Ok(())
    }

    /// Write a ROS time (seconds + nanoseconds as two u32s).
    /// Input is nanoseconds since UNIX epoch.
    pub fn write_time(&mut self, time_ns: u64) -> io::Result<()> {
        let secs = (time_ns / 1_000_000_000) as u32;
        let nsecs = (time_ns % 1_000_000_000) as u32;
        self.write_u32(secs)?;
        self.write_u32(nsecs)?;
        Ok(())
    }

    /// Write a length-prefixed chunk of data.
    pub fn write_chunk(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_u32(data.len() as u32)?;
        self.write_bytes(data)?;
        Ok(())
    }

    /// Write a header field in the format: len(name=value) | name=value
    pub fn write_header_field(&mut self, name: &str, value: &[u8]) -> io::Result<()> {
        let field_len = name.len() + 1 + value.len(); // +1 for '='
        self.write_u32(field_len as u32)?;
        self.write_bytes(name.as_bytes())?;
        self.write_bytes(b"=")?;
        self.write_bytes(value)?;
        Ok(())
    }

    /// Write a header field with a u32 value.
    pub fn write_header_field_u32(&mut self, name: &str, value: u32) -> io::Result<()> {
        let mut buf = [0u8; 4];
        LE::write_u32(&mut buf, value);
        self.write_header_field(name, &buf)
    }

    /// Write a header field with a u64 value.
    pub fn write_header_field_u64(&mut self, name: &str, value: u64) -> io::Result<()> {
        let mut buf = [0u8; 8];
        LE::write_u64(&mut buf, value);
        self.write_header_field(name, &buf)
    }

    /// Write a header field with a time value (nanoseconds since UNIX epoch).
    pub fn write_header_field_time(&mut self, name: &str, time_ns: u64) -> io::Result<()> {
        let secs = (time_ns / 1_000_000_000) as u32;
        let nsecs = (time_ns % 1_000_000_000) as u32;
        let mut buf = [0u8; 8];
        LE::write_u32(&mut buf[0..4], secs);
        LE::write_u32(&mut buf[4..8], nsecs);
        self.write_header_field(name, &buf)
    }

    /// Write a header field with a string value.
    pub fn write_header_field_str(&mut self, name: &str, value: &str) -> io::Result<()> {
        self.write_header_field(name, value.as_bytes())
    }

    /// Write a header field with an op code (single byte).
    pub fn write_header_field_op(&mut self, op: u8) -> io::Result<()> {
        self.write_header_field("op", &[op])
    }

    /// Write a complete record (header + data).
    ///
    /// Format: header_len (4 bytes) | header | data_len (4 bytes) | data
    pub fn write_record(&mut self, header: &[u8], data: &[u8]) -> io::Result<()> {
        self.write_chunk(header)?;
        self.write_chunk(data)?;
        Ok(())
    }

    /// Flush the underlying writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + io::Seek> WriteCursor<W> {
    /// Seek to an absolute position.
    pub fn seek(&mut self, pos: u64) -> io::Result<()> {
        self.writer.seek(io::SeekFrom::Start(pos))?;
        self.pos = pos;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_bytes() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_bytes(b"hello").unwrap();
        assert_eq!(cursor.pos(), 5);
        assert_eq!(cursor.into_inner(), b"hello");
    }

    #[test]
    fn test_write_u32() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_u32(0x12345678).unwrap();
        assert_eq!(cursor.pos(), 4);
        // Little-endian: least significant byte first
        assert_eq!(cursor.into_inner(), vec![0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_write_u64() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_u64(0x123456789ABCDEF0).unwrap();
        assert_eq!(cursor.pos(), 8);
        // Little-endian
        assert_eq!(
            cursor.into_inner(),
            vec![0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]
        );
    }

    #[test]
    fn test_write_time() {
        let mut cursor = WriteCursor::new(Vec::new());
        // 1.5 seconds = 1_500_000_000 nanoseconds
        let time_ns = 1_500_000_000u64;
        cursor.write_time(time_ns).unwrap();
        assert_eq!(cursor.pos(), 8);

        let data = cursor.into_inner();
        // secs = 1, nsecs = 500_000_000
        // secs in LE: [0x01, 0x00, 0x00, 0x00]
        // nsecs = 500_000_000 = 0x1DCD6500 in LE: [0x00, 0x65, 0xCD, 0x1D]
        assert_eq!(data[0..4], [0x01, 0x00, 0x00, 0x00]);
        assert_eq!(data[4..8], [0x00, 0x65, 0xCD, 0x1D]);
    }

    #[test]
    fn test_write_chunk() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_chunk(b"test").unwrap();
        assert_eq!(cursor.pos(), 8); // 4 bytes length + 4 bytes data

        let data = cursor.into_inner();
        // Length = 4 in LE
        assert_eq!(data[0..4], [0x04, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..8], b"test");
    }

    #[test]
    fn test_write_header_field() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_header_field("name", b"value").unwrap();

        let data = cursor.into_inner();
        // Field length = len("name=value") = 10
        assert_eq!(data[0..4], [0x0A, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..], b"name=value");
    }

    #[test]
    fn test_write_header_field_u32() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_header_field_u32("count", 42).unwrap();

        let data = cursor.into_inner();
        // Field length = len("count=") + 4 = 10
        assert_eq!(data[0..4], [0x0A, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..10], b"count=");
        // 42 in LE
        assert_eq!(data[10..14], [0x2A, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_write_header_field_u64() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_header_field_u64("offset", 0x100).unwrap();

        let data = cursor.into_inner();
        // Field length = len("offset=") + 8 = 15
        assert_eq!(data[0..4], [0x0F, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..11], b"offset=");
        // 0x100 in LE
        assert_eq!(
            data[11..19],
            [0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn test_write_header_field_time() {
        let mut cursor = WriteCursor::new(Vec::new());
        // 2 seconds + 123456789 nanoseconds
        let time_ns = 2_123_456_789u64;
        cursor.write_header_field_time("time", time_ns).unwrap();

        let data = cursor.into_inner();
        // Field length = len("time=") + 8 = 13
        assert_eq!(data[0..4], [0x0D, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..9], b"time=");
        // secs = 2, nsecs = 123456789
        assert_eq!(data[9..13], [0x02, 0x00, 0x00, 0x00]);
        // 123456789 = 0x075BCD15
        assert_eq!(data[13..17], [0x15, 0xCD, 0x5B, 0x07]);
    }

    #[test]
    fn test_write_header_field_str() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_header_field_str("topic", "/test").unwrap();

        let data = cursor.into_inner();
        // Field length = len("topic=/test") = 11
        assert_eq!(data[0..4], [0x0B, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..], b"topic=/test");
    }

    #[test]
    fn test_write_header_field_op() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_header_field_op(0x03).unwrap();

        let data = cursor.into_inner();
        // Field length = len("op=") + 1 = 4
        assert_eq!(data[0..4], [0x04, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..7], b"op=");
        assert_eq!(data[7], 0x03);
    }

    #[test]
    fn test_write_record() {
        let mut cursor = WriteCursor::new(Vec::new());
        cursor.write_record(b"header", b"data").unwrap();

        let data = cursor.into_inner();
        // header_len (4) + header (6) + data_len (4) + data (4) = 18 bytes
        assert_eq!(data.len(), 18);
        // Header length = 6
        assert_eq!(data[0..4], [0x06, 0x00, 0x00, 0x00]);
        assert_eq!(&data[4..10], b"header");
        // Data length = 4
        assert_eq!(data[10..14], [0x04, 0x00, 0x00, 0x00]);
        assert_eq!(&data[14..18], b"data");
    }

    #[test]
    fn test_seek() {
        use std::io::Cursor;
        let mut cursor = WriteCursor::new(Cursor::new(vec![0u8; 20]));
        cursor.write_bytes(b"hello").unwrap();
        assert_eq!(cursor.pos(), 5);

        cursor.seek(10).unwrap();
        assert_eq!(cursor.pos(), 10);

        cursor.write_bytes(b"world").unwrap();
        assert_eq!(cursor.pos(), 15);

        let data = cursor.into_inner().into_inner();
        assert_eq!(&data[0..5], b"hello");
        assert_eq!(&data[10..15], b"world");
    }

    #[test]
    fn test_pos_tracking() {
        let mut cursor = WriteCursor::new(Vec::new());
        assert_eq!(cursor.pos(), 0);

        cursor.write_u32(0).unwrap();
        assert_eq!(cursor.pos(), 4);

        cursor.write_u64(0).unwrap();
        assert_eq!(cursor.pos(), 12);

        cursor.write_bytes(b"test").unwrap();
        assert_eq!(cursor.pos(), 16);
    }
}
