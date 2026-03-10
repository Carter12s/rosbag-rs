//! Record serialization for writing rosbag files.

use super::WriteCursor;
use crate::record_types::Compression;
use std::io::{self, Write};

/// Op codes for record types
pub mod op {
    pub const MESSAGE_DATA: u8 = 0x02;
    pub const BAG_HEADER: u8 = 0x03;
    pub const INDEX_DATA: u8 = 0x04;
    pub const CHUNK: u8 = 0x05;
    pub const CHUNK_INFO: u8 = 0x06;
    pub const CONNECTION: u8 = 0x07;
}

/// The bag header record size (padded with spaces).
pub const BAG_HEADER_SIZE: u64 = 4096;

/// Build a header as a Vec<u8> using a closure.
fn build_header<F>(build_fn: F) -> io::Result<Vec<u8>>
where
    F: FnOnce(&mut WriteCursor<Vec<u8>>) -> io::Result<()>,
{
    let mut cursor = WriteCursor::new(Vec::new());
    build_fn(&mut cursor)?;
    Ok(cursor.into_inner())
}

/// Write the bag header record.
///
/// The bag header is padded to 4096 bytes to allow updating index_pos later.
pub fn write_bag_header<W: Write>(
    cursor: &mut WriteCursor<W>,
    index_pos: u64,
    conn_count: u32,
    chunk_count: u32,
) -> io::Result<()> {
    let header = build_header(|c| {
        c.write_header_field_op(op::BAG_HEADER)?;
        c.write_header_field_u64("index_pos", index_pos)?;
        c.write_header_field_u32("conn_count", conn_count)?;
        c.write_header_field_u32("chunk_count", chunk_count)?;
        Ok(())
    })?;

    // Calculate padding needed to make the record exactly BAG_HEADER_SIZE bytes
    // Record format: header_len (4) + header + data_len (4) + data
    // We need: 4 + header.len() + 4 + data.len() = BAG_HEADER_SIZE
    let data_len = BAG_HEADER_SIZE as usize - 4 - header.len() - 4;
    let padding = vec![b' '; data_len];

    cursor.write_record(&header, &padding)
}

/// Write a connection record.
#[allow(clippy::too_many_arguments)]
pub fn write_connection<W: Write>(
    cursor: &mut WriteCursor<W>,
    conn_id: u32,
    topic: &str,
    msg_type: &str,
    md5sum: &[u8; 16],
    message_definition: &str,
    caller_id: &str,
    latching: bool,
) -> io::Result<()> {
    // Build the record header
    let header = build_header(|c| {
        c.write_header_field_op(op::CONNECTION)?;
        c.write_header_field_u32("conn", conn_id)?;
        c.write_header_field_str("topic", topic)?;
        Ok(())
    })?;

    // Build the connection data (which is itself a header-style format)
    let data = build_header(|c| {
        c.write_header_field_str("topic", topic)?;
        c.write_header_field_str("type", msg_type)?;
        // md5sum is encoded as lowercase hex string
        let mut md5_hex = [0u8; 32];
        base16ct::lower::encode(md5sum, &mut md5_hex).expect("buffer is correct size");
        c.write_header_field("md5sum", &md5_hex)?;
        c.write_header_field_str("message_definition", message_definition)?;
        if !caller_id.is_empty() {
            c.write_header_field_str("callerid", caller_id)?;
        }
        c.write_header_field("latching", if latching { b"1" } else { b"0" })?;
        Ok(())
    })?;

    cursor.write_record(&header, &data)
}

/// Write a message data record.
pub fn write_message_data<W: Write>(
    cursor: &mut WriteCursor<W>,
    conn_id: u32,
    time_ns: u64,
    data: &[u8],
) -> io::Result<()> {
    let header = build_header(|c| {
        c.write_header_field_op(op::MESSAGE_DATA)?;
        c.write_header_field_u32("conn", conn_id)?;
        c.write_header_field_time("time", time_ns)?;
        Ok(())
    })?;

    cursor.write_record(&header, data)
}

/// Write a chunk record.
pub fn write_chunk<W: Write>(
    cursor: &mut WriteCursor<W>,
    compression: Compression,
    uncompressed_size: u32,
    compressed_data: &[u8],
) -> io::Result<()> {
    let compression_str = match compression {
        Compression::None => "none",
        Compression::Bzip2 => "bz2",
        Compression::Lz4 => "lz4",
    };

    let header = build_header(|c| {
        c.write_header_field_op(op::CHUNK)?;
        c.write_header_field_str("compression", compression_str)?;
        c.write_header_field_u32("size", uncompressed_size)?;
        Ok(())
    })?;

    cursor.write_record(&header, compressed_data)
}

/// An entry in the index data record.
#[derive(Debug, Clone)]
pub struct IndexDataEntry {
    /// Time at which the message was received (nanoseconds since UNIX epoch).
    pub time_ns: u64,
    /// Offset of message data record in uncompressed chunk data.
    pub offset: u32,
}

/// Write an index data record.
pub fn write_index_data<W: Write>(
    cursor: &mut WriteCursor<W>,
    conn_id: u32,
    entries: &[IndexDataEntry],
) -> io::Result<()> {
    let header = build_header(|c| {
        c.write_header_field_op(op::INDEX_DATA)?;
        c.write_header_field_u32("ver", 1)?;
        c.write_header_field_u32("conn", conn_id)?;
        c.write_header_field_u32("count", entries.len() as u32)?;
        Ok(())
    })?;

    // Build data: repeated (time, offset) entries
    // Each entry is 12 bytes: time (8 bytes) + offset (4 bytes)
    // Note: The data_len written by write_record serves as the 'n' field
    // that the reader expects (n = count * 12)
    let mut data_cursor = WriteCursor::new(Vec::new());
    for entry in entries {
        data_cursor.write_time(entry.time_ns)?;
        data_cursor.write_u32(entry.offset)?;
    }
    let data = data_cursor.into_inner();

    cursor.write_record(&header, &data)
}

/// An entry in the chunk info record.
#[derive(Debug, Clone)]
pub struct ChunkInfoEntry {
    /// Connection ID.
    pub conn_id: u32,
    /// Number of messages on this connection in the chunk.
    pub count: u32,
}

/// Write a chunk info record.
pub fn write_chunk_info<W: Write>(
    cursor: &mut WriteCursor<W>,
    chunk_pos: u64,
    start_time_ns: u64,
    end_time_ns: u64,
    entries: &[ChunkInfoEntry],
) -> io::Result<()> {
    let header = build_header(|c| {
        c.write_header_field_op(op::CHUNK_INFO)?;
        c.write_header_field_u32("ver", 1)?;
        c.write_header_field_u64("chunk_pos", chunk_pos)?;
        c.write_header_field_time("start_time", start_time_ns)?;
        c.write_header_field_time("end_time", end_time_ns)?;
        c.write_header_field_u32("count", entries.len() as u32)?;
        Ok(())
    })?;

    // Build data: repeated (conn_id, count) entries
    // Each entry is 8 bytes: conn_id (4 bytes) + count (4 bytes)
    // Note: The data_len written by write_record serves as the 'n' field
    // that the reader expects (n = count * 8)
    let mut data_cursor = WriteCursor::new(Vec::new());
    for entry in entries {
        data_cursor.write_u32(entry.conn_id)?;
        data_cursor.write_u32(entry.count)?;
    }
    let data = data_cursor.into_inner();

    cursor.write_record(&header, &data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find_header_field<'a>(header: &'a [u8], name: &str) -> Option<&'a [u8]> {
        let mut pos = 0;
        while pos + 4 <= header.len() {
            let field_len = u32::from_le_bytes([
                header[pos],
                header[pos + 1],
                header[pos + 2],
                header[pos + 3],
            ]) as usize;
            pos += 4;
            if pos + field_len > header.len() {
                break;
            }
            let field = &header[pos..pos + field_len];
            if let Some(eq_pos) = field.iter().position(|&b| b == b'=') {
                let field_name = &field[..eq_pos];
                if field_name == name.as_bytes() {
                    return Some(&field[eq_pos + 1..]);
                }
            }
            pos += field_len;
        }
        None
    }

    #[test]
    fn test_write_bag_header() {
        let mut cursor = WriteCursor::new(Vec::new());
        write_bag_header(&mut cursor, 12345, 3, 2).unwrap();

        let data = cursor.into_inner();
        // Should be exactly BAG_HEADER_SIZE bytes
        assert_eq!(data.len(), BAG_HEADER_SIZE as usize);

        // Parse header length
        let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header = &data[4..4 + header_len];

        // Check op field
        let op = find_header_field(header, "op").unwrap();
        assert_eq!(op, &[op::BAG_HEADER]);

        // Check index_pos
        let index_pos = find_header_field(header, "index_pos").unwrap();
        assert_eq!(u64::from_le_bytes(index_pos.try_into().unwrap()), 12345);

        // Check conn_count
        let conn_count = find_header_field(header, "conn_count").unwrap();
        assert_eq!(u32::from_le_bytes(conn_count.try_into().unwrap()), 3);

        // Check chunk_count
        let chunk_count = find_header_field(header, "chunk_count").unwrap();
        assert_eq!(u32::from_le_bytes(chunk_count.try_into().unwrap()), 2);
    }

    #[test]
    fn test_write_connection() {
        let mut cursor = WriteCursor::new(Vec::new());
        let md5sum = [
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        write_connection(
            &mut cursor,
            1,
            "/test_topic",
            "std_msgs/String",
            &md5sum,
            "string data",
            "",
            false,
        )
        .unwrap();

        let data = cursor.into_inner();

        // Parse header
        let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header = &data[4..4 + header_len];

        // Check op
        let op = find_header_field(header, "op").unwrap();
        assert_eq!(op, &[op::CONNECTION]);

        // Check conn ID
        let conn = find_header_field(header, "conn").unwrap();
        assert_eq!(u32::from_le_bytes(conn.try_into().unwrap()), 1);

        // Check topic in header
        let topic = find_header_field(header, "topic").unwrap();
        assert_eq!(topic, b"/test_topic");

        // Parse data section
        let data_start = 4 + header_len;
        let data_len = u32::from_le_bytes([
            data[data_start],
            data[data_start + 1],
            data[data_start + 2],
            data[data_start + 3],
        ]) as usize;
        let conn_data = &data[data_start + 4..data_start + 4 + data_len];

        // Check type in data
        let msg_type = find_header_field(conn_data, "type").unwrap();
        assert_eq!(msg_type, b"std_msgs/String");

        // Check md5sum (should be hex encoded)
        let md5 = find_header_field(conn_data, "md5sum").unwrap();
        assert_eq!(md5, b"123456789abcdef01122334455667788");
    }

    #[test]
    fn test_write_message_data() {
        let mut cursor = WriteCursor::new(Vec::new());
        let time_ns = 1_500_000_000u64; // 1.5 seconds
        write_message_data(&mut cursor, 2, time_ns, b"hello world").unwrap();

        let data = cursor.into_inner();

        // Parse header
        let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header = &data[4..4 + header_len];

        // Check op
        let op = find_header_field(header, "op").unwrap();
        assert_eq!(op, &[op::MESSAGE_DATA]);

        // Check conn
        let conn = find_header_field(header, "conn").unwrap();
        assert_eq!(u32::from_le_bytes(conn.try_into().unwrap()), 2);

        // Check time (secs=1, nsecs=500_000_000)
        let time = find_header_field(header, "time").unwrap();
        let secs = u32::from_le_bytes(time[0..4].try_into().unwrap());
        let nsecs = u32::from_le_bytes(time[4..8].try_into().unwrap());
        assert_eq!(secs, 1);
        assert_eq!(nsecs, 500_000_000);

        // Check message data
        let data_start = 4 + header_len;
        let data_len = u32::from_le_bytes([
            data[data_start],
            data[data_start + 1],
            data[data_start + 2],
            data[data_start + 3],
        ]) as usize;
        let msg_data = &data[data_start + 4..data_start + 4 + data_len];
        assert_eq!(msg_data, b"hello world");
    }

    #[test]
    fn test_write_chunk() {
        let mut cursor = WriteCursor::new(Vec::new());
        write_chunk(&mut cursor, Compression::None, 100, b"chunk data here").unwrap();

        let data = cursor.into_inner();

        // Parse header
        let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header = &data[4..4 + header_len];

        // Check op
        let op = find_header_field(header, "op").unwrap();
        assert_eq!(op, &[op::CHUNK]);

        // Check compression
        let compression = find_header_field(header, "compression").unwrap();
        assert_eq!(compression, b"none");

        // Check size (uncompressed)
        let size = find_header_field(header, "size").unwrap();
        assert_eq!(u32::from_le_bytes(size.try_into().unwrap()), 100);
    }

    #[test]
    fn test_write_index_data() {
        let mut cursor = WriteCursor::new(Vec::new());
        let entries = vec![
            IndexDataEntry {
                time_ns: 1_000_000_000,
                offset: 0,
            },
            IndexDataEntry {
                time_ns: 2_000_000_000,
                offset: 100,
            },
        ];
        write_index_data(&mut cursor, 5, &entries).unwrap();

        let data = cursor.into_inner();

        // Parse header
        let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header = &data[4..4 + header_len];

        // Check op
        let op = find_header_field(header, "op").unwrap();
        assert_eq!(op, &[op::INDEX_DATA]);

        // Check ver
        let ver = find_header_field(header, "ver").unwrap();
        assert_eq!(u32::from_le_bytes(ver.try_into().unwrap()), 1);

        // Check conn
        let conn = find_header_field(header, "conn").unwrap();
        assert_eq!(u32::from_le_bytes(conn.try_into().unwrap()), 5);

        // Check count
        let count = find_header_field(header, "count").unwrap();
        assert_eq!(u32::from_le_bytes(count.try_into().unwrap()), 2);
    }

    #[test]
    fn test_write_chunk_info() {
        let mut cursor = WriteCursor::new(Vec::new());
        let entries = vec![
            ChunkInfoEntry {
                conn_id: 0,
                count: 5,
            },
            ChunkInfoEntry {
                conn_id: 1,
                count: 3,
            },
        ];
        write_chunk_info(&mut cursor, 4096, 1_000_000_000, 2_000_000_000, &entries).unwrap();

        let data = cursor.into_inner();

        // Parse header
        let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let header = &data[4..4 + header_len];

        // Check op
        let op = find_header_field(header, "op").unwrap();
        assert_eq!(op, &[op::CHUNK_INFO]);

        // Check ver
        let ver = find_header_field(header, "ver").unwrap();
        assert_eq!(u32::from_le_bytes(ver.try_into().unwrap()), 1);

        // Check chunk_pos
        let chunk_pos = find_header_field(header, "chunk_pos").unwrap();
        assert_eq!(u64::from_le_bytes(chunk_pos.try_into().unwrap()), 4096);

        // Check count
        let count = find_header_field(header, "count").unwrap();
        assert_eq!(u32::from_le_bytes(count.try_into().unwrap()), 2);
    }
}
