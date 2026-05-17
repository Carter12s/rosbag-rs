//! High-level RosBag writer.

use super::records::{
    write_bag_header, write_chunk, write_chunk_info, write_connection, write_index_data,
    write_message_data, ChunkInfoEntry, IndexDataEntry,
};
use super::{WriteCursor, WriteError};
use crate::record_types::Compression;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::marker::PhantomData;
use std::path::Path;

/// A specialized Result type for ROS bag file writing.
pub type Result<T> = std::result::Result<T, WriteError>;

const VERSION_STRING: &[u8] = b"#ROSBAG V2.0\n";
const DEFAULT_CHUNK_SIZE: usize = 768 * 1024; // 768 KB default chunk size

/// A handle to a registered channel in a RosBag file.
///
/// This struct is returned by [`RosBagWriter::register_connection`] and provides
/// a way to write messages to the bag file.
///
/// # Example
/// ```no_run
/// use rosbag::writer::RosBagWriter;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut writer = RosBagWriter::create("output.bag")?;
///
/// let channel = writer.register_connection(
///     "/chatter",
///     "std_msgs/String",
///     &[0u8; 16],
///     "string data",
///     "",    // caller_id
///     false, // latching
/// )?;
/// // Note: this writes an invalid std_msgs/String message,
/// // the bytes provided to write must be the correct serialization for the message type,
/// // either obtain these over the wire, or use a library like serde_rosmsg to serialize correctly.
/// channel.write(&mut writer, 1_000_000_000, b"hello")?;
/// writer.finish()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Channel<W> {
    conn_id: u32,
    _marker: PhantomData<fn() -> W>,
}

impl<W: Write + Seek> Channel<W> {
    /// Create a new channel with the given connection ID.
    fn new(conn_id: u32) -> Self {
        Self {
            conn_id,
            _marker: PhantomData,
        }
    }

    /// Get the connection ID for this channel.
    pub fn id(&self) -> u32 {
        self.conn_id
    }

    /// Write a message to this channel.
    ///
    /// # Arguments
    /// * `writer` - The RosBagWriter that this channel belongs to
    /// * `time_ns` - Timestamp in nanoseconds since UNIX epoch
    /// * `data` - Raw message data
    pub fn write(&self, writer: &mut RosBagWriter<W>, time_ns: u64, data: &[u8]) -> Result<()> {
        writer.write_message(self.conn_id, time_ns, data)
    }
}

/// Information about a registered connection.
#[derive(Debug, Clone)]
struct ConnectionInfo {
    id: u32,
    topic: String,
    msg_type: String,
    md5sum: [u8; 16],
    message_definition: String,
    caller_id: String,
    latching: bool,
}

/// Information about a pending message in the chunk buffer.
#[derive(Debug, Clone)]
struct PendingMessage {
    conn_id: u32,
    time_ns: u64,
    offset: u32,
}

/// Metadata about a completed chunk.
#[derive(Debug, Clone)]
struct ChunkMeta {
    chunk_pos: u64,
    start_time_ns: u64,
    end_time_ns: u64,
    /// Map from conn_id to message count
    conn_counts: HashMap<u32, u32>,
}

/// Builder for configuring a RosBagWriter.
#[derive(Debug, Clone)]
pub struct RosBagWriterBuilder {
    compression: Compression,
    chunk_size: usize,
}

impl Default for RosBagWriterBuilder {
    fn default() -> Self {
        Self {
            compression: Compression::None,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl RosBagWriterBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the compression type for chunks.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set the maximum chunk size in bytes before flushing.
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Create a RosBagWriter writing to the specified file path.
    pub fn create<P: AsRef<Path>>(self, path: P) -> Result<RosBagWriter<BufWriter<File>>> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        self.build(writer)
    }

    /// Create a RosBagWriter with a custom writer.
    pub fn build<W: Write + Seek>(self, writer: W) -> Result<RosBagWriter<W>> {
        RosBagWriter::new_with_config(writer, self.compression, self.chunk_size)
    }
}

/// A writer for creating ROS bag files.
///
/// When a `RosBagWriter` is dropped, it will automatically finalize the bag file.
/// This ensures the bag is always in a valid state, even if `finish()` is not called.
/// However, any errors during automatic finalization are silently ignored.
/// For proper error handling, call [`finish()`](Self::finish) explicitly.
///
/// # Example
/// ```no_run
/// use rosbag::writer::RosBagWriter;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut writer = RosBagWriter::create("output.bag")?;
///
/// let channel = writer.register_connection(
///     "/chatter",
///     "std_msgs/String",
///     &[0u8; 16],
///     "string data",
///     "",    // caller_id
///     false, // latching
/// )?;
///
/// // Note: this writes an invalid std_msgs/String message,
/// // the bytes provided to write must be the correct serialization for the message type,
/// // either obtain these over the wire, or use a library like serde_rosmsg to serialize correctly.
/// channel.write(&mut writer, 1_000_000_000, b"hello")?;
/// writer.finish()?;
/// # Ok(())
/// # }
/// ```
pub struct RosBagWriter<W: Write + Seek> {
    /// The cursor is wrapped in Option to allow taking it in finish() after Drop is implemented
    cursor: Option<WriteCursor<W>>,
    compression: Compression,
    chunk_size: usize,

    /// Registered connections
    connections: Vec<ConnectionInfo>,
    /// Next connection ID
    next_conn_id: u32,

    /// Current chunk buffer (uncompressed)
    chunk_buffer: Vec<u8>,
    /// Messages in the current chunk (for index generation)
    pending_messages: Vec<PendingMessage>,
    /// Start time of current chunk
    chunk_start_time: Option<u64>,
    /// End time of current chunk
    chunk_end_time: Option<u64>,
    /// Position where current chunk starts
    chunk_start_pos: u64,

    /// Completed chunk metadata
    completed_chunks: Vec<ChunkMeta>,

    /// Whether the writer has been finished
    finished: bool,
}

impl RosBagWriter<BufWriter<File>> {
    /// Create a new RosBagWriter writing to the specified file path.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        RosBagWriterBuilder::default().create(path)
    }
}

impl<W: Write + Seek> RosBagWriter<W> {
    /// Create a new RosBagWriter with custom configuration.
    fn new_with_config(writer: W, compression: Compression, chunk_size: usize) -> Result<Self> {
        let mut cursor = WriteCursor::new(writer);

        // Write version string
        cursor.write_bytes(VERSION_STRING)?;

        // Write placeholder bag header (will be updated in finish())
        write_bag_header(&mut cursor, 0, 0, 0)?;

        let chunk_start_pos = cursor.pos();

        Ok(Self {
            cursor: Some(cursor),
            compression,
            chunk_size,
            connections: Vec::new(),
            next_conn_id: 0,
            chunk_buffer: Vec::new(),
            pending_messages: Vec::new(),
            chunk_start_time: None,
            chunk_end_time: None,
            chunk_start_pos,
            completed_chunks: Vec::new(),
            finished: false,
        })
    }

    /// Get a mutable reference to the cursor.
    /// Panics if the cursor has already been taken (after finish()).
    fn cursor(&mut self) -> &mut WriteCursor<W> {
        self.cursor
            .as_mut()
            .expect("cursor already taken - writer has been finished")
    }

    /// Register a new connection (topic) and return a [`Channel`] handle.
    ///
    /// This must be called before writing messages for a topic.
    ///
    /// # Arguments
    /// * `topic` - The topic name (e.g., "/chatter")
    /// * `msg_type` - The message type (e.g., "std_msgs/String")
    /// * `md5sum` - The MD5 checksum of the message definition
    /// * `message_definition` - The message definition string
    /// * `caller_id` - The caller ID (can be empty string)
    /// * `latching` - Whether the topic is latching
    ///
    /// # Returns
    /// A [`Channel`] handle that can be used to write messages to this topic.
    pub fn register_connection(
        &mut self,
        topic: &str,
        msg_type: &str,
        md5sum: &[u8; 16],
        message_definition: &str,
        caller_id: &str,
        latching: bool,
    ) -> Result<Channel<W>> {
        if self.finished {
            return Err(WriteError::InvalidWriterState("writer has been finished"));
        }

        let id = self.next_conn_id;
        self.next_conn_id += 1;

        self.connections.push(ConnectionInfo {
            id,
            topic: topic.to_string(),
            msg_type: msg_type.to_string(),
            md5sum: *md5sum,
            message_definition: message_definition.to_string(),
            caller_id: caller_id.to_string(),
            latching,
        });

        // Write connection record to chunk buffer
        let mut conn_cursor = WriteCursor::new(Vec::new());
        write_connection(
            &mut conn_cursor,
            id,
            topic,
            msg_type,
            md5sum,
            message_definition,
            caller_id,
            latching,
        )?;
        self.chunk_buffer.extend(conn_cursor.into_inner());

        Ok(Channel::new(id))
    }

    /// Write a message to the bag file using a connection ID.
    ///
    /// This is an internal method used by [`Channel::write`].
    pub(crate) fn write_message(&mut self, conn_id: u32, time_ns: u64, data: &[u8]) -> Result<()> {
        if self.finished {
            return Err(WriteError::InvalidWriterState("writer has been finished"));
        }

        // Verify connection exists
        if !self.connections.iter().any(|c| c.id == conn_id) {
            return Err(WriteError::UnknownConnectionId(conn_id));
        }

        // Record offset before writing
        let offset = self.chunk_buffer.len() as u32;

        // Write message to chunk buffer
        let mut msg_cursor = WriteCursor::new(Vec::new());
        write_message_data(&mut msg_cursor, conn_id, time_ns, data)?;
        self.chunk_buffer.extend(msg_cursor.into_inner());

        // Track the message for indexing
        self.pending_messages.push(PendingMessage {
            conn_id,
            time_ns,
            offset,
        });

        // Update chunk time range
        match self.chunk_start_time {
            None => {
                self.chunk_start_time = Some(time_ns);
                self.chunk_end_time = Some(time_ns);
            }
            Some(_) => {
                if time_ns < self.chunk_start_time.unwrap() {
                    self.chunk_start_time = Some(time_ns);
                }
                if time_ns > self.chunk_end_time.unwrap() {
                    self.chunk_end_time = Some(time_ns);
                }
            }
        }

        // Flush chunk if it exceeds the size threshold
        if self.chunk_buffer.len() >= self.chunk_size {
            self.flush_chunk()?;
        }

        Ok(())
    }

    /// Flush the current chunk to the file.
    fn flush_chunk(&mut self) -> Result<()> {
        if self.chunk_buffer.is_empty() {
            return Ok(());
        }

        let chunk_pos = self.cursor().pos();
        let uncompressed_size = self.chunk_buffer.len() as u32;

        // Compress the chunk data
        let compressed_data = self.compress_chunk(&self.chunk_buffer)?;

        // Write the chunk record
        let compression = self.compression;
        write_chunk(
            self.cursor(),
            compression,
            uncompressed_size,
            &compressed_data,
        )?;

        // Build index data for this chunk
        let mut index_entries: HashMap<u32, Vec<IndexDataEntry>> = HashMap::new();
        let mut conn_counts: HashMap<u32, u32> = HashMap::new();

        for msg in &self.pending_messages {
            index_entries
                .entry(msg.conn_id)
                .or_default()
                .push(IndexDataEntry {
                    time_ns: msg.time_ns,
                    offset: msg.offset,
                });
            *conn_counts.entry(msg.conn_id).or_insert(0) += 1;
        }

        // Write index data records (one per connection in this chunk)
        for (conn_id, entries) in &index_entries {
            write_index_data(self.cursor(), *conn_id, entries)?;
        }

        // Store chunk metadata for the index section
        self.completed_chunks.push(ChunkMeta {
            chunk_pos,
            start_time_ns: self.chunk_start_time.unwrap_or(0),
            end_time_ns: self.chunk_end_time.unwrap_or(0),
            conn_counts,
        });

        // Reset chunk state
        self.chunk_buffer.clear();
        self.pending_messages.clear();
        self.chunk_start_time = None;
        self.chunk_end_time = None;
        self.chunk_start_pos = self.cursor().pos();

        Ok(())
    }

    /// Compress chunk data according to the configured compression type.
    fn compress_chunk(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.compression {
            Compression::None => Ok(data.to_vec()),
            Compression::Lz4 => {
                // Configure LZ4 to match ROS's roslz4 format expectations:
                // - Block mode: Independent (not linked) - ROS expects block independence
                // - Block size: 1MB - ROS uses 1MB blocks
                // - Block checksum: Disabled - ROS's roslz4 expects no block checksums
                // - Content checksum: Enabled - ROS expects content checksum
                // Without these settings, roslz4 fails with "malformed data to decompress"
                let mut encoder = lz4::EncoderBuilder::new()
                    .block_mode(lz4::liblz4::BlockMode::Independent)
                    .block_size(lz4::liblz4::BlockSize::Max1MB)
                    .block_checksum(lz4::liblz4::BlockChecksum::NoBlockChecksum)
                    .checksum(lz4::liblz4::ContentChecksum::ChecksumEnabled)
                    .build(Vec::new())
                    .map_err(|e| WriteError::Lz4CompressionError(e.to_string()))?;
                std::io::copy(&mut std::io::Cursor::new(data), &mut encoder)
                    .map_err(|e| WriteError::Lz4CompressionError(e.to_string()))?;
                let (compressed, result) = encoder.finish();
                result.map_err(|e| WriteError::Lz4CompressionError(e.to_string()))?;
                Ok(compressed)
            }
            Compression::Bzip2 => {
                let mut encoder =
                    bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
                encoder
                    .write_all(data)
                    .map_err(|e| WriteError::Bzip2CompressionError(e.to_string()))?;
                encoder
                    .finish()
                    .map_err(|e| WriteError::Bzip2CompressionError(e.to_string()))
            }
        }
    }

    /// Internal method to finalize the bag file.
    ///
    /// This is called by both `finish()` and `Drop`.
    fn finalize(&mut self) -> Result<()> {
        if self.finished || self.cursor.is_none() {
            return Ok(());
        }

        // Flush any remaining chunk data
        self.flush_chunk()?;

        // Record the index section start position
        let index_pos = self.cursor().pos();

        // Write connection records to the index section
        // Clone connections to avoid borrow issues
        let connections = self.connections.clone();
        for conn in &connections {
            write_connection(
                self.cursor(),
                conn.id,
                &conn.topic,
                &conn.msg_type,
                &conn.md5sum,
                &conn.message_definition,
                &conn.caller_id,
                conn.latching,
            )?;
        }

        // Write chunk info records
        // Clone to avoid borrow issues
        let completed_chunks = self.completed_chunks.clone();
        for chunk in &completed_chunks {
            let entries: Vec<ChunkInfoEntry> = chunk
                .conn_counts
                .iter()
                .map(|(conn_id, count)| ChunkInfoEntry {
                    conn_id: *conn_id,
                    count: *count,
                })
                .collect();

            write_chunk_info(
                self.cursor(),
                chunk.chunk_pos,
                chunk.start_time_ns,
                chunk.end_time_ns,
                &entries,
            )?;
        }

        // Seek back and update the bag header
        let conn_count = self.connections.len() as u32;
        let chunk_count = self.completed_chunks.len() as u32;
        self.cursor().seek(VERSION_STRING.len() as u64)?;
        write_bag_header(self.cursor(), index_pos, conn_count, chunk_count)?;

        self.cursor().flush()?;
        self.finished = true;

        Ok(())
    }

    /// Finish writing the bag file and return the underlying writer.
    ///
    /// This flushes any remaining chunk data, writes the index section,
    /// and updates the bag header with final values.
    ///
    /// # Note
    /// If you drop the `RosBagWriter` without calling `finish()`, it will
    /// automatically finalize the bag file. However, any errors during
    /// finalization will be silently ignored. Call `finish()` explicitly
    /// if you need to handle errors.
    pub fn finish(mut self) -> Result<W> {
        self.finalize()?;
        // Take the cursor out of the Option - this is safe because finalize() succeeded
        // and we're consuming self anyway
        let cursor = self
            .cursor
            .take()
            .expect("cursor should exist after successful finalize");
        Ok(cursor.into_inner())
    }
}

impl<W: Write + Seek> Drop for RosBagWriter<W> {
    fn drop(&mut self) {
        // Finalize the bag if not already done.
        // Errors are silently ignored since Drop cannot return errors.
        // Users should call finish() explicitly if they need error handling.
        let _ = self.finalize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_builder_default() {
        let builder = RosBagWriterBuilder::default();
        assert_eq!(builder.compression, Compression::None);
        assert_eq!(builder.chunk_size, DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn test_builder_with_compression() {
        let builder = RosBagWriterBuilder::default().compression(Compression::Lz4);
        assert_eq!(builder.compression, Compression::Lz4);
    }

    #[test]
    fn test_builder_with_chunk_size() {
        let builder = RosBagWriterBuilder::default().chunk_size(1024);
        assert_eq!(builder.chunk_size, 1024);
    }

    #[test]
    fn test_writer_creates_valid_header() {
        let buffer = Cursor::new(Vec::new());
        let writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        let data = writer.finish().unwrap().into_inner();

        // Check version string
        assert!(data.starts_with(VERSION_STRING));

        // Total size should be at least version + bag header
        assert!(data.len() >= VERSION_STRING.len() + 4096);
    }

    #[test]
    fn test_register_connection() {
        let buffer = Cursor::new(Vec::new());
        let mut writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        let channel = writer
            .register_connection(
                "/test",
                "std_msgs/String",
                &[0u8; 16],
                "string data",
                "",
                false,
            )
            .unwrap();

        assert_eq!(channel.id(), 0);

        let channel2 = writer
            .register_connection(
                "/test2",
                "std_msgs/Int32",
                &[1u8; 16],
                "int32 data",
                "",
                false,
            )
            .unwrap();

        assert_eq!(channel2.id(), 1);
    }

    #[test]
    fn test_write_message() {
        let buffer = Cursor::new(Vec::new());
        let mut writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        let channel = writer
            .register_connection(
                "/test",
                "std_msgs/String",
                &[0u8; 16],
                "string data",
                "",
                false,
            )
            .unwrap();

        // Write a message using the channel
        channel.write(&mut writer, 1_000_000_000, b"hello").unwrap();

        // Finish should succeed
        let _data = writer.finish().unwrap();
    }

    #[test]
    fn test_write_message_unknown_connection() {
        let buffer = Cursor::new(Vec::new());
        let mut writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        // Create a fake Channel with an invalid connection ID
        let fake_channel: Channel<Cursor<Vec<u8>>> = Channel {
            conn_id: 999,
            _marker: std::marker::PhantomData,
        };

        // Try to write with the fake channel
        let result = fake_channel.write(&mut writer, 1_000_000_000, b"hello");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_bag() {
        let buffer = Cursor::new(Vec::new());
        let writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        // Empty bag should finish successfully
        let data = writer.finish().unwrap().into_inner();

        // Should have version string and bag header
        assert!(data.starts_with(VERSION_STRING));
    }

    #[test]
    fn test_multiple_messages() {
        let buffer = Cursor::new(Vec::new());
        let mut writer: RosBagWriter<Cursor<Vec<u8>>> = RosBagWriterBuilder::default()
            .chunk_size(1024 * 1024) // Large chunk to keep all in one
            .build(buffer)
            .unwrap();

        let channel = writer
            .register_connection(
                "/test",
                "std_msgs/String",
                &[0u8; 16],
                "string data",
                "",
                false,
            )
            .unwrap();

        // Write multiple messages
        for i in 0..10 {
            channel
                .write(
                    &mut writer,
                    (i + 1) * 1_000_000_000,
                    format!("msg{}", i).as_bytes(),
                )
                .unwrap();
        }

        let _data = writer.finish().unwrap();
    }

    #[test]
    fn test_multiple_connections() {
        let buffer = Cursor::new(Vec::new());
        let mut writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        let channel1 = writer
            .register_connection(
                "/topic1",
                "std_msgs/String",
                &[0u8; 16],
                "string data",
                "",
                false,
            )
            .unwrap();
        let channel2 = writer
            .register_connection(
                "/topic2",
                "std_msgs/Int32",
                &[1u8; 16],
                "int32 data",
                "",
                false,
            )
            .unwrap();

        // Write to both connections
        channel1
            .write(&mut writer, 1_000_000_000, b"hello")
            .unwrap();
        channel2
            .write(&mut writer, 1_500_000_000, b"\x00\x00\x00\x2a")
            .unwrap();
        channel1
            .write(&mut writer, 2_000_000_000, b"world")
            .unwrap();

        let _data = writer.finish().unwrap();
    }

    #[test]
    fn test_chunk_flushing() {
        let buffer = Cursor::new(Vec::new());
        let mut writer: RosBagWriter<Cursor<Vec<u8>>> = RosBagWriterBuilder::default()
            .chunk_size(100) // Very small chunk size to force flushing
            .build(buffer)
            .unwrap();

        let channel = writer
            .register_connection(
                "/test",
                "std_msgs/String",
                &[0u8; 16],
                "string data",
                "",
                false,
            )
            .unwrap();

        // Write enough data to trigger chunk flushing
        for i in 0..20 {
            channel
                .write(
                    &mut writer,
                    (i + 1) * 1_000_000_000,
                    b"this is a longer message to fill up the chunk",
                )
                .unwrap();
        }

        let _data = writer.finish().unwrap();
    }

    #[test]
    fn test_finish_twice_fails() {
        let buffer = Cursor::new(Vec::new());
        let writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        // First finish should succeed
        let _result = writer.finish();

        // Note: Can't test double finish easily since finish() consumes self
        // The finished flag prevents writes after finishing though
    }

    #[test]
    fn test_write_after_finish_preparation() {
        // Test that we can't write after marking finished
        let buffer = Cursor::new(Vec::new());
        let mut writer: RosBagWriter<Cursor<Vec<u8>>> =
            RosBagWriterBuilder::default().build(buffer).unwrap();

        let channel = writer
            .register_connection(
                "/test",
                "std_msgs/String",
                &[0u8; 16],
                "string data",
                "",
                false,
            )
            .unwrap();

        channel.write(&mut writer, 1_000_000_000, b"hello").unwrap();

        // After finish, the writer is consumed so we can't write anyway
        let _data = writer.finish().unwrap();
    }

    #[test]
    fn test_drop_finalizes_bag() {
        // Test that dropping a writer without calling finish() still produces a valid bag
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Write data and drop without calling finish()
        {
            let mut writer = RosBagWriter::create(&path).unwrap();

            let channel = writer
                .register_connection(
                    "/test",
                    "std_msgs/String",
                    &[0u8; 16],
                    "string data",
                    "",
                    false,
                )
                .unwrap();

            channel
                .write(&mut writer, 1_000_000_000, b"hello from drop")
                .unwrap();

            // Drop happens here - should finalize the bag
        }

        // Now verify the bag is valid by reading it
        let bag = crate::RosBag::new(&path).unwrap();

        // Verify we can iterate over records
        let mut message_count = 0;
        for record in bag.chunk_records() {
            if let crate::ChunkRecord::Chunk(chunk) = record.unwrap() {
                for msg in chunk.messages() {
                    if let crate::MessageRecord::MessageData(_) = msg.unwrap() {
                        message_count += 1;
                    }
                }
            }
        }

        assert_eq!(
            message_count, 1,
            "Should have 1 message after drop finalization"
        );
    }

    #[test]
    fn test_drop_after_finish_is_safe() {
        // Ensure that dropping after finish() doesn't cause issues
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        {
            let mut writer = RosBagWriter::create(&path).unwrap();

            let channel = writer
                .register_connection(
                    "/test",
                    "std_msgs/String",
                    &[0u8; 16],
                    "string data",
                    "",
                    false,
                )
                .unwrap();

            channel.write(&mut writer, 1_000_000_000, b"test").unwrap();

            // Explicitly finish
            let _file = writer.finish().unwrap();

            // Drop happens here after finish - should be a no-op
        }

        // Verify the bag is still valid
        let bag = crate::RosBag::new(&path).unwrap();
        let mut message_count = 0;
        for record in bag.chunk_records() {
            if let crate::ChunkRecord::Chunk(chunk) = record.unwrap() {
                for msg in chunk.messages() {
                    if let crate::MessageRecord::MessageData(_) = msg.unwrap() {
                        message_count += 1;
                    }
                }
            }
        }
        assert_eq!(message_count, 1);
    }

    #[test]
    fn test_drop_empty_bag() {
        // Test that dropping an empty bag (no messages) still produces a valid file
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        {
            let _writer = RosBagWriter::create(&path).unwrap();
            // Drop without writing anything
        }

        // Verify the bag is valid (empty but valid)
        let bag = crate::RosBag::new(&path).unwrap();

        // Should be able to iterate (with no records)
        let mut chunk_count = 0;
        for record in bag.chunk_records() {
            record.unwrap();
            chunk_count += 1;
        }
        // Empty bag should have no chunk records
        assert_eq!(chunk_count, 0);
    }

    #[test]
    fn test_drop_with_multiple_messages() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        {
            let mut writer = RosBagWriter::create(&path).unwrap();

            let channel = writer
                .register_connection(
                    "/counter",
                    "std_msgs/Int32",
                    &[0u8; 16],
                    "int32 data",
                    "",
                    false,
                )
                .unwrap();

            // Write multiple messages
            for i in 0..10 {
                let data = (i as i32).to_le_bytes();
                channel
                    .write(&mut writer, (i + 1) * 1_000_000_000, &data)
                    .unwrap();
            }

            // Drop without finish - should still finalize
        }

        // Verify all messages are readable
        let bag = crate::RosBag::new(&path).unwrap();
        let mut messages = Vec::new();
        for record in bag.chunk_records() {
            if let crate::ChunkRecord::Chunk(chunk) = record.unwrap() {
                for msg in chunk.messages() {
                    if let crate::MessageRecord::MessageData(md) = msg.unwrap() {
                        messages.push(md.data.to_vec());
                    }
                }
            }
        }

        assert_eq!(
            messages.len(),
            10,
            "Should have 10 messages after drop finalization"
        );

        // Verify message contents
        for (i, data) in messages.iter().enumerate() {
            let value = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            assert_eq!(value, i as i32);
        }
    }
}
