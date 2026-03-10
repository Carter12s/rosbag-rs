use crate::cursor::OutOfBounds;
use std::convert::From;
use std::fmt;

/// The error type for ROS bag file reading, parsing, and writing.
#[derive(Debug)]
pub enum Error {
    /// Invalid headed.
    InvalidHeader,
    /// Invalid record.
    InvalidRecord,
    /// Encountered unsupported version in record.
    UnsupportedVersion,
    /// Tried to access outside of rosbag file.
    OutOfBounds,
    /// Got unexpected record type in the chunk section.
    UnexpectedChunkSectionRecord(&'static str),
    /// Got unexpected record type in the index section.
    UnexpectedIndexSectionRecord(&'static str),
    /// Got unexpected record type inside [`Chunk`][crate::record_types::Chunk] payload.
    UnexpectedMessageRecord(&'static str),
    /// Bzip2 decompression failure.
    Bzip2DecompressionError(String),
    /// Lz4 decompression failure.
    Lz4DecompressionError(String),

    // Write-specific errors
    /// I/O error during write operations.
    IoError(std::io::Error),
    /// Bzip2 compression failure.
    Bzip2CompressionError(String),
    /// Lz4 compression failure.
    Lz4CompressionError(String),
    /// Invalid writer state (e.g., writing after finish).
    InvalidWriterState(&'static str),
    /// Unknown connection ID.
    UnknownConnectionId(u32),
}

impl From<OutOfBounds> for Error {
    fn from(_: OutOfBounds) -> Error {
        Error::OutOfBounds
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IoError(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        let s = match self {
            InvalidHeader => "invalid header".to_string(),
            InvalidRecord => "invalid record".to_string(),
            UnsupportedVersion => "unsupported version".to_string(),
            OutOfBounds => "out of bounds".to_string(),
            UnexpectedChunkSectionRecord(t) => format!("unexpected {} in the chunk section", t),
            UnexpectedIndexSectionRecord(t) => format!("unexpected {} in the index section", t),
            UnexpectedMessageRecord(t) => format!("unexpected {} in chunk payload", t),
            Bzip2DecompressionError(e) => format!("bzip2 decompression error: {}", e),
            Lz4DecompressionError(e) => format!("LZ4 decompression error: {}", e),
            IoError(e) => format!("I/O error: {}", e),
            Bzip2CompressionError(e) => format!("bzip2 compression error: {}", e),
            Lz4CompressionError(e) => format!("LZ4 compression error: {}", e),
            InvalidWriterState(msg) => format!("invalid writer state: {}", msg),
            UnknownConnectionId(id) => format!("unknown connection ID: {}", id),
        };
        write!(f, "rosbag::Error: {}", s)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IoError(e) => Some(e),
            _ => None,
        }
    }
}
