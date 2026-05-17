use std::fmt;

/// The error type for ROS bag file writing operations.
#[derive(Debug)]
pub enum WriteError {
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

impl From<std::io::Error> for WriteError {
    fn from(err: std::io::Error) -> WriteError {
        WriteError::IoError(err)
    }
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use WriteError::*;
        let s = match self {
            IoError(e) => format!("I/O error: {}", e),
            Bzip2CompressionError(e) => format!("bzip2 compression error: {}", e),
            Lz4CompressionError(e) => format!("LZ4 compression error: {}", e),
            InvalidWriterState(msg) => format!("invalid writer state: {}", msg),
            UnknownConnectionId(id) => format!("unknown connection ID: {}", id),
        };
        write!(f, "rosbag::writer::WriteError: {}", s)
    }
}

impl std::error::Error for WriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WriteError::IoError(e) => Some(e),
            _ => None,
        }
    }
}
