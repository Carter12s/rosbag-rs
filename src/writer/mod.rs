//! Utilities for writing ROS bag files.
//!
//! # Example
//! ```no_run
//! use rosbag::writer::{RosBagWriter, Compression};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut writer = RosBagWriter::create("output.bag")?;
//!
//! // Register a connection (topic) - returns a Channel handle
//! let channel = writer.register_connection(
//!     "/chatter",
//!     "std_msgs/String",
//!     &[0u8; 16], // md5sum
//!     "string data",
//!     "",    // caller_id
//!     false, // latching
//! )?;
//!
//! // Write messages using the channel
//! let time_ns = 1_000_000_000u64; // 1 second in nanoseconds
//! channel.write(&mut writer, time_ns, b"hello world")?;
//!
//! // Finalize the bag file
//! writer.finish()?;
//! # Ok(())
//! # }
//! ```

mod bag_writer;
mod error;
mod records;
mod write_cursor;

pub use crate::record_types::Compression;
pub use bag_writer::{Channel, RosBagWriter, RosBagWriterBuilder};
pub use error::WriteError;
pub use write_cursor::WriteCursor;
