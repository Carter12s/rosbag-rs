//! ROS1 Noetic compatibility tests.
//!
//! These tests generate bag files and validate them using the Python script
//! which invokes official ROS tools (rosbag info, rosbag check, Python rosbag API).
//!
//! These tests are intended to run in a ROS1 Noetic environment (e.g., the CI container).
//! They will be skipped if ROS is not available.

use rosbag::writer::{Compression, RosBagWriter, RosBagWriterBuilder};
use serde::Serialize;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

// MD5 sums for standard ROS message types
const STRING_MD5: [u8; 16] = hex_to_bytes("992ce8a1687cec8c8bd883ec73ca41d1");
const INT32_MD5: [u8; 16] = hex_to_bytes("da5909fbe378aeaf85e547e830cc1bb7");

/// Convert hex string to byte array at compile time
const fn hex_to_bytes(hex: &str) -> [u8; 16] {
    let bytes = hex.as_bytes();
    let mut result = [0u8; 16];
    let mut i = 0;
    while i < 16 {
        let hi = hex_char_to_nibble(bytes[i * 2]);
        let lo = hex_char_to_nibble(bytes[i * 2 + 1]);
        result[i] = (hi << 4) | lo;
        i += 1;
    }
    result
}

const fn hex_char_to_nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => panic!("invalid hex char"),
    }
}

/// std_msgs/String message type
#[derive(Serialize)]
struct StdMsgsString {
    data: String,
}

/// std_msgs/Int32 message type
#[derive(Serialize)]
struct StdMsgsInt32 {
    data: i32,
}

/// Check if ROS is available in the environment
fn ros_available() -> bool {
    Command::new("rosbag")
        .arg("--help")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Validate a bag file using the Python validation script.
fn validate_bag(bag_path: &Path) -> Result<(), String> {
    let script_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("ros_compat")
        .join("validate_bag.py");

    let output = Command::new("python3")
        .arg(&script_path)
        .arg(bag_path)
        .arg("-v")
        .output()
        .map_err(|e| format!("Failed to run validation script: {}", e))?;

    if output.status.success() {
        Ok(())
    } else {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!(
            "Validation failed:\nstdout: {}\nstderr: {}",
            stdout, stderr
        ))
    }
}

/// Serialize a ROS message using roslibrust_serde_rosmsg (skipping outer length prefix)
fn serialize_msg<T: Serialize>(msg: &T) -> Vec<u8> {
    roslibrust_serde_rosmsg::to_vec_skip_length(msg).expect("serialization should not fail")
}

#[test]
fn test_ros_compat_empty_bag() {
    if !ros_available() {
        eprintln!("Skipping test: ROS not available");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let bag_path = temp_dir.path().join("empty.bag");

    let writer = RosBagWriter::create(&bag_path).unwrap();
    writer.finish().unwrap();

    validate_bag(&bag_path).expect("Empty bag validation failed");
}

#[test]
fn test_ros_compat_single_message() {
    if !ros_available() {
        eprintln!("Skipping test: ROS not available");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let bag_path = temp_dir.path().join("single_message.bag");

    let mut writer = RosBagWriter::create(&bag_path).unwrap();
    let channel = writer
        .register_connection("/chatter", "std_msgs/String", &STRING_MD5, "string data", "", false)
        .unwrap();

    let msg = StdMsgsString {
        data: "Hello from rosbag-rs!".to_string(),
    };
    channel
        .write(&mut writer, 1_000_000_000, &serialize_msg(&msg))
        .unwrap();
    writer.finish().unwrap();

    validate_bag(&bag_path).expect("Single message bag validation failed");
}

#[test]
fn test_ros_compat_multiple_messages() {
    if !ros_available() {
        eprintln!("Skipping test: ROS not available");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let bag_path = temp_dir.path().join("multiple_messages.bag");

    let mut writer = RosBagWriter::create(&bag_path).unwrap();
    let channel = writer
        .register_connection("/chatter", "std_msgs/String", &STRING_MD5, "string data", "", false)
        .unwrap();

    for i in 0..100 {
        let msg = StdMsgsString {
            data: format!("Message {}", i),
        };
        let time_ns = (i as u64 + 1) * 100_000_000;
        channel
            .write(&mut writer, time_ns, &serialize_msg(&msg))
            .unwrap();
    }
    writer.finish().unwrap();

    validate_bag(&bag_path).expect("Multiple messages bag validation failed");
}

#[test]
fn test_ros_compat_multiple_topics() {
    if !ros_available() {
        eprintln!("Skipping test: ROS not available");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let bag_path = temp_dir.path().join("multiple_topics.bag");

    let mut writer = RosBagWriter::create(&bag_path).unwrap();
    let string_channel = writer
        .register_connection("/chatter", "std_msgs/String", &STRING_MD5, "string data", "", false)
        .unwrap();
    let int_channel = writer
        .register_connection("/counter", "std_msgs/Int32", &INT32_MD5, "int32 data", "", false)
        .unwrap();

    for i in 0..50 {
        let str_msg = StdMsgsString {
            data: format!("String {}", i),
        };
        let int_msg = StdMsgsInt32 { data: i };
        let time_ns = (i as u64 + 1) * 100_000_000;
        string_channel
            .write(&mut writer, time_ns, &serialize_msg(&str_msg))
            .unwrap();
        int_channel
            .write(&mut writer, time_ns + 50_000_000, &serialize_msg(&int_msg))
            .unwrap();
    }
    writer.finish().unwrap();

    validate_bag(&bag_path).expect("Multiple topics bag validation failed");
}

#[test]
fn test_ros_compat_lz4_compression() {
    if !ros_available() {
        eprintln!("Skipping test: ROS not available");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let bag_path = temp_dir.path().join("lz4_compressed.bag");

    let mut writer = RosBagWriterBuilder::default()
        .compression(Compression::Lz4)
        .create(&bag_path)
        .unwrap();
    let channel = writer
        .register_connection("/chatter", "std_msgs/String", &STRING_MD5, "string data", "", false)
        .unwrap();

    for i in 0..10 {
        let msg = StdMsgsString {
            data: format!("LZ4 compressed message {}", i),
        };
        channel
            .write(&mut writer, (i as u64 + 1) * 1_000_000_000, &serialize_msg(&msg))
            .unwrap();
    }
    writer.finish().unwrap();

    validate_bag(&bag_path).expect("LZ4 compressed bag validation failed");
}

#[test]
fn test_ros_compat_bz2_compression() {
    if !ros_available() {
        eprintln!("Skipping test: ROS not available");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let bag_path = temp_dir.path().join("bz2_compressed.bag");

    let mut writer = RosBagWriterBuilder::default()
        .compression(Compression::Bzip2)
        .create(&bag_path)
        .unwrap();
    let channel = writer
        .register_connection("/chatter", "std_msgs/String", &STRING_MD5, "string data", "", false)
        .unwrap();

    for i in 0..10 {
        let msg = StdMsgsString {
            data: format!("BZ2 compressed message {}", i),
        };
        channel
            .write(&mut writer, (i as u64 + 1) * 1_000_000_000, &serialize_msg(&msg))
            .unwrap();
    }
    writer.finish().unwrap();

    validate_bag(&bag_path).expect("BZ2 compressed bag validation failed");
}

#[test]
fn test_ros_compat_multiple_chunks() {
    if !ros_available() {
        eprintln!("Skipping test: ROS not available");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let bag_path = temp_dir.path().join("multiple_chunks.bag");

    let mut writer = RosBagWriterBuilder::default()
        .chunk_size(200) // Very small chunk size to force multiple chunks
        .create(&bag_path)
        .unwrap();
    let channel = writer
        .register_connection("/chatter", "std_msgs/String", &STRING_MD5, "string data", "", false)
        .unwrap();

    for i in 0..100 {
        let msg = StdMsgsString {
            data: format!("Chunk test message number {}", i),
        };
        channel
            .write(&mut writer, (i as u64 + 1) * 100_000_000, &serialize_msg(&msg))
            .unwrap();
    }
    writer.finish().unwrap();

    validate_bag(&bag_path).expect("Multiple chunks bag validation failed");
}
