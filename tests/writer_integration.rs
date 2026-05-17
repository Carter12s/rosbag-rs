//! Integration tests for the rosbag writer.
//!
//! These tests validate that written bag files can be read back correctly.

use rosbag::writer::{Compression, RosBagWriter, RosBagWriterBuilder};
use rosbag::{ChunkRecord, MessageRecord, RosBag};
use tempfile::NamedTempFile;

/// Helper to create a temporary bag file and return its path
fn write_bag_to_temp_file<F>(write_fn: F) -> NamedTempFile
where
    F: FnOnce(
        &mut RosBagWriter<std::io::BufWriter<std::fs::File>>,
    ) -> Result<(), rosbag::writer::WriteError>,
{
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    let mut writer = RosBagWriter::create(&path).unwrap();
    write_fn(&mut writer).unwrap();
    writer.finish().unwrap();

    temp_file
}

#[test]
fn test_roundtrip_single_message() {
    let temp_file = write_bag_to_temp_file(|writer| {
        let channel = writer.register_connection(
            "/test_topic",
            "std_msgs/String",
            &[0u8; 16],
            "string data",
            "",
            false,
        )?;
        channel.write(writer, 1_000_000_000, b"hello world")?;
        Ok(())
    });

    // Read the bag back
    let bag = RosBag::new(temp_file.path()).unwrap();

    // Should have 1 connection
    assert_eq!(bag.get_conn_count(), 1);

    // Should have 1 chunk
    assert_eq!(bag.get_chunk_count(), 1);

    // Read all messages
    let mut messages: Vec<(u32, u64, Vec<u8>)> = Vec::new();
    for record in bag.chunk_records() {
        if let ChunkRecord::Chunk(chunk) = record.unwrap() {
            for msg in chunk.messages() {
                if let MessageRecord::MessageData(md) = msg.unwrap() {
                    messages.push((md.conn_id, md.time, md.data.to_vec()));
                }
            }
        }
    }

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].0, 0); // conn_id
    assert_eq!(messages[0].1, 1_000_000_000); // time
    assert_eq!(messages[0].2, b"hello world");
}

#[test]
fn test_roundtrip_multiple_messages() {
    let temp_file = write_bag_to_temp_file(|writer| {
        let channel = writer.register_connection(
            "/test_topic",
            "std_msgs/String",
            &[0u8; 16],
            "string data",
            "",
            false,
        )?;
        for i in 0..5 {
            let msg = format!("message {}", i);
            channel.write(writer, (i + 1) * 1_000_000_000, msg.as_bytes())?;
        }
        Ok(())
    });

    let bag = RosBag::new(temp_file.path()).unwrap();

    let mut messages: Vec<(u32, u64, Vec<u8>)> = Vec::new();
    for record in bag.chunk_records() {
        if let ChunkRecord::Chunk(chunk) = record.unwrap() {
            for msg in chunk.messages() {
                if let MessageRecord::MessageData(md) = msg.unwrap() {
                    messages.push((md.conn_id, md.time, md.data.to_vec()));
                }
            }
        }
    }

    assert_eq!(messages.len(), 5);
    for (i, (conn_id, time, data)) in messages.iter().enumerate() {
        assert_eq!(*conn_id, 0);
        assert_eq!(*time, (i as u64 + 1) * 1_000_000_000);
        assert_eq!(*data, format!("message {}", i).as_bytes());
    }
}

#[test]
fn test_roundtrip_multiple_connections() {
    let temp_file = write_bag_to_temp_file(|writer| {
        let channel1 = writer.register_connection(
            "/topic1",
            "std_msgs/String",
            &[0u8; 16],
            "string data",
            "",
            false,
        )?;
        let channel2 = writer.register_connection(
            "/topic2",
            "std_msgs/Int32",
            &[1u8; 16],
            "int32 data",
            "",
            false,
        )?;

        channel1.write(writer, 1_000_000_000, b"msg1")?;
        channel2.write(writer, 1_500_000_000, b"\x2a\x00\x00\x00")?;
        channel1.write(writer, 2_000_000_000, b"msg2")?;

        Ok(())
    });

    let bag = RosBag::new(temp_file.path()).unwrap();
    assert_eq!(bag.get_conn_count(), 2);

    let mut messages: Vec<(u32, u64, Vec<u8>)> = Vec::new();
    for record in bag.chunk_records() {
        if let ChunkRecord::Chunk(chunk) = record.unwrap() {
            for msg in chunk.messages() {
                if let MessageRecord::MessageData(md) = msg.unwrap() {
                    messages.push((md.conn_id, md.time, md.data.to_vec()));
                }
            }
        }
    }

    assert_eq!(messages.len(), 3);
}

#[test]
fn test_roundtrip_empty_bag() {
    let temp_file = write_bag_to_temp_file(|_writer| Ok(()));

    let bag = RosBag::new(temp_file.path()).unwrap();
    assert_eq!(bag.get_conn_count(), 0);
    assert_eq!(bag.get_chunk_count(), 0);
}

#[test]
fn test_roundtrip_lz4_compression() {
    let temp_file = {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        let mut writer = RosBagWriterBuilder::default()
            .compression(Compression::Lz4)
            .create(&path)
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
        channel
            .write(&mut writer, 1_000_000_000, b"compressed message")
            .unwrap();
        writer.finish().unwrap();

        temp_file
    };

    let bag = RosBag::new(temp_file.path()).unwrap();
    assert_eq!(bag.get_conn_count(), 1);

    let mut messages: Vec<Vec<u8>> = Vec::new();
    for record in bag.chunk_records() {
        if let ChunkRecord::Chunk(chunk) = record.unwrap() {
            for msg in chunk.messages() {
                if let MessageRecord::MessageData(md) = msg.unwrap() {
                    messages.push(md.data.to_vec());
                }
            }
        }
    }

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0], b"compressed message");
}

#[test]
fn test_roundtrip_bzip2_compression() {
    let temp_file = {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        let mut writer = RosBagWriterBuilder::default()
            .compression(Compression::Bzip2)
            .create(&path)
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
        channel
            .write(&mut writer, 1_000_000_000, b"bzip2 compressed")
            .unwrap();
        writer.finish().unwrap();

        temp_file
    };

    let bag = RosBag::new(temp_file.path()).unwrap();

    let mut messages: Vec<Vec<u8>> = Vec::new();
    for record in bag.chunk_records() {
        if let ChunkRecord::Chunk(chunk) = record.unwrap() {
            for msg in chunk.messages() {
                if let MessageRecord::MessageData(md) = msg.unwrap() {
                    messages.push(md.data.to_vec());
                }
            }
        }
    }

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0], b"bzip2 compressed");
}

#[test]
fn test_roundtrip_multiple_chunks() {
    let temp_file = {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Use small chunk size to force multiple chunks
        let mut writer = RosBagWriterBuilder::default()
            .chunk_size(100)
            .create(&path)
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

        // Write many messages to force multiple chunks
        for i in 0..200 {
            let msg = format!("message number {}", i);
            channel
                .write(&mut writer, (i + 1) * 1_000_000_000, msg.as_bytes())
                .unwrap();
        }
        writer.finish().unwrap();

        temp_file
    };

    let bag = RosBag::new(temp_file.path()).unwrap();

    // Should have multiple chunks due to small chunk size
    assert!(bag.get_chunk_count() > 1);

    let mut message_count = 0;
    for record in bag.chunk_records() {
        if let ChunkRecord::Chunk(chunk) = record.unwrap() {
            for msg in chunk.messages() {
                if let MessageRecord::MessageData(_) = msg.unwrap() {
                    message_count += 1;
                }
            }
        }
    }

    assert_eq!(message_count, 200);
}
