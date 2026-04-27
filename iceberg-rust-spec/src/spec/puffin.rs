//! Puffin file format support.
//!
//! Puffin is a binary container for blobs (statistics, indexes, deletion vectors)
//! associated with an Iceberg table. The full specification lives in
//! `format/puffin-spec.md` of apache/iceberg.
//!
//! This module currently provides:
//!
//! - JSON serde for `FileMetadata` and `BlobMetadata` matching the Java
//!   `FileMetadataParser` field names (kebab-case).
//! - Parsing of a Puffin file's trailing footer (uncompressed payload only).
//!
//! Writing Puffin files and reading individual blob bodies (with zstd / lz4
//! decompression) are not yet implemented.
//!
//! Standard blob types are defined as constants — see [`STANDARD_BLOB_TYPE_*`].
//! [Reference](https://iceberg.apache.org/puffin-spec/).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Magic bytes that mark the start of a Puffin file and the start/end of its footer.
/// "PFA1" — Puffin _Fratercula arctica_, version 1.
pub const PUFFIN_MAGIC: [u8; 4] = [0x50, 0x46, 0x41, 0x31];

/// Length of the fixed-size portion of the footer (FooterPayloadSize + Flags + Magic).
pub const FOOTER_STRUCT_LENGTH: usize = 4 + 4 + 4;

/// Bit (byte 0, bit 0) flagging that the footer payload is LZ4-compressed.
pub const FLAG_FOOTER_PAYLOAD_COMPRESSED: u8 = 0b0000_0001;

/// Standard Iceberg blob type names. These mirror Java's `StandardBlobTypes`.
pub const STANDARD_BLOB_TYPE_THETA_V1: &str = "apache-datasketches-theta-v1";
/// Deletion vector blob added in Iceberg v3.
pub const STANDARD_BLOB_TYPE_DELETION_VECTOR_V1: &str = "deletion-vector-v1";

/// Compression applied to a Puffin blob (or the footer payload).
///
/// Wire form: absent on the JSON side means "no compression". For round-tripping
/// the absent form is represented as `None: Option<PuffinCompressionCodec>` at
/// the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PuffinCompressionCodec {
    /// LZ4 frame format.
    Lz4,
    /// Zstandard frame format.
    Zstd,
}

/// Metadata describing a single blob inside a Puffin file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct BlobMetadata {
    /// Blob type identifier; see [`STANDARD_BLOB_TYPE_*`].
    #[serde(rename = "type")]
    pub blob_type: String,
    /// Field IDs the blob was computed for. Order is significant.
    pub fields: Vec<i32>,
    /// Snapshot id of the source snapshot. -1 for blobs whose snapshot is not
    /// yet known (e.g. v3 deletion vectors, see spec).
    pub snapshot_id: i64,
    /// Sequence number of the source snapshot. -1 when not yet known.
    pub sequence_number: i64,
    /// Byte offset of the blob's payload from the start of the file.
    pub offset: i64,
    /// Length in bytes of the blob's payload (after compression, if any).
    pub length: i64,
    /// Compression applied to the blob payload. Absent = uncompressed.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub compression_codec: Option<PuffinCompressionCodec>,
    /// Arbitrary writer-defined metadata for this blob.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub properties: HashMap<String, String>,
}

/// Top-level metadata for a Puffin file. Mirrors Java's `FileMetadata`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FileMetadata {
    /// Blobs contained in the file, in the order they were written.
    pub blobs: Vec<BlobMetadata>,
    /// Arbitrary writer-defined file-level metadata (e.g. `created-by`).
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub properties: HashMap<String, String>,
}

impl FileMetadata {
    /// Parse a Puffin file's footer from the full file bytes.
    ///
    /// Reads the trailing footer (`Magic FooterPayload FooterPayloadSize Flags Magic`),
    /// validates both magic positions, and decodes the JSON payload.
    ///
    /// # Errors
    /// - File is too short to contain the footer.
    /// - Either magic position is wrong.
    /// - The footer payload is LZ4-compressed (not yet supported).
    /// - Payload size is negative or out of range.
    /// - JSON decoding fails.
    pub fn read_footer(bytes: &[u8]) -> Result<Self, Error> {
        // Minimum file: start magic + footer magic + 0-byte payload + size + flags + magic.
        let min_len = PUFFIN_MAGIC.len() + PUFFIN_MAGIC.len() + FOOTER_STRUCT_LENGTH;
        if bytes.len() < min_len {
            return Err(Error::InvalidFormat(format!(
                "Puffin file shorter than {min_len} bytes"
            )));
        }
        if bytes[..4] != PUFFIN_MAGIC {
            return Err(Error::InvalidFormat("Puffin start magic".to_string()));
        }
        let len = bytes.len();
        // Layout (from end): [..., footer_magic(4), payload(N), payload_size(4), flags(4), end_magic(4)]
        let end_magic = &bytes[len - 4..len];
        if end_magic != PUFFIN_MAGIC {
            return Err(Error::InvalidFormat("Puffin end magic".to_string()));
        }
        let flags = &bytes[len - 8..len - 4];
        if flags[0] & FLAG_FOOTER_PAYLOAD_COMPRESSED != 0 {
            return Err(Error::NotSupported(
                "LZ4-compressed Puffin footer payload".to_string(),
            ));
        }
        let payload_size = i32::from_le_bytes(bytes[len - 12..len - 8].try_into()?);
        if payload_size < 0 {
            return Err(Error::InvalidFormat(format!(
                "Puffin footer payload size {payload_size} is negative"
            )));
        }
        let payload_size = payload_size as usize;
        let payload_end = len - FOOTER_STRUCT_LENGTH;
        let payload_start = payload_end
            .checked_sub(payload_size)
            .ok_or_else(|| Error::InvalidFormat("Puffin payload size out of range".to_string()))?;
        let footer_magic_start = payload_start
            .checked_sub(PUFFIN_MAGIC.len())
            .ok_or_else(|| Error::InvalidFormat("Puffin footer magic out of range".to_string()))?;
        if bytes[footer_magic_start..payload_start] != PUFFIN_MAGIC {
            return Err(Error::InvalidFormat(
                "Puffin footer start magic".to_string(),
            ));
        }
        let payload = &bytes[payload_start..payload_end];
        Ok(serde_json::from_slice::<FileMetadata>(payload)?)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn read_fixture(name: &str) -> Vec<u8> {
        fs::read(format!("testdata/puffin/v1/{name}")).expect("fixture")
    }

    #[test]
    fn read_empty_puffin_uncompressed() {
        let bytes = read_fixture("empty-puffin-uncompressed.bin");
        // Layout from spec: 32 bytes total. Verify the parser agrees.
        assert_eq!(bytes.len(), 32);
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        assert!(meta.blobs.is_empty());
        assert!(meta.properties.is_empty());
    }

    #[test]
    fn read_sample_metric_data_uncompressed() {
        let bytes = read_fixture("sample-metric-data-uncompressed.bin");
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        // Two blobs in this fixture; matches Java TestPuffinReader.
        assert_eq!(meta.blobs.len(), 2);

        let first = &meta.blobs[0];
        assert_eq!(first.blob_type, "some-blob");
        assert_eq!(first.fields, vec![1]);
        assert_eq!(first.snapshot_id, 2);
        assert_eq!(first.sequence_number, 1);
        assert_eq!(first.compression_codec, None);
        // Offset of the first blob is right after the start magic (4 bytes).
        assert_eq!(first.offset, 4);

        let second = &meta.blobs[1];
        assert_eq!(second.blob_type, "some-other-blob");
        assert_eq!(second.fields, vec![2]);
        assert_eq!(second.compression_codec, None);

        assert_eq!(
            meta.properties.get("created-by").map(String::as_str),
            Some("Test 1234")
        );
    }

    #[test]
    fn read_sample_metric_data_compressed_zstd_footer_metadata() {
        // The zstd-compressed fixture compresses the *blobs*, not the footer
        // payload, so footer parsing succeeds without any decompression.
        let bytes = read_fixture("sample-metric-data-compressed-zstd.bin");
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        assert_eq!(meta.blobs.len(), 2);
        for blob in &meta.blobs {
            assert_eq!(blob.compression_codec, Some(PuffinCompressionCodec::Zstd));
        }
    }

    #[test]
    fn rejects_non_puffin_data() {
        let bytes = vec![0u8; 64];
        let err = FileMetadata::read_footer(&bytes).unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)));
    }

    #[test]
    fn rejects_short_input() {
        let bytes = b"PFA1".to_vec();
        let err = FileMetadata::read_footer(&bytes).unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)));
    }

    #[test]
    fn rejects_compressed_footer_payload() {
        // Hand-craft a minimal footer with the compressed-payload flag set.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&PUFFIN_MAGIC); // start
        bytes.extend_from_slice(&PUFFIN_MAGIC); // footer start magic
                                                // 0-byte payload
        bytes.extend_from_slice(&0i32.to_le_bytes()); // payload size
        bytes.extend_from_slice(&[FLAG_FOOTER_PAYLOAD_COMPRESSED, 0, 0, 0]); // flags
        bytes.extend_from_slice(&PUFFIN_MAGIC); // end magic
        let err = FileMetadata::read_footer(&bytes).unwrap_err();
        assert!(matches!(err, Error::NotSupported(_)));
    }

    #[test]
    fn file_metadata_json_round_trip() {
        let original = FileMetadata {
            blobs: vec![BlobMetadata {
                blob_type: STANDARD_BLOB_TYPE_DELETION_VECTOR_V1.to_string(),
                fields: vec![1, 2],
                snapshot_id: -1,
                sequence_number: -1,
                offset: 4,
                length: 128,
                compression_codec: None,
                properties: HashMap::from_iter([(
                    "referenced-data-file".to_string(),
                    "s3://bucket/data.parquet".to_string(),
                )]),
            }],
            properties: HashMap::from_iter([(
                "created-by".to_string(),
                "iceberg-rust test".to_string(),
            )]),
        };
        let json = serde_json::to_string(&original).unwrap();
        let parsed: FileMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }
}
