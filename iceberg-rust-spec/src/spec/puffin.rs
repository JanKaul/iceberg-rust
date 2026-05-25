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
//! - Reading individual blob payloads via `BlobMetadata::read_from`, with
//!   zstd decompression support.
//! - Writing Puffin files via [`PuffinWriter`], with optional zstd blob
//!   compression. Footer payload is emitted uncompressed.
//!
//! LZ4-compressed footer payloads and LZ4 blob compression are not yet
//! implemented.
//!
//! Standard blob types are defined as constants — see [`STANDARD_BLOB_TYPE_*`].
//! [Reference](https://iceberg.apache.org/puffin-spec/).

use std::{borrow::Cow, collections::HashMap};

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

impl BlobMetadata {
    /// Read this blob's payload from the full Puffin file bytes, decompressing
    /// it if a [`PuffinCompressionCodec`] is set.
    ///
    /// Returns a borrow when the blob is uncompressed; returns an owned vector
    /// when decompression was required.
    ///
    /// # Errors
    /// - `offset` / `length` are out of range for the supplied slice.
    /// - The blob is LZ4-compressed (not yet supported).
    /// - Zstd decompression fails.
    pub fn read_from<'a>(&self, file: &'a [u8]) -> Result<Cow<'a, [u8]>, Error> {
        let offset = usize::try_from(self.offset).map_err(|_| {
            Error::InvalidFormat(format!("blob offset {} is negative", self.offset))
        })?;
        let length = usize::try_from(self.length).map_err(|_| {
            Error::InvalidFormat(format!("blob length {} is negative", self.length))
        })?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::InvalidFormat("blob offset + length overflows".to_string()))?;
        if end > file.len() {
            return Err(Error::InvalidFormat(format!(
                "blob extends past end of Puffin file ({end} > {})",
                file.len()
            )));
        }
        let slice = &file[offset..end];
        match self.compression_codec {
            None => Ok(Cow::Borrowed(slice)),
            Some(PuffinCompressionCodec::Zstd) => Ok(Cow::Owned(zstd::stream::decode_all(slice)?)),
            Some(PuffinCompressionCodec::Lz4) => Err(Error::NotSupported(
                "LZ4-compressed Puffin blob".to_string(),
            )),
        }
    }
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

/// Input to [`PuffinWriter::write_blob`]. Borrows the payload to avoid copying
/// before compression.
pub struct Blob<'a> {
    /// Blob type identifier (e.g. [`STANDARD_BLOB_TYPE_DELETION_VECTOR_V1`]).
    pub blob_type: String,
    /// Field IDs the blob was computed for.
    pub fields: Vec<i32>,
    /// Source snapshot ID. Use `-1` for blobs whose snapshot is not yet known
    /// (the spec mandates this for v3 deletion vectors).
    pub snapshot_id: i64,
    /// Source sequence number. Use `-1` when not yet known.
    pub sequence_number: i64,
    /// Compression to apply to the payload. `None` writes the bytes unchanged.
    pub compression_codec: Option<PuffinCompressionCodec>,
    /// Blob-level metadata.
    pub properties: HashMap<String, String>,
    /// Raw, uncompressed blob bytes. The writer applies compression.
    pub payload: &'a [u8],
}

/// Builder that produces a complete Puffin file as a `Vec<u8>`.
///
/// Writes the start magic, then each blob's (optionally compressed) payload,
/// then the footer. The footer is emitted with an uncompressed JSON payload.
pub struct PuffinWriter {
    bytes: Vec<u8>,
    blobs: Vec<BlobMetadata>,
    file_properties: HashMap<String, String>,
}

impl Default for PuffinWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl PuffinWriter {
    /// Create an empty Puffin writer with the start magic already emitted.
    pub fn new() -> Self {
        Self {
            bytes: PUFFIN_MAGIC.to_vec(),
            blobs: Vec::new(),
            file_properties: HashMap::new(),
        }
    }

    /// Set a file-level property (e.g. `created-by`).
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.file_properties.insert(key.into(), value.into());
    }

    /// Write a single blob, applying the requested compression and computing
    /// its `offset` and `length`.
    ///
    /// # Errors
    /// - The blob requests LZ4 compression (not yet supported).
    /// - Zstd compression fails.
    pub fn write_blob(&mut self, blob: Blob<'_>) -> Result<(), Error> {
        let written: Cow<'_, [u8]> = match blob.compression_codec {
            None => Cow::Borrowed(blob.payload),
            Some(PuffinCompressionCodec::Zstd) => {
                Cow::Owned(zstd::stream::encode_all(blob.payload, 0)?)
            }
            Some(PuffinCompressionCodec::Lz4) => {
                return Err(Error::NotSupported(
                    "LZ4-compressed Puffin blob".to_string(),
                ));
            }
        };
        let offset = i64::try_from(self.bytes.len())
            .map_err(|_| Error::InvalidFormat("Puffin file offset overflows i64".to_string()))?;
        let length = i64::try_from(written.len())
            .map_err(|_| Error::InvalidFormat("Puffin blob length overflows i64".to_string()))?;
        self.bytes.extend_from_slice(&written);
        self.blobs.push(BlobMetadata {
            blob_type: blob.blob_type,
            fields: blob.fields,
            snapshot_id: blob.snapshot_id,
            sequence_number: blob.sequence_number,
            offset,
            length,
            compression_codec: blob.compression_codec,
            properties: blob.properties,
        });
        Ok(())
    }

    /// Append the footer and return the complete Puffin file bytes.
    pub fn finish(mut self) -> Result<Vec<u8>, Error> {
        let metadata = FileMetadata {
            blobs: self.blobs,
            properties: self.file_properties,
        };
        let payload = serde_json::to_vec(&metadata)?;
        let payload_size = i32::try_from(payload.len()).map_err(|_| {
            Error::InvalidFormat(format!(
                "Puffin footer payload too large: {} bytes",
                payload.len()
            ))
        })?;
        self.bytes.extend_from_slice(&PUFFIN_MAGIC);
        self.bytes.extend_from_slice(&payload);
        self.bytes.extend_from_slice(&payload_size.to_le_bytes());
        self.bytes.extend_from_slice(&[0u8; 4]);
        self.bytes.extend_from_slice(&PUFFIN_MAGIC);
        Ok(self.bytes)
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
    fn read_uncompressed_blob_payloads() {
        let bytes = read_fixture("sample-metric-data-uncompressed.bin");
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        let first = meta.blobs[0].read_from(&bytes).unwrap();
        assert_eq!(first.as_ref(), b"abcdefghi");
        let second = meta.blobs[1].read_from(&bytes).unwrap();
        assert!(second.starts_with(b"some blob"));
        // 83-byte blob from spec; matches BlobMetadata.length.
        assert_eq!(second.len() as i64, meta.blobs[1].length);
    }

    #[test]
    fn read_zstd_blob_payloads_match_uncompressed() {
        // The two fixtures store identical logical blobs; only blob compression
        // differs. Verify decompression yields exactly the uncompressed bytes.
        let plain = read_fixture("sample-metric-data-uncompressed.bin");
        let compressed = read_fixture("sample-metric-data-compressed-zstd.bin");

        let plain_meta = FileMetadata::read_footer(&plain).unwrap();
        let zstd_meta = FileMetadata::read_footer(&compressed).unwrap();

        assert_eq!(plain_meta.blobs.len(), zstd_meta.blobs.len());
        for (p, z) in plain_meta.blobs.iter().zip(zstd_meta.blobs.iter()) {
            let plain_payload = p.read_from(&plain).unwrap();
            let zstd_payload = z.read_from(&compressed).unwrap();
            assert_eq!(z.compression_codec, Some(PuffinCompressionCodec::Zstd));
            assert_eq!(plain_payload.as_ref(), zstd_payload.as_ref());
        }
    }

    #[test]
    fn rejects_lz4_blob_compression() {
        let bytes = read_fixture("sample-metric-data-uncompressed.bin");
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        let mut blob = meta.blobs[0].clone();
        blob.compression_codec = Some(PuffinCompressionCodec::Lz4);
        let err = blob.read_from(&bytes).unwrap_err();
        assert!(matches!(err, Error::NotSupported(_)));
    }

    #[test]
    fn rejects_blob_out_of_range() {
        let bytes = read_fixture("empty-puffin-uncompressed.bin");
        let blob = BlobMetadata {
            blob_type: "x".to_string(),
            fields: vec![],
            snapshot_id: 0,
            sequence_number: 0,
            offset: 4,
            length: 1024,
            compression_codec: None,
            properties: HashMap::new(),
        };
        let err = blob.read_from(&bytes).unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)));
    }

    #[test]
    fn writer_empty_matches_apache_fixture_byte_for_byte() {
        // The empty puffin fixture is fully deterministic: no properties, no
        // blobs, no compression. Our writer should produce the identical 32 bytes.
        let written = PuffinWriter::new().finish().unwrap();
        let fixture = read_fixture("empty-puffin-uncompressed.bin");
        assert_eq!(written, fixture);
    }

    #[test]
    fn writer_round_trips_uncompressed_blobs() {
        let mut writer = PuffinWriter::new();
        writer.set_property("created-by", "iceberg-rust test");
        writer
            .write_blob(Blob {
                blob_type: STANDARD_BLOB_TYPE_THETA_V1.to_string(),
                fields: vec![1],
                snapshot_id: 7,
                sequence_number: 3,
                compression_codec: None,
                properties: HashMap::from_iter([("ndv".to_string(), "42".to_string())]),
                payload: b"sketch-bytes",
            })
            .unwrap();
        writer
            .write_blob(Blob {
                blob_type: STANDARD_BLOB_TYPE_DELETION_VECTOR_V1.to_string(),
                fields: vec![],
                snapshot_id: -1,
                sequence_number: -1,
                compression_codec: None,
                properties: HashMap::from_iter([(
                    "referenced-data-file".to_string(),
                    "s3://bucket/data.parquet".to_string(),
                )]),
                payload: b"dv-bytes",
            })
            .unwrap();

        let bytes = writer.finish().unwrap();
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        assert_eq!(meta.blobs.len(), 2);
        assert_eq!(
            meta.properties.get("created-by").map(String::as_str),
            Some("iceberg-rust test")
        );

        assert_eq!(meta.blobs[0].blob_type, STANDARD_BLOB_TYPE_THETA_V1);
        assert_eq!(
            meta.blobs[0].read_from(&bytes).unwrap().as_ref(),
            b"sketch-bytes"
        );

        assert_eq!(
            meta.blobs[1].blob_type,
            STANDARD_BLOB_TYPE_DELETION_VECTOR_V1
        );
        assert_eq!(meta.blobs[1].snapshot_id, -1);
        assert_eq!(
            meta.blobs[1].read_from(&bytes).unwrap().as_ref(),
            b"dv-bytes"
        );
    }

    #[test]
    fn writer_round_trips_zstd_compressed_blob() {
        let payload = b"some payload bytes that compress okay zstd zstd zstd zstd zstd";
        let mut writer = PuffinWriter::new();
        writer
            .write_blob(Blob {
                blob_type: "x-test".to_string(),
                fields: vec![1],
                snapshot_id: 0,
                sequence_number: 0,
                compression_codec: Some(PuffinCompressionCodec::Zstd),
                properties: HashMap::new(),
                payload,
            })
            .unwrap();
        let bytes = writer.finish().unwrap();
        let meta = FileMetadata::read_footer(&bytes).unwrap();
        assert_eq!(
            meta.blobs[0].compression_codec,
            Some(PuffinCompressionCodec::Zstd)
        );
        // Stored length is the compressed length, distinct from the original.
        assert!(meta.blobs[0].length < payload.len() as i64);
        let decoded = meta.blobs[0].read_from(&bytes).unwrap();
        assert_eq!(decoded.as_ref(), payload);
    }

    #[test]
    fn writer_rejects_lz4_compression() {
        let mut writer = PuffinWriter::new();
        let err = writer
            .write_blob(Blob {
                blob_type: "x".to_string(),
                fields: vec![],
                snapshot_id: 0,
                sequence_number: 0,
                compression_codec: Some(PuffinCompressionCodec::Lz4),
                properties: HashMap::new(),
                payload: b"data",
            })
            .unwrap_err();
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
