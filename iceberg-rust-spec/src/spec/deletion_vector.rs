//! Iceberg v3 deletion-vector blob support.
//!
//! Deletion vectors are 64-bit roaring bitmaps stored as `deletion-vector-v1`
//! Puffin blobs. Each bitmap holds the absolute row positions that have been
//! logically deleted from a single data file.
//!
//! The serialized layout is identical to the format used by Delta Lake (for
//! cross-engine compatibility). The Apache Iceberg Java reference lives in
//! `BitmapPositionDeleteIndex` and `RoaringPositionBitmap`.
//!
//! Wire layout:
//!
//! ```text
//! [4 bytes BE]    bitmap_data_length = 4 + serialized_bitmap_size
//! [4 bytes LE]    magic = 0x6439_D3D1 (1681511377)
//! [N bytes LE]    serialized RoaringPositionBitmap (see below)
//! [4 bytes BE]    CRC-32 of (magic .. bitmap)
//! ```
//!
//! The serialized RoaringPositionBitmap is:
//!
//! ```text
//! [8 bytes LE]    bitmap_count (i64)
//! repeated bitmap_count times, in ascending key order:
//!   [4 bytes LE]  key (i32) — high 32 bits of position
//!   [M bytes]     32-bit RoaringBitmap in portable format
//! ```
//!
//! A 64-bit position is reconstructed as
//! `((key as u64) << 32) | (low as u32 as u64)`.

use std::io::Cursor;

use roaring::{RoaringBitmap, RoaringTreemap};

use crate::error::Error;

const LENGTH_SIZE: usize = 4;
const MAGIC_SIZE: usize = 4;
const CRC_SIZE: usize = 4;
const BITMAP_COUNT_SIZE: usize = 8;
const KEY_SIZE: usize = 4;

/// Cross-engine magic number for a `deletion-vector-v1` blob. Stored as a
/// little-endian `i32` in the bitmap data section.
pub const DELETION_VECTOR_MAGIC: u32 = 1_681_511_377;

/// In-memory representation of a parsed `deletion-vector-v1` blob.
///
/// Wraps a [`RoaringTreemap`] of 64-bit absolute row positions that have been
/// deleted from a single data file. Iceberg restricts positions to the lower
/// 63 bits (the high bit must be 0).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct DeletionVector(RoaringTreemap);

impl From<RoaringTreemap> for DeletionVector {
    fn from(treemap: RoaringTreemap) -> Self {
        Self(treemap)
    }
}

impl TryFrom<&[u8]> for DeletionVector {
    type Error = Error;

    /// Parse a `deletion-vector-v1` blob payload.
    ///
    /// # Errors
    /// - The blob is shorter than the minimum 12 bytes.
    /// - The encoded length disagrees with the payload size.
    /// - The magic number is wrong.
    /// - The CRC-32 does not match.
    /// - A 32-bit roaring sub-bitmap cannot be deserialized or its key is out of order.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        parse_blob(bytes)
    }
}

impl DeletionVector {
    /// Borrow the underlying [`RoaringTreemap`].
    pub fn as_treemap(&self) -> &RoaringTreemap {
        &self.0
    }

    /// Consume the wrapper and return the underlying [`RoaringTreemap`].
    pub fn into_inner(self) -> RoaringTreemap {
        self.0
    }

    /// Test whether the row at `pos` is marked deleted.
    #[inline]
    pub fn is_deleted(&self, pos: u64) -> bool {
        self.0.contains(pos)
    }

    /// Number of deleted positions.
    pub fn cardinality(&self) -> u64 {
        self.0.len()
    }

    /// Serialize into a `deletion-vector-v1` blob payload. The output is
    /// byte-for-byte compatible with the Apache Iceberg Java writer for the
    /// same bitmap contents.
    ///
    /// # Errors
    /// - A position exceeds the 63-bit limit.
    /// - The serialized blob would exceed `i32::MAX` bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        encode_blob(self)
    }
}

fn parse_blob(bytes: &[u8]) -> Result<DeletionVector, Error> {
    if bytes.len() < LENGTH_SIZE + MAGIC_SIZE + CRC_SIZE {
        return Err(Error::InvalidFormat(format!(
            "deletion vector blob of {} bytes is shorter than minimum {}",
            bytes.len(),
            LENGTH_SIZE + MAGIC_SIZE + CRC_SIZE
        )));
    }
    let bitmap_data_length = u32::from_be_bytes(bytes[..LENGTH_SIZE].try_into()?) as usize;
    let expected_total = LENGTH_SIZE + bitmap_data_length + CRC_SIZE;
    if bytes.len() != expected_total {
        return Err(Error::InvalidFormat(format!(
            "deletion vector blob has {} bytes, encoded length is {}",
            bytes.len(),
            expected_total
        )));
    }

    let bitmap_data = &bytes[LENGTH_SIZE..LENGTH_SIZE + bitmap_data_length];
    let crc_bytes = &bytes[LENGTH_SIZE + bitmap_data_length..];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bitmap_data);
    let computed_crc = hasher.finalize();
    let expected_crc = u32::from_be_bytes(crc_bytes.try_into()?);
    if computed_crc != expected_crc {
        return Err(Error::InvalidFormat(format!(
            "deletion vector CRC mismatch: computed 0x{computed_crc:08x}, expected 0x{expected_crc:08x}"
        )));
    }

    if bitmap_data.len() < MAGIC_SIZE + BITMAP_COUNT_SIZE {
        return Err(Error::InvalidFormat(
            "deletion vector bitmap data is truncated".to_string(),
        ));
    }
    let magic = u32::from_le_bytes(bitmap_data[..MAGIC_SIZE].try_into()?);
    if magic != DELETION_VECTOR_MAGIC {
        return Err(Error::InvalidFormat(format!(
            "deletion vector magic 0x{magic:08x} != 0x{DELETION_VECTOR_MAGIC:08x}"
        )));
    }

    let bitmap_count = i64::from_le_bytes(
        bitmap_data[MAGIC_SIZE..MAGIC_SIZE + BITMAP_COUNT_SIZE].try_into()?,
    );
    if !(0..=i32::MAX as i64).contains(&bitmap_count) {
        return Err(Error::InvalidFormat(format!(
            "deletion vector bitmap count {bitmap_count} out of range"
        )));
    }

    let mut treemap = RoaringTreemap::new();
    let mut cursor = Cursor::new(&bitmap_data[MAGIC_SIZE + BITMAP_COUNT_SIZE..]);
    let mut last_key: i64 = -1;
    for _ in 0..bitmap_count {
        let mut key_bytes = [0u8; KEY_SIZE];
        std::io::Read::read_exact(&mut cursor, &mut key_bytes)?;
        let key = i32::from_le_bytes(key_bytes);
        if key < 0 {
            return Err(Error::InvalidFormat(format!(
                "deletion vector key {key} is negative"
            )));
        }
        if (key as i64) <= last_key {
            return Err(Error::InvalidFormat(format!(
                "deletion vector keys must ascend: got {key} after {last_key}"
            )));
        }
        last_key = key as i64;

        let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
            .map_err(|e| Error::InvalidFormat(format!("deletion vector roaring bitmap: {e}")))?;

        let high = (key as u64) << 32;
        for low in &bitmap {
            treemap.insert(high | (low as u64));
        }
    }

    Ok(DeletionVector(treemap))
}

fn encode_blob(dv: &DeletionVector) -> Result<Vec<u8>, Error> {
    // Group 64-bit positions back into per-key 32-bit RoaringBitmaps.
    let mut groups: std::collections::BTreeMap<u32, RoaringBitmap> = Default::default();
    for pos in &dv.0 {
        if (pos >> 63) != 0 {
            return Err(Error::InvalidFormat(format!(
                "deletion vector position {pos} exceeds the 63-bit limit"
            )));
        }
        let key = (pos >> 32) as u32;
        let low = pos as u32;
        groups.entry(key).or_default().insert(low);
    }

    let mut bitmap_payload_size = BITMAP_COUNT_SIZE;
    for bitmap in groups.values() {
        bitmap_payload_size = bitmap_payload_size
            .checked_add(KEY_SIZE + bitmap.serialized_size())
            .ok_or_else(|| {
                Error::InvalidFormat("deletion vector payload size overflow".to_string())
            })?;
    }
    let bitmap_data_length = MAGIC_SIZE + bitmap_payload_size;
    let total_size = LENGTH_SIZE + bitmap_data_length + CRC_SIZE;
    if total_size > i32::MAX as usize {
        return Err(Error::InvalidFormat(format!(
            "deletion vector blob size {total_size} exceeds i32::MAX"
        )));
    }

    let mut out = Vec::with_capacity(total_size);
    out.extend_from_slice(&(bitmap_data_length as u32).to_be_bytes());
    let bitmap_data_start = out.len();
    out.extend_from_slice(&DELETION_VECTOR_MAGIC.to_le_bytes());
    out.extend_from_slice(&(groups.len() as i64).to_le_bytes());
    for (key, bitmap) in &groups {
        out.extend_from_slice(&(*key as i32).to_le_bytes());
        bitmap
            .serialize_into(&mut out)
            .map_err(|e| Error::InvalidFormat(format!("deletion vector roaring serialize: {e}")))?;
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&out[bitmap_data_start..]);
    out.extend_from_slice(&hasher.finalize().to_be_bytes());

    debug_assert_eq!(out.len(), total_size);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty() {
        let dv = DeletionVector::default();
        let bytes = dv.to_bytes().unwrap();
        let parsed = DeletionVector::try_from(bytes.as_slice()).unwrap();
        assert_eq!(dv, parsed);
        assert_eq!(parsed.cardinality(), 0);
    }

    #[test]
    fn round_trip_small_positions() {
        let mut tm = RoaringTreemap::new();
        for p in [0u64, 1, 7, 100, 4096] {
            tm.insert(p);
        }
        let dv = DeletionVector::from(tm);
        let bytes = dv.to_bytes().unwrap();
        let parsed = DeletionVector::try_from(bytes.as_slice()).unwrap();
        assert_eq!(dv, parsed);
        assert!(parsed.is_deleted(4096));
        assert!(!parsed.is_deleted(5000));
    }

    #[test]
    fn round_trip_across_high_bits() {
        // Spread positions across multiple 32-bit sub-bitmaps to exercise the
        // key-grouped wire format.
        let mut tm = RoaringTreemap::new();
        tm.insert(1);
        tm.insert((1u64 << 32) + 1);
        tm.insert((1u64 << 32) + 5_000_000);
        tm.insert((3u64 << 32) + 42);
        let dv = DeletionVector::from(tm);
        let bytes = dv.to_bytes().unwrap();
        let parsed = DeletionVector::try_from(bytes.as_slice()).unwrap();
        assert_eq!(dv, parsed);
        assert!(parsed.is_deleted((3u64 << 32) + 42));
        assert!(!parsed.is_deleted((2u64 << 32) + 42));
    }

    #[test]
    fn rejects_bad_magic() {
        let mut tm = RoaringTreemap::new();
        tm.insert(1);
        let dv = DeletionVector::from(tm);
        let mut bytes = dv.to_bytes().unwrap();
        // Flip a byte in the magic and recompute the CRC so the magic-check
        // (not the CRC-check) is exercised.
        bytes[LENGTH_SIZE] ^= 0xFF;
        let crc_offset = bytes.len() - CRC_SIZE;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes[LENGTH_SIZE..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&hasher.finalize().to_be_bytes());
        let err = DeletionVector::try_from(bytes.as_slice()).unwrap_err();
        match err {
            Error::InvalidFormat(msg) => assert!(msg.contains("magic"), "got: {msg}"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_bad_crc() {
        let mut tm = RoaringTreemap::new();
        tm.insert(1);
        let dv = DeletionVector::from(tm);
        let mut bytes = dv.to_bytes().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let err = DeletionVector::try_from(bytes.as_slice()).unwrap_err();
        match err {
            Error::InvalidFormat(msg) => assert!(msg.contains("CRC"), "got: {msg}"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_truncated_blob() {
        let err = DeletionVector::try_from([0u8; 8].as_slice()).unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)));
    }

    #[test]
    fn rejects_length_mismatch() {
        let mut tm = RoaringTreemap::new();
        tm.insert(1);
        let dv = DeletionVector::from(tm);
        let mut bytes = dv.to_bytes().unwrap();
        // Append a stray byte so the encoded length disagrees with the payload size.
        bytes.push(0);
        let err = DeletionVector::try_from(bytes.as_slice()).unwrap_err();
        match err {
            Error::InvalidFormat(msg) => assert!(msg.contains("encoded length"), "got: {msg}"),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
