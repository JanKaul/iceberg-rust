//! Java-parity tests for manifest writer behaviour across format
//! versions, snapshot-id inheritance, manifest rewrites, and
//! compression configuration.
//!
//! Mirrors `org.apache.iceberg.TestManifestWriterVersions` (20 @Test).
//! Java uses a direct `ManifestWriter` test fixture. Rust's
//! `ManifestWriter` is now `pub` (cycle J5), but `CreateTableBuilder`
//! still hardcodes V2 with no public `with_format_version` setter, so
//! V1/V3/V4 paths can't be exercised from an integration test.
//!
//! V2-write direct-API parity lives in cycle J1
//! (`test_manifest_writer_write_v2_per_java` exercises
//! `ManifestWriter::new` → `append` → `finish` and reads back via
//! `ManifestReader::new`). The scenarios in this file are the V1/V3/V4
//! variants and the inheritance/rewrite/compression behaviours that
//! still need either a format-version-aware table builder or compression
//! observability hooks.

#[tokio::test]
#[ignore = "feature gap: CreateTableBuilder hardcodes V2 — V1 table creation isn't exposed"]
async fn test_manifest_writer_v1_write_per_java() {
    // Java: testV1Write.
}

#[tokio::test]
#[ignore = "feature gap: same — V1 delete-manifest write requires V1 table + direct writer"]
async fn test_manifest_writer_v1_write_delete_per_java() {
    // Java: testV1WriteDelete.
}

#[tokio::test]
#[ignore = "feature gap: same — V1 write with snapshot-id inheritance enabled"]
async fn test_manifest_writer_v1_write_with_inheritance_per_java() {
    // Java: testV1WriteWithInheritance.
}

#[tokio::test]
#[ignore = "covered by cycle J1 `test_manifest_writer_write_v2_per_java` (direct ManifestWriter::new → append → finish + ManifestReader::new round-trip)"]
async fn test_manifest_writer_v2_write_per_java() {
    // Java: testV2Write.
}

#[tokio::test]
#[ignore = "feature gap: same — V2 write with snapshot-id inheritance enabled"]
async fn test_manifest_writer_v2_write_with_inheritance_per_java() {
    // Java: testV2WriteWithInheritance.
}

#[tokio::test]
#[ignore = "feature gap: V2+ delete-file write — Rust models delete files but the direct writer isn't exposed"]
async fn test_manifest_writer_v2_plus_write_delete_per_java() {
    // Java: testV2PlusWriteDelete.
}

#[tokio::test]
#[ignore = "feature gap: same — V2 delete-write with inheritance"]
async fn test_manifest_writer_v2_write_delete_with_inheritance_per_java() {
    // Java: testV2WriteDeleteWithInheritance.
}

#[tokio::test]
#[ignore = "feature gap: V2 manifest-list rewrite path (re-writing the manifest list to drop/replace entries) with snapshot-id inheritance"]
async fn test_manifest_writer_v2_manifest_list_rewrite_with_inheritance_per_java() {
    // Java: testV2ManifestListRewriteWithInheritance.
}

#[tokio::test]
#[ignore = "feature gap: same — V2 individual manifest rewrite (rewriting entries within a manifest) with inheritance"]
async fn test_manifest_writer_v2_manifest_rewrite_with_inheritance_per_java() {
    // Java: testV2ManifestRewriteWithInheritance.
}

#[tokio::test]
#[ignore = "feature gap: V3 table creation not exposed (no with_format_version on CreateTableBuilder)"]
async fn test_manifest_writer_v3_write_per_java() {
    // Java: testV3Write.
}

#[tokio::test]
#[ignore = "feature gap: same — V3 with snapshot-id inheritance"]
async fn test_manifest_writer_v3_write_with_inheritance_per_java() {
    // Java: testV3WriteWithInheritance.
}

#[tokio::test]
#[ignore = "feature gap: V3 first_row_id assignment requires both V3 table creation AND Snapshot.first_row_id on Snapshot (cycle H22 spec gap)"]
async fn test_manifest_writer_v3_write_first_row_id_assignment_per_java() {
    // Java: testV3WriteFirstRowIdAssignment.
}

#[tokio::test]
#[ignore = "feature gap: V3 manifest list rewrite with inheritance"]
async fn test_manifest_writer_v3_manifest_list_rewrite_with_inheritance_per_java() {
    // Java: testV3ManifestListRewriteWithInheritance.
}

#[tokio::test]
#[ignore = "feature gap: V3 individual manifest rewrite with inheritance"]
async fn test_manifest_writer_v3_manifest_rewrite_with_inheritance_per_java() {
    // Java: testV3ManifestRewriteWithInheritance.
}

#[tokio::test]
#[ignore = "feature gap: default manifest Avro compression codec — Rust uses an internal default; no Java-style readAvroCodec helper to assert against"]
async fn test_manifest_writer_default_manifest_compression_per_java() {
    // Java: testDefaultManifestCompression. Java asserts the default
    // Avro codec is 'snappy'.
}

#[tokio::test]
#[ignore = "feature gap: AVRO_COMPRESSION_DEFAULT table property override not honored by Rust's writer (or not exposed for assertion)"]
async fn test_manifest_writer_custom_manifest_compression_per_java() {
    // Java: testCustomManifestCompression. Java sets
    // write.avro.compression-codec=zstd and asserts the manifest avro
    // file uses zstd.
}

#[tokio::test]
#[ignore = "feature gap: V4 (Iceberg V4) is not implemented; FormatVersion enum tops out at V3"]
async fn test_manifest_writer_v4_write_partitioned_per_java() {
    // Java: testV4WritePartitioned.
}

#[tokio::test]
#[ignore = "feature gap: same — V4 unpartitioned write"]
async fn test_manifest_writer_v4_write_unpartitioned_per_java() {
    // Java: testV4WriteUnpartitioned.
}

#[tokio::test]
#[ignore = "feature gap: same — V4 delete write (partitioned)"]
async fn test_manifest_writer_v4_write_delete_partitioned_per_java() {
    // Java: testV4WriteDeletePartitioned.
}

#[tokio::test]
#[ignore = "feature gap: same — V4 delete write (unpartitioned)"]
async fn test_manifest_writer_v4_write_delete_unpartitioned_per_java() {
    // Java: testV4WriteDeleteUnpartitioned.
}
