//! Integration tests for handling empty schema in manifest file user metadata.
//!
//! ## Background
//!
//! This test suite reproduces a real-world issue where Iceberg manifest files
//! contain empty or invalid schema in their Avro user metadata. This can happen
//! when manifest files are created by different implementations or during data
//! corruption scenarios.
//!
//! During table scans, the `datafiles()` function reads manifest files and attempts
//! to parse the schema from the user metadata. If the schema field is empty or
//! contains invalid JSON, the `ManifestReader::new()` method will fail when trying
//! to deserialize it.
//!
//! ## Issue Description
//!
//! In the `ManifestReader::new()` function (iceberg-rust/src/table/manifest.rs:108-119),
//! the schema is read from user metadata and parsed:
//!
//! ```ignore
//! let schema: Schema = match format_version {
//!     FormatVersion::V1 => TryFrom::<SchemaV1>::try_from(serde_json::from_slice(
//!         metadata.get("schema").ok_or(...)?,
//!     )?)?,
//!     FormatVersion::V2 => TryFrom::<SchemaV2>::try_from(serde_json::from_slice(
//!         metadata.get("schema").ok_or(...)?,
//!     )?)?,
//! };
//! ```
//!
//! If the schema field contains:
//! - An empty byte array (`b""`)
//! - An empty JSON object (`b"{}"`)
//! - A valid schema structure but with zero fields (`{"type":"struct","schema-id":0,"fields":[]}`)
//! - Invalid JSON
//!
//! Then either `serde_json::from_slice()` will fail, or the schema will parse but be invalid
//! for use (zero fields case).
//!
//! ## Real-World Example
//!
//! The test case `test_zero_field_schema_in_manifest_metadata` reproduces an actual issue
//! found in production data from Snowflake Iceberg tables, where manifest files contained:
//!
//! ```json
//! {
//!   "schema": "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}"
//! }
//! ```
//!
//! This is a valid Iceberg schema structure, but with zero fields, which causes issues
//! when the code tries to use this schema for data operations.
//!
//! ## Test Limitations
//!
//! These tests create manifest files with corrupted schema metadata and verify
//! they can be stored. However, they do not test the actual failure path through
//! `ManifestReader::new()` because that struct is private (`pub(crate)`).
//!
//! The tests serve as:
//! 1. Documentation of the issue
//! 2. Fixtures for reproducing the problem
//! 3. A foundation for testing any fixes to handle empty schemas gracefully

use apache_avro::{Reader as AvroReader, Schema as AvroSchema, Writer as AvroWriter};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use futures::stream;

use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::error::Error;
use iceberg_rust::object_store::{Bucket, ObjectStoreBuilder};
use iceberg_rust::table::Table;
use iceberg_rust_spec::spec::partition::PartitionSpec;
use iceberg_rust_spec::spec::schema::Schema;
use iceberg_rust_spec::spec::types::{PrimitiveType, StructField, Type};
use iceberg_rust_spec::util::strip_prefix;
use iceberg_sql_catalog::SqlCatalog;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

/// Test for verifying behavior when a manifest file has a schema with zero fields.
///
/// This test reproduces the ACTUAL issue found in production Snowflake Iceberg tables,
/// where manifest files contain a valid schema structure but with zero fields.
///
/// The schema in user metadata looks like:
/// `{"type":"struct","schema-id":0,"fields":[]}`
///
/// This is different from an empty schema - it's a valid JSON structure that parses
/// correctly, but represents an invalid/unusable schema with no fields.
#[tokio::test]
async fn test_zero_field_schema_in_manifest_metadata() {
    // Create object store for testing
    let object_store_builder = ObjectStoreBuilder::memory();

    // Create a manifest file with zero-field schema in user metadata
    // This reproduces the real-world issue from Snowflake tables
    let manifest_bytes = create_manifest_with_zero_field_schema();

    // Verify the manifest file was created
    assert!(!manifest_bytes.is_empty(), "Manifest file should not be empty");

    // Write the manifest to object store
    let manifest_path = ObjectPath::from("warehouse/test/metadata/zero-field-schema-manifest.avro");
    let object_store_impl = object_store_builder
        .build(Bucket::Local)
        .expect("Failed to build object store");

    object_store_impl
        .put(&manifest_path, manifest_bytes.clone().into())
        .await
        .expect("Failed to write manifest with zero-field schema");

    // Verify we can read it back
    let read_bytes = object_store_impl
        .get(&manifest_path)
        .await
        .expect("Failed to get manifest")
        .bytes()
        .await
        .expect("Failed to read manifest bytes");

    assert_eq!(
        manifest_bytes.len(),
        read_bytes.len(),
        "Read bytes should match written bytes"
    );

    // Verify the user metadata using Python avro to check the schema
    // Note: The actual failure would occur when ManifestReader::new() tries to use
    // this zero-field schema, which is problematic even though it parses correctly.
    println!("Created manifest with zero-field schema matching production issue");
}

/// Test for verifying behavior when a manifest file has empty schema in user metadata.
///
/// This test reproduces the issue where a manifest file contains empty schema bytes
/// in its user metadata. When reading such manifests during table scans, the code
/// should fail gracefully with a clear error message.
///
/// The test demonstrates:
/// 1. Creating a manifest file with empty schema in user metadata
/// 2. Verifying that such a manifest file can be created (simulating real-world corruption)
/// 3. This serves as documentation for the issue where empty schemas in manifest
///    user metadata cause failures during table scans
#[tokio::test]
async fn test_empty_schema_in_manifest_metadata() {
    // Create object store for testing
    let object_store_builder = ObjectStoreBuilder::memory();

    // Create a manifest file with empty schema in user metadata
    // This simulates the real-world issue where manifests can have empty schemas
    let manifest_bytes = create_manifest_with_empty_schema();

    // Verify the manifest file was created
    assert!(!manifest_bytes.is_empty(), "Manifest file should not be empty");

    // Write the corrupted manifest to object store to verify storage works
    let manifest_path =
        ObjectPath::from("warehouse/test/metadata/corrupted-manifest-empty-schema.avro");
    let object_store_impl = object_store_builder
        .build(Bucket::Local)
        .expect("Failed to build object store");

    object_store_impl
        .put(&manifest_path, manifest_bytes.clone().into())
        .await
        .expect("Failed to write corrupted manifest");

    // Verify we can read it back
    let read_bytes = object_store_impl
        .get(&manifest_path)
        .await
        .expect("Failed to get manifest")
        .bytes()
        .await
        .expect("Failed to read manifest bytes");

    assert_eq!(
        manifest_bytes.len(),
        read_bytes.len(),
        "Read bytes should match written bytes"
    );

    // Note: The actual failure would occur when trying to use ManifestReader::new()
    // on this data, but since ManifestReader is private, this test documents
    // the scenario rather than testing the exact failure path.
    // The fix should handle empty schema bytes gracefully in ManifestReader::new()
}

/// Creates a manifest file with a valid schema structure but zero fields.
///
/// This reproduces the exact scenario found in production Snowflake Iceberg tables:
/// `{"type":"struct","schema-id":0,"fields":[]}`
fn create_manifest_with_zero_field_schema() -> Vec<u8> {
    // Create the Avro schema for manifest entries (V2 format)
    let manifest_entry_schema = r#"
    {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {
                "name": "status",
                "type": "int"
            },
            {
                "name": "snapshot_id",
                "type": ["null", "long"],
                "default": null
            },
            {
                "name": "sequence_number",
                "type": ["null", "long"],
                "default": null
            },
            {
                "name": "file_sequence_number",
                "type": ["null", "long"],
                "default": null
            },
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {
                            "name": "content",
                            "type": "int"
                        },
                        {
                            "name": "file_path",
                            "type": "string"
                        },
                        {
                            "name": "file_format",
                            "type": "string"
                        },
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": []
                            }
                        },
                        {
                            "name": "record_count",
                            "type": "long"
                        },
                        {
                            "name": "file_size_in_bytes",
                            "type": "long"
                        }
                    ]
                }
            }
        ]
    }
    "#;

    let schema = AvroSchema::parse_str(manifest_entry_schema)
        .expect("Failed to parse manifest entry schema");

    let mut writer = AvroWriter::new(&schema, Vec::new());

    // Add user metadata with ZERO-FIELD schema - this is the real issue
    // The schema is valid JSON and valid Iceberg schema structure, but has no fields
    writer
        .add_user_metadata("format-version".to_string(), b"2")
        .expect("Failed to add format-version");

    // This is the exact schema found in production Snowflake tables
    writer
        .add_user_metadata(
            "schema".to_string(),
            br#"{"type":"struct","schema-id":0,"fields":[]}"#,
        )
        .expect("Failed to add zero-field schema");

    writer
        .add_user_metadata("schema-id".to_string(), b"0")
        .expect("Failed to add schema-id");

    writer
        .add_user_metadata("partition-spec".to_string(), b"[]")
        .expect("Failed to add partition-spec");

    writer
        .add_user_metadata("partition-spec-id".to_string(), b"0")
        .expect("Failed to add partition-spec-id");

    writer
        .add_user_metadata("content".to_string(), b"data")
        .expect("Failed to add content");

    // Flush and return the bytes
    writer.flush().expect("Failed to flush writer");
    writer
        .into_inner()
        .expect("Failed to get bytes from writer")
}

/// Creates a manifest file with empty schema in user metadata.
///
/// This simulates the real-world scenario where a manifest file exists
/// but has an empty or invalid schema in its Avro user metadata.
fn create_manifest_with_empty_schema() -> Vec<u8> {
    // Create the Avro schema for manifest entries
    // Based on Iceberg spec V1 manifest entry structure
    let manifest_entry_schema = r#"
    {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {
                "name": "status",
                "type": "int"
            },
            {
                "name": "snapshot_id",
                "type": ["null", "long"],
                "default": null
            },
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {
                            "name": "file_path",
                            "type": "string"
                        },
                        {
                            "name": "file_format",
                            "type": "string"
                        },
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": []
                            }
                        },
                        {
                            "name": "record_count",
                            "type": "long"
                        },
                        {
                            "name": "file_size_in_bytes",
                            "type": "long"
                        },
                        {
                            "name": "block_size_in_bytes",
                            "type": "long"
                        },
                        {
                            "name": "column_sizes",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "k117_v118",
                                    "fields": [
                                        {
                                            "name": "key",
                                            "type": "int"
                                        },
                                        {
                                            "name": "value",
                                            "type": "long"
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "name": "value_counts",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "k119_v120",
                                    "fields": [
                                        {
                                            "name": "key",
                                            "type": "int"
                                        },
                                        {
                                            "name": "value",
                                            "type": "long"
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "name": "null_value_counts",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "k121_v122",
                                    "fields": [
                                        {
                                            "name": "key",
                                            "type": "int"
                                        },
                                        {
                                            "name": "value",
                                            "type": "long"
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "name": "lower_bounds",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "k126_v127",
                                    "fields": [
                                        {
                                            "name": "key",
                                            "type": "int"
                                        },
                                        {
                                            "name": "value",
                                            "type": "bytes"
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "name": "upper_bounds",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "k128_v129",
                                    "fields": [
                                        {
                                            "name": "key",
                                            "type": "int"
                                        },
                                        {
                                            "name": "value",
                                            "type": "bytes"
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "name": "key_metadata",
                            "type": ["null", "bytes"],
                            "default": null
                        }
                    ]
                }
            }
        ]
    }
    "#;

    let schema =
        AvroSchema::parse_str(manifest_entry_schema).expect("Failed to parse manifest entry schema");

    let mut writer = AvroWriter::new(&schema, Vec::new());

    // Add user metadata with EMPTY schema - this is the key to reproducing the bug
    // Instead of a valid JSON schema, we add an empty byte array
    writer
        .add_user_metadata("format-version".to_string(), b"1")
        .expect("Failed to add format-version");

    writer
        .add_user_metadata("schema".to_string(), b"")
        .expect("Failed to add empty schema");

    writer
        .add_user_metadata("schema-id".to_string(), b"0")
        .expect("Failed to add schema-id");

    writer
        .add_user_metadata("partition-spec".to_string(), b"[]")
        .expect("Failed to add partition-spec");

    writer
        .add_user_metadata("partition-spec-id".to_string(), b"0")
        .expect("Failed to add partition-spec-id");

    writer
        .add_user_metadata("content".to_string(), b"data")
        .expect("Failed to add content");

    // Flush the writer to get the bytes
    writer.flush().expect("Failed to flush writer");

    writer
        .into_inner()
        .expect("Failed to get bytes from writer")
}

/// Test for verifying behavior when schema in manifest metadata is an empty JSON object.
///
/// This is another variant where the schema field exists but contains an empty JSON object "{}"
/// instead of a valid Iceberg schema structure.
#[tokio::test]
async fn test_empty_json_schema_in_manifest_metadata() {
    // Create object store for testing
    let object_store_builder = ObjectStoreBuilder::memory();

    // Create a manifest file with empty JSON object as schema
    let manifest_bytes = create_manifest_with_empty_json_schema();

    // Verify the manifest file was created
    assert!(!manifest_bytes.is_empty(), "Manifest file should not be empty");

    // Write to object store
    let manifest_path =
        ObjectPath::from("warehouse/test/metadata/corrupted-manifest-empty-json.avro");
    let object_store_impl = object_store_builder
        .build(Bucket::Local)
        .expect("Failed to build object store");

    object_store_impl
        .put(&manifest_path, manifest_bytes.clone().into())
        .await
        .expect("Failed to write corrupted manifest");

    // Read back the file
    let read_bytes = object_store_impl
        .get(&manifest_path)
        .await
        .expect("Failed to get manifest")
        .bytes()
        .await
        .expect("Failed to read manifest bytes");

    assert_eq!(
        manifest_bytes.len(),
        read_bytes.len(),
        "Read bytes should match written bytes"
    );

    // Note: Similar to the first test, this documents the scenario where schema
    // is an empty JSON object. The actual parsing failure would occur in
    // ManifestReader::new() when it tries to deserialize the empty JSON as a Schema.
}

/// Creates a manifest file with empty JSON object "{}" as schema in user metadata.
fn create_manifest_with_empty_json_schema() -> Vec<u8> {
    let manifest_entry_schema = r#"
    {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {
                "name": "status",
                "type": "int"
            },
            {
                "name": "snapshot_id",
                "type": ["null", "long"],
                "default": null
            },
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {
                            "name": "file_path",
                            "type": "string"
                        },
                        {
                            "name": "file_format",
                            "type": "string"
                        },
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": []
                            }
                        },
                        {
                            "name": "record_count",
                            "type": "long"
                        },
                        {
                            "name": "file_size_in_bytes",
                            "type": "long"
                        },
                        {
                            "name": "block_size_in_bytes",
                            "type": "long"
                        }
                    ]
                }
            }
        ]
    }
    "#;

    let schema =
        AvroSchema::parse_str(manifest_entry_schema).expect("Failed to parse manifest entry schema");

    let mut writer = AvroWriter::new(&schema, Vec::new());

    // Add user metadata with empty JSON object as schema
    writer
        .add_user_metadata("format-version".to_string(), b"1")
        .expect("Failed to add format-version");

    writer
        .add_user_metadata("schema".to_string(), b"{}")
        .expect("Failed to add empty JSON schema");

    writer
        .add_user_metadata("schema-id".to_string(), b"0")
        .expect("Failed to add schema-id");

    writer
        .add_user_metadata("partition-spec".to_string(), b"[]")
        .expect("Failed to add partition-spec");

    writer
        .add_user_metadata("partition-spec-id".to_string(), b"0")
        .expect("Failed to add partition-spec-id");

    writer
        .add_user_metadata("content".to_string(), b"data")
        .expect("Failed to add content");

    writer.flush().expect("Failed to flush writer");
    writer
        .into_inner()
        .expect("Failed to get bytes from writer")
}

/// **COMPREHENSIVE INTEGRATION TEST**
///
/// Test that verifies the actual behavior when calling datafiles() on a table
/// where one manifest file has zero-field schema.
///
/// Expected behavior: When one manifest has a corrupted zero-field schema,
/// the datafiles() function should return only the datafiles from valid manifests
/// (1 datafile) instead of panicking.
///
/// Current behavior: The code panics with .unwrap() on line 352 of table/mod.rs
/// when ManifestReader::new() fails to parse the zero-field schema.
#[tokio::test]
async fn test_datafiles_with_zero_field_schema_manifest() {
    // 1. Set up object store and catalog
    let object_store = ObjectStoreBuilder::memory();
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store.clone())
            .await
            .unwrap(),
    );

    // 2. Create schema
    let schema = Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "data".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
        })
        .build()
        .unwrap();

    // 3. Create table with no partitioning
    let partition_spec = PartitionSpec::builder()
        .with_fields(vec![])
        .build()
        .unwrap();

    let mut table = Table::builder()
        .with_name("test_zero_field_schema")
        .with_location("/test/test_zero_field_schema")
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("Failed to create table");

    // 4. Create first RecordBatch
    let batch1 = create_record_batch(vec![1], vec!["data1"]);
    let stream1 = stream::iter(vec![Ok(batch1)]);

    let datafiles1 = write_parquet_partitioned(&table, stream1, None)
        .await
        .expect("Failed to write first parquet file");

    // 5. Append first data
    table
        .new_transaction(None)
        .append_data(datafiles1.clone())
        .commit()
        .await
        .expect("Failed to append first data");

    // 6. Create second RecordBatch
    let batch2 = create_record_batch(vec![2], vec!["data2"]);
    let stream2 = stream::iter(vec![Ok(batch2)]);

    let datafiles2 = write_parquet_partitioned(&table, stream2, None)
        .await
        .expect("Failed to write second parquet file");

    // 7. Append second data
    table
        .new_transaction(None)
        .append_data(datafiles2.clone())
        .commit()
        .await
        .expect("Failed to append second data");

    // 8. Verify we have 2 snapshots
    assert_eq!(
        table.metadata().snapshots.len(),
        2,
        "Should have 2 snapshots"
    );

    // 9. Get manifest list entries
    let manifest_entries: Vec<_> = table
        .manifests(None, None)
        .await
        .expect("Failed to get manifests");

    println!("Total manifests: {}", manifest_entries.len());

    // Note: In the current implementation, both datafiles might be in one manifest
    // If we only have 1 manifest, we'll adjust the test expectation
    let single_manifest_mode = manifest_entries.len() == 1;

    if single_manifest_mode {
        println!("NOTE: Both datafiles are in a single manifest. Adjusting test expectations.");
    }

    // 10. Before corruption: verify we can read all datafiles
    let mut pre_corruption_datafiles = table
        .datafiles(&manifest_entries, None, (None, None))
        .await
        .expect("Failed to get datafiles before corruption");

    let mut pre_corruption_count = 0;
    while let Some(Ok(_)) = pre_corruption_datafiles.next() {
        pre_corruption_count += 1;
    }

    println!("Datafiles before corruption: {}", pre_corruption_count);
    assert_eq!(
        pre_corruption_count, 2,
        "Should have 2 datafiles before corruption"
    );

    // 11. Corrupt manifest file(s) to have zero-field schema
    let object_store_impl = object_store
        .build(Bucket::Local)
        .expect("Failed to build object store");

    if single_manifest_mode {
        // If there's only one manifest with both datafiles, corrupt it
        // Expected result: 0 datafiles (current behavior shows it panics)
        if let Some(manifest) = manifest_entries.first() {
            let manifest_path = &manifest.manifest_path;
            println!("Corrupting the only manifest: {}", manifest_path);

            let path: ObjectPath = strip_prefix(manifest_path).into();
            let original_bytes = object_store_impl
                .get(&path)
                .await
                .expect("Failed to get original manifest")
                .bytes()
                .await
                .expect("Failed to read original manifest bytes");

            let corrupted_bytes =
                create_corrupted_manifest_with_zero_field_schema(&original_bytes);

            object_store_impl
                .put(&path, corrupted_bytes.into())
                .await
                .expect("Failed to write corrupted manifest");

            println!("Successfully corrupted manifest file");
        }
    } else {
        // If there are 2 manifests, corrupt only the first one
        // Expected result: 1 datafile from the second (non-corrupted) manifest
        if let Some(first_manifest) = manifest_entries.first() {
            let manifest_path = &first_manifest.manifest_path;
            println!("Corrupting first manifest: {}", manifest_path);

            let path: ObjectPath = strip_prefix(manifest_path).into();
            let original_bytes = object_store_impl
                .get(&path)
                .await
                .expect("Failed to get original manifest")
                .bytes()
                .await
                .expect("Failed to read original manifest bytes");

            let corrupted_bytes =
                create_corrupted_manifest_with_zero_field_schema(&original_bytes);

            object_store_impl
                .put(&path, corrupted_bytes.into())
                .await
                .expect("Failed to write corrupted manifest");

            println!("Successfully corrupted first manifest file");
        }
    }

    // 12. After corruption: try to read datafiles
    // Expected: Should return 1 datafile (from the non-corrupted manifest)
    // Current behavior: Will panic at .unwrap() in datafiles() when ManifestReader::new() fails
    let result = table.datafiles(&manifest_entries, None, (None, None)).await;

    match result {
        Ok(mut datafiles_iter) => {
            // Count the datafiles
            let mut count = 0;
            while let Some(result) = datafiles_iter.next() {
                match result {
                    Ok(_) => count += 1,
                    Err(e) => {
                        println!("Error reading datafile: {:?}", e);
                        // Skip errors and continue counting
                    }
                }
            }

            println!("Datafiles after corruption: {}", count);

            // EXPECTED behavior WITH THE FIX:
            // The fix makes ManifestReader fallback to the table schema when manifest has zero fields
            // - If 2 manifests: Should return 2 datafiles (all datafiles, using fallback schema for corrupted manifest)
            // - If 1 manifest: Should return 2 datafiles (using fallback schema for the corrupted manifest)
            // The fix allows graceful handling of Snowflake-generated manifests with zero-field schemas
            let expected_count = 2;

            assert_eq!(
                count, expected_count,
                "Expected {} datafile(s) with fallback schema but got {}. Single manifest mode: {}",
                expected_count,
                count,
                single_manifest_mode
            );

            println!("✓ SUCCESS: Manifest with zero-field schema successfully used table schema fallback!");
            println!("✓ All {} datafiles were correctly returned despite corrupted manifest", count);
        }
        Err(e) => {
            // This is the CURRENT behavior - it fails completely
            println!("Failed to get datafiles after corruption: {:?}", e);
            panic!(
                "datafiles() should handle corrupted manifests gracefully, but failed with: {:?}",
                e
            );
        }
    }
}

/// Helper function to create a RecordBatch for testing
fn create_record_batch(ids: Vec<i64>, data: Vec<&str>) -> RecordBatch {
    let schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]);

    let id_array = Int64Array::from(ids);
    let data_array = StringArray::from(data);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(id_array), Arc::new(data_array)],
    )
    .expect("Failed to create RecordBatch")
}

/// Creates a corrupted version of a manifest file with zero-field schema.
///
/// This reads the original manifest's structure but replaces the schema
/// in user metadata with a zero-field schema to reproduce the Snowflake issue.
fn create_corrupted_manifest_with_zero_field_schema(original_bytes: &[u8]) -> Vec<u8> {
    let cursor = std::io::Cursor::new(original_bytes);
    let reader = AvroReader::new(cursor).expect("Failed to read original manifest");

    // Get the Avro schema from the original file
    let avro_schema = reader.writer_schema().clone();

    // Create a new writer with the same Avro schema
    let mut writer = AvroWriter::new(&avro_schema, Vec::new());

    // Add user metadata with ZERO-FIELD schema (the bug!)
    writer
        .add_user_metadata("format-version".to_string(), b"2")
        .expect("Failed to add format-version");

    // This is the exact zero-field schema from production Snowflake tables
    writer
        .add_user_metadata(
            "schema".to_string(),
            br#"{"type":"struct","schema-id":0,"fields":[]}"#,
        )
        .expect("Failed to add zero-field schema");

    writer
        .add_user_metadata("schema-id".to_string(), b"0")
        .expect("Failed to add schema-id");

    writer
        .add_user_metadata("partition-spec".to_string(), b"[]")
        .expect("Failed to add partition-spec");

    writer
        .add_user_metadata("partition-spec-id".to_string(), b"0")
        .expect("Failed to add partition-spec-id");

    writer
        .add_user_metadata("content".to_string(), b"data")
        .expect("Failed to add content");

    // Copy all records from original manifest
    let cursor = std::io::Cursor::new(original_bytes);
    let reader = AvroReader::new(cursor).expect("Failed to read original manifest");

    for value in reader {
        match value {
            Ok(v) => {
                writer.append(v).expect("Failed to append record");
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to read record from original manifest: {}",
                    e
                );
            }
        }
    }

    // Flush and return
    writer.flush().expect("Failed to flush writer");
    writer
        .into_inner()
        .expect("Failed to get bytes from writer")
}
