//! Java-parity tests for manifest read/write.
//!
//! Mirrors a subset of `org.apache.iceberg.TestManifestReader` (15
//! @Test) + `TestManifestWriter` (9 @Test). Both Java classes test the
//! ManifestReader/Writer APIs directly. Rust's `ManifestReader` and
//! `ManifestWriter` are `pub(crate)` — not exposed to integration
//! tests — so this port exercises the manifest layer indirectly
//! through `table.manifests()` / `table.datafiles()` after an append.
//!
//! Observable scenarios that can be exercised that way:
//!   - Single-append round-trip: data files written via the write path
//!     are reachable through table.datafiles().
//!   - Partition values preserved across write→read.
//!   - DataFile core metadata (record_count, file_size_in_bytes,
//!     file_path) preserved.
//!   - Multi-append: multiple manifests reachable.
//!   - Empty manifest handling.
//!
//! Scenarios that require the internal reader/writer API (filter,
//! inheritable metadata, split offsets, V3 deletion vectors, V3
//! row-id assignment) stay #[ignore].

use std::sync::Arc;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use futures::stream;
use iceberg_rust::{
    arrow::write::write_parquet_partitioned,
    catalog::Catalog,
    object_store::ObjectStoreBuilder,
    table::{
        manifest::{ManifestReader, ManifestWriter},
        Table,
    },
};
use iceberg_rust_spec::spec::{
    manifest::{partition_value_schema, DataFile, ManifestEntry, Status},
    manifest_list::Content as ManifestListContent,
    partition::{PartitionField, PartitionSpec, Transform},
    schema::Schema,
    table_metadata::FormatVersion,
    types::{PrimitiveType, StructField, Type},
    values::Value,
};
use iceberg_rust_spec::util::strip_prefix;
use iceberg_sql_catalog::SqlCatalog;
use object_store::{path::Path as ObjectStorePath, ObjectStoreExt};

// --- Shared test fixtures --------------------------------------------------

fn schema() -> Schema {
    Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "region".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .build()
        .unwrap()
}

fn partition_spec() -> PartitionSpec {
    PartitionSpec::builder()
        .with_partition_field(PartitionField::new(2, 1000, "region", Transform::Identity))
        .build()
        .unwrap()
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ])
}

fn batch(rows: &[(i64, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let regions: Vec<&str> = rows.iter().map(|(_, r)| *r).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .unwrap()
}

async fn fresh_table(name: &str) -> Table {
    let object_store = ObjectStoreBuilder::memory();
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store)
            .await
            .unwrap(),
    );
    Table::builder()
        .with_name(name)
        .with_location(format!("/test/{name}"))
        .with_schema(schema())
        .with_partition_spec(partition_spec())
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("create table")
}

// --- Passing tests ---------------------------------------------------------

/// Java parity for the basic ManifestReader round-trip: data files
/// written via the table.append path must be reachable via
/// table.datafiles(), with their file_path preserved.
#[tokio::test]
async fn test_manifest_round_trip_preserves_file_paths_per_java() {
    let mut table = fresh_table("manifest_round_trip").await;

    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[
            (1, "us-east"),
            (2, "us-east"),
            (3, "us-west"),
        ]))]),
        None,
    )
    .await
    .expect("write");
    let written_paths: Vec<String> = files.iter().map(|f| f.file_path().to_owned()).collect();
    assert!(
        !written_paths.is_empty(),
        "writer must produce at least one DataFile"
    );

    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("append commit");

    // Read the manifest entries back via the public table API.
    let manifests = table.manifests(None, None).await.expect("manifests");
    assert!(
        !manifests.is_empty(),
        "must have at least one manifest entry after commit"
    );

    let mut read_paths = Vec::new();
    let data_files = table
        .datafiles(&manifests, None, (None, None))
        .await
        .expect("datafiles");
    for result in data_files {
        let (_, entry) = result.expect("entry");
        read_paths.push(entry.data_file().file_path().to_owned());
    }
    assert_eq!(
        read_paths.len(),
        written_paths.len(),
        "every written file must be readable"
    );
    for p in &written_paths {
        assert!(
            read_paths.iter().any(|r| r == p),
            "written file {p} must be reachable via datafiles(); got {read_paths:?}",
        );
    }
}

/// Java parity for testManifestReaderWithPartitionMetadata: partition
/// values must round-trip from write to read.
#[tokio::test]
async fn test_manifest_round_trip_preserves_partition_values_per_java() {
    let mut table = fresh_table("manifest_partition").await;

    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[
            (1, "us-east"),
            (2, "us-east"),
            (3, "us-west"),
            (4, "eu-central"),
        ]))]),
        None,
    )
    .await
    .expect("write");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit");

    let manifests = table.manifests(None, None).await.expect("manifests");
    let mut regions_seen = std::collections::HashSet::new();
    let data_files = table
        .datafiles(&manifests, None, (None, None))
        .await
        .expect("datafiles");
    for result in data_files {
        let (_, entry) = result.expect("entry");
        if let Some(Some(Value::String(s))) = entry.data_file().partition().get("region") {
            regions_seen.insert(s.clone());
        }
    }
    assert!(
        regions_seen.contains("us-east")
            && regions_seen.contains("us-west")
            && regions_seen.contains("eu-central"),
        "all three written partition values must round-trip; got {regions_seen:?}",
    );
}

/// Java parity for testDataFilePositions etc. — core DataFile metadata
/// (record_count, file_size_in_bytes) is preserved through the write→
/// read round-trip and is greater-than-zero for non-empty input.
#[tokio::test]
async fn test_manifest_round_trip_preserves_core_data_file_metadata_per_java() {
    let mut table = fresh_table("manifest_metadata").await;

    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[
            (1, "us-east"),
            (2, "us-east"),
            (3, "us-east"),
        ]))]),
        None,
    )
    .await
    .expect("write");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit");

    let manifests = table.manifests(None, None).await.expect("manifests");
    let data_files = table
        .datafiles(&manifests, None, (None, None))
        .await
        .expect("datafiles");
    let mut saw_any = false;
    for result in data_files {
        let (_, entry) = result.expect("entry");
        let df = entry.data_file();
        assert!(
            *df.record_count() > 0,
            "record_count must be > 0 for a non-empty file",
        );
        assert!(
            *df.file_size_in_bytes() > 0,
            "file_size_in_bytes must be > 0",
        );
        saw_any = true;
    }
    assert!(saw_any, "expected at least one data file in the manifest");
}

/// Java parity: after two separate appends, both batches' data files
/// remain reachable via the latest snapshot's manifest list — fast-
/// append doesn't merge older manifests away.
#[tokio::test]
async fn test_manifest_multiple_appends_all_reachable_per_java() {
    let mut table = fresh_table("manifest_multi_appends").await;

    let first = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(1, "us-east")]))]),
        None,
    )
    .await
    .expect("write first");
    let first_paths: Vec<String> = first.iter().map(|f| f.file_path().to_owned()).collect();
    table
        .new_transaction(None)
        .append_data(first)
        .commit()
        .await
        .expect("commit first");

    let second = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(2, "us-west")]))]),
        None,
    )
    .await
    .expect("write second");
    let second_paths: Vec<String> = second.iter().map(|f| f.file_path().to_owned()).collect();
    table
        .new_transaction(None)
        .append_data(second)
        .commit()
        .await
        .expect("commit second");

    let manifests = table.manifests(None, None).await.expect("manifests");
    let mut read_paths = Vec::new();
    let data_files = table
        .datafiles(&manifests, None, (None, None))
        .await
        .expect("datafiles");
    for result in data_files {
        let (_, entry) = result.expect("entry");
        read_paths.push(entry.data_file().file_path().to_owned());
    }
    for p in first_paths.iter().chain(second_paths.iter()) {
        assert!(
            read_paths.iter().any(|r| r == p),
            "file {p} from earlier append must still be reachable; got {read_paths:?}",
        );
    }
}

/// Java parity for the table.manifests() shape: each manifest entry has
/// a non-empty manifest_path that points at an actual Avro file the
/// table.datafiles() call can resolve.
#[tokio::test]
async fn test_manifest_entries_carry_resolvable_path_per_java() {
    let mut table = fresh_table("manifest_path").await;
    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(1, "us-east")]))]),
        None,
    )
    .await
    .expect("write");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit");

    let manifests = table.manifests(None, None).await.expect("manifests");
    for m in &manifests {
        assert!(
            !m.manifest_path.is_empty(),
            "manifest_path must be non-empty",
        );
        assert!(
            m.manifest_path.ends_with(".avro"),
            "manifest_path must point at an Avro file, got {}",
            m.manifest_path,
        );
    }
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "feature gap: ManifestReader::filter(rows_filter) is internal pub(crate); not exposed via table API"]
async fn test_manifest_reader_with_filter_without_select_per_java() {
    // Java: testReaderWithFilterWithoutSelect.
}

#[tokio::test]
#[ignore = "feature gap: empty inheritable metadata handling is an internal detail of ManifestReader::read"]
async fn test_manifest_reader_with_empty_inheritable_metadata_per_java() {
    // Java: testManifestReaderWithEmptyInheritableMetadata.
}

/// Java: `testInvalidUsage` — ManifestReader::new with garbage input
/// must error rather than panic or produce a broken iterator.
#[tokio::test]
async fn test_manifest_reader_invalid_usage_per_java() {
    // Pass non-Avro bytes; reader construction must fail.
    let garbage = b"not an avro manifest file at all".to_vec();
    let result = ManifestReader::new(&garbage[..]);
    assert!(
        result.is_err(),
        "ManifestReader::new on garbage bytes must error",
    );
}

/// Use the now-public `ManifestReader::new` to read the manifest a
/// table commit wrote, and verify the entries' file paths match the
/// data files we appended. This exercises the V2-default Rust path.
#[tokio::test]
async fn test_manifest_reader_round_trip_via_public_reader_per_java() {
    let mut table = fresh_table("manifest_reader_public").await;
    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(1, "us-east"), (2, "us-west")]))]),
        None,
    )
    .await
    .expect("write");
    let written_paths: Vec<String> = files.iter().map(|f| f.file_path().to_owned()).collect();
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit");

    // Get the manifest path from the snapshot's manifest list.
    let manifests = table.manifests(None, None).await.expect("manifests");
    assert!(!manifests.is_empty());
    let manifest_path = manifests[0].manifest_path.clone();

    // Fetch the manifest bytes via object_store.
    let object_store = table.object_store();
    let stripped = strip_prefix(&manifest_path);
    let bytes = object_store
        .get(&ObjectStorePath::from(stripped))
        .await
        .expect("fetch manifest bytes")
        .bytes()
        .await
        .expect("read bytes")
        .to_vec();

    // Use the now-public ManifestReader to iterate entries.
    let reader = ManifestReader::new(&bytes[..]).expect("ManifestReader::new");
    let mut paths_from_reader = Vec::new();
    for entry in reader {
        let entry = entry.expect("manifest entry");
        paths_from_reader.push(entry.data_file().file_path().to_owned());
    }
    assert!(
        !paths_from_reader.is_empty(),
        "manifest must have at least one entry",
    );
    for p in &written_paths {
        assert!(
            paths_from_reader.iter().any(|r| r == p),
            "every written file must appear in the manifest; got {paths_from_reader:?}",
        );
    }
}

#[tokio::test]
#[ignore = "feature gap: V1-table partition-metadata update path — Rust doesn't have updateSpec() to test against"]
async fn test_manifest_reader_with_updated_partition_metadata_v1_per_java() {
    // Java: testManifestReaderWithUpdatedPartitionMetadataForV1Table.
}

#[tokio::test]
#[ignore = "feature gap: data file position assignment is internal manifest detail; not exposed by table.datafiles()"]
async fn test_manifest_reader_data_file_positions_per_java() {
    // Java: testDataFilePositions.
}

#[tokio::test]
#[ignore = "feature gap: same — data file manifest paths are exposed but not the position-within-manifest accessor"]
async fn test_manifest_reader_data_file_manifest_paths_per_java() {
    // Java: testDataFileManifestPaths.
}

#[tokio::test]
#[ignore = "feature gap: V2 delete files don't have a public reader path through table.datafiles() — they require a separate delete-file iterator"]
async fn test_manifest_reader_delete_file_positions_per_java() {
    // Java: testDeleteFilePositions.
}

#[tokio::test]
#[ignore = "feature gap: same — delete file manifest paths"]
async fn test_manifest_reader_delete_file_manifest_paths_per_java() {
    // Java: testDeleteFileManifestPaths.
}

#[tokio::test]
#[ignore = "feature gap: V2+ delete-files-with-references is a delete-file feature Rust doesn't expose"]
async fn test_manifest_reader_delete_files_with_references_per_java() {
    // Java: testDeleteFilesWithReferences.
}

#[tokio::test]
#[ignore = "feature gap: V3 deletion vectors (DVs) — Rust doesn't model them in DataFile or the read path"]
async fn test_manifest_reader_dvs_per_java() {
    // Java: testDVs.
}

#[tokio::test]
#[ignore = "feature gap: V3 first-row-id assignment + committed-vs-uncommitted manifest distinction not modelled in Rust (cycle H22 pins spec-side gap)"]
async fn test_manifest_reader_committed_manifest_nullifies_entry_row_id_per_java() {
    // Java: testReadCommitedManifestNullifiesEntryRowId.
}

#[tokio::test]
#[ignore = "feature gap: same — uncommitted manifest preserves entry row-id"]
async fn test_manifest_reader_uncommitted_manifest_preserves_entry_row_id_per_java() {
    // Java: testReadUncommittedManifestPreservesEntryRowId.
}

#[tokio::test]
#[ignore = "feature gap: DataFile split offsets — Rust doesn't model the offsets field on DataFile; Java's test verifies nullification for invalid sequences"]
async fn test_manifest_reader_data_file_split_offsets_null_when_invalid_per_java() {
    // Java: testDataFileSplitOffsetsNullWhenInvalid.
}

#[tokio::test]
#[ignore = "feature gap: deprecated read-without-specs-by-id path — Rust never had this API"]
async fn test_manifest_reader_deprecated_read_without_specs_by_id_per_java() {
    // Java: testDeprecatedReadWithoutSpecsById.
}

// TestManifestWriter scenarios (subset reachable via public API)

#[tokio::test]
#[ignore = "feature gap: CreateTableBuilder hardcodes V2 — no `with_format_version` setter to create a V1 table for direct writer testing"]
async fn test_manifest_writer_write_v1_per_java() {
    // Java: testV1Write.
}

/// Java: `testV2Write` — direct ManifestWriter access for the V2 manifest
/// schema. Uses the now-public `ManifestWriter::new` + `append` + `finish`
/// chain to write a manifest, then `ManifestReader::new` to read it back.
#[tokio::test]
async fn test_manifest_writer_write_v2_per_java() {
    let mut table = fresh_table("manifest_writer_v2").await;

    // First commit some real DataFiles via the table path so we have
    // valid DataFile structs (with metrics, partition values, etc.) to
    // feed into our direct ManifestWriter.
    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[
            (1, "us-east"),
            (2, "us-east"),
            (3, "us-west"),
        ]))]),
        None,
    )
    .await
    .expect("write");
    let expected_count = files.len();
    table
        .new_transaction(None)
        .append_data(files.clone())
        .commit()
        .await
        .expect("commit");

    // Build the Avro schema for V2 manifest entries using the table's
    // current partition fields.
    let metadata = table.metadata();
    let partition_fields = metadata
        .current_partition_fields(None)
        .expect("partition fields");
    let part_schema = partition_value_schema(&partition_fields).expect("partition value schema");
    let avro_schema =
        ManifestEntry::schema(&part_schema, &FormatVersion::V2).expect("manifest entry schema");

    // Construct a fresh ManifestWriter pointed at a new manifest path.
    let manifest_path = format!("{}/metadata/direct-manifest-v2.avro", metadata.location);
    let snapshot_id = 9876_5432_1098_7654_i64;
    let mut writer = ManifestWriter::new(
        &manifest_path,
        snapshot_id,
        &avro_schema,
        metadata,
        ManifestListContent::Data,
        None,
    )
    .expect("ManifestWriter::new");

    // Append manifest entries for each data file.
    for data_file in &files {
        let entry = ManifestEntry::builder()
            .with_format_version(FormatVersion::V2)
            .with_status(Status::Added)
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(1)
            .with_data_file(data_file.clone())
            .build()
            .expect("manifest entry build");
        writer.append(entry).expect("append");
    }

    // Finish writes the file to the object store and returns the
    // ManifestListEntry.
    let object_store = table.object_store();
    let manifest_list_entry = writer.finish(object_store.clone()).await.expect("finish");
    assert_eq!(
        manifest_list_entry.added_snapshot_id, snapshot_id,
        "manifest list entry must record the snapshot id we wrote with",
    );
    assert!(manifest_list_entry.manifest_length > 0);

    // Read the manifest back via ManifestReader and verify entries.
    let bytes = object_store
        .get(&ObjectStorePath::from(strip_prefix(&manifest_path)))
        .await
        .expect("fetch")
        .bytes()
        .await
        .expect("bytes")
        .to_vec();
    let reader = ManifestReader::new(&bytes[..]).expect("ManifestReader::new");
    let read_paths: Vec<String> = reader
        .map(|r| r.expect("entry").data_file().file_path().to_owned())
        .collect();
    assert_eq!(
        read_paths.len(),
        expected_count,
        "every entry written must read back",
    );
    for file in &files {
        assert!(
            read_paths.iter().any(|p| p == file.file_path()),
            "written file {} must appear in the manifest read-back",
            file.file_path(),
        );
    }
}

#[tokio::test]
#[ignore = "feature gap: CreateTableBuilder hardcodes V2 — V3 table creation not exposed"]
async fn test_manifest_writer_write_v3_per_java() {
    // Java: testV3Write.
}

#[tokio::test]
#[ignore = "feature gap: V3 first_row_id assignment in the manifest writer (cycle H22 spec-side gap)"]
async fn test_manifest_writer_v3_first_row_id_assignment_per_java() {
    // Java: testV3WriteFirstRowIdAssignment.
}

#[tokio::test]
#[ignore = "feature gap: same — mixed row-id assignment"]
async fn test_manifest_writer_v3_mixed_row_id_assignment_per_java() {
    // Java: testV3WriteMixedRowIdAssignment.
}

#[tokio::test]
#[ignore = "feature gap: V1 forward-compatibility read of V2-written manifest"]
async fn test_manifest_writer_v1_forward_compatibility_per_java() {
    // Java: testV1ForwardCompatibility.
}

#[tokio::test]
#[ignore = "feature gap: V2 forward-compatibility read of V3-written manifest"]
async fn test_manifest_writer_v2_forward_compatibility_per_java() {
    // Java: testV2ForwardCompatibility.
}

/// Java: `testManifestsWithoutRowStats` — a manifest entry's `DataFile`
/// is allowed to omit the per-column statistics maps (`column_sizes`,
/// `value_counts`, `null_value_counts`, `nan_value_counts`,
/// `distinct_counts`, `lower_bounds`, `upper_bounds`). Required fields
/// (`record_count`, `file_size_in_bytes`) must still be present. This
/// test writes a manifest whose entries carry None for every optional
/// stats field, then verifies they round-trip back through
/// `ManifestReader::new` as None.
#[tokio::test]
async fn test_manifest_writer_without_row_stats_per_java() {
    let table = fresh_table("manifest_writer_without_row_stats").await;

    // Generate real DataFiles so we have valid partition values + paths
    // to copy over into the stripped-stats entries.
    let original_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[
            (10, "us-east"),
            (20, "us-east"),
            (30, "us-west"),
        ]))]),
        None,
    )
    .await
    .expect("write");
    assert!(!original_files.is_empty());

    // Build a "no stats" twin of each DataFile: required fields preserved,
    // every optional stats map set to None.
    let stripped_files: Vec<DataFile> = original_files
        .iter()
        .map(|df| {
            DataFile::builder()
                .with_content(df.content().clone())
                .with_file_path(df.file_path().clone())
                .with_file_format(df.file_format().clone())
                .with_partition(df.partition().clone())
                .with_record_count(*df.record_count())
                .with_file_size_in_bytes(*df.file_size_in_bytes())
                .with_column_sizes(None)
                .with_value_counts(None)
                .with_null_value_counts(None)
                .with_nan_value_counts(None)
                .with_distinct_counts(None)
                .with_lower_bounds(None)
                .with_upper_bounds(None)
                .build()
                .expect("strip-stats DataFile build")
        })
        .collect();

    let metadata = table.metadata();
    let partition_fields = metadata
        .current_partition_fields(None)
        .expect("partition fields");
    let part_schema = partition_value_schema(&partition_fields).expect("partition value schema");
    let avro_schema =
        ManifestEntry::schema(&part_schema, &FormatVersion::V2).expect("manifest entry schema");

    let manifest_path = format!("{}/metadata/no-row-stats-manifest.avro", metadata.location);
    let snapshot_id = 1357_2468_9999_0001_i64;
    let mut writer = ManifestWriter::new(
        &manifest_path,
        snapshot_id,
        &avro_schema,
        metadata,
        ManifestListContent::Data,
        None,
    )
    .expect("ManifestWriter::new");

    for data_file in &stripped_files {
        let entry = ManifestEntry::builder()
            .with_format_version(FormatVersion::V2)
            .with_status(Status::Added)
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(1)
            .with_data_file(data_file.clone())
            .build()
            .expect("manifest entry build");
        writer.append(entry).expect("append");
    }

    let object_store = table.object_store();
    let manifest_list_entry = writer.finish(object_store.clone()).await.expect("finish");
    assert_eq!(manifest_list_entry.added_snapshot_id, snapshot_id);

    let bytes = object_store
        .get(&ObjectStorePath::from(strip_prefix(&manifest_path)))
        .await
        .expect("fetch")
        .bytes()
        .await
        .expect("bytes")
        .to_vec();

    let reader = ManifestReader::new(&bytes[..]).expect("ManifestReader::new");
    let mut entry_count = 0;
    for r in reader {
        let entry = r.expect("entry");
        let df = entry.data_file();
        assert!(
            df.column_sizes().is_none(),
            "column_sizes must read back as None for stats-less manifest, got {:?}",
            df.column_sizes(),
        );
        assert!(df.value_counts().is_none(), "value_counts must be None");
        assert!(
            df.null_value_counts().is_none(),
            "null_value_counts must be None",
        );
        assert!(
            df.nan_value_counts().is_none(),
            "nan_value_counts must be None",
        );
        assert!(
            df.distinct_counts().is_none(),
            "distinct_counts must be None",
        );
        assert!(df.lower_bounds().is_none(), "lower_bounds must be None");
        assert!(df.upper_bounds().is_none(), "upper_bounds must be None");
        // Required fields must still be present and non-zero.
        assert!(
            *df.record_count() > 0,
            "record_count must round-trip as the original value",
        );
        assert!(*df.file_size_in_bytes() > 0, "file_size_in_bytes must > 0");
        entry_count += 1;
    }
    assert_eq!(
        entry_count,
        stripped_files.len(),
        "every stats-less entry must read back",
    );
}

/// Java: `testManifestsPartitionSummary` — the manifest list entry
/// carries `partitions: Vec<FieldSummary>` summarising the partition
/// values of the contained data files. Rust models this as
/// `ManifestListEntry.partitions: Option<Vec<FieldSummary>>`.
#[tokio::test]
async fn test_manifest_writer_partition_summary_per_java() {
    let mut table = fresh_table("manifest_writer_partition_summary").await;
    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[
            (1, "us-east"),
            (2, "us-west"),
            (3, "eu-central"),
        ]))]),
        None,
    )
    .await
    .expect("write");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit");

    // The manifest list entry must carry partition summaries because
    // the table has partition fields.
    let manifests = table.manifests(None, None).await.expect("manifests");
    let any_with_summary = manifests
        .iter()
        .any(|m| m.partitions.as_ref().is_some_and(|p| !p.is_empty()));
    assert!(
        any_with_summary,
        "partitioned table must produce at least one manifest list entry \
         with a non-empty partitions summary; got: {:?}",
        manifests
            .iter()
            .map(|m| m.partitions.as_ref().map(|p| p.len()))
            .collect::<Vec<_>>(),
    );
}
