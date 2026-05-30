//! Java-parity tests for the `ReplacePartitions` operation surface.
//!
//! Mirrors `org.apache.iceberg.TestReplacePartitions` (TestBase +
//! @TestTemplate over V1/V2/V3 × main/branch). Java's
//! `newReplacePartitions().addFile(f)` is "dynamic overwrite": for each
//! partition touched by the added files, remove all existing files in
//! that partition and replace them with the new ones. Other partitions
//! are untouched.
//!
//! Rust's `Transaction::replace(files)` is full-table-replace: it
//! removes ALL existing data files and replaces them with the
//! provided files. There is no per-partition variant, no validation
//! hook (`validateAppendOnly` / `validateNoConflictingAppends` /
//! `validateNoConflictingDeletes`), and no `validateFromSnapshot` /
//! `validateWithDefaultSnapshotId` flags.
//!
//! The two scenarios where Rust's behaviour matches Java are the
//! unpartitioned-table tests — replace-partitions on an unpartitioned
//! table degenerates to replace-all-data, which IS Rust's
//! `replace`. Everything else is a feature gap.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use futures::stream;
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::Catalog, object_store::ObjectStoreBuilder,
    table::Table,
};
use iceberg_rust_spec::spec::{
    partition::PartitionSpec,
    schema::Schema,
    snapshot::Operation,
    types::{PrimitiveType, StructField, Type},
};
use iceberg_sql_catalog::SqlCatalog;

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
            name: "data".to_string(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .build()
        .unwrap()
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ])
}

fn batch(start_id: i64, n_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + n_rows as i64).collect();
    let data: Vec<Option<String>> = ids.iter().map(|i| Some(format!("row-{i}"))).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(data)),
        ],
    )
    .unwrap()
}

async fn fresh_table_unpartitioned(name: &str) -> Table {
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
        .with_partition_spec(PartitionSpec::default())
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("create table")
}

// --- Passing tests ---------------------------------------------------------

/// Java: `testReplaceWithUnpartitionedTable` — replace on an unpartitioned
/// table removes the prior files and installs the new ones. The
/// resulting snapshot's operation is OVERWRITE (Java) / Overwrite (Rust).
///
/// Rust's `replace(new_files)` does exactly this: ALL existing files are
/// replaced by the supplied list. On an unpartitioned table, the Java
/// semantic and the Rust semantic agree.
#[tokio::test]
async fn test_replace_partitions_with_unpartitioned_table_per_java() {
    let mut table = fresh_table_unpartitioned("replace_unpart").await;

    let initial = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(0, 3))]), None)
        .await
        .expect("write initial");
    table
        .new_transaction(None)
        .append_data(initial)
        .commit()
        .await
        .expect("commit initial");
    let snapshots_after_seed = table.metadata().snapshots.len();
    assert_eq!(snapshots_after_seed, 1);

    let replacement =
        write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(100, 2))]), None)
            .await
            .expect("write replacement");
    table
        .new_transaction(None)
        .replace(replacement)
        .commit()
        .await
        .expect("commit replace");

    assert_eq!(
        table.metadata().snapshots.len(),
        2,
        "replace must produce a new snapshot",
    );
    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert!(
        matches!(snapshot.summary().operation, Operation::Overwrite),
        "replace must emit Operation::Overwrite, got {:?}",
        snapshot.summary().operation,
    );
}

/// Java: `testReplaceAndMergeWithUnpartitionedTable` — same as above but
/// after a manifest-merge property is set. Rust has no per-table
/// MANIFEST_MIN_MERGE_COUNT property; the replace still works and
/// produces an Overwrite snapshot. We pin the part that's observable:
/// the operation type.
#[tokio::test]
async fn test_replace_partitions_and_merge_with_unpartitioned_table_per_java() {
    let mut table = fresh_table_unpartitioned("replace_merge_unpart").await;

    // Two seed commits so a hypothetical merge would have something to merge.
    let a = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(0, 2))]), None)
        .await
        .expect("write a");
    table
        .new_transaction(None)
        .append_data(a)
        .commit()
        .await
        .expect("commit a");
    let b = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(10, 2))]), None)
        .await
        .expect("write b");
    table
        .new_transaction(None)
        .append_data(b)
        .commit()
        .await
        .expect("commit b");

    let replacement =
        write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(100, 3))]), None)
            .await
            .expect("write replacement");
    table
        .new_transaction(None)
        .replace(replacement)
        .commit()
        .await
        .expect("commit replace");

    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert!(
        matches!(snapshot.summary().operation, Operation::Overwrite),
        "replace must emit Operation::Overwrite",
    );
    assert_eq!(table.metadata().snapshots.len(), 3);
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------
//
// All the remaining Java tests exercise per-partition overwrite + the
// validation hooks Rust's `replace()` doesn't implement.

#[tokio::test]
#[ignore = "feature gap: Rust's replace() is full-table; no per-partition (dynamic) overwrite of just the touched partitions"]
async fn test_replace_partitions_one_partition_per_java() {
    // Java: testReplaceOnePartition. Adds FILE_E to data_bucket=0;
    // FILE_B in data_bucket=1 stays.
}

#[tokio::test]
#[ignore = "feature gap: same per-partition gap"]
async fn test_replace_partitions_and_merge_one_partition_per_java() {
    // Java: testReplaceAndMergeOnePartition.
}

#[tokio::test]
#[ignore = "feature gap: alwaysNull (void) partition transforms on every column"]
async fn test_replace_partitions_all_void_unpartitioned_table_per_java() {
    // Java: testReplaceAllVoidUnpartitionedTable.
}

#[tokio::test]
#[ignore = "feature gap: no validateNoConflictingAppends() hook"]
async fn test_replace_partitions_validation_failure_per_java() {
    // Java: testValidationFailure.
}

#[tokio::test]
#[ignore = "feature gap: no validation hook"]
async fn test_replace_partitions_validation_success_per_java() {
    // Java: testValidationSuccess.
}

#[tokio::test]
#[ignore = "feature gap: no validateFromSnapshot()"]
async fn test_replace_partitions_validation_not_invoked_per_java() {
    // Java: testValidationNotInvoked.
}

#[tokio::test]
#[ignore = "feature gap: no validateWithDefaultSnapshotId()"]
async fn test_replace_partitions_validate_with_default_snapshot_id_per_java() {
    // Java: testValidateWithDefaultSnapshotId.
}

#[tokio::test]
#[ignore = "feature gap: null-partition data files require a specific 'data_bucket=__HIVE_DEFAULT_PARTITION__' value handling"]
async fn test_replace_partitions_validate_with_null_partition_per_java() {
    // Java: testValidateWithNullPartition.
}

#[tokio::test]
#[ignore = "feature gap: void-transform partition validation"]
async fn test_replace_partitions_validate_with_void_transform_per_java() {
    // Java: testValidateWithVoidTransform.
}

#[tokio::test]
#[ignore = "feature gap: no concurrent-commit conflict detection on replace path"]
async fn test_replace_partitions_concurrent_replace_conflict_per_java() {
    // Java: testConcurrentReplaceConflict.
}

#[tokio::test]
#[ignore = "feature gap: same — no conflict detection means no 'no conflict' classification either"]
async fn test_replace_partitions_concurrent_replace_no_conflict_per_java() {
    // Java: testConcurrentReplaceNoConflict.
}

#[tokio::test]
#[ignore = "feature gap: same conflict detection on the non-partitioned path"]
async fn test_replace_partitions_concurrent_replace_conflict_non_partitioned_per_java() {
    // Java: testConcurrentReplaceConflictNonPartitioned.
}

#[tokio::test]
#[ignore = "feature gap: no append-vs-replace conflict detection"]
async fn test_replace_partitions_append_replace_conflict_per_java() {
    // Java: testAppendReplaceConflict.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_replace_partitions_append_replace_no_conflict_per_java() {
    // Java: testAppendReplaceNoConflict.
}

#[tokio::test]
#[ignore = "feature gap: same on the non-partitioned path"]
async fn test_replace_partitions_append_replace_conflict_non_partitioned_per_java() {
    // Java: testAppendReplaceConflictNonPartitioned.
}

#[tokio::test]
#[ignore = "feature gap: no delete-vs-replace conflict detection"]
async fn test_replace_partitions_delete_replace_conflict_per_java() {
    // Java: testDeleteReplaceConflict.
}

#[tokio::test]
#[ignore = "feature gap: same on the non-partitioned path"]
async fn test_replace_partitions_delete_replace_conflict_non_partitioned_per_java() {
    // Java: testDeleteReplaceConflictNonPartitioned.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_replace_partitions_delete_replace_no_conflict_per_java() {
    // Java: testDeleteReplaceNoConflict.
}

#[tokio::test]
#[ignore = "feature gap: no overwrite-vs-replace conflict detection"]
async fn test_replace_partitions_overwrite_replace_conflict_per_java() {
    // Java: testOverwriteReplaceConflict.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_replace_partitions_overwrite_replace_no_conflict_per_java() {
    // Java: testOverwriteReplaceNoConflict.
}

#[tokio::test]
#[ignore = "feature gap: same on the non-partitioned path"]
async fn test_replace_partitions_overwrite_replace_conflict_non_partitioned_per_java() {
    // Java: testOverwriteReplaceConflictNonPartitioned.
}

#[tokio::test]
#[ignore = "feature gap: no 'validate only deletes' option that lets a replace skip add-conflict checks"]
async fn test_replace_partitions_validate_only_deletes_per_java() {
    // Java: testValidateOnlyDeletes.
}

#[tokio::test]
#[ignore = "feature gap: empty partition path '' on unpartitioned table — not specifically validated by Rust"]
async fn test_replace_partitions_empty_partition_path_with_unpartitioned_table_per_java() {
    // Java: testEmptyPartitionPathWithUnpartitionedTable.
}
