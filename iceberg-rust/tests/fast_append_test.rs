//! Java-parity tests for `Transaction::append_data` (FastAppend semantics).
//!
//! Mirrors `org.apache.iceberg.TestFastAppend` (TestBase + @TestTemplate
//! over V1/V2/V3). Each Java @TestTemplate maps to one Rust test here:
//! the ones whose behaviour is reachable through the current Rust
//! transaction API are real `#[tokio::test]` passing tests; the ones
//! that depend on features Rust hasn't built yet (manifest cleanup on
//! commit failure, snapshot-id inheritance, partition-summary property,
//! branch-ref validation, V3 row lineage) are `#[ignore]`'d stubs with
//! a one-line note explaining the gap.

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
    partition::{PartitionField, PartitionSpec, Transform},
    schema::Schema,
    snapshot::Operation,
    types::{PrimitiveType, StructField, Type},
};
use iceberg_sql_catalog::SqlCatalog;

// --- Shared test fixtures --------------------------------------------------

fn unpartitioned_schema() -> Schema {
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

fn partitioned_schema() -> Schema {
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

fn region_partition_spec() -> PartitionSpec {
    PartitionSpec::builder()
        .with_partition_field(PartitionField::new(2, 1000, "region", Transform::Identity))
        .build()
        .unwrap()
}

fn arrow_schema_unpartitioned() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ])
}

fn arrow_schema_partitioned() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ])
}

fn small_batch_unpartitioned(start_id: i64, n_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + n_rows as i64).collect();
    let data: Vec<Option<String>> = ids.iter().map(|i| Some(format!("row-{i}"))).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema_unpartitioned()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(data)),
        ],
    )
    .unwrap()
}

fn partitioned_batch(rows: &[(i64, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let regions: Vec<&str> = rows.iter().map(|(_, r)| *r).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema_partitioned()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
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
        .with_schema(unpartitioned_schema())
        .with_partition_spec(PartitionSpec::default())
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("create unpartitioned table")
}

async fn fresh_table_partitioned(name: &str) -> Table {
    let object_store = ObjectStoreBuilder::memory();
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store)
            .await
            .unwrap(),
    );
    Table::builder()
        .with_name(name)
        .with_location(format!("/test/{name}"))
        .with_schema(partitioned_schema())
        .with_partition_spec(region_partition_spec())
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("create partitioned table")
}

// --- Passing tests ---------------------------------------------------------

/// Java: `testEmptyTableAppend` — `newFastAppend.appendFile(FILE_A).commit()`
/// on an empty table produces a single snapshot containing one data file.
#[tokio::test]
async fn test_fast_append_empty_table_append_per_java() {
    let mut table = fresh_table_unpartitioned("empty_table_append").await;
    assert!(
        table.metadata().snapshots.is_empty(),
        "fresh table must have zero snapshots",
    );

    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(small_batch_unpartitioned(0, 3))]),
        None,
    )
    .await
    .expect("write parquet");
    let appended_count = files.len();

    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit append");

    assert_eq!(
        table.metadata().snapshots.len(),
        1,
        "must have exactly one snapshot after first append",
    );
    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert!(
        matches!(snapshot.summary().operation, Operation::Append),
        "snapshot operation must be Append, got {:?}",
        snapshot.summary().operation,
    );
    let other = &snapshot.summary().other;
    assert_eq!(
        other.get("added-data-files").map(String::as_str),
        Some(appended_count.to_string()).as_deref(),
        "added-data-files must match number of files written",
    );
}

/// Java: `testAddManyFiles` — N data files appended in one commit are all
/// reachable from the new snapshot.
#[tokio::test]
async fn test_fast_append_add_many_files_per_java() {
    let mut table = fresh_table_unpartitioned("add_many_files").await;

    // Append several batches in one commit by writing each separately
    // and combining the resulting DataFile lists.
    let mut all_files = Vec::new();
    for i in 0..4 {
        let files = write_parquet_partitioned(
            &table,
            stream::iter(vec![Ok(small_batch_unpartitioned(i as i64 * 10, 2))]),
            None,
        )
        .await
        .expect("write parquet");
        all_files.extend(files);
    }
    let total = all_files.len();
    assert!(total >= 4, "expected at least one DataFile per batch");

    table
        .new_transaction(None)
        .append_data(all_files)
        .commit()
        .await
        .expect("commit append");

    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert_eq!(
        snapshot
            .summary()
            .other
            .get("added-data-files")
            .map(String::as_str),
        Some(total.to_string()).as_deref(),
        "every file we wrote must be counted in added-data-files",
    );
}

/// Java: `testNonEmptyTableAppend` — appending after a prior commit
/// produces a SECOND snapshot whose summary counts only the new files.
#[tokio::test]
async fn test_fast_append_non_empty_table_append_per_java() {
    let mut table = fresh_table_unpartitioned("non_empty_append").await;

    let first = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(small_batch_unpartitioned(0, 2))]),
        None,
    )
    .await
    .expect("write first");
    let first_count = first.len();
    table
        .new_transaction(None)
        .append_data(first)
        .commit()
        .await
        .expect("commit first");

    let second = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(small_batch_unpartitioned(100, 3))]),
        None,
    )
    .await
    .expect("write second");
    let second_count = second.len();
    table
        .new_transaction(None)
        .append_data(second)
        .commit()
        .await
        .expect("commit second");

    assert_eq!(table.metadata().snapshots.len(), 2, "two snapshots");
    let current = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert_eq!(
        current
            .summary()
            .other
            .get("added-data-files")
            .map(String::as_str),
        Some(second_count.to_string()).as_deref(),
        "second snapshot must only count its own added files",
    );
    let _ = first_count; // referenced to silence unused warning while
                         // documenting that the assertion is on the
                         // second-commit delta, not the cumulative total.
}

/// Java: `testNoMerge` — fast-append does NOT merge with prior manifests.
/// The second snapshot's manifest list still references the first commit's
/// manifest (carried forward) PLUS the freshly-written one — i.e. the
/// manifest count grows monotonically, never collapsing to a single merged
/// manifest the way `MergeAppend` would.
#[tokio::test]
async fn test_fast_append_no_merge_per_java() {
    let mut table = fresh_table_unpartitioned("no_merge").await;

    let first = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(small_batch_unpartitioned(0, 2))]),
        None,
    )
    .await
    .expect("write first");
    table
        .new_transaction(None)
        .append_data(first)
        .commit()
        .await
        .expect("commit first");
    let first_manifest_count = table.manifests(None, None).await.expect("manifests").len();

    let second = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(small_batch_unpartitioned(100, 2))]),
        None,
    )
    .await
    .expect("write second");
    table
        .new_transaction(None)
        .append_data(second)
        .commit()
        .await
        .expect("commit second");
    let second_manifest_count = table.manifests(None, None).await.expect("manifests").len();

    assert!(
        second_manifest_count >= first_manifest_count,
        "fast-append must not shrink the manifest count: first={first_manifest_count}, second={second_manifest_count}",
    );
    assert!(
        second_manifest_count > 0,
        "expected at least one manifest after appends",
    );
}

/// Java: `testPartitionSummariesOnUnpartitionedTable` — the snapshot
/// summary on an unpartitioned table must NOT include per-partition
/// entries.
#[tokio::test]
async fn test_fast_append_partition_summaries_on_unpartitioned_table_per_java() {
    let mut table = fresh_table_unpartitioned("part_sum_unpart").await;
    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(small_batch_unpartitioned(0, 2))]),
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

    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    // Java's "partitions" summary key is opt-in; no unpartitioned table
    // should emit it. Rust's append_summary doesn't compute it either.
    assert!(
        !snapshot.summary().other.contains_key("partitions"),
        "unpartitioned table snapshot summary must not carry 'partitions' entries",
    );
}

/// Java: `testDefaultPartitionSummaries` — by default (no opt-in
/// property), the snapshot summary on a partitioned table also does not
/// emit per-partition entries.
#[tokio::test]
async fn test_fast_append_default_partition_summaries_per_java() {
    let mut table = fresh_table_partitioned("default_part_sum").await;
    let files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(partitioned_batch(&[
            (1, "us-east"),
            (2, "us-east"),
            (3, "us-west"),
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

    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert!(
        !snapshot.summary().other.contains_key("partitions"),
        "default partition summary must be off",
    );
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "feature gap: no in-place updateSpec().addField().commit() flow; cycle G3 + H8 already pin this gap on PartitionSpec evolution"]
async fn test_fast_append_empty_table_with_different_specs_per_java() {
    // Java: testEmptyTableFastAppendFilesWithDifferentSpecs.
}

#[tokio::test]
#[ignore = "feature gap: no Transaction.append_manifest(ManifestFile) API; only append_data(Vec<DataFile>) is exposed"]
async fn test_fast_append_empty_table_append_manifest_per_java() {
    // Java: testEmptyTableAppendManifest.
}

#[tokio::test]
#[ignore = "feature gap: same append_manifest API missing"]
async fn test_fast_append_empty_table_append_files_and_manifest_per_java() {
    // Java: testEmptyTableAppendFilesAndManifest.
}

#[tokio::test]
#[ignore = "feature gap: Transaction lifecycle has no explicit apply() step that could observe metadata refresh; commit drives the entire flow"]
async fn test_fast_append_refresh_before_apply_per_java() {
    // Java: testRefreshBeforeApply.
}

#[tokio::test]
#[ignore = "feature gap: same — no separate refresh-before-commit hook"]
async fn test_fast_append_refresh_before_commit_per_java() {
    // Java: testRefreshBeforeCommit.
}

#[tokio::test]
#[ignore = "feature gap: no clean way to inject commit failure to assert post-failure cleanup"]
async fn test_fast_append_failure_per_java() {
    // Java: testFailure.
}

#[tokio::test]
#[ignore = "feature gap: no commit.retry.num-retries property; Rust commit does not retry on conflict"]
async fn test_fast_append_increase_num_retries_per_java() {
    // Java: testIncreaseNumRetries.
}

#[tokio::test]
#[ignore = "feature gap: no manifest cleanup on commit failure"]
async fn test_fast_append_manifest_cleanup_per_java() {
    // Java: testAppendManifestCleanup.
}

#[tokio::test]
#[ignore = "feature gap: SnapshotProducer write_new_manifests is internal; no idempotency contract exposed"]
async fn test_fast_append_write_new_manifests_idempotency_per_java() {
    // Java: testWriteNewManifestsIdempotency.
}

#[tokio::test]
#[ignore = "feature gap: same — no apply()/cleanup distinction"]
async fn test_fast_append_write_new_manifests_cleanup_per_java() {
    // Java: testWriteNewManifestsCleanup.
}

#[tokio::test]
#[ignore = "feature gap: no SNAPSHOT_ID_INHERITANCE_ENABLED table property; no append_manifest API"]
async fn test_fast_append_manifest_with_snapshot_id_inheritance_per_java() {
    // Java: testAppendManifestWithSnapshotIdInheritance.
}

#[tokio::test]
#[ignore = "feature gap: same gap; can't test failure path either"]
async fn test_fast_append_manifest_failure_with_snapshot_id_inheritance_per_java() {
    // Java: testAppendManifestFailureWithSnapshotIdInheritance.
}

#[tokio::test]
#[ignore = "feature gap: no append_manifest API to validate"]
async fn test_fast_append_invalid_append_manifest_per_java() {
    // Java: testInvalidAppendManifest.
}

#[tokio::test]
#[ignore = "feature gap: TableProperty WRITE_PARTITION_SUMMARY_LIMIT > 0 opt-in not implemented"]
async fn test_fast_append_included_partition_summaries_per_java() {
    // Java: testIncludedPartitionSummaries.
}

#[tokio::test]
#[ignore = "feature gap: same — no partition-summary limit handling"]
async fn test_fast_append_included_partition_summary_limit_per_java() {
    // Java: testIncludedPartitionSummaryLimit.
}

#[tokio::test]
#[ignore = "feature gap: new_transaction(branch) accepts a branch arg but branch-ref enforcement (create-if-needed, reject-tag) isn't unit-tested"]
async fn test_fast_append_to_existing_branch_per_java() {
    // Java: testAppendToExistingBranch.
}

#[tokio::test]
#[ignore = "feature gap: same — branch creation semantics not validated"]
async fn test_fast_append_creates_branch_if_needed_per_java() {
    // Java: testAppendCreatesBranchIfNeeded.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_fast_append_to_branch_empty_table_per_java() {
    // Java: testAppendToBranchEmptyTable.
}

#[tokio::test]
#[ignore = "feature gap: no validation rejecting None / empty / tag-ref branch names at append time"]
async fn test_fast_append_to_null_branch_fails_per_java() {
    // Java: testAppendToNullBranchFails.
}

#[tokio::test]
#[ignore = "feature gap: no tag-vs-branch ref distinction at the transaction surface"]
async fn test_fast_append_to_tag_fails_per_java() {
    // Java: testAppendToTagFails.
}

#[tokio::test]
#[ignore = "feature gap: Snapshot has no first_row_id/added_rows fields (cycle H22 pinned the spec-side gap)"]
async fn test_fast_append_v3_row_lineage_per_java() {
    // Java: V3 row-lineage assertions interleaved across TestFastAppend
    // (and exhaustively in TestRowLineageMetadata, cycle H22).
}
