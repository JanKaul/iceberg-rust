//! Java-parity tests for `Transaction::overwrite` (the rough equivalent
//! of Java's `OverwriteFiles`).
//!
//! Mirrors `org.apache.iceberg.TestOverwrite` (TestBase + @TestTemplate
//! over V1/V2/V3 × main/testBranch). Each Java @TestTemplate maps to
//! one Rust test here. Java's `OverwriteFiles` exposes
//! `addFile / deleteFile / overwriteByRowFilter /
//! validateAddedFilesMatchOverwriteFilter`. Rust's
//! `Transaction::overwrite(new_files, files_to_overwrite_map)` covers
//! the add + delete-by-path subset only; row-filter overwrite and the
//! filter-match validation hook are NOT modelled.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use futures::stream;
use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::Catalog, error::Error,
    object_store::ObjectStoreBuilder, table::Table,
};
use iceberg_rust_spec::spec::{
    partition::{PartitionField, PartitionSpec, Transform},
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

/// Build a `files_to_overwrite` map (manifest_path -> Vec<file_path>)
/// for every data file matching `target_region`. Mirrors the helper in
/// the existing overwrite_test.rs.
async fn files_to_overwrite_for_region(
    table: &Table,
    target_region: &str,
) -> Result<HashMap<String, Vec<String>>, Error> {
    let mut out = HashMap::new();
    let manifests = table.manifests(None, None).await?;
    for manifest in manifests {
        let manifest_path = manifest.manifest_path.clone();
        let manifests = vec![manifest];
        let files = table.datafiles(&manifests, None, (None, None)).await?;
        let mut to_remove = Vec::new();
        for result in files {
            let (_, entry) = result?;
            let matches = entry
                .data_file()
                .partition()
                .get("region")
                .and_then(|v| v.as_ref())
                .and_then(|v| match v {
                    iceberg_rust_spec::spec::values::Value::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .map(|r| r == target_region)
                .unwrap_or(false);
            if matches {
                to_remove.push(entry.data_file().file_path().to_owned());
            }
        }
        if !to_remove.is_empty() {
            out.insert(manifest_path, to_remove);
        }
    }
    Ok(out)
}

// --- Passing tests ---------------------------------------------------------

/// Java: `addAndDeleteDataFilesProducesOverwriteOperation` — combining
/// new files with files-to-delete in a single `overwrite()` call yields
/// a snapshot with operation == Overwrite.
#[tokio::test]
async fn test_overwrite_add_and_delete_produces_overwrite_operation_per_java() {
    let mut table = fresh_table("overwrite_add_and_delete").await;

    // Seed with two regions.
    let seed_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[
            (1, "us-east"),
            (2, "us-east"),
            (3, "us-west"),
        ]))]),
        None,
    )
    .await
    .expect("write seed");
    table
        .new_transaction(None)
        .append_data(seed_files)
        .commit()
        .await
        .expect("seed commit");

    // Overwrite the us-east files with a new batch.
    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(10, "us-east"), (11, "us-east")]))]),
        None,
    )
    .await
    .expect("write new");
    let to_overwrite = files_to_overwrite_for_region(&table, "us-east")
        .await
        .expect("build files_to_overwrite");
    assert!(
        !to_overwrite.is_empty(),
        "files_to_overwrite must include the us-east files we seeded",
    );

    table
        .new_transaction(None)
        .overwrite(new_files, to_overwrite)
        .commit()
        .await
        .expect("commit overwrite");

    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert!(
        matches!(snapshot.summary().operation, Operation::Overwrite),
        "add + delete must produce an Overwrite snapshot, got {:?}",
        snapshot.summary().operation,
    );
}

/// Variant of the above: assert that the resulting snapshot's
/// added-data-files summary matches the number of new files written.
#[tokio::test]
async fn test_overwrite_records_added_data_files_summary_per_java() {
    let mut table = fresh_table("overwrite_summary").await;

    let seed_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(1, "us-east"), (2, "us-east")]))]),
        None,
    )
    .await
    .expect("write seed");
    table
        .new_transaction(None)
        .append_data(seed_files)
        .commit()
        .await
        .expect("seed commit");

    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(10, "us-east")]))]),
        None,
    )
    .await
    .expect("write new");
    let new_count = new_files.len();
    let to_overwrite = files_to_overwrite_for_region(&table, "us-east")
        .await
        .expect("files_to_overwrite");

    table
        .new_transaction(None)
        .overwrite(new_files, to_overwrite)
        .commit()
        .await
        .expect("commit overwrite");

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
        Some(new_count.to_string()).as_deref(),
        "added-data-files must equal the new file count",
    );
}

/// Java: validating that a stale `files_to_overwrite` entry (a manifest
/// path that doesn't exist in the current table) causes the overwrite to
/// fail. The Rust implementation rejects this with an error — see the
/// existing `overwrite_test.rs` for the same assertion.
#[tokio::test]
async fn test_overwrite_rejects_unknown_manifest_path_in_files_to_overwrite_per_java() {
    let mut table = fresh_table("overwrite_unknown_manifest").await;

    let seed_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(1, "us-east")]))]),
        None,
    )
    .await
    .expect("write seed");
    table
        .new_transaction(None)
        .append_data(seed_files)
        .commit()
        .await
        .expect("seed commit");

    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(2, "us-east")]))]),
        None,
    )
    .await
    .expect("write new");

    // Build a deliberately-bogus files_to_overwrite map: points at a
    // manifest the table has never seen.
    let mut bogus = HashMap::new();
    bogus.insert(
        "/nonexistent/manifest.avro".to_owned(),
        vec!["/nonexistent/data.parquet".to_owned()],
    );

    let result = table
        .new_transaction(None)
        .overwrite(new_files, bogus)
        .commit()
        .await;
    assert!(
        result.is_err(),
        "overwrite with unknown manifest path must error, got Ok",
    );
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "feature gap: Rust's Transaction::overwrite always emits Operation::Overwrite even when only deleting (no new files); Java distinguishes Delete vs Overwrite by content"]
async fn test_overwrite_delete_data_files_produces_delete_operation_per_java() {
    // Java: deleteDataFilesProducesDeleteOperation.
    // newOverwrite.deleteFile(a).deleteFile(b) -> snapshot op == DELETE.
}

#[tokio::test]
#[ignore = "feature gap: no overwriteByRowFilter(predicate) — Rust requires explicit file paths to delete"]
async fn test_overwrite_by_row_filter_produces_delete_operation_per_java() {
    // Java: overwriteByRowFilterProducesDeleteOperation.
}

#[tokio::test]
#[ignore = "feature gap: same row-filter gap"]
async fn test_overwrite_add_and_by_row_filter_produces_overwrite_operation_per_java() {
    // Java: addAndOverwriteByRowFilterProducesOverwriteOperation.
}

/// Rust divergence: Java's `newOverwrite().addFile(a).addFile(b)` (no
/// delete) auto-promotes the resulting snapshot to operation type
/// APPEND. Rust's `Transaction::overwrite(new_files, empty_map)`
/// always emits `Operation::Overwrite` — there's no content-aware
/// snapshot-op selection.
///
/// Pinning the Rust behaviour as a passing test makes the divergence
/// explicit and visible. (Rust users wanting the APPEND op should use
/// `append_data` directly instead.)
#[tokio::test]
async fn test_overwrite_add_only_emits_overwrite_op_in_rust_per_java() {
    let mut table = fresh_table("overwrite_add_only_op").await;
    let seed = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(1, "us-east")]))]),
        None,
    )
    .await
    .expect("seed");
    table
        .new_transaction(None)
        .append_data(seed)
        .commit()
        .await
        .expect("seed commit");

    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(2, "us-east")]))]),
        None,
    )
    .await
    .expect("new");

    table
        .new_transaction(None)
        .overwrite(new_files, HashMap::new())
        .commit()
        .await
        .expect("commit");
    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    assert!(
        matches!(snapshot.summary().operation, Operation::Overwrite),
        "Rust always emits Overwrite from .overwrite(), even with no \
         files to delete. Java would emit APPEND here. Got {:?}",
        snapshot.summary().operation,
    );
}

#[tokio::test]
#[ignore = "feature gap: full manifest-entry validation (DELETED/EXISTING status per file across manifests) requires Rust manifest inspection helpers not yet exposed"]
async fn test_overwrite_without_append_per_java() {
    // Java: testOverwriteWithoutAppend.
    // Asserts the resulting single manifest has [(FILE_0_TO_4, DELETED),
    // (FILE_5_TO_9, EXISTING)].
}

#[tokio::test]
#[ignore = "feature gap: no row-filter delete; can't test 'Cannot delete file where some, but not all, rows match filter' rejection"]
async fn test_overwrite_fails_delete_per_java() {
    // Java: testOverwriteFailsDelete.
}

#[tokio::test]
#[ignore = "feature gap: row-filter overwrite with add files outside the filter"]
async fn test_overwrite_with_append_outside_of_delete_per_java() {
    // Java: testOverwriteWithAppendOutsideOfDelete.
}

#[tokio::test]
#[ignore = "feature gap: same + manifest-merging based on MANIFEST_MIN_MERGE_COUNT property"]
async fn test_overwrite_with_merged_append_outside_of_delete_per_java() {
    // Java: testOverwriteWithMergedAppendOutsideOfDelete.
}

#[tokio::test]
#[ignore = "feature gap: no validateAddedFilesMatchOverwriteFilter() validation hook"]
async fn test_validated_overwrite_with_append_outside_of_delete_per_java() {
    // Java: testValidatedOverwriteWithAppendOutsideOfDelete.
}

#[tokio::test]
#[ignore = "feature gap: same — validateAddedFilesMatchOverwriteFilter() also checks metric-bound mismatches"]
async fn test_validated_overwrite_with_append_outside_of_delete_metrics_per_java() {
    // Java: testValidatedOverwriteWithAppendOutsideOfDeleteMetrics.
}

#[tokio::test]
#[ignore = "feature gap: same — validation hook missing; Java's 'success' variant of this test still throws because of pre-existing data outside the filter"]
async fn test_validated_overwrite_with_append_success_per_java() {
    // Java: testValidatedOverwriteWithAppendSuccess (which actually
    // exercises the failure path despite the name — Java asserts the
    // overwrite is rejected because pre-existing rows in the same
    // partition don't match the filter).
}
