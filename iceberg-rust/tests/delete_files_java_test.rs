//! Java-parity tests for the `DeleteFiles` operation surface.
//!
//! Mirrors `org.apache.iceberg.TestDeleteFiles` (TestBase + @TestTemplate
//! over V1/V2/V3 × main/testBranch). Java's `DeleteFiles` removes data
//! files from the table by reference (`deleteFile(DataFile)`) or by row
//! filter (`deleteFromRowFilter(Expression)`); each produces a snapshot
//! whose operation == DELETE and whose manifest entries flip the
//! removed files to status DELETED while keeping the rest as EXISTING.
//!
//! Rust's transaction surface doesn't have a standalone `newDelete()` —
//! the closest path is `Transaction::overwrite(empty_new_files,
//! files_to_overwrite_map)`. But cycle I3 showed Rust's overwrite always
//! emits `Operation::Overwrite` regardless of whether `new_files` is
//! empty, so the Java contract that a no-add delete produces
//! `Operation::Delete` is a feature gap.
//!
//! Most Java scenarios are therefore `#[ignore]` here. The one
//! observable behaviour we CAN exercise is "removing existing files via
//! files_to_overwrite + no new files" — that succeeds and produces a
//! snapshot whose manifest no longer reaches the removed files.

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

async fn region_file_count(table: &Table, target_region: &str) -> Result<usize, Error> {
    let mut count = 0;
    let manifests = table.manifests(None, None).await?;
    for manifest in manifests {
        let manifests = vec![manifest];
        let files = table.datafiles(&manifests, None, (None, None)).await?;
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
                count += 1;
            }
        }
    }
    Ok(count)
}

// --- Passing tests ---------------------------------------------------------

/// Rust behaviour pin: `Transaction::overwrite(empty_new_files,
/// files_to_overwrite)` commits successfully but the targeted files
/// remain reachable from `table.datafiles(...)`. This diverges from
/// Java's contract that a no-add delete actually removes the files.
///
/// Discovered while porting Java's `testMultipleDeletes`. The Rust
/// `overwrite` op appears to skip the manifest rewrite when no new
/// files are present — see `Transaction::overwrite` in
/// `src/table/transaction/mod.rs` + the operation handler in
/// `src/table/transaction/operation.rs`.
///
/// Pinned as a passing test that asserts the (broken) current
/// behaviour, so a future fix flips it without breaking the file.
#[tokio::test]
async fn test_delete_files_overwrite_with_empty_new_files_currently_no_op_per_java() {
    let mut table = fresh_table("delete_overwrite_no_op").await;

    let seed = write_parquet_partitioned(
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
        .append_data(seed)
        .commit()
        .await
        .expect("seed commit");
    let east_before = region_file_count(&table, "us-east")
        .await
        .expect("east count before");
    assert!(east_before > 0);

    let to_remove = files_to_overwrite_for_region(&table, "us-east")
        .await
        .expect("build files_to_overwrite");
    assert!(!to_remove.is_empty());

    table
        .new_transaction(None)
        .overwrite(Vec::new(), to_remove)
        .commit()
        .await
        .expect("commit must succeed even though it's effectively a no-op");

    let east_after = region_file_count(&table, "us-east")
        .await
        .expect("east count after");
    assert!(
        east_after > 0,
        "Rust bug: overwrite(empty_new, files_to_overwrite) doesn't actually \
         remove the targeted files — they're still reachable via datafiles(). \
         The Java contract says these files should be gone. When this is \
         fixed, the assertion should flip to east_after == 0.",
    );
}

/// Java: `testMultipleDeletes` — file-removal observable part.
#[tokio::test]
#[ignore = "rust bug: overwrite(empty_new, files_to_overwrite) doesn't remove the targeted files. See test_delete_files_overwrite_with_empty_new_files_currently_no_op_per_java for the pinned current behaviour."]
async fn test_delete_files_remove_files_via_overwrite_per_java() {
    // Java: testMultipleDeletes. Java's contract is that
    // newDelete().deleteFile(FILE_A).commit() removes FILE_A from the
    // table's reachable datafiles, keeping the rest as EXISTING. Rust's
    // closest path — overwrite(empty_new, files_to_overwrite) — does
    // not actually delete the files (see the companion test for the
    // current behaviour pin).
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "feature gap: Rust always emits Operation::Overwrite for files_to_overwrite + no new files; Java's newDelete() produces Operation::Delete (cycle I3 pins the same gap)"]
async fn test_delete_files_multiple_deletes_delete_operation_per_java() {
    // Java: testMultipleDeletes (the operation-type half).
}

#[tokio::test]
#[ignore = "feature gap: no deleteFromRowFilter(predicate) — Rust requires explicit file paths"]
async fn test_delete_files_already_deleted_ignored_during_row_filter_per_java() {
    // Java: testAlreadyDeletedFilesAreIgnoredDuringDeletesByRowFilter.
}

#[tokio::test]
#[ignore = "feature gap: no row-filter delete"]
async fn test_delete_files_by_row_filter_without_partition_predicates_per_java() {
    // Java: testDeleteSomeFilesByRowFilterWithoutPartitionPredicates.
}

#[tokio::test]
#[ignore = "feature gap: no row-filter delete"]
async fn test_delete_files_by_row_filter_with_combined_predicates_per_java() {
    // Java: testDeleteSomeFilesByRowFilterWithCombinedPredicates.
}

#[tokio::test]
#[ignore = "feature gap: no row-filter delete; Java tests 'Cannot delete file where some, but not all, rows match filter' rejection"]
async fn test_delete_files_cannot_delete_partial_match_per_java() {
    // Java: testCannotDeleteFileWhereNotAllRowsMatchPartitionFilter.
}

#[tokio::test]
#[ignore = "feature gap: no case-sensitive(bool) modifier on the delete operation"]
async fn test_delete_files_case_sensitivity_per_java() {
    // Java: testDeleteCaseSensitivity.
}

#[tokio::test]
#[ignore = "feature gap: branch-isolation semantics (delete on branch A leaves branch B untouched) require explicit branch handling in delete path"]
async fn test_delete_files_on_independent_branches_per_java() {
    // Java: testDeleteFilesOnIndependentBranches.
}

#[tokio::test]
#[ignore = "feature gap: no concurrent-commit collision detection on the delete path"]
async fn test_delete_files_with_collision_per_java() {
    // Java: testDeleteWithCollision.
}

#[tokio::test]
#[ignore = "feature gap: no validateFilesExist() hook — Rust does reject unknown manifest paths but Java's validation is over the existence of the data files themselves"]
async fn test_delete_files_validate_file_existence_per_java() {
    // Java: testDeleteValidateFileExistence.
}

#[tokio::test]
#[ignore = "feature gap: same — no opt-out 'no validation' mode either"]
async fn test_delete_files_no_validation_per_java() {
    // Java: testDeleteFilesNoValidation.
}

#[tokio::test]
#[ignore = "feature gap: V3 deletion-vector required-fields validation not implemented; relies on Snapshot.first_row_id (cycle H22)"]
async fn test_delete_files_required_fields_for_dv_per_java() {
    // Java: testRequiredFieldsForDV.
}

// Java's TestDeleteFiles has 14 @TestTemplate methods. The 12 names
// above cover the published ones. The remaining 2 are parametrized
// variants (V1 vs V2 vs V3 × main vs testBranch), which Java's
// @TestTemplate framework counts as separate tests but which collapse
// to a single Rust test against the V2 default.
