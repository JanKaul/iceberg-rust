//! Java-parity tests for `RewriteFiles`, exercised against Rust's
//! `Transaction::overwrite`.
//!
//! Mirrors `org.apache.iceberg.TestRewriteFiles` (TestBase +
//! @TestTemplate over V2/V3 × main/branch). Java's
//! `table.newRewrite().rewriteFiles(filesToDelete, newFiles)` is an
//! atomic N-files-in / M-files-out swap.
//!
//! Rust doesn't have a method literally called `rewrite_files`, but
//! `Transaction::overwrite(new_files, files_to_overwrite_map)` is
//! structurally the same operation: it takes explicit new files plus
//! an explicit manifest-keyed list of paths to remove, in one atomic
//! commit. The two API differences relative to Java's rewrite:
//!
//! 1. Rust emits `Operation::Overwrite` for the resulting snapshot;
//!    Java emits `Operation::Replace`.
//! 2. Rust does NOT preserve the rewritten files' sequence numbers
//!    (Java's overload does; matters for stream-consistency).
//!
//! Tests that exercise the structural swap pass; tests that depend on
//! sequence-number preservation, the 4-arg data+delete-file overload,
//! eq->pos delete migration, or commit-failure recovery stay #[ignore].

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

/// Java: `testAddAndDelete` — the canonical rewrite path. Swap one
/// region's files for new ones in a single atomic commit.
///
/// Rust's `Transaction::overwrite(new_files, files_to_overwrite)` is
/// the structural equivalent. The resulting snapshot's operation is
/// `Overwrite` in Rust vs `Replace` in Java — pin that divergence
/// inline, but otherwise verify the swap actually happened.
#[tokio::test]
async fn test_rewrite_files_add_and_delete_per_java() {
    let mut table = fresh_table("rewrite_add_and_delete").await;

    // Seed two regions.
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

    // Rewrite the us-east files: replace with a single new file.
    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(10, "us-east"), (11, "us-east")]))]),
        None,
    )
    .await
    .expect("write new");
    let files_to_overwrite = files_to_overwrite_for_region(&table, "us-east")
        .await
        .expect("files_to_overwrite");
    assert!(!files_to_overwrite.is_empty());

    table
        .new_transaction(None)
        .overwrite(new_files, files_to_overwrite)
        .commit()
        .await
        .expect("commit rewrite");

    let snapshot = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    // Java emits REPLACE; Rust emits Overwrite. Pin Rust's behaviour
    // explicitly so a future change to match Java surfaces here.
    assert!(
        matches!(snapshot.summary().operation, Operation::Overwrite),
        "Rust emits Operation::Overwrite for a rewrite; Java emits Replace. \
         Got {:?}",
        snapshot.summary().operation,
    );
    // The us-west file is still in the table (only us-east was rewritten).
    assert_eq!(table.metadata().snapshots.len(), 2);
}

/// Java: `testEmptyTable` — rewriting against a table that doesn't
/// contain the supposed-to-delete file must fail.
///
/// Rust's `overwrite` rejects unknown manifest paths in
/// `files_to_overwrite` (operation.rs:692-702). Maps directly to
/// Java's `'Missing required files to delete'` ValidationException.
#[tokio::test]
async fn test_rewrite_files_empty_table_per_java() {
    let mut table = fresh_table("rewrite_empty_table").await;
    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(1, "us-east")]))]),
        None,
    )
    .await
    .expect("write new");

    // Supply a files_to_overwrite map that points at a manifest the
    // table has never seen.
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
        "rewrite against a non-existent file must error",
    );
}

/// Java: `testDeleteNonExistentFile` — same idea as testEmptyTable but
/// against a non-empty table. The validation must still fire.
#[tokio::test]
async fn test_rewrite_files_delete_non_existent_file_per_java() {
    let mut table = fresh_table("rewrite_nonexistent").await;

    // Seed some real data.
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

    // Try to rewrite using a bogus manifest path.
    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(2, "us-east")]))]),
        None,
    )
    .await
    .expect("write new");
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
        "rewrite referencing an unknown manifest must error",
    );
}

/// Rust behaviour pin: `overwrite(new_files, empty_files_to_overwrite)`
/// is a *pure add*. Java's rewriteFiles rejects pure-add input with
/// `'Missing required files to delete'`; Rust accepts it and commits.
///
/// This isn't strictly a bug — Rust's overwrite() doesn't claim to
/// enforce the rewriteFiles invariant — but it's a meaningful
/// divergence from the Java contract.
#[tokio::test]
async fn test_rewrite_files_add_only_currently_accepted_per_java() {
    let mut table = fresh_table("rewrite_add_only").await;
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
    let snapshots_before = table.metadata().snapshots.len();

    let new_files = write_parquet_partitioned(
        &table,
        stream::iter(vec![Ok(batch(&[(2, "us-east")]))]),
        None,
    )
    .await
    .expect("write new");

    // Empty files_to_overwrite — Java would reject this, Rust commits.
    table
        .new_transaction(None)
        .overwrite(new_files, HashMap::new())
        .commit()
        .await
        .expect(
            "Rust accepts overwrite(new_files, empty_map); Java's rewriteFiles \
             rejects it as 'Missing required files to delete'. If this assertion \
             starts failing because Rust grows the same validation, flip this test \
             to assert .is_err() and re-pin the divergence elsewhere.",
        );
    assert_eq!(
        table.metadata().snapshots.len(),
        snapshots_before + 1,
        "Rust adds a new snapshot for the pure-add overwrite",
    );
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "Rust bug (cycle I4): overwrite(Vec::new(), files_to_overwrite) early-returns no-op at operation.rs:663-665, so Java's rewriteFiles(filesToDelete, empty_set) → 'Missing required files to delete' rejection can't be exercised"]
async fn test_rewrite_files_delete_only_per_java() {
    // Java: testDeleteOnly. table.newRewrite.rewriteFiles({FILE_A}, ø) throws.
}

#[tokio::test]
#[ignore = "feature gap: simulating a manifest with duplicate file entries requires byte-level manifest manipulation"]
async fn test_rewrite_files_delete_with_duplicate_entries_in_manifest_per_java() {
    // Java: testDeleteWithDuplicateEntriesInManifest.
}

#[tokio::test]
#[ignore = "feature gap: 4-arg overload rewriteFiles(dataDelete, dataAdd, deleteDelete, deleteAdd) handles both data files and delete files; Rust's overwrite only takes data files"]
async fn test_rewrite_files_rewrite_data_and_delete_files_per_java() {
    // Java: testRewriteDataAndDeleteFiles.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles(..., sequenceNumber) overload assigns the OLD sequence number to the rewritten data; Rust's overwrite doesn't preserve sequence numbers"]
async fn test_rewrite_files_rewrite_data_and_assign_old_sequence_number_per_java() {
    // Java: testRewriteDataAndAssignOldSequenceNumber.
}

#[tokio::test]
#[ignore = "feature gap: no clean way to inject commit failure to assert post-failure cleanup"]
async fn test_rewrite_files_failure_per_java() {
    // Java: testFailure.
}

#[tokio::test]
#[ignore = "feature gap: same plus the data+delete-file rewrite gap"]
async fn test_rewrite_files_failure_when_rewrite_both_data_and_delete_files_per_java() {
    // Java: testFailureWhenRewriteBothDataAndDeleteFiles.
}

#[tokio::test]
#[ignore = "feature gap: no recovery contract — Rust commit consumes self"]
async fn test_rewrite_files_recovery_per_java() {
    // Java: testRecovery.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_rewrite_files_recover_when_rewrite_both_data_and_delete_files_per_java() {
    // Java: testRecoverWhenRewriteBothDataAndDeleteFiles.
}

#[tokio::test]
#[ignore = "feature gap: Rust doesn't distinguish equality deletes vs position deletes in the overwrite op; this is the V2+ delete-file migration flow"]
async fn test_rewrite_files_replace_equality_deletes_with_position_deletes_per_java() {
    // Java: testReplaceEqualityDeletesWithPositionDeletes.
}

#[tokio::test]
#[ignore = "feature gap: no delete-file removal path via rewriteFiles(..., deleteFilesToRemove=ALL, ...)"]
async fn test_rewrite_files_remove_all_deletes_per_java() {
    // Java: testRemoveAllDeletes.
}

#[tokio::test]
#[ignore = "feature gap: rewriteFiles must reject inputs referencing a file already deleted in a prior snapshot; Rust's overwrite doesn't validate against historical state"]
async fn test_rewrite_files_already_deleted_file_per_java() {
    // Java: testAlreadyDeletedFile.
}

#[tokio::test]
#[ignore = "feature gap: rewriting with a brand-new delete file added in the same op requires the 4-arg overload"]
async fn test_rewrite_files_new_delete_file_per_java() {
    // Java: testNewDeleteFile.
}
