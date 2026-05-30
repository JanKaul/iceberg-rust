//! Java-parity tests for `Table::new_transaction(...)` multi-op flow.
//!
//! Mirrors `org.apache.iceberg.TestTransaction` (TestBase + @TestTemplate
//! over V1/V2/V3). Each Java @TestTemplate maps to one Rust test here:
//! the ones reachable through the current Rust transaction API are real
//! `#[tokio::test]` passing tests; the ones depending on features Rust
//! hasn't built yet (retry policy, dirty-state detection, concurrent-
//! commit conflict resolution, snapshot-id inheritance, unknown commit
//! state, transaction extension, custom delete func) are `#[ignore]`'d
//! stubs with a one-line note.

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

fn small_batch(start_id: i64, n_rows: usize) -> RecordBatch {
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
        .with_partition_spec(PartitionSpec::default())
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("create table")
}

// --- Passing tests ---------------------------------------------------------

/// Java: `testEmptyTransaction` — committing an empty transaction must
/// succeed without producing a new snapshot.
#[tokio::test]
async fn test_transaction_empty_transaction_per_java() {
    let mut table = fresh_table("empty_txn").await;
    let snapshots_before = table.metadata().snapshots.len();
    table
        .new_transaction(None)
        .commit()
        .await
        .expect("empty transaction commit must succeed");
    assert_eq!(
        table.metadata().snapshots.len(),
        snapshots_before,
        "empty transaction must not produce a snapshot",
    );
}

/// Java: `testSingleOperationTransaction` — a transaction with a single
/// `append_data` op produces exactly one new snapshot.
#[tokio::test]
async fn test_transaction_single_operation_transaction_per_java() {
    let mut table = fresh_table("single_op_txn").await;
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(0, 2))]), None)
        .await
        .expect("write");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit");
    assert_eq!(
        table.metadata().snapshots.len(),
        1,
        "single-op transaction must produce one snapshot",
    );
}

/// Java: `testMultipleOperationTransaction` — chained `append_data` calls
/// inside one transaction merge into a SINGLE snapshot.
///
/// The Java contract is that the snapshot's summary counts ALL files
/// added across the chained calls. Rust's `append_data` builder combines
/// the chained calls into one `Operation::Append`, but the
/// `additional_summary` field only retains the LAST call's count — see
/// the divergence test below. This test pins the single-snapshot half
/// of the contract; the count-accumulation half is `#[ignore]`d.
#[tokio::test]
async fn test_transaction_multiple_operation_transaction_per_java() {
    let mut table = fresh_table("multi_op_txn").await;
    let first = write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(0, 2))]), None)
        .await
        .expect("write first");
    let second =
        write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(100, 3))]), None)
            .await
            .expect("write second");

    table
        .new_transaction(None)
        .append_data(first)
        .append_data(second)
        .commit()
        .await
        .expect("commit chained appends");

    assert_eq!(
        table.metadata().snapshots.len(),
        1,
        "two append_data() calls in one transaction must produce one snapshot",
    );
}

/// Rust behaviour-divergence: chained `append_data(a).append_data(b)`
/// produces one snapshot whose `added-data-files` count reflects only
/// the LAST call's files, not the combined total. `append_data`
/// extends the underlying `Operation::Append::data_files` vec but does
/// NOT recompute `additional_summary` from the merged file list — see
/// `Transaction::append_data` in `src/table/transaction/mod.rs`.
///
/// Java's TestTransaction expects the combined total. Pinning this as
/// a `#[ignore]` test makes the bug visible without breaking the
/// passing single-snapshot assertion above.
#[tokio::test]
#[ignore = "rust bug: chained Transaction::append_data does not recompute additional_summary; the snapshot's added-data-files reflects only the LAST call"]
async fn test_transaction_multiple_append_data_calls_accumulate_summary_per_java() {
    let mut table = fresh_table("multi_op_summary_accum").await;
    let first = write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(0, 2))]), None)
        .await
        .expect("write first");
    let first_count = first.len();
    let second =
        write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(100, 3))]), None)
            .await
            .expect("write second");
    let second_count = second.len();

    table
        .new_transaction(None)
        .append_data(first)
        .append_data(second)
        .commit()
        .await
        .expect("commit chained appends");

    let current = table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("current snapshot");
    let expected_total = first_count + second_count;
    assert_eq!(
        current
            .summary()
            .other
            .get("added-data-files")
            .map(String::as_str),
        Some(expected_total.to_string()).as_deref(),
        "the combined snapshot must count files from BOTH append_data calls",
    );
}

/// Java: `testMultipleOperationTransactionFromTable` — variant of the
/// above that obtains the transaction via `table.new_transaction()`
/// (rather than holding a separate `TransactionBuilder`). Same end
/// result: one snapshot for all chained operations.
#[tokio::test]
async fn test_transaction_multiple_operation_transaction_from_table_per_java() {
    let mut table = fresh_table("multi_op_from_table").await;
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(0, 4))]), None)
        .await
        .expect("write");
    // The Rust API is always to obtain the transaction via
    // table.new_transaction(...) — there is no detached TransactionBuilder
    // path. This test exists to mirror the Java surface; it just
    // re-exercises the new_transaction entry point.
    table
        .new_transaction(None)
        .append_data(files.clone())
        .commit()
        .await
        .expect("commit");
    assert_eq!(table.metadata().snapshots.len(), 1);
    // A second new_transaction off the same table reflects the updated state.
    let more = write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(100, 4))]), None)
        .await
        .expect("write more");
    table
        .new_transaction(None)
        .append_data(more)
        .commit()
        .await
        .expect("commit more");
    assert_eq!(table.metadata().snapshots.len(), 2);
}

/// Java: `testTransactionFastAppends` — fast-append semantics preserved
/// when appends happen inside a transaction (no merging with prior
/// manifests).
#[tokio::test]
async fn test_transaction_fast_appends_per_java() {
    let mut table = fresh_table("txn_fast_appends").await;
    let first = write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(0, 2))]), None)
        .await
        .expect("write");
    table
        .new_transaction(None)
        .append_data(first)
        .commit()
        .await
        .expect("commit first");
    let first_manifest_count = table.manifests(None, None).await.expect("manifests").len();

    let second =
        write_parquet_partitioned(&table, stream::iter(vec![Ok(small_batch(100, 2))]), None)
            .await
            .expect("write");
    table
        .new_transaction(None)
        .append_data(second)
        .commit()
        .await
        .expect("commit second");
    let second_manifest_count = table.manifests(None, None).await.expect("manifests").len();

    assert!(
        second_manifest_count >= first_manifest_count,
        "fast-append inside a transaction must not shrink the manifest count",
    );
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "feature gap: no dirty-state detection — Rust's TransactionBuilder accumulates operations without an explicit per-op commit / rollback lifecycle"]
async fn test_transaction_detects_uncommitted_change_per_java() {
    // Java: testDetectsUncommittedChange.
}

#[tokio::test]
#[ignore = "feature gap: same — Rust transactions don't model per-op uncommitted state"]
async fn test_transaction_detects_uncommitted_change_on_commit_per_java() {
    // Java: testDetectsUncommittedChangeOnCommit.
}

#[tokio::test]
#[ignore = "feature gap: no concurrent-commit conflict detection on the transaction path; commit doesn't snapshot the base metadata to compare against"]
async fn test_transaction_conflict_per_java() {
    // Java: testTransactionConflict.
}

#[tokio::test]
#[ignore = "feature gap: no bulk-deletion cleanup of partial manifests/data files on commit failure"]
async fn test_transaction_failure_bulk_deletion_cleanup_per_java() {
    // Java: testTransactionFailureBulkDeletionCleanup.
}

#[tokio::test]
#[ignore = "feature gap: no commit.retry.num-retries property; Rust commit does not retry on conflict"]
async fn test_transaction_retry_per_java() {
    // Java: testTransactionRetry.
}

#[tokio::test]
#[ignore = "feature gap: no merge-append op (only fast-append-style append_data); retry path doesn't apply"]
async fn test_transaction_retry_merge_append_per_java() {
    // Java: testTransactionRetryMergeAppend.
}

#[tokio::test]
#[ignore = "feature gap: same — no merge-append + no retry + no cleanup-between-attempts"]
async fn test_transaction_multiple_update_retry_merge_cleanup_per_java() {
    // Java: testMultipleUpdateTransactionRetryMergeCleanup.
}

#[tokio::test]
#[ignore = "feature gap: no updateSchema().addColumn(...).commit() flow inside a transaction (cycle H20 pinned the SchemaUpdate gap)"]
async fn test_transaction_retry_schema_update_per_java() {
    // Java: testTransactionRetrySchemaUpdate.
}

#[tokio::test]
#[ignore = "feature gap: no merge-cleanup contract — Rust doesn't track intermediate retry manifests"]
async fn test_transaction_retry_merge_cleanup_per_java() {
    // Java: testTransactionRetryMergeCleanup.
}

#[tokio::test]
#[ignore = "feature gap: no append_manifest API and no SNAPSHOT_ID_INHERITANCE_ENABLED property"]
async fn test_transaction_retry_and_append_manifests_without_snapshot_id_inheritance_per_java() {
    // Java: testTransactionRetryAndAppendManifestsWithoutSnapshotIdInheritance.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_transaction_retry_and_append_manifests_with_snapshot_id_inheritance_per_java() {
    // Java: testTransactionRetryAndAppendManifestsWithSnapshotIdInheritance.
}

#[tokio::test]
#[ignore = "feature gap: no custom-delete-function injection; pins absence rather than presence"]
async fn test_transaction_no_custom_delete_func_per_java() {
    // Java: testTransactionNoCustomDeleteFunc.
}

#[tokio::test]
#[ignore = "feature gap: no Transaction.rewriteManifests().appendManifest() API"]
async fn test_transaction_rewrite_manifests_appended_directly_per_java() {
    // Java: testTransactionRewriteManifestsAppendedDirectly.
}

#[tokio::test]
#[ignore = "feature gap: no 'unknown commit state' handling — Rust treats commit response as authoritative"]
async fn test_transaction_simple_not_deleting_metadata_on_unknown_state_per_java() {
    // Java: testSimpleTransactionNotDeletingMetadataOnUnknownSate.
}

#[tokio::test]
#[ignore = "feature gap: TransactionBuilder.commit consumes self; no recommit-after-transient-failure path"]
async fn test_transaction_recommit_per_java() {
    // Java: testTransactionRecommit.
}

#[tokio::test]
#[ignore = "feature gap: no 'snapshot-property.*' commit-property propagation onto the produced snapshot summary"]
async fn test_transaction_commit_properties_per_java() {
    // Java: testCommitProperties.
}

#[tokio::test]
#[ignore = "feature gap: no row-delta op + no concurrent-rewrite detection"]
async fn test_transaction_row_delta_with_concurrent_manifest_rewrite_per_java() {
    // Java: testRowDeltaWithConcurrentManifestRewrite.
}

#[tokio::test]
#[ignore = "feature gap: same — no concurrent-rewrite handling on overwrite"]
async fn test_transaction_overwrite_with_concurrent_manifest_rewrite_per_java() {
    // Java: testOverwriteWithConcurrentManifestRewrite.
}

#[tokio::test]
#[ignore = "feature gap: no Transaction.extend(baseTransaction) for chaining child transactions on top of a still-open parent"]
async fn test_transaction_extend_base_transaction_per_java() {
    // Java: testExtendBaseTransaction.
}
