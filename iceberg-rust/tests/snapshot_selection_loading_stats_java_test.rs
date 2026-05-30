//! Java-parity tests for three related snapshot APIs.
//!
//! Mirrors:
//!   - `org.apache.iceberg.TestSnapshotSelection` (2 @TestTemplate) —
//!     snapshot lookup by id and added-files stats.
//!   - `org.apache.iceberg.TestSetStatistics` (6 @TestTemplate) —
//!     StatisticsFile registration via `table.updateStatistics()`.
//!   - `org.apache.iceberg.TestSnapshotLoading` (8 @TestTemplate) —
//!     lazy loading of snapshots from the manifest list.
//!
//! Rust state:
//!   - SnapshotSelection: `TableMetadata.snapshots` is a HashMap; lookup
//!     by snapshot_id is O(1) and eager. Lazy-load assertions don't
//!     apply.
//!   - SetStatistics: no `TableUpdate::SetStatistics` variant (cycle F
//!     pinned this earlier). All 6 scenarios are gaps.
//!   - SnapshotLoading: Rust loads all snapshots eagerly via serde when
//!     reading TableMetadata; the lazy-load contract isn't applicable.

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

// --- TestSnapshotSelection -------------------------------------------------

/// Java: `testSnapshotSelectionById` — `table.snapshot(id)` retrieves
/// any past snapshot by id, not just the current one.
#[tokio::test]
async fn test_snapshot_selection_by_id_per_java() {
    let mut table = fresh_table("snapshot_selection").await;

    // First append.
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(0, 2))]), None)
        .await
        .expect("write first");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit first");
    let first_id = *table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("first snapshot")
        .snapshot_id();

    // Second append.
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch(100, 2))]), None)
        .await
        .expect("write second");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .expect("commit second");
    let second_id = *table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .expect("second snapshot")
        .snapshot_id();

    assert_ne!(first_id, second_id);
    assert_eq!(table.metadata().snapshots.len(), 2);
    // Both snapshots must be retrievable by id.
    assert!(
        table.metadata().snapshots.contains_key(&first_id),
        "first snapshot must remain in snapshots map after a second commit",
    );
    assert!(
        table.metadata().snapshots.contains_key(&second_id),
        "second snapshot must be in snapshots map",
    );
    // Per Java: snapshot(id) returns the matching snapshot.
    let first = &table.metadata().snapshots[&first_id];
    assert_eq!(*first.snapshot_id(), first_id);
}

#[tokio::test]
#[ignore = "feature gap: SnapshotChanges.addedDataFiles() not implemented (cycle H21 pins SnapshotChanges absence); cannot iterate stats on added files"]
async fn test_snapshot_selection_stats_for_added_files_per_java() {
    // Java: testSnapshotStatsForAddedFiles. Uses
    // SnapshotChanges.builderFor(table).snapshot(s).build().addedDataFiles()
    // to recover the DataFile entries with their full metrics
    // (valueCounts/nullValueCounts/lowerBounds/upperBounds). Rust has
    // no SnapshotChanges accessor.
}

// --- TestSetStatistics -----------------------------------------------------

#[tokio::test]
#[ignore = "feature gap: no TableUpdate::SetStatistics variant; cycle F (catalog/commit.rs) already pins the spec-side gap for StatisticsFile carrying puffin path/size/blob-metadata"]
async fn test_set_statistics_empty_update_per_java() {
    // Java: testEmptyUpdateStatistics. updateStatistics().commit() with
    // no setStatistics() calls still bumps version().
}

#[tokio::test]
#[ignore = "feature gap: same — no SetStatistics op, plus no version() bump for empty txn"]
async fn test_set_statistics_empty_transactional_update_per_java() {
    // Java: testEmptyTransactionalUpdateStatistics.
}

#[tokio::test]
#[ignore = "feature gap: TableMetadata has no statistics_files field; updateStatistics().setStatistics(f).commit() can't be exercised"]
async fn test_set_statistics_update_statistics_per_java() {
    // Java: testUpdateStatistics. Constructs a GenericStatisticsFile
    // with puffin path + size + footer size + blob metadata; verifies
    // metadata.statisticsFiles() contains exactly that file.
}

#[tokio::test]
#[ignore = "feature gap: no RemoveStatistics op either"]
async fn test_set_statistics_remove_statistics_per_java() {
    // Java: testRemoveStatistics.
}

#[tokio::test]
#[ignore = "feature gap: setStatistics is idempotent in Java (multiple calls keep only the last); Rust has no SetStatistics path at all"]
async fn test_set_statistics_overwrite_same_snapshot_per_java() {
    // Java: testUpdateStatisticsOverwriteSameSnapshot.
}

#[tokio::test]
#[ignore = "feature gap: same"]
async fn test_set_statistics_multiple_snapshots_per_java() {
    // Java: testUpdateStatisticsMultipleSnapshots.
}

// --- TestSnapshotLoading ---------------------------------------------------

#[tokio::test]
#[ignore = "feature gap: Rust loads all snapshots eagerly via serde when reading TableMetadata JSON; the lazy-load 'loaded once' contract isn't applicable"]
async fn test_snapshot_loading_snapshots_loaded_once_per_java() {
    // Java: testSnapshotsAreLoadedOnce. The lazy snapshot-loader caches
    // loaded snapshots so they aren't deserialized again on subsequent
    // accesses. Rust has no lazy load.
}

#[tokio::test]
#[ignore = "feature gap: Rust has no separate 'current snapshot' vs 'all snapshots' loading paths"]
async fn test_snapshot_loading_current_and_main_does_not_load_per_java() {
    // Java: testCurrentAndMainSnapshotDoesNotLoad.
}

#[tokio::test]
#[ignore = "feature gap: same — eager load means there's no 'unloaded' state"]
async fn test_snapshot_loading_unloaded_snapshot_loads_once_per_java() {
    // Java: testUnloadedSnapshotLoadsOnce.
}

#[tokio::test]
#[ignore = "feature gap: same — TableScan doesn't have a lazy-snapshot dependency in Rust"]
async fn test_snapshot_loading_current_table_scan_does_not_load_per_java() {
    // Java: testCurrentTableScanDoesNotLoad.
}

#[tokio::test]
#[ignore = "feature gap: Java's loader removes snapshots from the future (timestamp_ms > current) — Rust loads them as-is from JSON"]
async fn test_snapshot_loading_future_snapshots_are_removed_per_java() {
    // Java: testFutureSnapshotsAreRemoved.
}

#[tokio::test]
#[ignore = "feature gap: Rust doesn't validate that current_snapshot_id is reachable from snapshots map at load time"]
async fn test_snapshot_loading_removed_current_snapshot_fails_per_java() {
    // Java: testRemovedCurrentSnapshotFails.
}

#[tokio::test]
#[ignore = "feature gap: same — Rust doesn't validate ref.snapshot_id is in snapshots map"]
async fn test_snapshot_loading_removed_ref_snapshot_fails_per_java() {
    // Java: testRemovedRefSnapshotFails.
}

#[tokio::test]
#[ignore = "feature gap: Rust builds TableMetadata in one step via serde; there's no metadata-builder-triggered snapshot-load hook"]
async fn test_snapshot_loading_building_new_metadata_triggers_load_per_java() {
    // Java: testBuildingNewMetadataTriggersSnapshotLoad.
}
