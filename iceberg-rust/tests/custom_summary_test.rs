//! Custom snapshot summary entries on a table transaction: they end up next to the computed
//! entries of every snapshot of the transaction, and never overwrite an entry that is already set.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use futures::stream;

use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::error::Error;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::manifest::DataFile;
use iceberg_rust::table::Table;
use iceberg_rust_spec::spec::schema::Schema;
use iceberg_rust_spec::spec::types::{PrimitiveType, StructField, Type};
use iceberg_sql_catalog::SqlCatalog;

fn entries(entries: &[(&str, &str)]) -> HashMap<String, String> {
    entries
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

async fn create_table(name: &str) -> Table {
    let object_store = ObjectStoreBuilder::memory();
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store.clone())
            .await
            .unwrap(),
    );

    let schema = Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .build()
        .unwrap();
    Table::builder()
        .with_name(name)
        .with_location(format!("/test/{name}"))
        .with_schema(schema)
        .build(&["test".to_owned()], catalog)
        .await
        .expect("Failed to create table")
}

/// Writes one parquet file with the rows 1, 2 and 3
async fn write_files(table: &Table) -> Vec<DataFile> {
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    write_parquet_partitioned(table, stream::iter(vec![Ok(batch)]), None)
        .await
        .unwrap()
}

fn current_summary(table: &Table) -> HashMap<String, String> {
    table
        .metadata()
        .current_snapshot(None)
        .unwrap()
        .unwrap()
        .summary()
        .other
        .clone()
}

/// Custom summary entries end up in the summary of the snapshot of the transaction, next to
/// the computed entries, whatever the order of the calls on the transaction.
#[tokio::test]
async fn test_table_transaction_custom_summary() {
    let mut table = create_table("test_custom_summary").await;

    // The entries are given before the files
    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .with_custom_summary(entries(&[("alice", "1"), ("joe", "2")]))
        .append_data(files)
        .commit()
        .await
        .unwrap();

    let summary = current_summary(&table);
    assert_eq!(summary["alice"], "1");
    assert_eq!(summary["joe"], "2");
    assert_eq!(summary["added-records"], "3");

    // The entries are given after the files, and are per transaction: the next snapshot has
    // only its own
    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .append_data(files)
        .with_custom_summary(entries(&[("alice", "3")]))
        .commit()
        .await
        .unwrap();

    let summary = current_summary(&table);
    assert_eq!(summary["alice"], "3");
    assert!(!summary.contains_key("joe"));
    assert_eq!(summary["total-records"], "6");
}

/// Entries that are already in the summary, like the computed ones, are never overwritten
#[tokio::test]
async fn test_table_transaction_custom_summary_does_not_overwrite() {
    let mut table = create_table("test_custom_summary_conflict").await;
    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .unwrap();
    let snapshots = table.metadata().snapshots.len();

    for key in ["total-records", "added-records"] {
        let files = write_files(&table).await;
        let result = table
            .new_transaction(None)
            .append_data(files)
            .with_custom_summary(entries(&[("alice", "1"), (key, "999")]))
            .commit()
            .await;
        assert!(
            matches!(&result, Err(Error::SummaryEntryConflict(conflict)) if conflict == key),
            "{key}: {result:?}"
        );

        // nothing is committed
        assert_eq!(table.metadata().snapshots.len(), snapshots);
        let summary = current_summary(&table);
        assert_eq!(summary["total-records"], "3");
        assert!(!summary.contains_key("alice"));
    }
}

/// Calling the method again adds to the entries, and an entry with the same key is replaced
#[tokio::test]
async fn test_table_transaction_custom_summary_accumulates() {
    let mut table = create_table("test_custom_summary_accumulates").await;
    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .with_custom_summary(entries(&[("alice", "1"), ("joe", "2")]))
        .append_data(files)
        .with_custom_summary(entries(&[("joe", "3"), ("jane", "4")]))
        .commit()
        .await
        .unwrap();

    let summary = current_summary(&table);
    assert_eq!(summary["alice"], "1");
    assert_eq!(summary["joe"], "3");
    assert_eq!(summary["jane"], "4");
}

/// A replace operation creates a snapshot with the entries too
#[tokio::test]
async fn test_table_transaction_custom_summary_replace() {
    let mut table = create_table("test_custom_summary_replace").await;
    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .unwrap();

    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .replace(files)
        .with_custom_summary(entries(&[("alice", "1")]))
        .commit()
        .await
        .unwrap();

    let summary = current_summary(&table);
    assert_eq!(summary["alice"], "1");
    assert_eq!(summary["total-records"], "3");
}

/// A transaction that creates several snapshots gives all of them the entries
#[tokio::test]
async fn test_table_transaction_custom_summary_multiple_snapshots() {
    let mut table = create_table("test_custom_summary_multiple").await;
    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .unwrap();
    let before = table.metadata().snapshots.len();

    let appended = write_files(&table).await;
    let replacement = write_files(&table).await;
    table
        .new_transaction(None)
        .append_data(appended)
        .replace(replacement)
        .with_custom_summary(entries(&[("alice", "1")]))
        .commit()
        .await
        .unwrap();

    let metadata = table.metadata();
    assert_eq!(metadata.snapshots.len(), before + 2);
    let with_entries = metadata
        .snapshots
        .values()
        .filter(|snapshot| {
            snapshot
                .summary()
                .other
                .get("alice")
                .is_some_and(|value| value == "1")
        })
        .count();
    assert_eq!(with_entries, 2);
}

/// Without an operation that creates a snapshot the entries are not used, and do not end up in
/// the summary of an existing snapshot
#[tokio::test]
async fn test_table_transaction_custom_summary_without_snapshot() {
    let mut table = create_table("test_custom_summary_no_snapshot").await;
    let files = write_files(&table).await;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .unwrap();
    let summary_before = current_summary(&table);
    let snapshots_before = table.metadata().snapshots.len();

    table
        .new_transaction(None)
        .update_properties(vec![("owner".to_string(), "alice".to_string())])
        .with_custom_summary(entries(&[("alice", "1")]))
        .commit()
        .await
        .unwrap();

    assert_eq!(table.metadata().properties["owner"], "alice");
    assert_eq!(table.metadata().snapshots.len(), snapshots_before);
    assert_eq!(current_summary(&table), summary_before);
}
