//! The DataFusion provider claims the table's declared sort order only for
//! files that attest it, so a fully attested table can drop a redundant sort
//! while a table holding any unattested file keeps it — and stays correct.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::stream;

use datafusion_iceberg::DataFusionTable;
use iceberg_rust::arrow::write::{write_parquet_partitioned, write_sorted_parquet_partitioned};
use iceberg_rust::catalog::Catalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::partition::Transform;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::sort::{NullOrder, SortDirection, SortField, SortOrderBuilder};
use iceberg_rust::spec::types::{PrimitiveType, StructField, Type};
use iceberg_rust::table::Table;
use iceberg_sql_catalog::SqlCatalog;

fn schema() -> Schema {
    let mut builder = Schema::builder();
    builder
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
            name: "name".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        });
    builder.build().unwrap()
}

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// An unpartitioned table sorted by `id ASC`.
async fn sorted_table(name: &str) -> Table {
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", ObjectStoreBuilder::memory())
            .await
            .unwrap(),
    );
    let sort_order = SortOrderBuilder::default()
        .with_order_id(1)
        .with_sort_field(SortField {
            source_id: 1,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        })
        .build()
        .unwrap();
    Table::builder()
        .with_name(name)
        .with_location(format!("/test/{name}"))
        .with_schema(schema())
        .with_sort_order(sort_order)
        .build(&["test".to_owned()], catalog)
        .await
        .expect("Failed to create table")
}

async fn append_sorted(table: &mut Table, ids: &[i64]) {
    let files = write_sorted_parquet_partitioned(table, stream::iter(vec![Ok(batch(ids))]), None)
        .await
        .unwrap();
    assert!(files.iter().all(|f| *f.sort_order_id() == Some(1)));
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .unwrap();
}

async fn append_unsorted(table: &mut Table, ids: &[i64]) {
    let files = write_parquet_partitioned(table, stream::iter(vec![Ok(batch(ids))]), None)
        .await
        .unwrap();
    assert!(files.iter().all(|f| f.sort_order_id().is_none()));
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await
        .unwrap();
}

fn context(split_file_groups_by_statistics: bool) -> SessionContext {
    let mut config = SessionConfig::new().with_target_partitions(4);
    config
        .options_mut()
        .execution
        .split_file_groups_by_statistics = split_file_groups_by_statistics;
    SessionContext::new_with_config(config)
}

async fn physical_plan(ctx: &SessionContext, table: &Table, sql: &str) -> String {
    ctx.register_table("t", Arc::new(DataFusionTable::from(table.clone())))
        .unwrap();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    ctx.deregister_table("t").unwrap();
    rendered
}

async fn ids(ctx: &SessionContext, table: &Table, sql: &str) -> Vec<i64> {
    ctx.register_table("t", Arc::new(DataFusionTable::from(table.clone())))
        .unwrap();
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    ctx.deregister_table("t").unwrap();
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn fully_attested_scan_declares_the_order_and_elides_the_sort() {
    let mut table = sorted_table("attested").await;
    append_sorted(&mut table, &[1, 2, 3, 4]).await;
    append_sorted(&mut table, &[5, 6, 7, 8]).await;

    let ctx = context(true);
    let plan = physical_plan(&ctx, &table, "SELECT id FROM t ORDER BY id ASC").await;
    assert!(
        plan.contains("output_ordering=[id@0 ASC]"),
        "scan must declare the table's sort order:\n{plan}"
    );
    assert!(
        !plan.contains("SortExec"),
        "non-overlapping attested files satisfy ORDER BY without a sort:\n{plan}"
    );

    assert_eq!(
        ids(&ctx, &table, "SELECT id FROM t ORDER BY id ASC").await,
        vec![1, 2, 3, 4, 5, 6, 7, 8]
    );
    assert_eq!(
        ids(&ctx, &table, "SELECT id FROM t ORDER BY id DESC LIMIT 3").await,
        vec![8, 7, 6]
    );
}

#[tokio::test]
async fn one_unattested_file_keeps_the_sort_and_the_results_exact() {
    let mut table = sorted_table("mixed").await;
    append_sorted(&mut table, &[1, 2, 3, 4]).await;
    append_sorted(&mut table, &[9, 10, 11, 12]).await;
    // Rows that interleave with both attested files, written without a sort
    // and without attestation.
    append_unsorted(&mut table, &[6, 5, 8, 7]).await;

    let ctx = context(true);
    let plan = physical_plan(&ctx, &table, "SELECT id FROM t ORDER BY id ASC").await;
    assert!(
        plan.contains("SortExec") || plan.contains("SortPreservingMergeExec"),
        "a mixed table must keep an explicit sort:\n{plan}"
    );

    assert_eq!(
        ids(&ctx, &table, "SELECT id FROM t ORDER BY id ASC").await,
        (1..=12).collect::<Vec<_>>()
    );
    assert_eq!(
        ids(&ctx, &table, "SELECT id FROM t ORDER BY id DESC LIMIT 5").await,
        vec![12, 11, 10, 9, 8]
    );
}

#[tokio::test]
async fn overlapping_attested_files_keep_the_sort_and_the_results_exact() {
    let mut table = sorted_table("overlapping").await;
    // Each file is sorted, but their ranges overlap: reading them back to back
    // is not sorted, and DataFusion must not pretend otherwise.
    append_sorted(&mut table, &[1, 3, 5, 7]).await;
    append_sorted(&mut table, &[2, 4, 6, 8]).await;

    for split in [true, false] {
        let ctx = context(split);
        assert_eq!(
            ids(&ctx, &table, "SELECT id FROM t ORDER BY id ASC").await,
            (1..=8).collect::<Vec<_>>(),
            "split_file_groups_by_statistics={split}"
        );
        assert_eq!(
            ids(&ctx, &table, "SELECT id FROM t ORDER BY id DESC LIMIT 3").await,
            vec![8, 7, 6],
            "split_file_groups_by_statistics={split}"
        );
    }
}
