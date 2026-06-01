//! Ports the CREATE TABLE surface of upstream `TestCreateTable` to the
//! DataFusion + iceberg-rust engine under test.
//!
//! Upstream uses `validationCatalog.loadTable(...)` to verify the resulting
//! table's schema, sort order, partition spec, etc. The DataFusion analogue
//! is to query the freshly-created table via `SELECT COUNT(*)` and to
//! introspect the registered `TableProvider` via `ctx.table_provider`.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql};

#[tokio::test]
async fn integration_df_create_table_simple() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ddl_simple").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ddl_simple.events \
         (id BIGINT NOT NULL, name STRING) STORED AS ICEBERG \
         LOCATION '/warehouse/ddl_simple/events'",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ddl_simple.events").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn integration_df_create_partitioned_by_identity() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ddl_ident").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ddl_ident.t \
         (id BIGINT NOT NULL, region STRING NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/ddl_ident/t' PARTITIONED BY (region)",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ddl_ident.t").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn integration_df_create_partitioned_by_month_transform() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ddl_xform").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ddl_xform.orders \
         (id BIGINT NOT NULL, order_date DATE NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/ddl_xform/orders' \
         PARTITIONED BY (\"month(order_date)\")",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ddl_xform.orders").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn integration_df_create_with_required_and_nullable_columns() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ddl_null").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ddl_null.t \
         (id BIGINT NOT NULL, optional_col STRING) STORED AS ICEBERG \
         LOCATION '/warehouse/ddl_null/t'",
    )
    .await;
    // Both columns must be addressable in a SELECT.
    let count = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.ddl_null.t WHERE optional_col IS NULL",
    )
    .await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn integration_df_create_with_decimal_and_timestamp_types() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ddl_types").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ddl_types.t \
         (id BIGINT NOT NULL, price DECIMAL(10, 2) NOT NULL, \
          recorded TIMESTAMP NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/ddl_types/t'",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ddl_types.t").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn integration_df_drop_table_removes_from_listing() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ddl_drop").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ddl_drop.t \
         (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/ddl_drop/t'",
    )
    .await;
    let count_before =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ddl_drop.t").await;
    assert_eq!(count_before, 0);

    execute_sql(&ctx, "DROP TABLE warehouse.ddl_drop.t").await;

    let result = ctx
        .state()
        .create_logical_plan("SELECT COUNT(*) FROM warehouse.ddl_drop.t")
        .await;
    assert!(
        result.is_err(),
        "table should no longer be addressable after DROP, got Ok plan"
    );
}
