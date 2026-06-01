//! Ports the read-path / SELECT surface of upstream `TestSelect` family to
//! the DataFusion + iceberg-rust engine under test.
//!
//! Coverage: filter pushdown observable via row count, projection,
//! aggregation, ordering, GROUP BY, BETWEEN, IS NULL, simple join.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql, total_rows};

async fn seed_orders(ctx: &datafusion::execution::context::SessionContext) {
    execute_sql(ctx, "CREATE SCHEMA warehouse.q").await;
    execute_sql(
        ctx,
        "CREATE EXTERNAL TABLE warehouse.q.orders \
         (id BIGINT NOT NULL, region STRING NOT NULL, amount BIGINT NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/q/orders'",
    )
    .await;
    execute_sql(
        ctx,
        "INSERT INTO warehouse.q.orders VALUES \
         (1, 'EU', 100), (2, 'EU', 50), (3, 'US', 200), \
         (4, 'US', 75), (5, 'APAC', 25), (6, 'APAC', 175)",
    )
    .await;
}

#[tokio::test]
async fn integration_df_select_all_rows() {
    let ctx = boot_df_stack().await;
    seed_orders(&ctx).await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.q.orders").await;
    assert_eq!(count, 6);
}

#[tokio::test]
async fn integration_df_select_with_equality_filter() {
    let ctx = boot_df_stack().await;
    seed_orders(&ctx).await;
    let count = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.q.orders WHERE region = 'EU'",
    )
    .await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn integration_df_select_with_range_filter() {
    let ctx = boot_df_stack().await;
    seed_orders(&ctx).await;
    let count = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.q.orders WHERE amount BETWEEN 50 AND 150",
    )
    .await;
    assert_eq!(count, 3);
}

#[tokio::test]
async fn integration_df_select_sum_aggregate() {
    let ctx = boot_df_stack().await;
    seed_orders(&ctx).await;
    let sum = execute_scalar_i64(&ctx, "SELECT SUM(amount) FROM warehouse.q.orders").await;
    assert_eq!(sum, 625);
}

#[tokio::test]
async fn integration_df_select_group_by_region() {
    let ctx = boot_df_stack().await;
    seed_orders(&ctx).await;
    let batches = df_common::execute_sql(
        &ctx,
        "SELECT region, SUM(amount) AS s FROM warehouse.q.orders \
         GROUP BY region ORDER BY region",
    )
    .await;
    assert_eq!(total_rows(&batches), 3, "three regions expected");
}

#[tokio::test]
async fn integration_df_select_order_by_with_limit() {
    let ctx = boot_df_stack().await;
    seed_orders(&ctx).await;
    let batches = execute_sql(
        &ctx,
        "SELECT id FROM warehouse.q.orders ORDER BY amount DESC LIMIT 2",
    )
    .await;
    assert_eq!(total_rows(&batches), 2, "LIMIT 2 should yield two rows");
}

#[tokio::test]
async fn integration_df_select_join_two_iceberg_tables() {
    let ctx = boot_df_stack().await;
    seed_orders(&ctx).await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.q.regions \
         (region STRING NOT NULL, manager STRING NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/q/regions'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.q.regions VALUES \
         ('EU', 'Alice'), ('US', 'Bob'), ('APAC', 'Carol')",
    )
    .await;
    let count = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.q.orders o \
         JOIN warehouse.q.regions r ON o.region = r.region \
         WHERE r.manager = 'Alice'",
    )
    .await;
    assert_eq!(count, 2, "two EU orders should join to manager Alice");
}

#[tokio::test]
async fn integration_df_select_with_is_null_filter() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.q_null").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.q_null.t \
         (id BIGINT NOT NULL, label STRING) STORED AS ICEBERG \
         LOCATION '/warehouse/q_null/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.q_null.t VALUES (1, NULL), (2, 'a'), (3, NULL), (4, 'b')",
    )
    .await;
    let nulls = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.q_null.t WHERE label IS NULL",
    )
    .await;
    assert_eq!(nulls, 2);
    let nonnulls = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.q_null.t WHERE label IS NOT NULL",
    )
    .await;
    assert_eq!(nonnulls, 2);
}
