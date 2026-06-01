//! Ports the CTAS (`CREATE TABLE ... AS SELECT`) surface of upstream
//! `TestCreateTableAsSelect` to the DataFusion + iceberg-rust engine.
//!
//! Upstream CTAS goes through `Spark.createTable(..., asSelect)` and yields
//! a materialised Iceberg table whose contents are the SELECT's result set.
//! DataFusion's `CREATE EXTERNAL TABLE` form does not accept an `AS SELECT`
//! suffix on the same statement; the equivalent flow is `CREATE EXTERNAL
//! TABLE` with the projected schema followed by `INSERT INTO ... SELECT ...`.
//! Tests here use that two-step form and verify the resulting row count and
//! aggregate matches.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql};

async fn seed_src(ctx: &datafusion::execution::context::SessionContext) {
    execute_sql(ctx, "CREATE SCHEMA warehouse.ctas").await;
    execute_sql(
        ctx,
        "CREATE EXTERNAL TABLE warehouse.ctas.src \
         (id BIGINT NOT NULL, region STRING NOT NULL, amount BIGINT NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/ctas/src'",
    )
    .await;
    execute_sql(
        ctx,
        "INSERT INTO warehouse.ctas.src VALUES \
         (1, 'EU', 100), (2, 'EU', 50), (3, 'US', 200), \
         (4, 'US', 75), (5, 'APAC', 25)",
    )
    .await;
}

#[tokio::test]
async fn integration_df_ctas_via_create_then_insert_full_copy() {
    let ctx = boot_df_stack().await;
    seed_src(&ctx).await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ctas.copy_all \
         (id BIGINT NOT NULL, region STRING NOT NULL, amount BIGINT NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/ctas/copy_all'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.ctas.copy_all \
         SELECT id, region, amount FROM warehouse.ctas.src",
    )
    .await;
    let src_count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ctas.src").await;
    let dst_count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ctas.copy_all").await;
    assert_eq!(src_count, dst_count);
}

#[tokio::test]
async fn integration_df_ctas_with_filter_projection() {
    let ctx = boot_df_stack().await;
    seed_src(&ctx).await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ctas.eu_only \
         (id BIGINT NOT NULL, amount BIGINT NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/ctas/eu_only'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.ctas.eu_only \
         SELECT id, amount FROM warehouse.ctas.src WHERE region = 'EU'",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ctas.eu_only").await;
    assert_eq!(count, 2);
    let sum = execute_scalar_i64(&ctx, "SELECT SUM(amount) FROM warehouse.ctas.eu_only").await;
    assert_eq!(sum, 150);
}

#[tokio::test]
async fn integration_df_ctas_with_aggregation() {
    let ctx = boot_df_stack().await;
    seed_src(&ctx).await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ctas.totals \
         (region STRING NOT NULL, total BIGINT NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/ctas/totals'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.ctas.totals \
         SELECT region, SUM(amount) AS total FROM warehouse.ctas.src \
         GROUP BY region",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ctas.totals").await;
    assert_eq!(count, 3, "three distinct regions");
    let sum = execute_scalar_i64(&ctx, "SELECT SUM(total) FROM warehouse.ctas.totals").await;
    assert_eq!(sum, 450, "should match SUM over src");
}
