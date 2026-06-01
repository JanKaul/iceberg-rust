//! Ports upstream `TestViews` / view-DDL coverage to the
//! DataFusion + iceberg-rust engine under test.
//!
//! datafusion_iceberg's planner handles two view-style statements:
//! - `CREATE VIEW <name> AS <select>` → persistent Iceberg view
//! - `CREATE TEMPORARY VIEW <name> AS <select>` → Iceberg materialised view
//!
//! Both rely on the `iceberg_transform` to route the parsed plan into the
//! Iceberg catalog rather than the DataFusion-local view registry.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql};

async fn seed_view_base(ctx: &datafusion::execution::context::SessionContext) {
    execute_sql(ctx, "CREATE SCHEMA warehouse.v").await;
    execute_sql(
        ctx,
        "CREATE EXTERNAL TABLE warehouse.v.events \
         (id BIGINT NOT NULL, kind STRING NOT NULL, weight INT NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/v/events'",
    )
    .await;
    execute_sql(
        ctx,
        "INSERT INTO warehouse.v.events VALUES \
         (1, 'click', 10), (2, 'view', 1), (3, 'click', 20), \
         (4, 'purchase', 100), (5, 'click', 5), (6, 'view', 1)",
    )
    .await;
}

#[tokio::test]
#[ignore = "planner.rs hardcodes s3:// view location when not built with cfg(test); blocks in-memory object store"]
async fn integration_df_create_view_then_select() {
    let ctx = boot_df_stack().await;
    seed_view_base(&ctx).await;
    execute_sql(
        &ctx,
        "CREATE VIEW warehouse.v.clicks AS \
         SELECT id, weight FROM warehouse.v.events WHERE kind = 'click'",
    )
    .await;
    let n = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.v.clicks").await;
    assert_eq!(n, 3);
    let sum = execute_scalar_i64(&ctx, "SELECT SUM(weight) FROM warehouse.v.clicks").await;
    assert_eq!(sum, 35);
}

#[tokio::test]
#[ignore = "planner.rs hardcodes s3:// view location when not built with cfg(test); blocks in-memory object store"]
async fn integration_df_create_view_with_aggregation() {
    let ctx = boot_df_stack().await;
    seed_view_base(&ctx).await;
    execute_sql(
        &ctx,
        "CREATE VIEW warehouse.v.weights_by_kind AS \
         SELECT kind, SUM(weight) AS total FROM warehouse.v.events GROUP BY kind",
    )
    .await;
    let total = execute_scalar_i64(
        &ctx,
        "SELECT SUM(total) FROM warehouse.v.weights_by_kind",
    )
    .await;
    assert_eq!(total, 137);
}

#[tokio::test]
#[ignore = "planner.rs hardcodes s3:// MV location when not built with cfg(test); blocks in-memory object store"]
async fn integration_df_create_temporary_view_materializes_query() {
    let ctx = boot_df_stack().await;
    seed_view_base(&ctx).await;
    // Materialised view: the SELECT result is captured at create-time. We
    // verify the basic addressable-as-a-table behaviour; refresh semantics
    // are exercised separately by the materialized-view unit suite.
    execute_sql(
        &ctx,
        "CREATE TEMPORARY VIEW warehouse.v.high_weight AS \
         SELECT id, weight FROM warehouse.v.events WHERE weight >= 10",
    )
    .await;
    let n = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.v.high_weight").await;
    assert_eq!(n, 3, "weights 10, 20, 100 satisfy >= 10");
}

#[tokio::test]
#[ignore = "planner.rs hardcodes s3:// view location when not built with cfg(test); blocks in-memory object store"]
async fn integration_df_view_over_view() {
    let ctx = boot_df_stack().await;
    seed_view_base(&ctx).await;
    execute_sql(
        &ctx,
        "CREATE VIEW warehouse.v.clicks AS \
         SELECT id, weight FROM warehouse.v.events WHERE kind = 'click'",
    )
    .await;
    execute_sql(
        &ctx,
        "CREATE VIEW warehouse.v.big_clicks AS \
         SELECT id FROM warehouse.v.clicks WHERE weight > 5",
    )
    .await;
    let n = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.v.big_clicks").await;
    // clicks: weights 10, 20, 5; > 5 ⇒ 10 and 20 ⇒ 2 rows.
    assert_eq!(n, 2);
}
