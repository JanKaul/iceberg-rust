//! Ports the predicate / filter-pushdown surface of upstream
//! `TestSparkV2Filters` and `TestSparkFilters` to the DataFusion +
//! iceberg-rust engine under test.
//!
//! Upstream verifies that each SQL predicate translates correctly to an
//! Iceberg expression and that the resulting row set matches Spark
//! semantics. Here we run each predicate through DataFusion against an
//! iceberg-rust table and verify the count.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql};

async fn seed_filters(ctx: &datafusion::execution::context::SessionContext) {
    execute_sql(ctx, "CREATE SCHEMA warehouse.flt").await;
    execute_sql(
        ctx,
        "CREATE EXTERNAL TABLE warehouse.flt.t \
         (id BIGINT NOT NULL, score INT NOT NULL, label STRING) \
         STORED AS ICEBERG LOCATION '/warehouse/flt/t'",
    )
    .await;
    execute_sql(
        ctx,
        "INSERT INTO warehouse.flt.t VALUES \
         (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma'), \
         (4, 40, 'delta'), (5, 50, NULL), (6, 60, 'alpha'), \
         (7, 70, 'epsilon'), (8, 80, 'beta'), (9, 90, NULL), (10, 100, 'zeta')",
    )
    .await;
}

#[tokio::test]
async fn integration_df_filter_equal() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE score = 30").await;
    assert_eq!(n, 1);
}

#[tokio::test]
async fn integration_df_filter_not_equal() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE score <> 30").await;
    assert_eq!(n, 9);
}

#[tokio::test]
async fn integration_df_filter_less_than() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE score < 40").await;
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_filter_less_or_equal() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE score <= 40").await;
    assert_eq!(n, 4);
}

#[tokio::test]
async fn integration_df_filter_greater_than() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE score > 70").await;
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_filter_greater_or_equal() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE score >= 70").await;
    assert_eq!(n, 4);
}

#[tokio::test]
async fn integration_df_filter_in_list() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score IN (10, 50, 90)",
    )
    .await;
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_filter_not_in_list() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score NOT IN (10, 50, 90)",
    )
    .await;
    assert_eq!(n, 7);
}

#[tokio::test]
async fn integration_df_filter_between() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score BETWEEN 30 AND 60",
    )
    .await;
    assert_eq!(n, 4);
}

#[tokio::test]
async fn integration_df_filter_is_null() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE label IS NULL").await;
    assert_eq!(n, 2);
}

#[tokio::test]
async fn integration_df_filter_is_not_null() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.flt.t WHERE label IS NOT NULL")
            .await;
    assert_eq!(n, 8);
}

#[tokio::test]
async fn integration_df_filter_like_prefix() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE label LIKE 'a%'",
    )
    .await;
    assert_eq!(n, 2, "alpha twice");
}

#[tokio::test]
async fn integration_df_filter_string_equality() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE label = 'beta'",
    )
    .await;
    assert_eq!(n, 2);
}

#[tokio::test]
async fn integration_df_filter_conjunction() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score >= 30 AND label = 'gamma'",
    )
    .await;
    assert_eq!(n, 1);
}

#[tokio::test]
async fn integration_df_filter_disjunction() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score < 20 OR score > 90",
    )
    .await;
    assert_eq!(n, 2);
}

#[tokio::test]
async fn integration_df_filter_negation() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE NOT (score < 50)",
    )
    .await;
    assert_eq!(n, 6);
}
