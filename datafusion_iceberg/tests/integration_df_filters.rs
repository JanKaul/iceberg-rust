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
use rstest::rstest;

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
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score = 30",
    )
    .await;
    assert_eq!(n, 1);
}

#[tokio::test]
async fn integration_df_filter_not_equal() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score <> 30",
    )
    .await;
    assert_eq!(n, 9);
}

#[tokio::test]
async fn integration_df_filter_less_than() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score < 40",
    )
    .await;
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_filter_less_or_equal() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score <= 40",
    )
    .await;
    assert_eq!(n, 4);
}

#[tokio::test]
async fn integration_df_filter_greater_than() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score > 70",
    )
    .await;
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_filter_greater_or_equal() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE score >= 70",
    )
    .await;
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
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE label IS NULL",
    )
    .await;
    assert_eq!(n, 2);
}

#[tokio::test]
async fn integration_df_filter_is_not_null() {
    let ctx = boot_df_stack().await;
    seed_filters(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.flt.t WHERE label IS NOT NULL",
    )
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

// Number of parquet data files the physical plan for `sql` would scan
async fn plan_files(ctx: &datafusion::execution::context::SessionContext, sql: &str) -> usize {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let displayed = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    displayed.matches(".parquet").count()
}

// A filter must prune whole data files at plan time from Iceberg metadata
// alone, not just push the predicate down to the parquet reader.
#[rstest]
#[case::unpartitioned(false)]
#[case::partitioned(true)]
#[tokio::test]
async fn integration_df_filter_prunes_files(#[case] partitioned: bool) {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.flt_prune").await;
    execute_sql(
        &ctx,
        &format!(
            "CREATE EXTERNAL TABLE warehouse.flt_prune.t \
             (id BIGINT NOT NULL, region STRING NOT NULL, score INT NOT NULL) \
             STORED AS ICEBERG LOCATION '/warehouse/flt_prune/t'{}",
            if partitioned {
                " PARTITIONED BY (region)"
            } else {
                ""
            }
        ),
    )
    .await;
    // One single-region insert per commit, with disjoint score ranges ->
    // two data files (and two manifests) whose region and score bounds
    // don't overlap
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.flt_prune.t VALUES \
         (1, 'emea', 10), (2, 'emea', 20), (3, 'emea', 30)",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.flt_prune.t VALUES \
         (4, 'amer', 40), (5, 'amer', 50)",
    )
    .await;

    assert_eq!(
        plan_files(&ctx, "SELECT * FROM warehouse.flt_prune.t").await,
        2
    );

    // Each predicate isolates one of the two files (or neither) through a
    // different column's bounds; pruning must not change the row counts
    for (predicate, files, rows) in [
        ("id < 3", 1, 2),
        ("id > 4", 1, 1),
        ("region = 'emea'", 1, 3),
        ("region = 'amer'", 1, 2),
        ("region = 'apac'", 0, 0),
        ("score <= 30", 1, 3),
        ("score >= 40", 1, 2),
    ] {
        assert_eq!(
            plan_files(
                &ctx,
                &format!("SELECT * FROM warehouse.flt_prune.t WHERE {predicate}")
            )
            .await,
            files,
            "{predicate}"
        );
        assert_eq!(
            execute_scalar_i64(
                &ctx,
                &format!("SELECT COUNT(*) FROM warehouse.flt_prune.t WHERE {predicate}")
            )
            .await,
            rows,
            "{predicate}"
        );
    }
}
