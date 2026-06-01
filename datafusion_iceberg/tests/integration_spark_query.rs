//! Ports portable scenarios from upstream `TestSelect`, `TestFilterPushDown`,
//! and `TestAggregatePushDown` to the Spark integration fixture. The upstream
//! tests focus on partition-pruning + predicate-pushdown effectiveness; here
//! we re-pin them as observable-result tests: the SQL must return the right
//! values, regardless of which side of the engine did the filtering.

#[path = "spark_common/mod.rs"]
mod spark_common;

use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_query";

#[tokio::test]
async fn integration_spark_query_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    // Seed a partitioned table with 8 rows across two regions.
    spark_sql_ok(
        &stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.sales \
             (id BIGINT, region STRING, category STRING, amount INT, ordered_at TIMESTAMP) \
             USING ICEBERG \
             PARTITIONED BY (region, days(ordered_at))"
        ),
    )
    .await;
    spark_sql_ok(
        &stack,
        &format!(
            "INSERT INTO demo.{TEST_NS}.sales VALUES \
              (1, 'us-east',  'book',       10, TIMESTAMP '2025-04-01 09:30:00'), \
              (2, 'us-east',  'book',       25, TIMESTAMP '2025-04-01 11:00:00'), \
              (3, 'us-east',  'electronic', 100, TIMESTAMP '2025-04-02 14:15:00'), \
              (4, 'us-west',  'book',       15, TIMESTAMP '2025-04-01 10:00:00'), \
              (5, 'us-west',  'electronic', 200, TIMESTAMP '2025-04-02 16:45:00'), \
              (6, 'us-west',  'electronic', 50,  TIMESTAMP '2025-04-03 09:00:00'), \
              (7, 'eu-central','book',      8,   TIMESTAMP '2025-04-01 18:00:00'), \
              (8, 'eu-central','electronic', 75, TIMESTAMP '2025-04-03 20:30:00')"
        ),
    )
    .await;

    step_filter_equality(&stack).await;
    step_filter_in_list(&stack).await;
    step_filter_compound(&stack).await;
    step_filter_starts_with(&stack).await;
    step_aggregate_sum_group_by(&stack).await;
    step_aggregate_count_distinct(&stack).await;
    step_aggregate_min_max(&stack).await;

    spark_sql_ok(&stack, &format!("DROP TABLE demo.{TEST_NS}.sales")).await;
    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

async fn assert_stdout_contains(stack: &SparkStack, sql: &str, expected: &str) {
    let res = spark_sql(stack, sql).await;
    assert!(res.is_success(), "{}", res.dump());
    assert!(
        res.stdout.contains(expected),
        "expected `{expected}` in output of {sql}\nstdout=\n{}",
        res.stdout
    );
}

/// Upstream: `TestFilterPushDown` (subset). WHERE on an Identity partition
/// column returns the matching rows.
async fn step_filter_equality(stack: &SparkStack) {
    assert_stdout_contains(
        stack,
        &format!("SELECT SUM(amount) FROM demo.{TEST_NS}.sales WHERE region = 'us-east'"),
        "135", // 10 + 25 + 100
    )
    .await;
}

/// Upstream: `TestFilterPushDown` (IN-list variant). A multi-value predicate
/// against the partition column returns the union.
async fn step_filter_in_list(stack: &SparkStack) {
    assert_stdout_contains(
        stack,
        &format!(
            "SELECT SUM(amount) FROM demo.{TEST_NS}.sales \
             WHERE region IN ('us-west', 'eu-central')"
        ),
        "348", // 15 + 200 + 50 + 8 + 75
    )
    .await;
}

/// Upstream: `TestFilterPushDown` (compound AND predicate over a partitioned
/// and a non-partitioned column).
async fn step_filter_compound(stack: &SparkStack) {
    assert_stdout_contains(
        stack,
        &format!(
            "SELECT COUNT(*) FROM demo.{TEST_NS}.sales \
             WHERE region = 'us-east' AND category = 'book'"
        ),
        "2",
    )
    .await;
}

/// Upstream: `TestSelect` `testExpressionPushdown` (subset). LIKE / STARTS WITH
/// predicates return matching rows.
async fn step_filter_starts_with(stack: &SparkStack) {
    assert_stdout_contains(
        stack,
        &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.sales WHERE region LIKE 'us-%'"),
        "6",
    )
    .await;
}

/// Upstream: `TestAggregatePushDown` (subset). GROUP BY a partition column
/// with SUM() returns one row per group.
async fn step_aggregate_sum_group_by(stack: &SparkStack) {
    let res = spark_sql(
        stack,
        &format!(
            "SELECT region, SUM(amount) FROM demo.{TEST_NS}.sales \
             GROUP BY region ORDER BY region"
        ),
    )
    .await;
    assert!(res.is_success(), "{}", res.dump());
    for (region, sum) in [
        ("eu-central", "83"), // 8 + 75
        ("us-east", "135"),   // 10 + 25 + 100
        ("us-west", "265"),   // 15 + 200 + 50
    ] {
        assert!(
            res.stdout.contains(region) && res.stdout.contains(sum),
            "expected `{region}` and SUM=`{sum}` in output; stdout=\n{}",
            res.stdout
        );
    }
}

/// Upstream: `TestAggregatePushDown` (distinct variant). COUNT(DISTINCT col)
/// returns the cardinality of a column.
async fn step_aggregate_count_distinct(stack: &SparkStack) {
    assert_stdout_contains(
        stack,
        &format!("SELECT COUNT(DISTINCT region) FROM demo.{TEST_NS}.sales"),
        "3",
    )
    .await;

    assert_stdout_contains(
        stack,
        &format!("SELECT COUNT(DISTINCT category) FROM demo.{TEST_NS}.sales"),
        "2",
    )
    .await;
}

/// Upstream: `TestAggregatePushDown` (MIN/MAX variant). MIN and MAX over a
/// non-partition column return the expected scalar values.
async fn step_aggregate_min_max(stack: &SparkStack) {
    let res = spark_sql(
        stack,
        &format!("SELECT MIN(amount), MAX(amount) FROM demo.{TEST_NS}.sales"),
    )
    .await;
    assert!(res.is_success(), "{}", res.dump());
    assert!(
        res.stdout.contains("8") && res.stdout.contains("200"),
        "expected MIN=8 and MAX=200; stdout=\n{}",
        res.stdout
    );
}
