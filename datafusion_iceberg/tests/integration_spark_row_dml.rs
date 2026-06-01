//! Ports portable scenarios from upstream row-level DML test classes —
//! TestDelete, TestUpdate, TestMerge (spark-extensions) — to the Spark
//! integration fixture. Asserts observable post-state via SELECTs.

#[path = "spark_common/mod.rs"]
mod spark_common;

use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_rowdml";

#[tokio::test]
async fn integration_spark_row_dml_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_delete_by_predicate(&stack).await;
    step_delete_all(&stack).await;
    step_update_set_constant(&stack).await;
    step_update_with_predicate(&stack).await;
    step_merge_insert_only(&stack).await;
    step_merge_matched_update_and_not_matched_insert(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

async fn count(stack: &SparkStack, table: &str) -> String {
    let res = spark_sql(
        stack,
        &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.{table}"),
    )
    .await;
    assert!(res.is_success(), "{}", res.dump());
    res.stdout.clone()
}

/// Upstream: `TestDelete.testDeleteByPredicate`. DELETE FROM … WHERE drops
/// matching rows and a follow-up COUNT(*) is reduced.
async fn step_delete_by_predicate(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_del (id BIGINT, region STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!(
            "INSERT INTO demo.{TEST_NS}.t_del VALUES \
              (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'c')"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("DELETE FROM demo.{TEST_NS}.t_del WHERE region = 'a'"),
    )
    .await;
    assert!(count(stack, "t_del").await.contains("3"));
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_del")).await;
}

/// Upstream: `TestDelete.testDeleteAll`. DELETE FROM with no predicate clears
/// the table.
async fn step_delete_all(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_del_all (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_del_all VALUES (1), (2), (3)"),
    )
    .await;
    spark_sql_ok(stack, &format!("DELETE FROM demo.{TEST_NS}.t_del_all")).await;
    assert!(count(stack, "t_del_all").await.contains("0"));
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_del_all")).await;
}

/// Upstream: `TestUpdate.testUpdate`. UPDATE … SET col = const updates the
/// column for every row when there is no WHERE.
async fn step_update_set_constant(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_upd (id BIGINT, amount INT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_upd VALUES (1, 10), (2, 20), (3, 30)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("UPDATE demo.{TEST_NS}.t_upd SET amount = 100"),
    )
    .await;
    let res = spark_sql(
        stack,
        &format!("SELECT SUM(amount) FROM demo.{TEST_NS}.t_upd"),
    )
    .await;
    assert!(res.is_success(), "{}", res.dump());
    assert!(
        res.stdout.contains("300"),
        "expected SUM(amount)=300 after blanket UPDATE; stdout=\n{}",
        res.stdout
    );
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_upd")).await;
}

/// Upstream: `TestUpdate.testUpdateWithPredicate`. UPDATE … SET … WHERE only
/// modifies the matching rows.
async fn step_update_with_predicate(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_upd_pred \
             (id BIGINT, region STRING, amount INT) USING ICEBERG"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!(
            "INSERT INTO demo.{TEST_NS}.t_upd_pred VALUES \
              (1, 'a', 1), (2, 'a', 2), (3, 'b', 3), (4, 'b', 4)"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("UPDATE demo.{TEST_NS}.t_upd_pred SET amount = 99 WHERE region = 'a'"),
    )
    .await;
    let res = spark_sql(
        stack,
        &format!(
            "SELECT region, SUM(amount) FROM demo.{TEST_NS}.t_upd_pred \
             GROUP BY region ORDER BY region"
        ),
    )
    .await;
    assert!(res.is_success(), "{}", res.dump());
    // a -> 99+99=198, b -> 3+4=7
    assert!(
        res.stdout.contains("198") && res.stdout.contains("7"),
        "expected sums {{a:198, b:7}}; stdout=\n{}",
        res.stdout
    );
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_upd_pred")).await;
}

/// Upstream: `TestMerge.testMergeOnlyInsert`. MERGE INTO with only
/// `WHEN NOT MATCHED THEN INSERT` appends new rows from the source.
async fn step_merge_insert_only(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_merge_ins (id BIGINT, val INT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_merge_ins VALUES (1, 1), (2, 2)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_merge_ins_src (id BIGINT, val INT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_merge_ins_src VALUES (2, 20), (3, 30)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!(
            "MERGE INTO demo.{TEST_NS}.t_merge_ins t \
             USING demo.{TEST_NS}.t_merge_ins_src s \
             ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT *"
        ),
    )
    .await;
    assert!(count(stack, "t_merge_ins").await.contains("3"));
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_merge_ins")).await;
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_merge_ins_src")).await;
}

/// Upstream: `TestMerge.testMergeWithMatchedAndNotMatched`. MERGE INTO with
/// both branches updates existing rows and inserts new ones.
async fn step_merge_matched_update_and_not_matched_insert(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_mrg_full (id BIGINT, val INT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_mrg_full VALUES (1, 1), (2, 2), (3, 3)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_mrg_full_src (id BIGINT, val INT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_mrg_full_src VALUES (2, 200), (3, 300), (4, 400)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!(
            "MERGE INTO demo.{TEST_NS}.t_mrg_full t \
             USING demo.{TEST_NS}.t_mrg_full_src s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.val = s.val \
             WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)"
        ),
    )
    .await;
    let res = spark_sql(
        stack,
        &format!("SELECT SUM(val) FROM demo.{TEST_NS}.t_mrg_full"),
    )
    .await;
    assert!(res.is_success(), "{}", res.dump());
    // 1 (unmatched) + 200 (updated) + 300 (updated) + 400 (inserted) = 901
    assert!(
        res.stdout.contains("901"),
        "expected SUM(val)=901; stdout=\n{}",
        res.stdout
    );
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_mrg_full")).await;
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_mrg_full_src")).await;
}
