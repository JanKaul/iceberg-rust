//! Additional upstream Spark stored-procedure ports. Each step exercises a
//! `CALL system.<proc>(...)` and asserts observable post-state via the
//! REST catalog or follow-up SELECTs.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::table_metadata::TableMetadata;

use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_proc_extra";

fn ident(name: &str) -> Identifier {
    Identifier::try_new(&[TEST_NS.to_string(), name.to_string()], None).unwrap()
}

async fn load_md(stack: &SparkStack, name: &str) -> TableMetadata {
    let tabular = stack
        .catalog
        .clone()
        .load_tabular(&ident(name))
        .await
        .unwrap_or_else(|e| panic!("load_tabular({name}): {e:?}"));
    match tabular {
        Tabular::Table(t) => t.metadata().clone(),
        _ => panic!("expected `{name}` to be a Table"),
    }
}

#[tokio::test]
async fn integration_spark_procedures_extra_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_expire_snapshots(&stack).await;
    step_rollback_to_timestamp(&stack).await;
    step_set_write_order(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `TestExpireSnapshotsProcedure`. After producing 3 snapshots and
/// calling `expire_snapshots` with a future timestamp, only the latest
/// snapshot remains.
async fn step_expire_snapshots(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_exp (id BIGINT) USING ICEBERG"),
    )
    .await;
    for v in 1..=3 {
        spark_sql_ok(
            stack,
            &format!("INSERT INTO demo.{TEST_NS}.t_exp VALUES ({v})"),
        )
        .await;
    }
    let before = load_md(stack, "t_exp").await;
    assert!(
        before.snapshots.len() >= 3,
        "expected >=3 snapshots before expire; got {}",
        before.snapshots.len()
    );

    // Use a far-future timestamp so every non-current snapshot becomes
    // eligible for expiration. `older_than` is expressed as a TIMESTAMP.
    let far_future = "2099-12-31 00:00:00";
    spark_sql_ok(
        stack,
        &format!(
            "CALL demo.system.expire_snapshots(table => '{TEST_NS}.t_exp', \
             older_than => TIMESTAMP '{far_future}', retain_last => 1)"
        ),
    )
    .await;

    let after = load_md(stack, "t_exp").await;
    assert_eq!(
        after.snapshots.len(),
        1,
        "expected exactly 1 snapshot after expire_snapshots(retain_last=1); got {}",
        after.snapshots.len()
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_exp")).await;
}

/// Upstream: `TestRollbackToTimestampProcedure`. `rollback_to_timestamp`
/// rewinds the current snapshot to the latest one at or before the given
/// timestamp. We sample the table's first snapshot timestamp + 1ms.
async fn step_rollback_to_timestamp(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_rbt (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rbt VALUES (1)"),
    )
    .await;

    let md_after_first = load_md(stack, "t_rbt").await;
    let first_snapshot = md_after_first.snapshots.values().next().unwrap();
    let cutoff_ms = first_snapshot.timestamp_ms() + 1; // 1ms after the first INSERT

    // Sleep briefly so subsequent INSERTs land strictly after `cutoff_ms`.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rbt VALUES (2)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rbt VALUES (3)"),
    )
    .await;

    // The cutoff falls between first and second INSERT, so rollback returns
    // to the first snapshot.
    let cutoff_secs = cutoff_ms / 1000;
    let cutoff_nanos = (cutoff_ms % 1000) * 1_000_000;
    let cutoff_ts = chrono::DateTime::from_timestamp(cutoff_secs, cutoff_nanos as u32)
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string();

    spark_sql_ok(
        stack,
        &format!(
            "CALL demo.system.rollback_to_timestamp(table => '{TEST_NS}.t_rbt', \
             timestamp => TIMESTAMP '{cutoff_ts}')"
        ),
    )
    .await;

    let count = spark_sql(stack, &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_rbt")).await;
    assert!(count.is_success(), "{}", count.dump());
    assert!(
        count.stdout.contains("1"),
        "expected 1 row after rollback_to_timestamp; stdout=\n{}",
        count.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_rbt")).await;
}

/// Upstream: `TestSetWriteDistributionAndOrdering` (subset). The
/// `ALTER TABLE … WRITE ORDERED BY col` extension records a sort order on
/// the table metadata.
async fn step_set_write_order(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_ord (id BIGINT, region STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_ord WRITE ORDERED BY region"),
    )
    .await;

    let md = load_md(stack, "t_ord").await;
    // After WRITE ORDERED BY, the table should have a non-default sort order
    // (i.e. at least one sort field).
    let sort_orders = &md.sort_orders;
    let default_sort_order = sort_orders
        .get(&md.default_sort_order_id)
        .expect("default sort order present");
    assert!(
        !default_sort_order.fields.is_empty(),
        "expected at least one sort field; got default order = {:?}",
        default_sort_order
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_ord")).await;
}
