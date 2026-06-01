//! Ports portable scenarios from upstream stored-procedure test classes
//! (spark-extensions). Each step invokes a `CALL system.<proc>(...)` and
//! asserts observable post-state via the REST catalog or via follow-up
//! SELECTs.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::table_metadata::TableMetadata;

use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_proc";

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
async fn integration_spark_procedures_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_rewrite_data_files(&stack).await;
    step_rewrite_manifests(&stack).await;
    step_rollback_to_snapshot(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `TestRewriteDataFilesProcedure.testRewriteDataFilesAll`. After
/// three small INSERTs, `CALL system.rewrite_data_files('table')` produces
/// a new snapshot and the row count is preserved.
async fn step_rewrite_data_files(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_rdf (id BIGINT, val INT) USING ICEBERG"),
    )
    .await;
    for chunk in ["(1, 10), (2, 20)", "(3, 30), (4, 40)", "(5, 50), (6, 60)"] {
        spark_sql_ok(
            stack,
            &format!("INSERT INTO demo.{TEST_NS}.t_rdf VALUES {chunk}"),
        )
        .await;
    }

    let before_snapshots = load_md(stack, "t_rdf").await.snapshots.len();
    assert!(
        before_snapshots >= 3,
        "expected >=3 snapshots before rewrite; got {before_snapshots}"
    );

    spark_sql_ok(
        stack,
        &format!("CALL demo.system.rewrite_data_files('{TEST_NS}.t_rdf')"),
    )
    .await;

    let after = load_md(stack, "t_rdf").await;
    assert!(
        after.snapshots.len() > before_snapshots,
        "rewrite_data_files should add a snapshot; before={before_snapshots}, after={}",
        after.snapshots.len()
    );

    // Row count preserved.
    let count = spark_sql(stack, &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_rdf")).await;
    assert!(count.is_success(), "{}", count.dump());
    assert!(
        count.stdout.contains("6"),
        "expected 6 rows post-rewrite; stdout=\n{}",
        count.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_rdf")).await;
}

/// Upstream: `TestRewriteManifestsProcedure.testRewriteManifestsInEmptyTable`
/// (variant). `CALL system.rewrite_manifests('table')` succeeds and the
/// catalog still loads cleanly.
async fn step_rewrite_manifests(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_rm (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rm VALUES (1), (2), (3)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rm VALUES (4), (5)"),
    )
    .await;

    spark_sql_ok(
        stack,
        &format!("CALL demo.system.rewrite_manifests('{TEST_NS}.t_rm')"),
    )
    .await;

    let count = spark_sql(stack, &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_rm")).await;
    assert!(count.is_success(), "{}", count.dump());
    assert!(
        count.stdout.contains("5"),
        "expected 5 rows post rewrite_manifests; stdout=\n{}",
        count.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_rm")).await;
}

/// Upstream: `TestRollbackToSnapshotProcedure.testRollbackToSnapshotUsingPositionalArgs`.
/// After three INSERTs, rolling back to the first snapshot brings the table
/// back to a single row.
async fn step_rollback_to_snapshot(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_rb (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rb VALUES (1)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rb VALUES (2)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_rb VALUES (3)"),
    )
    .await;

    // Find the snapshot id from the first INSERT (the oldest one by
    // timestamp). `snapshots` is keyed by snapshot id.
    let md = load_md(stack, "t_rb").await;
    let mut snaps: Vec<(i64, i64)> = md
        .snapshots
        .iter()
        .map(|(id, s)| (*s.timestamp_ms(), *id))
        .collect();
    snaps.sort();
    let first_snapshot_id = snaps[0].1;

    spark_sql_ok(
        stack,
        &format!("CALL demo.system.rollback_to_snapshot('{TEST_NS}.t_rb', {first_snapshot_id})"),
    )
    .await;

    let count = spark_sql(stack, &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_rb")).await;
    assert!(count.is_success(), "{}", count.dump());
    assert!(
        count.stdout.contains("1"),
        "expected 1 row after rollback; stdout=\n{}",
        count.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_rb")).await;
}
