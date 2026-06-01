//! Ports a portable subset of upstream `TestDropTable`, `TestSelect`,
//! `TestRefreshTable`, and the unpartitioned-writes path of
//! `TestPartitionedWrites` / `TestUnpartitionedWrites` to the Spark
//! integration fixture.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::catalog::Catalog;

use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_dml";

fn ns() -> Namespace {
    Namespace::try_new(&[TEST_NS.to_owned()]).unwrap()
}

async fn table_exists(stack: &SparkStack, name: &str) -> bool {
    let listed = stack.catalog.list_tabulars(&ns()).await.unwrap_or_default();
    listed.iter().any(|i| i.name() == name)
}

#[tokio::test]
async fn integration_spark_dml_suite() {
    let stack = boot_spark_stack().await;

    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_drop_table(&stack).await;
    step_drop_table_if_exists(&stack).await;
    step_purge_table(&stack).await;
    step_select_basic(&stack).await;
    step_select_projection(&stack).await;
    step_insert_into_unpartitioned(&stack).await;
    step_insert_into_partitioned(&stack).await;
    step_refresh_table(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `testDropTable`. CREATE TABLE then DROP TABLE removes it from
/// the catalog listing.
async fn step_drop_table(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_drop (id BIGINT) USING ICEBERG"),
    )
    .await;
    assert!(table_exists(stack, "t_drop").await);
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_drop")).await;
    assert!(!table_exists(stack, "t_drop").await);
}

/// Variant of `testDropTable` covering DROP TABLE IF EXISTS on a missing
/// table (no error) and a present one (drops it).
async fn step_drop_table_if_exists(stack: &SparkStack) {
    // Missing → no-op.
    spark_sql_ok(
        stack,
        &format!("DROP TABLE IF EXISTS demo.{TEST_NS}.t_drop_ifexists"),
    )
    .await;

    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_drop_ifexists (id BIGINT) USING ICEBERG"),
    )
    .await;
    assert!(table_exists(stack, "t_drop_ifexists").await);

    spark_sql_ok(
        stack,
        &format!("DROP TABLE IF EXISTS demo.{TEST_NS}.t_drop_ifexists"),
    )
    .await;
    assert!(!table_exists(stack, "t_drop_ifexists").await);
}

/// Upstream: `testPurgeTable`. DROP TABLE PURGE removes the table from the
/// catalog (the data-file cleanup itself is FileIO-dependent; here we just
/// pin that the SQL is accepted and the table disappears).
async fn step_purge_table(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_purge (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_purge VALUES (1), (2), (3)"),
    )
    .await;
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_purge PURGE")).await;
    assert!(!table_exists(stack, "t_purge").await);
}

/// Upstream: `testSelect`. INSERT then `SELECT *` returns the rows in any
/// order. We assert via stdout containing each value once.
async fn step_select_basic(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_sel (id BIGINT, data STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_sel VALUES (1, 'one'), (2, 'two'), (3, 'three')"),
    )
    .await;

    let res = spark_sql(stack, &format!("SELECT * FROM demo.{TEST_NS}.t_sel")).await;
    assert!(res.is_success(), "{}", res.dump());
    for v in ["one", "two", "three"] {
        assert!(
            res.stdout.contains(v),
            "SELECT output missing `{v}`; stdout=\n{}",
            res.stdout
        );
    }

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_sel")).await;
}

/// Upstream: `testProjection`. Select a single column and verify only that
/// column appears in the row output.
async fn step_select_projection(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_proj (id BIGINT, data STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_proj VALUES (10, 'alpha'), (20, 'beta')"),
    )
    .await;

    let res = spark_sql(stack, &format!("SELECT data FROM demo.{TEST_NS}.t_proj")).await;
    assert!(res.is_success(), "{}", res.dump());
    for v in ["alpha", "beta"] {
        assert!(
            res.stdout.contains(v),
            "expected `{v}` in projected output; stdout=\n{}",
            res.stdout
        );
    }
    // ids should not appear when projecting only `data`.
    // (spark-sql prints raw row values separated by tabs; the column is the
    // string, no numeric noise expected.)
    assert!(
        !res.stdout.contains("\t"),
        "single-column projection should not emit tab-separated rows; stdout=\n{}",
        res.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_proj")).await;
}

/// Upstream: `TestUnpartitionedWrites` (subset). INSERT into an unpartitioned
/// table accumulates rows and SUM aggregates correctly.
async fn step_insert_into_unpartitioned(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_unp (id BIGINT, amount INT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_unp VALUES (1, 10), (2, 20), (3, 30)"),
    )
    .await;
    // Append again to verify the snapshot chain.
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_unp VALUES (4, 40)"),
    )
    .await;

    let res = spark_sql(
        stack,
        &format!("SELECT SUM(amount) FROM demo.{TEST_NS}.t_unp"),
    )
    .await;
    assert!(res.is_success(), "{}", res.dump());
    assert!(
        res.stdout.contains("100"),
        "expected SUM(amount)=100; stdout=\n{}",
        res.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_unp")).await;
}

/// Upstream: `TestPartitionedWrites` (subset). INSERT into a table partitioned
/// by `bucket(4, id)` lands rows and a follow-up COUNT(*) reads them back.
async fn step_insert_into_partitioned(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_par (id BIGINT, amount INT) USING ICEBERG \
             PARTITIONED BY (bucket(4, id))"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!(
            "INSERT INTO demo.{TEST_NS}.t_par VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"
        ),
    )
    .await;

    let res = spark_sql(stack, &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_par")).await;
    assert!(res.is_success(), "{}", res.dump());
    assert!(
        res.stdout.contains("5"),
        "expected COUNT(*)=5; stdout=\n{}",
        res.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_par")).await;
}

/// Upstream: `testRefreshTable`. `REFRESH TABLE` is a no-op for Iceberg
/// tables (the catalog is the source of truth, no Spark-side cache to
/// invalidate); we just pin that the SQL is accepted.
async fn step_refresh_table(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_ref (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(stack, &format!("REFRESH TABLE demo.{TEST_NS}.t_ref")).await;
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_ref")).await;
}
