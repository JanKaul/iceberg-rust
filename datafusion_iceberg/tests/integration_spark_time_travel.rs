//! Ports time-travel and snapshot-navigation scenarios from upstream
//! `TestSelect` (testVersionAsOf, testTimestampAsOf, testBranchReference)
//! and the cherrypick/rollback procedures' read paths to the Spark
//! integration fixture.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::table_metadata::TableMetadata;

use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_tt";

fn ident(name: &str) -> Identifier {
    Identifier::try_new(&[TEST_NS.to_string(), name.to_string()], None).unwrap()
}

async fn load_md(stack: &SparkStack, name: &str) -> TableMetadata {
    let tabular = stack
        .catalog
        .clone()
        .load_tabular(&ident(name))
        .await
        .expect("load_tabular");
    match tabular {
        Tabular::Table(t) => t.metadata().clone(),
        _ => panic!("expected `{name}` to be a Table"),
    }
}

#[tokio::test]
async fn integration_spark_time_travel_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_version_as_of(&stack).await;
    step_branch_read(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `testVersionAsOf` (subset). After two INSERTs there are two
/// snapshots; SELECT … VERSION AS OF <snapshot_id> returns the rows as of
/// the named snapshot.
async fn step_version_as_of(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_version (id BIGINT, label STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_version VALUES (1, 'first')"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_version VALUES (2, 'second')"),
    )
    .await;

    // Read the snapshot ids from the loaded metadata; we need the older
    // snapshot (the one after the first INSERT) to assert time-travel.
    let md = load_md(stack, "t_version").await;
    let mut snapshots: Vec<(i64, i64)> = md
        .snapshots
        .iter()
        .map(|(id, snap)| (*snap.timestamp_ms(), *id))
        .collect();
    snapshots.sort();
    assert_eq!(snapshots.len(), 2, "expected 2 snapshots after 2 INSERTs");
    let first_snapshot_id = snapshots[0].1;

    // Sanity: current SELECT sees both rows.
    let now = spark_sql(
        stack,
        &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_version"),
    )
    .await;
    assert!(now.is_success(), "{}", now.dump());
    assert!(
        now.stdout.contains("2"),
        "expected 2 rows currently; stdout=\n{}",
        now.stdout
    );

    // VERSION AS OF the first snapshot sees only the first row.
    let past = spark_sql(
        stack,
        &format!("SELECT label FROM demo.{TEST_NS}.t_version VERSION AS OF {first_snapshot_id}"),
    )
    .await;
    assert!(past.is_success(), "{}", past.dump());
    assert!(
        past.stdout.contains("first") && !past.stdout.contains("second"),
        "VERSION AS OF should see only 'first'; stdout=\n{}",
        past.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_version")).await;
}

/// Upstream: `testBranchReference` (subset). After creating a branch with
/// `ALTER TABLE … CREATE BRANCH`, SELECT … `VERSION AS OF '<branch>'` reads
/// from the branch's snapshot.
async fn step_branch_read(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_branch (id BIGINT, label STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_branch VALUES (1, 'main-row')"),
    )
    .await;

    // Snapshot the current state under a named branch.
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_branch CREATE BRANCH archive_v1"),
    )
    .await;

    // Continue writing on main.
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_branch VALUES (2, 'newer-row')"),
    )
    .await;

    // Read the branch — should still see only the first row.
    let branch = spark_sql(
        stack,
        &format!("SELECT label FROM demo.{TEST_NS}.t_branch VERSION AS OF 'archive_v1'"),
    )
    .await;
    assert!(branch.is_success(), "{}", branch.dump());
    assert!(
        branch.stdout.contains("main-row") && !branch.stdout.contains("newer-row"),
        "branch read should see only 'main-row'; stdout=\n{}",
        branch.stdout
    );

    // Main should see both rows.
    let main = spark_sql(
        stack,
        &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_branch"),
    )
    .await;
    assert!(main.is_success(), "{}", main.dump());
    assert!(
        main.stdout.contains("2"),
        "main read should see 2 rows; stdout=\n{}",
        main.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_branch")).await;
}
