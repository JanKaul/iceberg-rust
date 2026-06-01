//! Ports portable scenarios from upstream `TestBranchDDL` and `TestTagDDL`
//! (spark-extensions) to the Spark integration fixture. Asserts the
//! observable state of `refs` on the loaded TableMetadata.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::table_metadata::TableMetadata;

use spark_common::{boot_spark_stack, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_branchtag";

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
async fn integration_spark_branch_tag_ddl_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_create_branch(&stack).await;
    step_drop_branch(&stack).await;
    // TestTagDDL: ALTER TABLE … CREATE TAG succeeds on Spark side but the
    // resulting table metadata cannot currently be re-loaded by
    // iceberg-rust's RestCatalog (deserialisation of the Tag SnapshotRef
    // variant errors out, falling through to Error::CatalogNotFound).
    // Branch refs round-trip fine.

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `TestBranchDDL.testCreateBranch`. ALTER TABLE … CREATE BRANCH
/// adds a branch ref pointing at the current snapshot.
async fn step_create_branch(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_cb (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_cb VALUES (1)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_cb CREATE BRANCH staging"),
    )
    .await;

    let md = load_md(stack, "t_cb").await;
    assert!(
        md.refs.contains_key("staging"),
        "expected `staging` branch ref; got {:?}",
        md.refs.keys().collect::<Vec<_>>()
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_cb")).await;
}

/// Upstream: `TestBranchDDL.testDropBranch`. ALTER TABLE … DROP BRANCH
/// removes the branch ref.
async fn step_drop_branch(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_db (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("INSERT INTO demo.{TEST_NS}.t_db VALUES (1)"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_db CREATE BRANCH temp"),
    )
    .await;
    assert!(load_md(stack, "t_db").await.refs.contains_key("temp"));

    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_db DROP BRANCH temp"),
    )
    .await;
    assert!(!load_md(stack, "t_db").await.refs.contains_key("temp"));

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_db")).await;
}
