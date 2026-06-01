//! Ports the portable subset of upstream `TestCreateTableAsSelect` to the
//! Spark integration fixture. We test CREATE TABLE … AS SELECT (CTAS) and
//! REPLACE TABLE … AS SELECT (RTAS) round-trips by issuing the SQL via
//! spark-sql, then inspecting the resulting tables through the REST
//! catalog and re-reading them back through Spark.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::partition::Transform;
use iceberg_rust::spec::table_metadata::TableMetadata;

use spark_common::{boot_spark_stack, spark_sql, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_ctas";

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
async fn integration_spark_ctas_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    // Seed table for CTAS sources.
    spark_sql_ok(
        &stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.source (id BIGINT, region STRING, amount INT) \
             USING ICEBERG"
        ),
    )
    .await;
    spark_sql_ok(
        &stack,
        &format!(
            "INSERT INTO demo.{TEST_NS}.source VALUES \
              (1, 'us-east', 10), \
              (2, 'us-east', 20), \
              (3, 'us-west', 30), \
              (4, 'eu-central', 40)"
        ),
    )
    .await;

    step_unpartitioned_ctas(&stack).await;
    step_partitioned_ctas(&stack).await;
    step_replace_table_as_select(&stack).await;

    spark_sql_ok(&stack, &format!("DROP TABLE demo.{TEST_NS}.source")).await;
    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `testUnpartitionedCTAS`. CREATE TABLE … AS SELECT … materialises
/// the source rows into a new unpartitioned table.
async fn step_unpartitioned_ctas(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_ctas_unp USING ICEBERG AS \
             SELECT id, amount FROM demo.{TEST_NS}.source"
        ),
    )
    .await;

    let md = load_md(stack, "t_ctas_unp").await;
    let names: Vec<&str> = md
        .current_schema(None)
        .unwrap()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, vec!["id", "amount"]);
    assert!(
        md.default_partition_spec().unwrap().fields().is_empty(),
        "CTAS target should be unpartitioned"
    );

    let count = spark_sql(
        stack,
        &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_ctas_unp"),
    )
    .await;
    assert!(count.is_success(), "{}", count.dump());
    assert!(
        count.stdout.contains("4"),
        "expected COUNT(*)=4; stdout=\n{}",
        count.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_ctas_unp")).await;
}

/// Upstream: `testPartitionedCTAS`. CREATE TABLE … PARTITIONED BY (col) AS
/// SELECT … materialises into a partitioned target.
async fn step_partitioned_ctas(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_ctas_par USING ICEBERG \
             PARTITIONED BY (region) AS \
             SELECT id, region, amount FROM demo.{TEST_NS}.source"
        ),
    )
    .await;

    let md = load_md(stack, "t_ctas_par").await;
    let spec = md.default_partition_spec().unwrap();
    assert_eq!(spec.fields().len(), 1, "expected 1 partition field");
    assert!(
        matches!(spec.fields()[0].transform(), Transform::Identity),
        "expected Identity on region, got {:?}",
        spec.fields()[0].transform()
    );

    let sum_eu = spark_sql(
        stack,
        &format!("SELECT SUM(amount) FROM demo.{TEST_NS}.t_ctas_par WHERE region = 'eu-central'"),
    )
    .await;
    assert!(sum_eu.is_success(), "{}", sum_eu.dump());
    assert!(
        sum_eu.stdout.contains("40"),
        "expected SUM(amount)=40 for eu-central; stdout=\n{}",
        sum_eu.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_ctas_par")).await;
}

/// Upstream: `testRTAS`. REPLACE TABLE … AS SELECT replaces the table
/// contents while keeping the catalog identifier.
async fn step_replace_table_as_select(stack: &SparkStack) {
    // Seed an initial target.
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_rtas USING ICEBERG AS \
             SELECT id, amount FROM demo.{TEST_NS}.source"
        ),
    )
    .await;
    let initial_count = spark_sql(
        stack,
        &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_rtas"),
    )
    .await;
    assert!(initial_count.stdout.contains("4"));

    // Replace with a 1-row filtered projection.
    spark_sql_ok(
        stack,
        &format!(
            "REPLACE TABLE demo.{TEST_NS}.t_rtas USING ICEBERG AS \
             SELECT id, amount FROM demo.{TEST_NS}.source WHERE region = 'eu-central'"
        ),
    )
    .await;

    let after = spark_sql(
        stack,
        &format!("SELECT COUNT(*) FROM demo.{TEST_NS}.t_rtas"),
    )
    .await;
    assert!(after.is_success(), "{}", after.dump());
    assert!(
        after.stdout.contains("1"),
        "expected COUNT(*)=1 after REPLACE TABLE AS SELECT; stdout=\n{}",
        after.stdout
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_rtas")).await;
}
