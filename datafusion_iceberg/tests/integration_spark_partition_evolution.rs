//! Ports portable scenarios from upstream
//! `TestAlterTablePartitionFields` (spark-extensions) to the Spark
//! integration fixture. Schema-evolution flows are issued via
//! `ALTER TABLE … {ADD,DROP,REPLACE} PARTITION FIELD …` and the resulting
//! partition spec is inspected via `RestCatalog.load_tabular`.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::partition::Transform;
use iceberg_rust::spec::table_metadata::TableMetadata;

use spark_common::{boot_spark_stack, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_partevo";

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
async fn integration_spark_partition_evolution_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_add_identity_partition(&stack).await;
    step_add_bucket_partition(&stack).await;
    step_add_truncate_partition(&stack).await;
    step_add_day_partition(&stack).await;
    step_drop_identity_partition(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `testAddIdentityPartition`. ALTER TABLE … ADD PARTITION FIELD col
/// adds an Identity partition.
async fn step_add_identity_partition(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_id (id BIGINT, region STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_id ADD PARTITION FIELD region"),
    )
    .await;

    let md = load_md(stack, "t_id").await;
    let spec = md.default_partition_spec().unwrap();
    assert_eq!(spec.fields().len(), 1);
    assert!(
        matches!(spec.fields()[0].transform(), Transform::Identity),
        "expected Identity, got {:?}",
        spec.fields()[0].transform()
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_id")).await;
}

/// Upstream: `testAddBucketPartition`. ALTER TABLE … ADD PARTITION FIELD
/// bucket(N, col).
async fn step_add_bucket_partition(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_bk (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_bk ADD PARTITION FIELD bucket(8, id)"),
    )
    .await;

    let md = load_md(stack, "t_bk").await;
    let spec = md.default_partition_spec().unwrap();
    assert!(
        matches!(spec.fields()[0].transform(), Transform::Bucket(8)),
        "expected Bucket(8), got {:?}",
        spec.fields()[0].transform()
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_bk")).await;
}

/// Upstream: `testAddTruncatePartition`. ALTER TABLE … ADD PARTITION FIELD
/// truncate(N, col).
async fn step_add_truncate_partition(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_tr (id BIGINT, data STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_tr ADD PARTITION FIELD truncate(4, data)"),
    )
    .await;

    let md = load_md(stack, "t_tr").await;
    let spec = md.default_partition_spec().unwrap();
    assert!(
        matches!(spec.fields()[0].transform(), Transform::Truncate(4)),
        "expected Truncate(4), got {:?}",
        spec.fields()[0].transform()
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_tr")).await;
}

/// Upstream: `testAddDaysPartition`. ALTER TABLE … ADD PARTITION FIELD
/// days(col).
async fn step_add_day_partition(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_dy (id BIGINT, ts TIMESTAMP) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_dy ADD PARTITION FIELD days(ts)"),
    )
    .await;

    let md = load_md(stack, "t_dy").await;
    let spec = md.default_partition_spec().unwrap();
    assert!(
        matches!(spec.fields()[0].transform(), Transform::Day),
        "expected Day, got {:?}",
        spec.fields()[0].transform()
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_dy")).await;
}

/// Upstream: `testDropIdentityPartition`. After ADD then DROP PARTITION FIELD,
/// the partition is no longer active in the *default* spec. Note: Iceberg
/// keeps every historical spec in `partition_specs`, so the default spec id
/// advances and the most-recent spec has 0 active partition fields.
async fn step_drop_identity_partition(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_drop_id (id BIGINT, region STRING) USING ICEBERG \
             PARTITIONED BY (region)"
        ),
    )
    .await;
    let before = load_md(stack, "t_drop_id").await;
    assert_eq!(before.default_partition_spec().unwrap().fields().len(), 1);

    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_drop_id DROP PARTITION FIELD region"),
    )
    .await;

    let after = load_md(stack, "t_drop_id").await;
    let default_spec = after.default_partition_spec().unwrap();
    // After a drop the V2 spec keeps the field as Void, so we check that no
    // active (non-Void) field remains.
    let active: Vec<_> = default_spec
        .fields()
        .iter()
        .filter(|f| !matches!(f.transform(), Transform::Void))
        .collect();
    assert!(
        active.is_empty(),
        "expected no active partition fields after drop, got {:?}",
        active
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_drop_id")).await;
}
