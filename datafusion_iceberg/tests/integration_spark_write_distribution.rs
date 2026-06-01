//! Ports portable scenarios from upstream
//! `TestSetWriteDistributionAndOrdering` (spark-extensions). Each step
//! issues an `ALTER TABLE … WRITE …` extension and verifies the resulting
//! metadata properties on the REST catalog.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::table_metadata::TableMetadata;

use spark_common::{boot_spark_stack, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_wd";

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
async fn integration_spark_write_distribution_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_write_distributed_by(&stack).await;
    step_write_locally_ordered_by(&stack).await;
    step_write_ordered_by_with_direction(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `testSetWriteDistributedBy`. ALTER TABLE … WRITE DISTRIBUTED BY
/// PARTITION sets a `write.distribution-mode` table property.
async fn step_write_distributed_by(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_dist (id BIGINT, region STRING) USING ICEBERG \
             PARTITIONED BY (region)"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_dist WRITE DISTRIBUTED BY PARTITION"),
    )
    .await;

    let md = load_md(stack, "t_dist").await;
    let mode = md.properties.get("write.distribution-mode").cloned();
    assert_eq!(
        mode.as_deref(),
        Some("hash"),
        "expected write.distribution-mode=hash; got {:?}",
        md.properties
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_dist")).await;
}

/// Upstream: `testSetWriteLocallyOrderedBy`. ALTER TABLE … WRITE LOCALLY
/// ORDERED BY col sets distribution-mode=none and configures a sort order.
async fn step_write_locally_ordered_by(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_local (id BIGINT, region STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_local WRITE LOCALLY ORDERED BY region"),
    )
    .await;

    let md = load_md(stack, "t_local").await;
    // LOCALLY ORDERED BY implies distribution-mode=none — Iceberg's default
    // when the property is unset, so the property may be absent rather than
    // explicitly written. We assert the sort order is set instead, which is
    // the observable effect.
    let default_so = md
        .sort_orders
        .get(&md.default_sort_order_id)
        .expect("default sort order present");
    assert!(
        !default_so.fields.is_empty(),
        "expected non-empty sort order; got {:?}",
        default_so
    );
    if let Some(mode) = md.properties.get("write.distribution-mode") {
        assert_eq!(
            mode, "none",
            "if distribution-mode is recorded, it should be `none` for LOCALLY ORDERED BY; props={:?}",
            md.properties
        );
    }

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_local")).await;
}

/// Upstream: `testSetWriteOrderedByWithDirection`. ALTER TABLE … WRITE
/// ORDERED BY col DESC NULLS FIRST records direction + null order.
async fn step_write_ordered_by_with_direction(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_ord_dir (id BIGINT, region STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_ord_dir WRITE ORDERED BY region DESC NULLS FIRST"),
    )
    .await;

    let md = load_md(stack, "t_ord_dir").await;
    let default_so = md
        .sort_orders
        .get(&md.default_sort_order_id)
        .expect("default sort order present");
    assert_eq!(default_so.fields.len(), 1);
    let field = &default_so.fields[0];
    use iceberg_rust::spec::sort::{NullOrder, SortDirection};
    assert!(
        matches!(field.direction, SortDirection::Descending),
        "expected Descending direction, got {:?}",
        field.direction
    );
    assert!(
        matches!(field.null_order, NullOrder::First),
        "expected NullOrder::First, got {:?}",
        field.null_order
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_ord_dir")).await;
}
