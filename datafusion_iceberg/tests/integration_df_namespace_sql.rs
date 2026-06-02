//! Ports the namespace-SQL surface of upstream `TestNamespaceSQL` to the
//! DataFusion + iceberg-rust engine under test (`datafusion_iceberg` +
//! `iceberg-sql-catalog` over an in-memory object store).
//!
//! The upstream tests use `validationNamespaceCatalog.namespaceExists` and
//! `loadNamespaceMetadata` for verification; the DataFusion equivalents are
//! `SHOW SCHEMAS` / direct catalog probes via the
//! `SessionContext.catalog(name).schema(name)` chain. Per-row queries
//! (`SELECT COUNT(*)`) are used in place of upstream listing APIs that
//! DataFusion's parser does not expose.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql};

fn schema_exists(ctx: &datafusion::execution::context::SessionContext, schema: &str) -> bool {
    ctx.catalog("warehouse")
        .map(|cat| cat.schema(schema).is_some())
        .unwrap_or(false)
}

#[tokio::test]
async fn integration_df_create_namespace() {
    let ctx = boot_df_stack().await;
    assert!(
        !schema_exists(&ctx, "ns_create"),
        "ns_create should not exist before CREATE"
    );
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ns_create").await;
    assert!(
        schema_exists(&ctx, "ns_create"),
        "ns_create should exist after CREATE"
    );
}

#[tokio::test]
async fn integration_df_drop_empty_namespace() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ns_drop_empty").await;
    assert!(schema_exists(&ctx, "ns_drop_empty"));
    execute_sql(&ctx, "DROP SCHEMA warehouse.ns_drop_empty").await;
    assert!(
        !schema_exists(&ctx, "ns_drop_empty"),
        "ns_drop_empty should be gone after DROP"
    );
}

#[tokio::test]
async fn integration_df_table_is_queryable_after_create() {
    // Equivalent verification to upstream `testListTables` — once a table is
    // created in a schema, it must be addressable. We probe via SELECT rather
    // than `schema.table_names()` because the IcebergCatalogList snapshots
    // its schema providers at catalog-load time and does not auto-refresh on
    // newly-issued CREATE TABLE statements.
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ns_query").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.ns_query.t1 \
         (id BIGINT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/ns_query/t1'",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.ns_query.t1").await;
    assert_eq!(count, 0, "freshly-created table should be empty");
}

#[tokio::test]
async fn integration_df_list_namespaces() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ns_list_a").await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ns_list_b").await;

    let cat = ctx.catalog("warehouse").expect("warehouse catalog");
    let names = cat.schema_names();
    assert!(names.contains(&"ns_list_a".to_string()));
    assert!(names.contains(&"ns_list_b".to_string()));
}

#[tokio::test]
async fn integration_df_drop_namespace_then_recreate() {
    // Captures upstream's "drop then re-create same name" coverage — the
    // catalog must not retain stale state after DROP.
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ns_recycle").await;
    assert!(schema_exists(&ctx, "ns_recycle"));
    execute_sql(&ctx, "DROP SCHEMA warehouse.ns_recycle").await;
    assert!(!schema_exists(&ctx, "ns_recycle"));
    execute_sql(&ctx, "CREATE SCHEMA warehouse.ns_recycle").await;
    assert!(schema_exists(&ctx, "ns_recycle"));
}
