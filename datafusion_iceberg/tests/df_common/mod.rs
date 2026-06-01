//! Shared fixture for DataFusion-driven integration tests.
//!
//! Each test file pulls this in via
//! `#[path = "df_common/mod.rs"] mod df_common;`. The fixture spins up an
//! in-memory `SqlCatalogList` over an in-memory object store, registers it
//! with a DataFusion `SessionContext`, and exposes a small helper for
//! executing Iceberg-aware SQL (`execute_sql`), which routes the parsed
//! logical plan through `iceberg_transform` before execution.

#![allow(dead_code)]

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::execution::context::SessionContext;
use datafusion::execution::SessionStateBuilder;
use datafusion_expr::ScalarUDF;
use datafusion_iceberg::catalog::catalog_list::IcebergCatalogList;
use datafusion_iceberg::planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView};
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalogList;

/// Bring up an Iceberg-aware DataFusion `SessionContext` backed by an
/// in-memory SQL catalog and an in-memory object store. The default catalog
/// is named `warehouse`.
pub async fn boot_df_stack() -> SessionContext {
    let object_store = ObjectStoreBuilder::memory();
    let iceberg_catalog_list = Arc::new(
        SqlCatalogList::new("sqlite://", object_store)
            .await
            .expect("SqlCatalogList"),
    );
    let catalog_list = Arc::new(
        IcebergCatalogList::new(iceberg_catalog_list.clone())
            .await
            .expect("IcebergCatalogList"),
    );

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_catalog_list(catalog_list)
        .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(ScalarUDF::from(RefreshMaterializedView::new(
        iceberg_catalog_list,
    )));
    ctx
}

/// Execute a SQL string through the Iceberg-aware planner. Panics on any
/// planning / transformation / execution / collection error so the caller
/// can use this as the equivalent of `unwrap()` on every step.
pub async fn execute_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .unwrap_or_else(|e| panic!("create_logical_plan failed for `{sql}`: {e:?}"));
    let transformed = plan
        .transform(iceberg_transform)
        .data()
        .unwrap_or_else(|e| panic!("iceberg_transform failed for `{sql}`: {e:?}"));
    ctx.execute_logical_plan(transformed)
        .await
        .unwrap_or_else(|e| panic!("execute_logical_plan failed for `{sql}`: {e:?}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("collect failed for `{sql}`: {e:?}"))
}

/// Execute a SQL string and return only the integer in row 0 col 0 of the
/// first non-empty batch. Useful for `SELECT COUNT(*)` / `SELECT SUM(x)`
/// patterns.
pub async fn execute_scalar_i64(ctx: &SessionContext, sql: &str) -> i64 {
    use datafusion::arrow::array::Int64Array;
    let batches = execute_sql(ctx, sql).await;
    let batch = batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows from `{sql}`"));
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("expected Int64 in column 0 of `{sql}`"));
    array.value(0)
}

/// Execute a SQL string and return the f64 in row 0 col 0 of the first
/// non-empty batch.
pub async fn execute_scalar_f64(ctx: &SessionContext, sql: &str) -> f64 {
    use datafusion::arrow::array::Float64Array;
    let batches = execute_sql(ctx, sql).await;
    let batch = batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows from `{sql}`"));
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("expected Float64 in column 0 of `{sql}`"));
    array.value(0)
}

/// Total row count across all returned batches.
pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}
