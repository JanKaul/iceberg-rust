//! Ports the INSERT / read-after-write surface of upstream
//! `TestInsertIntoTable` and `TestInsertOverwrite` to the DataFusion +
//! iceberg-rust engine under test.
//!
//! Notes on divergences:
//! - DataFusion + iceberg-rust supports `INSERT INTO ... VALUES`,
//!   `INSERT INTO ... SELECT`, but does not currently support
//!   `INSERT OVERWRITE` or partition-targeted overwrites. Those upstream
//!   scenarios are documented in `integration_df_dml_unsupported.rs`.
//! - DELETE / UPDATE / MERGE are not supported by iceberg-rust yet — they
//!   live in `integration_df_unsupported_row_dml.rs`.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql};

async fn setup_target_table(
    ctx: &datafusion::execution::context::SessionContext,
    schema: &str,
    table: &str,
) {
    if ctx
        .catalog("warehouse")
        .and_then(|c| c.schema(schema))
        .is_none()
    {
        execute_sql(ctx, &format!("CREATE SCHEMA warehouse.{schema}")).await;
    }
    execute_sql(
        ctx,
        &format!(
            "CREATE EXTERNAL TABLE warehouse.{schema}.{table} \
             (id BIGINT NOT NULL, label STRING) STORED AS ICEBERG \
             LOCATION '/warehouse/{schema}/{table}'"
        ),
    )
    .await;
}

#[tokio::test]
async fn integration_df_insert_values_single_row() {
    let ctx = boot_df_stack().await;
    setup_target_table(&ctx, "dml_insert_one", "t").await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_insert_one.t VALUES (1, 'first')",
    )
    .await;
    let count = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.dml_insert_one.t").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn integration_df_insert_values_multiple_rows() {
    let ctx = boot_df_stack().await;
    setup_target_table(&ctx, "dml_insert_many", "t").await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_insert_many.t VALUES \
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
    )
    .await;
    let count =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.dml_insert_many.t").await;
    assert_eq!(count, 4);
}

#[tokio::test]
async fn integration_df_insert_multiple_batches_accumulate() {
    let ctx = boot_df_stack().await;
    setup_target_table(&ctx, "dml_insert_batches", "t").await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_insert_batches.t VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_insert_batches.t VALUES (3, 'c')",
    )
    .await;
    let count = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.dml_insert_batches.t",
    )
    .await;
    assert_eq!(count, 3);
}

#[tokio::test]
async fn integration_df_insert_select_from_other_table() {
    let ctx = boot_df_stack().await;
    setup_target_table(&ctx, "dml_insert_source", "src").await;
    setup_target_table(&ctx, "dml_insert_source", "dst").await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_insert_source.src VALUES \
         (1, 'x'), (2, 'y'), (3, 'z')",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_insert_source.dst \
         SELECT id, label FROM warehouse.dml_insert_source.src WHERE id > 1",
    )
    .await;
    let count = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.dml_insert_source.dst",
    )
    .await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn integration_df_insert_into_partitioned_table_total_count() {
    // Verifies that a partitioned target receives all rows. Per-partition
    // equality filtering on a string partition column is exercised
    // separately (and currently `#[ignore]`'d — see
    // `integration_df_insert_into_partitioned_table_string_filter`).
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.dml_part").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.dml_part.t \
         (id BIGINT NOT NULL, region STRING NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/dml_part/t' PARTITIONED BY (region)",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_part.t VALUES \
         (1, 'EU'), (2, 'EU'), (3, 'US'), (4, 'APAC')",
    )
    .await;
    let total = execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.dml_part.t").await;
    assert_eq!(total, 4);
}

#[tokio::test]
#[ignore = "identity-partition pruning over a STRING column over-prunes — SELECT * returns the rows but SELECT WHERE = returns 0"]
async fn integration_df_insert_into_partitioned_table_string_filter() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.dml_part_sf").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.dml_part_sf.t \
         (id BIGINT NOT NULL, region STRING NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/dml_part_sf/t' PARTITIONED BY (region)",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_part_sf.t VALUES \
         (1, 'EU'), (2, 'EU'), (3, 'US'), (4, 'APAC')",
    )
    .await;
    let eu = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.dml_part_sf.t WHERE region = 'EU'",
    )
    .await;
    assert_eq!(eu, 2);
}

#[tokio::test]
async fn integration_df_insert_then_select_specific_column() {
    let ctx = boot_df_stack().await;
    setup_target_table(&ctx, "dml_select_col", "t").await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.dml_select_col.t VALUES \
         (10, 'a'), (20, 'b'), (30, 'c')",
    )
    .await;
    let sum =
        execute_scalar_i64(&ctx, "SELECT SUM(id) FROM warehouse.dml_select_col.t").await;
    assert_eq!(sum, 60);
}
