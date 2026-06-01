//! Ports the type-specific filter / aggregation scenarios from upstream
//! `TestSparkV2Filters`, `TestSparkFilters`, and the type-coverage portion of
//! upstream's parameterised DDL tests, to DataFusion + iceberg-rust.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_f64, execute_scalar_i64, execute_sql};

#[tokio::test]
async fn integration_df_date_equality_and_range() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tp_date").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tp_date.t \
         (id BIGINT NOT NULL, d DATE NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tp_date/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tp_date.t VALUES \
         (1, DATE '2024-01-01'), \
         (2, DATE '2024-06-15'), \
         (3, DATE '2024-12-31'), \
         (4, DATE '2025-04-15')",
    )
    .await;
    let eq = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tp_date.t WHERE d = DATE '2024-06-15'",
    )
    .await;
    assert_eq!(eq, 1);
    let range = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tp_date.t \
         WHERE d >= DATE '2024-06-01' AND d < DATE '2025-01-01'",
    )
    .await;
    assert_eq!(range, 2);
}

#[tokio::test]
async fn integration_df_timestamp_range_filter() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tp_ts").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tp_ts.t \
         (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tp_ts/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tp_ts.t VALUES \
         (1, TIMESTAMP '2024-01-15 09:00:00'), \
         (2, TIMESTAMP '2024-01-15 12:30:00'), \
         (3, TIMESTAMP '2024-01-15 18:45:30'), \
         (4, TIMESTAMP '2024-01-16 03:00:00')",
    )
    .await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tp_ts.t \
         WHERE ts >= TIMESTAMP '2024-01-15 10:00:00' AND \
               ts <  TIMESTAMP '2024-01-16 00:00:00'",
    )
    .await;
    assert_eq!(n, 2);
}

#[tokio::test]
async fn integration_df_decimal_aggregation() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tp_dec").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tp_dec.t \
         (id BIGINT NOT NULL, price DECIMAL(10, 2) NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tp_dec/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tp_dec.t VALUES \
         (1, CAST(12.50 AS DECIMAL(10, 2))), \
         (2, CAST(7.25 AS DECIMAL(10, 2))), \
         (3, CAST(100.00 AS DECIMAL(10, 2))), \
         (4, CAST(0.05 AS DECIMAL(10, 2)))",
    )
    .await;
    let sum_str = df_common::execute_sql(&ctx, "SELECT CAST(SUM(price) AS DOUBLE) FROM warehouse.tp_dec.t")
        .await;
    use datafusion::arrow::array::Float64Array;
    let val = sum_str[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!((val - 119.80).abs() < 1e-6, "sum was {val}");
}

#[tokio::test]
async fn integration_df_float_double_filter() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tp_real").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tp_real.t \
         (id BIGINT NOT NULL, ratio DOUBLE NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tp_real/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tp_real.t VALUES \
         (1, 0.1), (2, 0.5), (3, 0.9), (4, 1.5), (5, 3.14)",
    )
    .await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tp_real.t WHERE ratio < 1.0",
    )
    .await;
    assert_eq!(n, 3);
    let avg = execute_scalar_f64(&ctx, "SELECT AVG(ratio) FROM warehouse.tp_real.t").await;
    assert!((avg - 1.228).abs() < 1e-2, "avg was {avg}");
}

#[tokio::test]
async fn integration_df_boolean_filter() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tp_bool").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tp_bool.t \
         (id BIGINT NOT NULL, active BOOLEAN NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tp_bool/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tp_bool.t VALUES \
         (1, true), (2, false), (3, true), (4, true), (5, false)",
    )
    .await;
    let truthy =
        execute_scalar_i64(&ctx, "SELECT COUNT(*) FROM warehouse.tp_bool.t WHERE active").await;
    assert_eq!(truthy, 3);
    let falsy = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.tp_bool.t WHERE NOT active",
    )
    .await;
    assert_eq!(falsy, 2);
}

#[tokio::test]
async fn integration_df_int_overflow_safe_aggregate() {
    let ctx = boot_df_stack().await;
    execute_sql(&ctx, "CREATE SCHEMA warehouse.tp_int").await;
    execute_sql(
        &ctx,
        "CREATE EXTERNAL TABLE warehouse.tp_int.t \
         (id BIGINT NOT NULL, n INT NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/tp_int/t'",
    )
    .await;
    execute_sql(
        &ctx,
        "INSERT INTO warehouse.tp_int.t VALUES \
         (1, 2147483600), (2, 1000), (3, -500), (4, 99)",
    )
    .await;
    // SUM widens to BIGINT — no overflow.
    let s = execute_scalar_i64(&ctx, "SELECT SUM(n) FROM warehouse.tp_int.t").await;
    assert_eq!(s, 2_147_484_199);
}
