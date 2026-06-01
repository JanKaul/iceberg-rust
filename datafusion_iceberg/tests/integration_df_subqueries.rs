//! Ports upstream subquery / CTE / DISTINCT / set-op coverage to the
//! DataFusion + iceberg-rust engine under test.

#[path = "df_common/mod.rs"]
mod df_common;

use df_common::{boot_df_stack, execute_scalar_i64, execute_sql, total_rows};

async fn seed_orders_customers(ctx: &datafusion::execution::context::SessionContext) {
    execute_sql(ctx, "CREATE SCHEMA warehouse.sub").await;
    execute_sql(
        ctx,
        "CREATE EXTERNAL TABLE warehouse.sub.customers \
         (cid BIGINT NOT NULL, country STRING NOT NULL) STORED AS ICEBERG \
         LOCATION '/warehouse/sub/customers'",
    )
    .await;
    execute_sql(
        ctx,
        "CREATE EXTERNAL TABLE warehouse.sub.orders \
         (oid BIGINT NOT NULL, cid BIGINT NOT NULL, amount BIGINT NOT NULL) \
         STORED AS ICEBERG LOCATION '/warehouse/sub/orders'",
    )
    .await;
    execute_sql(
        ctx,
        "INSERT INTO warehouse.sub.customers VALUES \
         (1, 'DE'), (2, 'US'), (3, 'DE'), (4, 'JP')",
    )
    .await;
    execute_sql(
        ctx,
        "INSERT INTO warehouse.sub.orders VALUES \
         (101, 1, 100), (102, 1, 50), (103, 2, 200), \
         (104, 3, 75), (105, 4, 25), (106, 4, 175)",
    )
    .await;
}

#[tokio::test]
async fn integration_df_scalar_subquery_in_where() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.sub.orders \
         WHERE amount > (SELECT AVG(amount) FROM warehouse.sub.orders)",
    )
    .await;
    // avg = (100+50+200+75+25+175)/6 = 104.16, rows with amount > 104.16 are
    // 200 and 175.
    assert_eq!(n, 2);
}

#[tokio::test]
async fn integration_df_in_subquery() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.sub.orders \
         WHERE cid IN (SELECT cid FROM warehouse.sub.customers WHERE country = 'DE')",
    )
    .await;
    // DE customers are 1 and 3, orders for them: 101, 102, 104 → 3 rows.
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_not_in_subquery() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.sub.orders \
         WHERE cid NOT IN (SELECT cid FROM warehouse.sub.customers WHERE country = 'DE')",
    )
    .await;
    // Non-DE customers are 2 and 4, orders 103, 105, 106 → 3 rows.
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_exists_correlated_subquery() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(*) FROM warehouse.sub.customers c \
         WHERE EXISTS (SELECT 1 FROM warehouse.sub.orders o WHERE o.cid = c.cid)",
    )
    .await;
    // All 4 customers have orders.
    assert_eq!(n, 4);
}

#[tokio::test]
async fn integration_df_cte_simple() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "WITH big AS (SELECT * FROM warehouse.sub.orders WHERE amount >= 100) \
         SELECT COUNT(*) FROM big",
    )
    .await;
    // amounts >= 100: 100, 200, 175 → 3 rows.
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_cte_with_join() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let batches = execute_sql(
        &ctx,
        "WITH totals AS (\
            SELECT cid, SUM(amount) AS s FROM warehouse.sub.orders GROUP BY cid\
         ) \
         SELECT c.country, t.s FROM warehouse.sub.customers c \
         JOIN totals t ON c.cid = t.cid \
         ORDER BY c.cid",
    )
    .await;
    assert_eq!(total_rows(&batches), 4, "one row per customer");
}

#[tokio::test]
async fn integration_df_select_distinct() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let batches = execute_sql(
        &ctx,
        "SELECT DISTINCT country FROM warehouse.sub.customers ORDER BY country",
    )
    .await;
    assert_eq!(total_rows(&batches), 3, "three distinct countries");
}

#[tokio::test]
async fn integration_df_count_distinct() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let n = execute_scalar_i64(
        &ctx,
        "SELECT COUNT(DISTINCT country) FROM warehouse.sub.customers",
    )
    .await;
    assert_eq!(n, 3);
}

#[tokio::test]
async fn integration_df_union_all() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let batches = execute_sql(
        &ctx,
        "SELECT cid FROM warehouse.sub.customers \
         UNION ALL \
         SELECT cid FROM warehouse.sub.orders",
    )
    .await;
    // 4 customers + 6 orders = 10 rows.
    assert_eq!(total_rows(&batches), 10);
}

#[tokio::test]
async fn integration_df_union_distinct() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let batches = execute_sql(
        &ctx,
        "SELECT cid FROM warehouse.sub.customers \
         UNION \
         SELECT cid FROM warehouse.sub.orders",
    )
    .await;
    // Distinct cids: 1,2,3,4 → 4 rows.
    assert_eq!(total_rows(&batches), 4);
}

#[tokio::test]
async fn integration_df_having_clause() {
    let ctx = boot_df_stack().await;
    seed_orders_customers(&ctx).await;
    let batches = execute_sql(
        &ctx,
        "SELECT cid, SUM(amount) AS s FROM warehouse.sub.orders \
         GROUP BY cid HAVING SUM(amount) > 150 ORDER BY cid",
    )
    .await;
    // cid 1 → 150 (excl), cid 2 → 200 (incl), cid 3 → 75 (excl), cid 4 → 200 (incl)
    assert_eq!(total_rows(&batches), 2);
}
