use std::sync::Arc;

use datafusion::{
    common::{
        stats::Precision,
        tree_node::{TransformedResult, TreeNode},
        ScalarValue,
    },
    execution::{context::SessionContext, SessionStateBuilder},
};
use datafusion_expr::ScalarUDF;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalogList;

use datafusion_iceberg::{
    catalog::catalog_list::IcebergCatalogList,
    planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView},
};

#[tokio::test]
async fn test_table_statistics() {
    let object_store = ObjectStoreBuilder::memory();
    let iceberg_catalog_list = Arc::new(
        SqlCatalogList::new("sqlite://", object_store)
            .await
            .unwrap(),
    );

    let catalog_list = {
        Arc::new(
            IcebergCatalogList::new(iceberg_catalog_list.clone())
                .await
                .unwrap(),
        )
    };

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_catalog_list(catalog_list)
        .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
        .build();

    let ctx = SessionContext::new_with_state(state);

    ctx.register_udf(ScalarUDF::from(RefreshMaterializedView::new(
        iceberg_catalog_list,
    )));

    let sql = "CREATE EXTERNAL TABLE lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION 'testdata/tpch/lineitem.csv' OPTIONS ('has_header' 'false');";

    let plan = ctx.state().create_logical_plan(sql).await.unwrap();

    let transformed = plan.transform(iceberg_transform).data().unwrap();

    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to execute query plan.");

    let sql = "CREATE SCHEMA warehouse.tpch;";

    let plan = ctx.state().create_logical_plan(sql).await.unwrap();

    let transformed = plan.transform(iceberg_transform).data().unwrap();

    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to execute query plan.");

    let sql = "CREATE EXTERNAL TABLE warehouse.tpch.lineitem ( 
    L_ORDERKEY BIGINT NOT NULL, 
    L_PARTKEY BIGINT NOT NULL, 
    L_SUPPKEY BIGINT NOT NULL, 
    L_LINENUMBER INT NOT NULL, 
    L_QUANTITY DOUBLE NOT NULL, 
    L_EXTENDED_PRICE DOUBLE NOT NULL, 
    L_DISCOUNT DOUBLE NOT NULL, 
    L_TAX DOUBLE NOT NULL, 
    L_RETURNFLAG CHAR NOT NULL, 
    L_LINESTATUS CHAR NOT NULL, 
    L_SHIPDATE DATE NOT NULL, 
    L_COMMITDATE DATE NOT NULL, 
    L_RECEIPTDATE DATE NOT NULL, 
    L_SHIPINSTRUCT VARCHAR NOT NULL, 
    L_SHIPMODE VARCHAR NOT NULL, 
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION '/warehouse/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );";

    let plan = ctx.state().create_logical_plan(sql).await.unwrap();

    let transformed = plan.transform(iceberg_transform).data().unwrap();

    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to execute query plan.");

    let sql = "insert into warehouse.tpch.lineitem select * from lineitem;";

    let plan = ctx.state().create_logical_plan(sql).await.unwrap();

    let transformed = plan.transform(iceberg_transform).data().unwrap();

    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to execute query plan.");

    let sql = ctx
        .sql("select sum(L_QUANTITY), L_PARTKEY from warehouse.tpch.lineitem group by L_PARTKEY;")
        .await
        .expect("Failed to create plan for select");

    let physical_plan = sql.create_physical_plan().await.unwrap();

    let stats = physical_plan.partition_statistics(None).unwrap();

    // Validate table-level statistics
    assert_eq!(
        stats.num_rows,
        Precision::Inexact(47048),
        "num_rows should match the total rows in the CSV file"
    );
    assert!(
        matches!(stats.total_byte_size, Precision::Inexact(size) if size > 0),
        "total_byte_size should be Inexact and greater than 0"
    );

    // Validate column count (sum(L_QUANTITY) and L_PARTKEY)
    assert_eq!(
        stats.column_statistics.len(),
        2,
        "Should have statistics for 2 columns"
    );

    // Validate first column (sum(L_QUANTITY)) - aggregate column should have Absent statistics
    assert!(
        matches!(stats.column_statistics[0].min_value, Precision::Absent),
        "Aggregate column min_value should be Absent"
    );
    assert!(
        matches!(stats.column_statistics[0].max_value, Precision::Absent),
        "Aggregate column max_value should be Absent"
    );
    assert!(
        matches!(stats.column_statistics[0].null_count, Precision::Absent),
        "Aggregate column null_count should be Absent"
    );

    // Validate second column (L_PARTKEY) - should have min/max bounds from Iceberg metadata
    assert_eq!(
        stats.column_statistics[1].min_value,
        Precision::Exact(ScalarValue::Int64(Some(2))),
        "L_PARTKEY min_value should be 2"
    );
    assert_eq!(
        stats.column_statistics[1].max_value,
        Precision::Exact(ScalarValue::Int64(Some(200000))),
        "L_PARTKEY max_value should be 200000"
    );
}
