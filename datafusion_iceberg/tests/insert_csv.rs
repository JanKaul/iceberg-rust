use std::sync::Arc;

use datafusion::{
    arrow::array::{Float64Array, Int64Array},
    common::tree_node::{TransformedResult, TreeNode},
    execution::{context::SessionContext, SessionStateBuilder},
};
use datafusion_expr::ScalarUDF;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalogList;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_iceberg::{
    catalog::catalog_list::IcebergCatalogList,
    planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView},
};

#[tokio::test]
async fn test_insert_csv() {
    let temp_dir = TempDir::new().unwrap();
    let warehouse_path = format!("{}/warehouse", temp_dir.path().to_str().unwrap());
    let object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));
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

    let sql = &format!("CREATE EXTERNAL TABLE warehouse.tpch.lineitem ( 
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
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION '{}/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );", warehouse_path);

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

    let batches = ctx
        .sql("select sum(L_QUANTITY), L_PARTKEY from warehouse.tpch.lineitem group by L_PARTKEY;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    let mut once = false;

    for batch in batches {
        if batch.num_rows() != 0 {
            let (amounts, product_ids) = (
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap(),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            );
            for (product_id, amount) in product_ids.iter().zip(amounts) {
                if product_id.unwrap() == 24027 {
                    assert_eq!(amount.unwrap(), 24.0)
                } else if product_id.unwrap() == 63700 {
                    assert_eq!(amount.unwrap(), 23.0)
                }
            }
            once = true
        }
    }

    assert!(once);
}
