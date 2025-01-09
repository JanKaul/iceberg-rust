use datafusion::{
    arrow::array::{Float64Array, Int64Array},
    common::tree_node::{TransformedResult, TreeNode},
    execution::SessionStateBuilder,
    prelude::SessionContext,
};
use datafusion_iceberg::{
    catalog::catalog::IcebergCatalog,
    planner::{iceberg_transform, IcebergQueryPlanner},
};
use iceberg_rust::{
    catalog::{namespace::Namespace, Catalog},
    object_store::ObjectStoreBuilder,
    spec::util::strip_prefix,
};

use iceberg_sql_catalog::SqlCatalog;

use object_store::memory::InMemory;

use std::sync::Arc;

#[tokio::test]
async fn test_materialized_view_incremental_aggregate() {
    let object_store = ObjectStoreBuilder::Memory(Arc::new(InMemory::new()));

    let iceberg_catalog = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store)
            .await
            .unwrap(),
    );

    let catalog = Arc::new(
        IcebergCatalog::new(iceberg_catalog.clone(), None)
            .await
            .unwrap(),
    );

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(IcebergQueryPlanner {}))
        .build();

    let ctx = SessionContext::new_with_state(state);

    ctx.register_catalog("warehouse", catalog);

    let sql = "CREATE EXTERNAL TABLE lineitem1 ( 
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
    L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../datafusion_iceberg/testdata/tpch/lineitem_1.csv' OPTIONS ('has_header' 'false');";

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

    let tables = iceberg_catalog
        .clone()
        .list_tabulars(
            &Namespace::try_new(&["tpch".to_owned()]).expect("Failed to create namespace"),
        )
        .await
        .expect("Failed to list Tables");
    assert_eq!(tables[0].to_string(), "tpch.lineitem".to_owned());

    let sql = "insert into warehouse.tpch.lineitem select * from lineitem1;";

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
                    assert_eq!(amount.unwrap(), 8.0)
                }
            }
            once = true
        }
    }

    assert!(once);

    let object_store = iceberg_catalog.object_store(iceberg_rust::object_store::Bucket::Local);

    let version_hint = object_store
        .get(&strip_prefix("/warehouse/tpch/lineitem/metadata/version-hint.text").into())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert!(std::str::from_utf8(&version_hint)
        .unwrap()
        .ends_with(".metadata.json"));
}
