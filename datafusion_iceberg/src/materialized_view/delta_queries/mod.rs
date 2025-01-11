pub mod aggregate_functions;
pub mod delta_node;
pub mod transform;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use datafusion::{
        arrow::array::{Float64Array, StringArray},
        common::tree_node::{TransformedResult, TreeNode},
        execution::SessionStateBuilder,
        prelude::SessionContext,
    };
    use datafusion_expr::ScalarUDF;
    use iceberg_rust::object_store::{Bucket, ObjectStoreBuilder};

    use iceberg_sql_catalog::SqlCatalogList;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;
    use tokio::time::sleep;
    use url::Url;

    use crate::{
        catalog::catalog_list::IcebergCatalogList,
        planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView},
    };

    #[tokio::test]
    async fn test_materialized_view_incremental_join() {
        let temp_dir = TempDir::new().unwrap();

        let object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));
        let iceberg_catalog_list = Arc::new(
            SqlCatalogList::new("sqlite://", object_store.clone())
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

        let state = SessionStateBuilder::default()
            .with_default_features()
            .with_catalog_list(catalog_list)
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .with_object_store(
                &Url::try_from("file://").unwrap(),
                object_store.build(Bucket::Local).unwrap(),
            )
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
    L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../datafusion_iceberg/testdata/tpch/lineitem.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

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
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION '{}/warehouse/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );", temp_dir.path().to_str().unwrap());

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "insert into warehouse.tpch.lineitem select * from lineitem1;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "CREATE EXTERNAL TABLE orders ( 
    O_ORDERKEY BIGINT NOT NULL, 
    O_CUSTKEY BIGINT NOT NULL, 
    O_ORDERSTATUS CHAR NOT NULL, 
    O_TOTALPRICE DOUBLE NOT NULL, 
    O_ORDERDATE DATE NOT NULL, 
    O_ORDERPRIORITY VARCHAR NOT NULL, 
    O_CLERK VARCHAR NOT NULL, 
    O_SHIPPRIORITY INTEGER NOT NULL, 
    O_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../datafusion_iceberg/testdata/tpch/orders.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = &format!("CREATE EXTERNAL TABLE warehouse.tpch.orders ( 
    O_ORDERKEY BIGINT NOT NULL, 
    O_CUSTKEY BIGINT NOT NULL, 
    O_ORDERSTATUS CHAR NOT NULL, 
    O_TOTALPRICE DOUBLE NOT NULL, 
    O_ORDERDATE DATE NOT NULL, 
    O_ORDERPRIORITY VARCHAR NOT NULL, 
    O_CLERK VARCHAR NOT NULL, 
    O_SHIPPRIORITY INTEGER NOT NULL, 
    O_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION '{}/warehouse/tpch/orders' PARTITIONED BY ( \"month(O_ORDERDATE)\" );", temp_dir.path().to_str().unwrap());

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "insert into warehouse.tpch.orders select * from orders;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let plan = ctx
            .state()
            .create_logical_plan(
                "CREATE TEMPORARY VIEW warehouse.tpch.lineitem_orders AS SELECT 
    O.O_ORDERKEY,
    O.O_CUSTKEY,
    O.O_ORDERSTATUS,
    O.O_TOTALPRICE,
    O.O_ORDERDATE,
    L.L_PARTKEY,
    L.L_QUANTITY,
    L.L_EXTENDED_PRICE,
    L.L_DISCOUNT,
    L.L_TAX,
    L.L_SHIPDATE
FROM warehouse.tpch.orders O
JOIN warehouse.tpch.lineitem L
ON O.O_ORDERKEY = L.L_ORDERKEY;
",
            )
            .await
            .expect("Failed to create plan for select");

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        ctx.sql("select refresh_materialized_view('warehouse.tpch.lineitem_orders');")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        sleep(Duration::from_millis(1_000)).await;

        let batches = ctx
        .sql("select sum(L_QUANTITY), O_ORDERSTATUS from warehouse.tpch.lineitem_orders group by O_ORDERSTATUS;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 4296.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 61350.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);

        let batches = ctx
        .sql("select sum(L.L_QUANTITY), O.O_ORDERSTATUS from lineitem1 L join orders O ON O.O_ORDERKEY = L.L_ORDERKEY group by O.O_ORDERSTATUS;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 4296.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 61350.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);

        let sql = "CREATE EXTERNAL TABLE lineitem2 (
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
        L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../datafusion_iceberg/testdata/tpch/lineitem_2.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "insert into warehouse.tpch.lineitem select * from lineitem2;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        ctx.sql("select refresh_materialized_view('warehouse.tpch.lineitem_orders');")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        sleep(Duration::from_millis(1_000)).await;

        let batches = ctx
        .sql("select sum(L_QUANTITY), O_ORDERSTATUS from warehouse.tpch.lineitem_orders group by O_ORDERSTATUS;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 7871.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 126359.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);

        let batches = ctx
        .sql("select sum(L.L_QUANTITY), O.O_ORDERSTATUS from lineitem L join orders O ON O.O_ORDERKEY = L.L_ORDERKEY group by O.O_ORDERSTATUS;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 7871.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 126359.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);
    }

    #[tokio::test]
    async fn test_materialized_view_incremental_aggregate() {
        let temp_dir = TempDir::new().unwrap();

        let object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));
        let iceberg_catalog_list = Arc::new(
            SqlCatalogList::new("sqlite://", object_store.clone())
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

        let state = SessionStateBuilder::default()
            .with_default_features()
            .with_catalog_list(catalog_list)
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .with_object_store(
                &Url::try_from("file://").unwrap(),
                object_store.build(Bucket::Local).unwrap(),
            )
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
    L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../datafusion_iceberg/testdata/tpch/lineitem.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

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
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION '{}/warehouse/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );", temp_dir.path().to_str().unwrap());

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "insert into warehouse.tpch.lineitem select * from lineitem1;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "CREATE EXTERNAL TABLE orders ( 
    O_ORDERKEY BIGINT NOT NULL, 
    O_CUSTKEY BIGINT NOT NULL, 
    O_ORDERSTATUS CHAR NOT NULL, 
    O_TOTALPRICE DOUBLE NOT NULL, 
    O_ORDERDATE DATE NOT NULL, 
    O_ORDERPRIORITY VARCHAR NOT NULL, 
    O_CLERK VARCHAR NOT NULL, 
    O_SHIPPRIORITY INTEGER NOT NULL, 
    O_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../datafusion_iceberg/testdata/tpch/orders.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = &format!("CREATE EXTERNAL TABLE warehouse.tpch.orders ( 
    O_ORDERKEY BIGINT NOT NULL, 
    O_CUSTKEY BIGINT NOT NULL, 
    O_ORDERSTATUS CHAR NOT NULL, 
    O_TOTALPRICE DOUBLE NOT NULL, 
    O_ORDERDATE DATE NOT NULL, 
    O_ORDERPRIORITY VARCHAR NOT NULL, 
    O_CLERK VARCHAR NOT NULL, 
    O_SHIPPRIORITY INTEGER NOT NULL, 
    O_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION '{}/warehouse/tpch/orders' PARTITIONED BY ( \"month(O_ORDERDATE)\" );", temp_dir.path().to_str().unwrap());

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "insert into warehouse.tpch.orders select * from orders;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let plan = ctx
            .state()
            .create_logical_plan(
                "CREATE TEMPORARY VIEW warehouse.tpch.lineitem_orders AS select sum(L.L_QUANTITY) AS total, O.O_ORDERSTATUS from warehouse.tpch.lineitem L join warehouse.tpch.orders O ON O.O_ORDERKEY = L.L_ORDERKEY group by O.O_ORDERSTATUS;;
",
            )
            .await
            .expect("Failed to create plan for select");

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        ctx.sql("select refresh_materialized_view('warehouse.tpch.lineitem_orders');")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        sleep(Duration::from_millis(1_000)).await;

        let batches = ctx
            .sql("select * from warehouse.tpch.lineitem_orders;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 4296.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 61350.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);

        let batches = ctx
        .sql("select sum(L.L_QUANTITY), O.O_ORDERSTATUS from lineitem1 L join orders O ON O.O_ORDERKEY = L.L_ORDERKEY group by O.O_ORDERSTATUS;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 4296.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 61350.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);

        let sql = "CREATE EXTERNAL TABLE lineitem2 (
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
        L_COMMENT VARCHAR NOT NULL ) STORED AS CSV LOCATION '../datafusion_iceberg/testdata/tpch/lineitem_2.csv' OPTIONS ('has_header' 'false');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let sql = "insert into warehouse.tpch.lineitem select * from lineitem2;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        ctx.sql("select refresh_materialized_view('warehouse.tpch.lineitem_orders');")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        sleep(Duration::from_millis(1_000)).await;

        let batches = ctx
            .sql("select * from warehouse.tpch.lineitem_orders;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 7871.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 126359.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);

        let batches = ctx
        .sql("select sum(L.L_QUANTITY), O.O_ORDERSTATUS from lineitem L join orders O ON O.O_ORDERKEY = L.L_ORDERKEY group by O.O_ORDERSTATUS;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (amounts, customer_id) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                );
                for (customer_id, amount) in customer_id.iter().zip(amounts) {
                    if customer_id.unwrap() == "P" {
                        assert_eq!(amount.unwrap(), 7871.0);
                        once = true
                    } else if customer_id.unwrap() == "O" {
                        assert_eq!(amount.unwrap(), 126359.0);
                        once = true
                    }
                }
            }
        }

        assert!(once);
    }
}
