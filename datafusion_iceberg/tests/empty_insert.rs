use std::sync::Arc;

use datafusion::{
    common::tree_node::{TransformedResult, TreeNode},
    execution::SessionStateBuilder,
    prelude::SessionContext,
};
use datafusion_expr::ScalarUDF;
use datafusion_iceberg::{
    catalog::catalog_list::IcebergCatalogList,
    planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView},
};
use futures::TryStreamExt;
use iceberg_rust::object_store::{Bucket, ObjectStoreBuilder};
use iceberg_sql_catalog::SqlCatalogList;
use object_store::ObjectMeta;
use testcontainers::{core::ExecCommand, runners::AsyncRunner, ImageExt};
use testcontainers_modules::localstack::LocalStack;

#[tokio::test]
pub async fn test_empty_insert() {
    let localstack = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .start()
        .await
        .unwrap();

    let mut command = localstack
        .exec(ExecCommand::new(vec![
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "warehouse",
        ]))
        .await
        .unwrap();

    command.stdout_to_vec().await.unwrap();

    let localstack_host = localstack.get_host().await.unwrap();
    let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

    let object_store = ObjectStoreBuilder::s3()
        .with_config("aws_access_key_id", "user")
        .unwrap()
        .with_config("aws_secret_access_key", "password")
        .unwrap()
        .with_config(
            "endpoint",
            format!("http://{localstack_host}:{localstack_port}"),
        )
        .unwrap()
        .with_config("region", "us-east-1")
        .unwrap()
        .with_config("allow_http", "true")
        .unwrap();

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

    let sql = &"CREATE SCHEMA warehouse.tpch;".to_string();

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
    L_COMMENT VARCHAR NOT NULL ) STORED AS ICEBERG LOCATION 's3://warehouse/tpch/lineitem' PARTITIONED BY ( \"month(L_SHIPDATE)\" );";

    let plan = ctx.state().create_logical_plan(sql).await.unwrap();

    let transformed = plan.transform(iceberg_transform).data().unwrap();

    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to execute query plan.");

    let sql = "insert into warehouse.tpch.lineitem select * from lineitem where l_quantity > 9999;";

    let plan = ctx.state().create_logical_plan(sql).await.unwrap();

    let transformed = plan.transform(iceberg_transform).data().unwrap();

    ctx.execute_logical_plan(transformed)
        .await
        .unwrap()
        .collect()
        .await
        .expect("Failed to execute query plan.");

    let object_store = object_store.build(Bucket::S3("warehouse")).unwrap();
    let datafiles: Vec<ObjectMeta> = object_store
        .list(Some(&"s3://warehouse/tpch/lineitem/data".into()))
        .try_collect()
        .await
        .unwrap();

    assert!(datafiles.is_empty());
}
