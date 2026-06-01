//! End-to-end Spark integration smoke test (mirrors `integration_trino.rs`).
//!
//! Drives a Spark container against a REST catalog + LocalStack S3, runs the
//! setup script under `tests/spark/spark.sql`, then exercises the three cross-
//! engine paths the Trino test covers: REST catalog visibility, DataFusion
//! reads of a Spark-written table, and Spark reads of a Rust-written table.

#[path = "spark_common/mod.rs"]
mod spark_common;

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::execution::context::SessionContext;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::partition::{PartitionField, PartitionSpec, Transform};
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{PrimitiveType, StructField, Type};
use iceberg_rust::table::Table;
use testcontainers::core::{wait::LogWaitStrategy, AccessMode, ExecCommand, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::localstack::LocalStack;
use tokio::time::sleep;

use spark_common::{spark_sql, spark_sql_file};

#[tokio::test]
async fn integration_spark_rest() {
    // We want the setup SQL file mounted into the container, so we re-do the
    // stack setup inline (the shared fixture is mount-free).
    let container_host = if iceberg_rust::test_utils::is_podman() {
        "host.containers.internal"
    } else {
        "172.17.0.1"
    };

    let localstack = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .start()
        .await
        .unwrap();

    let command = localstack
        .exec(ExecCommand::new(vec![
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "warehouse",
        ]))
        .await
        .unwrap();
    while command.exit_code().await.unwrap().is_none() {
        sleep(Duration::from_millis(100)).await;
    }

    let localstack_host = localstack.get_host().await.unwrap();
    let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

    let object_store = iceberg_rust::object_store::ObjectStoreBuilder::s3()
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

    iceberg_rust::test_utils::wait_for_s3_bucket(&object_store, "s3://warehouse", None).await;

    let rest = GenericImage::new("apache/iceberg-rest-fixture", "latest")
        .with_wait_for(WaitFor::Log(LogWaitStrategy::stderr(
            "INFO org.eclipse.jetty.server.Server - Started ",
        )))
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("CATALOG_WAREHOUSE", "s3://warehouse/")
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var(
            "CATALOG_S3_ENDPOINT",
            format!("http://{}:{}", container_host, localstack_port),
        )
        .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .start()
        .await
        .unwrap();

    let rest_host = rest.get_host().await.unwrap();
    let rest_port = rest.get_host_port_ipv4(8181).await.unwrap();

    let cwd = std::env::current_dir()
        .unwrap()
        .into_os_string()
        .into_string()
        .unwrap();

    let sql_mount = Mount::bind_mount(cwd.clone() + "/tests/spark/spark.sql", "/tmp/spark.sql")
        .with_access_mode(AccessMode::ReadOnly);

    let spark = GenericImage::new("tabulario/spark-iceberg", "latest")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(2),
        })
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_mount(sql_mount)
        // tabulario/spark-iceberg's entrypoint runs `eval "$1"` after starting
        // the Spark daemons, so the keep-alive command must arrive as a single arg.
        .with_cmd(vec!["sleep infinity"])
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await
        .unwrap();

    let rest_endpoint_for_spark = format!("http://{container_host}:{rest_port}");
    let s3_endpoint_for_spark = format!("http://{container_host}:{localstack_port}");
    let conf_args = vec![
        "--master".to_string(),
        "local[*]".to_string(),
        "--conf".to_string(),
        format!("spark.sql.catalog.demo.uri={rest_endpoint_for_spark}"),
        "--conf".to_string(),
        "spark.sql.catalog.demo.warehouse=s3://warehouse/".to_string(),
        "--conf".to_string(),
        format!("spark.sql.catalog.demo.s3.endpoint={s3_endpoint_for_spark}"),
        "--conf".to_string(),
        "spark.sql.catalog.demo.s3.path-style-access=true".to_string(),
        "--conf".to_string(),
        "spark.sql.catalog.demo.s3.access-key-id=user".to_string(),
        "--conf".to_string(),
        "spark.sql.catalog.demo.s3.secret-access-key=password".to_string(),
    ];

    let catalog = Arc::new(iceberg_rest_catalog::catalog::RestCatalog::new(
        None,
        iceberg_rest_catalog::apis::configuration::Configuration {
            base_path: format!("http://{}:{}", rest_host, rest_port),
            user_agent: None,
            client: reqwest::Client::new(),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: None,
            api_key: None,
            aws_v4_key: None,
        },
        Some(object_store),
        false,
    ));

    let stack = spark_common::SparkStack {
        localstack,
        rest,
        spark,
        catalog: catalog.clone(),
        conf_args,
    };

    // Run the setup SQL via spark-sql.
    let setup = spark_sql_file(&stack, "/tmp/spark.sql").await;
    assert!(
        setup.is_success(),
        "spark-sql setup failed.\n{}",
        setup.dump()
    );

    // Verify Rust can see the tables Spark wrote.
    let tables = catalog
        .list_tabulars(&Namespace::try_new(&["test".to_owned()]).unwrap())
        .await
        .unwrap();
    assert_eq!(tables.len(), 3, "expected 3 tables created by Spark");

    // DataFusion reads a Spark-written table.
    let ctx = SessionContext::new();
    ctx.register_catalog(
        "iceberg",
        Arc::new(IcebergCatalog::new(catalog.clone(), None).await.unwrap()),
    );

    let df = ctx
        .sql("SELECT SUM(totalprice) FROM iceberg.test.orders;")
        .await
        .unwrap();
    let results: Vec<RecordBatch> = df.collect().await.expect("Failed to execute query plan.");
    let batch = results
        .into_iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("All record batches are empty");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Failed to get values from batch.");
    assert!(
        (values.value(0) - 802.0).abs() < 0.1,
        "expected SUM(totalprice)=802.0, got {}",
        values.value(0)
    );

    // Now have Rust create a table, write rows via DataFusion, and have Spark
    // read it back.
    let schema = Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "customer_id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 3,
            name: "product_id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 4,
            name: "date".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Date),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 5,
            name: "amount".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Int),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .build()
        .unwrap();

    let partition_spec = PartitionSpec::builder()
        .with_partition_field(PartitionField::new(4, 1000, "day", Transform::Day))
        .build()
        .expect("Failed to create partition spec");

    Table::builder()
        .with_name("test_orders")
        .with_location("s3://warehouse/test_orders")
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("Failed to create table");

    let _ = ctx
        .sql(
            "INSERT INTO iceberg.test.test_orders (id, customer_id, product_id, date, amount) VALUES
                (1, 1, 1, '2020-01-01', 1),
                (2, 2, 1, '2020-01-01', 1),
                (3, 3, 1, '2020-01-01', 3),
                (4, 1, 2, '2020-02-02', 1),
                (5, 1, 1, '2020-02-02', 2),
                (6, 3, 3, '2020-02-02', 3);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await;

    // Have Spark read the table Rust wrote and assert SUM(amount)=11.
    let read = spark_sql(&stack, "SELECT SUM(amount) FROM demo.test.test_orders;").await;
    assert!(read.is_success(), "spark-sql read failed.\n{}", read.dump());
    assert!(
        read.stdout.contains("11"),
        "expected Spark SUM(amount)=11 in stdout, got:\n{}",
        read.dump()
    );
}
