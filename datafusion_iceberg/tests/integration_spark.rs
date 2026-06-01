use core::str;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::execution::context::SessionContext;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rest_catalog::apis::configuration::Configuration;
use iceberg_rest_catalog::catalog::RestCatalog;
use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::partition::{PartitionField, PartitionSpec, Transform};
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{PrimitiveType, StructField, Type};
use iceberg_rust::table::Table;
use iceberg_rust::test_utils::is_podman;
use testcontainers::core::CmdWaitFor;
use testcontainers::{
    core::{wait::LogWaitStrategy, AccessMode, ExecCommand, Mount, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use testcontainers_modules::localstack::LocalStack;
use tokio::time::sleep;

fn configuration(host: &str, port: u16) -> Configuration {
    Configuration {
        base_path: format!("http://{host}:{port}"),
        user_agent: None,
        client: reqwest::Client::new(),
        basic_auth: None,
        oauth_access_token: None,
        bearer_access_token: None,
        api_key: None,
        aws_v4_key: None,
    }
}

/// Build the list of `--conf` arguments that retarget the tabulario image's
/// pre-registered `iceberg` catalog at the test's REST and S3 endpoints.
///
/// The image's stock `spark-defaults.conf` registers `spark.sql.catalog.demo`
/// pointing at `http://rest:8181`. We override the same catalog so the
/// default catalog (`demo`) is what our SQL targets via `demo.<schema>.<table>`.
fn spark_conf_args(rest_endpoint: &str, s3_endpoint: &str) -> Vec<String> {
    vec![
        "--master".to_string(),
        "local[*]".to_string(),
        "--conf".to_string(),
        format!("spark.sql.catalog.demo.uri={rest_endpoint}"),
        "--conf".to_string(),
        "spark.sql.catalog.demo.warehouse=s3://warehouse/".to_string(),
        "--conf".to_string(),
        format!("spark.sql.catalog.demo.s3.endpoint={s3_endpoint}"),
        "--conf".to_string(),
        "spark.sql.catalog.demo.s3.path-style-access=true".to_string(),
        "--conf".to_string(),
        "spark.sql.catalog.demo.s3.access-key-id=user".to_string(),
        "--conf".to_string(),
        "spark.sql.catalog.demo.s3.secret-access-key=password".to_string(),
    ]
}

#[tokio::test]
async fn integration_spark_rest() {
    let container_host = if is_podman() {
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

    // Spark image: bring it up with a long-running no-op command so we can
    // `exec` `spark-sql` invocations against it (matching the Trino test's
    // pattern of running the CLI repeatedly).
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

    let rest_endpoint_for_spark = format!("http://{}:{}", container_host, rest_port);
    let s3_endpoint_for_spark = format!("http://{}:{}", container_host, localstack_port);
    let conf_args = spark_conf_args(&rest_endpoint_for_spark, &s3_endpoint_for_spark);

    // Run the setup SQL via spark-sql.
    let mut setup_cmd: Vec<String> = vec!["spark-sql".to_string()];
    setup_cmd.extend(conf_args.iter().cloned());
    setup_cmd.extend(vec!["-f".to_string(), "/tmp/spark.sql".to_string()]);

    let mut result = spark
        .exec(ExecCommand::new(setup_cmd).with_cmd_ready_condition(CmdWaitFor::Exit { code: None }))
        .await
        .unwrap();

    while result.exit_code().await.unwrap().is_none() {
        sleep(Duration::from_millis(500)).await;
    }

    let exit_code = result.exit_code().await.unwrap().unwrap();
    if exit_code != 0 {
        let stdout = String::from_utf8_lossy(&result.stdout_to_vec().await.unwrap()).into_owned();
        let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await.unwrap()).into_owned();
        panic!(
            "spark-sql setup failed (exit={exit_code}).\nstdout tail:\n{}\nstderr tail:\n{}",
            tail(&stdout, 4000),
            tail(&stderr, 4000)
        );
    }

    // Verify Rust can see the tables Spark wrote.
    let catalog = Arc::new(RestCatalog::new(
        None,
        configuration(&rest_host.to_string(), rest_port),
        Some(object_store),
        false,
    ));

    let tables = catalog
        .list_tabulars(&Namespace::try_new(&["test".to_owned()]).unwrap())
        .await
        .unwrap();

    assert_eq!(tables.len(), 3, "expected 3 tables created by Spark");

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "iceberg",
        Arc::new(IcebergCatalog::new(catalog.clone(), None).await.unwrap()),
    );

    // DataFusion reads a Spark-written table.
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
    let mut read_cmd: Vec<String> = vec!["spark-sql".to_string()];
    read_cmd.extend(conf_args.iter().cloned());
    read_cmd.extend(vec![
        "-e".to_string(),
        "SELECT SUM(amount) FROM demo.test.test_orders;".to_string(),
    ]);

    let mut result = spark
        .exec(ExecCommand::new(read_cmd).with_cmd_ready_condition(CmdWaitFor::Exit { code: None }))
        .await
        .unwrap();
    while result.exit_code().await.unwrap().is_none() {
        sleep(Duration::from_millis(500)).await;
    }

    let exit_code = result.exit_code().await.unwrap().unwrap();
    let stdout = String::from_utf8_lossy(&result.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await.unwrap()).into_owned();
    assert_eq!(
        exit_code,
        0,
        "spark-sql read failed. stdout:\n{stdout}\nstderr tail:\n{}",
        tail(&stderr, 4000)
    );
    assert!(
        stdout.contains("11"),
        "expected Spark SUM(amount)=11 in stdout, got:\n{stdout}\nstderr tail:\n{}",
        tail(&stderr, 4000)
    );
}

fn tail(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("...{}", &s[s.len() - n..])
    }
}
