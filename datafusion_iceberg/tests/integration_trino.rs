use std::io::Write;
use std::sync::Arc;
use std::{fs::File, time::Duration};

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::execution::context::SessionContext;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rest_catalog::apis::configuration::Configuration;
use iceberg_rest_catalog::catalog::RestCatalog;
use iceberg_rust::catalog::bucket::ObjectStoreBuilder;
use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::catalog::Catalog;
use object_store::aws::AmazonS3Builder;
use tempfile::TempDir;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{CmdWaitFor, ContainerPort};
use testcontainers::{
    core::{wait::LogWaitStrategy, AccessMode, ExecCommand, Mount, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use testcontainers_modules::localstack::LocalStack;

fn configuration(host: &str, port: u16) -> Configuration {
    Configuration {
        base_path: format!("http://{}:{}", host, port.to_string()),
        user_agent: None,
        client: reqwest::Client::new().into(),
        basic_auth: None,
        oauth_access_token: None,
        bearer_access_token: None,
        api_key: None,
    }
}

#[tokio::test]
async fn integration_trino() {
    let docker_host = "172.17.0.1";

    let localstack = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .start()
        .await
        .unwrap();

    localstack
        .exec(ExecCommand::new(vec![
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "warehouse",
        ]))
        .await
        .unwrap();

    let localstack_host = localstack.get_host().await.unwrap();
    let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

    let rest = GenericImage::new("tabulario/iceberg-rest", "latest")
        .with_wait_for(WaitFor::Log(LogWaitStrategy::stdout(
            "INFO  [org.eclipse.jetty.server.Server] - Started ",
        )))
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("CATALOG_WAREHOUSE", "s3://warehouse/")
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var(
            "CATALOG_S3_ENDPOINT",
            format!("http://{}:{}", &docker_host, &localstack_port),
        )
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

    let tmp_dir = TempDir::new().unwrap();
    let file_path = tmp_dir.path().join("iceberg-properties");
    let mut tmp_file = File::create(&file_path).unwrap();

    writeln!(tmp_file, "connector.name=iceberg").unwrap();
    writeln!(tmp_file, "iceberg.catalog.type=rest").unwrap();
    writeln!(
        tmp_file,
        "iceberg.rest-catalog.uri=http://{}:{}",
        docker_host, rest_port
    )
    .unwrap();
    writeln!(tmp_file, "iceberg.rest-catalog.warehouse=s3://warehouse/").unwrap();
    writeln!(tmp_file, "iceberg.file-format=PARQUET").unwrap();
    writeln!(
        tmp_file,
        "hive.s3.endpoint=http://{}:{}",
        docker_host, localstack_port
    )
    .unwrap();
    writeln!(tmp_file, "hive.s3.path-style-access=true").unwrap();
    writeln!(tmp_file, "hive.s3.aws-access-key=user").unwrap();
    writeln!(tmp_file, "hive.s3.aws-secret-key=password").unwrap();

    let catalog_mount = Mount::bind_mount(
        file_path.as_os_str().to_str().unwrap(),
        "/etc/trino/catalog/iceberg.properties",
    )
    .with_access_mode(AccessMode::ReadOnly);

    let sql_mount = Mount::bind_mount(cwd.clone() + "/tests/trino/trino.sql", "/tmp/trino.sql")
        .with_access_mode(AccessMode::ReadOnly);

    let trino = GenericImage::new("trinodb/trino", "latest")
        .with_wait_for(WaitFor::Http(
            HttpWaitStrategy::new("/v1/info")
                .with_port(ContainerPort::Tcp(8080))
                .with_response_matcher_async(|response| async move {
                    response.json::<serde_json::Value>().await.unwrap()["starting"] == false
                }),
        ))
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_mount(catalog_mount)
        .with_mount(sql_mount)
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .unwrap();

    let trino_port = trino.get_host_port_ipv4(8080).await.unwrap();

    trino
        .exec(
            ExecCommand::new(vec![
                "trino",
                "--catalog",
                "iceberg",
                "--file",
                "/tmp/trino.sql",
                &format!("http://{}:{}", docker_host, trino_port),
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .unwrap();

    let object_store = ObjectStoreBuilder::S3(
        AmazonS3Builder::new()
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_access_key_id("user")
            .with_secret_access_key("password")
            .with_endpoint(format!("http://{}:{}", localstack_host, localstack_port)),
    );

    let catalog = Arc::new(RestCatalog::new(
        None,
        configuration(&rest_host.to_string(), rest_port),
        object_store,
    ));

    let tables = catalog
        .list_tabulars(&Namespace::try_new(&["test".to_owned()]).unwrap())
        .await
        .unwrap();

    assert_eq!(tables.len(), 8);

    let ctx = SessionContext::new();

    ctx.register_catalog(
        "iceberg",
        Arc::new(IcebergCatalog::new(catalog, None).await.unwrap()),
    );

    let df = ctx
        .sql("SELECT SUM(totalprice) FROM iceberg.test.orders;")
        .await
        .unwrap();

    // execute the plan
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

    assert!(values.value(0) - 2127396830.0 < 0.1)
}
