//! Shared fixture for Spark + REST + LocalStack integration tests.
//!
//! Each test file `#[path = "spark_common/mod.rs"] mod spark_common;` to pull
//! this in. Cargo treats integration tests as separate binaries so this module
//! gets compiled per file; the duplication is fine because it's the only
//! non-test code shared across these binaries.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use iceberg_rest_catalog::apis::configuration::Configuration;
use iceberg_rest_catalog::catalog::RestCatalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::test_utils::is_podman;
use testcontainers::core::CmdWaitFor;
use testcontainers::ContainerAsync;
use testcontainers::{
    core::{wait::LogWaitStrategy, AccessMode, ExecCommand, Mount, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use testcontainers_modules::localstack::LocalStack;
use tokio::time::sleep;

pub struct SparkStack {
    pub localstack: ContainerAsync<LocalStack>,
    pub rest: ContainerAsync<GenericImage>,
    pub spark: ContainerAsync<GenericImage>,
    pub catalog: Arc<RestCatalog>,
    pub conf_args: Vec<String>,
}

pub struct SparkExecResult {
    pub exit_code: i64,
    pub stdout: String,
    pub stderr: String,
}

impl SparkExecResult {
    /// `spark-sql` exits 0 even when individual SQL statements fail, so we
    /// also have to scan stderr for the markers Spark emits on SQL or
    /// underlying-catalog errors. The most reliable marker is
    /// `ERROR SparkSQLDriver: Failed in [<sql>]`, which Spark logs for every
    /// SQL statement that raised an exception (regardless of whether the
    /// exception was wrapped with `Caused by:`).
    pub fn is_success(&self) -> bool {
        self.exit_code == 0 && !self.stderr.contains("ERROR SparkSQLDriver:")
    }

    /// Returns true iff stderr mentions any of the given exception markers.
    /// Useful for expecting a specific SQL failure mode.
    pub fn stderr_contains_any(&self, markers: &[&str]) -> bool {
        markers.iter().any(|m| self.stderr.contains(m))
    }

    /// Combined stdout+stderr tails, capped at `cap` chars each, for panic
    /// messages.
    pub fn dump(&self) -> String {
        format!(
            "exit={}\nstdout:\n{}\nstderr (full):\n{}",
            self.exit_code,
            tail(&self.stdout, 2000),
            self.stderr,
        )
    }
}

pub fn tail(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("...{}", &s[s.len() - n..])
    }
}

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

/// Bring up the LocalStack + REST catalog + Spark stack used by all of the
/// SQL-level integration tests. Returns a `SparkStack` whose handles outlive
/// the calling test (they are dropped at the end of the test, which tears
/// down the containers).
pub async fn boot_spark_stack() -> SparkStack {
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

    let spark = GenericImage::new("tabulario/spark-iceberg", "latest")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(2),
        })
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        // tabulario/spark-iceberg's entrypoint runs `eval "$1"` after starting
        // the Spark daemons, so the keep-alive command must be a single arg.
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

    let catalog = Arc::new(RestCatalog::new(
        None,
        configuration(&rest_host.to_string(), rest_port),
        Some(object_store),
        false,
    ));

    SparkStack {
        localstack,
        rest,
        spark,
        catalog,
        conf_args,
    }
}

/// Execute a SQL string against the Spark container via `spark-sql -e`.
/// Returns exit code, stdout, stderr without panicking; callers assert.
pub async fn spark_sql(stack: &SparkStack, sql: &str) -> SparkExecResult {
    let mut cmd: Vec<String> = vec!["spark-sql".to_string()];
    cmd.extend(stack.conf_args.iter().cloned());
    cmd.extend(vec!["-e".to_string(), sql.to_string()]);

    let mut result = stack
        .spark
        .exec(ExecCommand::new(cmd).with_cmd_ready_condition(CmdWaitFor::Exit { code: None }))
        .await
        .expect("spark-sql exec failed to start");

    while result.exit_code().await.unwrap().is_none() {
        sleep(Duration::from_millis(500)).await;
    }

    let exit_code = result.exit_code().await.unwrap().unwrap();
    let stdout = String::from_utf8_lossy(&result.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await.unwrap()).into_owned();
    SparkExecResult {
        exit_code,
        stdout,
        stderr,
    }
}

/// Execute a SQL string and panic with a useful message if it exits non-zero.
pub async fn spark_sql_ok(stack: &SparkStack, sql: &str) {
    let res = spark_sql(stack, sql).await;
    assert!(
        res.is_success(),
        "spark-sql failed on SQL: {sql}\n{}",
        res.dump()
    );
}

/// Execute a SQL string mounted as a file. Useful for multi-statement scripts.
pub async fn spark_sql_file(stack: &SparkStack, file_in_container: &str) -> SparkExecResult {
    let mut cmd: Vec<String> = vec!["spark-sql".to_string()];
    cmd.extend(stack.conf_args.iter().cloned());
    cmd.extend(vec!["-f".to_string(), file_in_container.to_string()]);

    let mut result = stack
        .spark
        .exec(ExecCommand::new(cmd).with_cmd_ready_condition(CmdWaitFor::Exit { code: None }))
        .await
        .expect("spark-sql exec failed to start");

    while result.exit_code().await.unwrap().is_none() {
        sleep(Duration::from_millis(500)).await;
    }

    let exit_code = result.exit_code().await.unwrap().unwrap();
    let stdout = String::from_utf8_lossy(&result.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await.unwrap()).into_owned();
    SparkExecResult {
        exit_code,
        stdout,
        stderr,
    }
}

/// Mount an additional file into the Spark container at the given path.
/// Must be called before `boot_spark_stack` (currently the fixture doesn't
/// support adding mounts after start; tests that need files mount them via
/// a follow-up `boot_spark_stack` variant or via env vars). Provided here
/// for symmetry with the Trino-style pattern.
pub fn mount_ro(host_path: &str, container_path: &str) -> Mount {
    Mount::bind_mount(host_path, container_path).with_access_mode(AccessMode::ReadOnly)
}
