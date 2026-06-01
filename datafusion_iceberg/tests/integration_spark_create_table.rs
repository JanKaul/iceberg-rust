//! Ports the CREATE TABLE surface of upstream `TestCreateTable` to the Spark
//! integration fixture.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::partition::Transform;
use iceberg_rust::spec::table_metadata::TableMetadata;
use iceberg_rust::spec::types::{PrimitiveType, Type};

use spark_common::{boot_spark_stack, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_create_table";

fn ident(name: &str) -> Identifier {
    Identifier::try_new(&[TEST_NS.to_string(), name.to_string()], None).unwrap()
}

async fn load_table_metadata(stack: &SparkStack, name: &str) -> TableMetadata {
    let tabular = stack
        .catalog
        .clone()
        .load_tabular(&ident(name))
        .await
        .expect("load_tabular");
    match tabular {
        Tabular::Table(t) => t.metadata().clone(),
        _ => panic!("expected `{name}` to load as a Table"),
    }
}

async fn drop_table(stack: &SparkStack, name: &str) {
    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.{name}")).await;
}

#[tokio::test]
async fn integration_spark_create_table_suite() {
    let stack = boot_spark_stack().await;

    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_create_table(&stack).await;
    step_create_table_partitioned_by(&stack).await;
    step_create_table_column_comments(&stack).await;
    step_create_table_comment(&stack).await;
    step_create_table_properties(&stack).await;
    step_transform_singular_form(&stack).await;
    step_transform_plural_form(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `testCreateTable`. CREATE TABLE produces a table with the
/// expected required/optional column flags, no partition spec, and no
/// file-format override.
async fn step_create_table(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_create \
             (id BIGINT NOT NULL, data STRING) \
             USING ICEBERG"
        ),
    )
    .await;

    let md = load_table_metadata(stack, "t_create").await;
    let schema = md.current_schema(None).unwrap();
    let fields = schema.fields();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name, "id");
    assert!(fields[0].required, "id should be required");
    assert_eq!(
        fields[0].field_type,
        Type::Primitive(PrimitiveType::Long),
        "id should be Long"
    );
    assert_eq!(fields[1].name, "data");
    assert!(!fields[1].required, "data should be optional");
    assert_eq!(fields[1].field_type, Type::Primitive(PrimitiveType::String));

    // Default spec is unpartitioned (no PartitionField entries).
    let default_spec = md.default_partition_spec().unwrap();
    assert!(
        default_spec.fields().is_empty(),
        "expected unpartitioned default spec, got {:?}",
        default_spec.fields()
    );

    // No explicit file-format property override.
    assert!(
        !md.properties.contains_key("write.format.default"),
        "expected `write.format.default` to be absent, got {:?}",
        md.properties
    );

    drop_table(stack, "t_create").await;
}

/// Upstream: `testCreateTablePartitionedBy`. CREATE TABLE … PARTITIONED BY
/// (identity, bucket, days) lands a 3-field partition spec.
async fn step_create_table_partitioned_by(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_part \
             (id BIGINT NOT NULL, created_at TIMESTAMP, category STRING, data STRING) \
             USING ICEBERG \
             PARTITIONED BY (category, bucket(8, id), days(created_at))"
        ),
    )
    .await;

    let md = load_table_metadata(stack, "t_part").await;
    let spec = md.default_partition_spec().unwrap();
    let transforms: Vec<Transform> = spec
        .fields()
        .iter()
        .map(|f| f.transform().clone())
        .collect();
    assert_eq!(transforms.len(), 3, "expected 3 partition fields");
    assert!(
        matches!(transforms[0], Transform::Identity),
        "expected category to be Identity, got {:?}",
        transforms[0]
    );
    assert!(
        matches!(transforms[1], Transform::Bucket(8)),
        "expected bucket(8) on id, got {:?}",
        transforms[1]
    );
    assert!(
        matches!(transforms[2], Transform::Day),
        "expected day on created_at, got {:?}",
        transforms[2]
    );

    drop_table(stack, "t_part").await;
}

/// Upstream: `testCreateTableColumnComments`. Column doc strings round-trip
/// to the loaded schema.
async fn step_create_table_column_comments(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_col_comments \
             (id BIGINT NOT NULL COMMENT 'primary key', data STRING COMMENT 'payload') \
             USING ICEBERG"
        ),
    )
    .await;

    let md = load_table_metadata(stack, "t_col_comments").await;
    let fields = md.current_schema(None).unwrap().fields();
    assert_eq!(fields[0].doc.as_deref(), Some("primary key"));
    assert_eq!(fields[1].doc.as_deref(), Some("payload"));

    drop_table(stack, "t_col_comments").await;
}

/// Upstream: `testCreateTableComment`. CREATE TABLE … COMMENT 'x' surfaces
/// as the `comment` table property.
async fn step_create_table_comment(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_comment (id BIGINT) USING ICEBERG \
             COMMENT 'this is an iceberg-rust integration table'"
        ),
    )
    .await;

    let md = load_table_metadata(stack, "t_comment").await;
    let comment = md.properties.get("comment").cloned().unwrap_or_default();
    assert_eq!(
        comment, "this is an iceberg-rust integration table",
        "expected `comment` table property, got props={:?}",
        md.properties
    );

    drop_table(stack, "t_comment").await;
}

/// Upstream: `testCreateTableProperties`. Custom keys passed via TBLPROPERTIES
/// land in the table metadata properties map.
async fn step_create_table_properties(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_props (id BIGINT) USING ICEBERG \
             TBLPROPERTIES ('custom.owner' = 'iceberg-rust-it', 'custom.tier' = 'gold')"
        ),
    )
    .await;

    let md = load_table_metadata(stack, "t_props").await;
    assert_eq!(
        md.properties.get("custom.owner").map(String::as_str),
        Some("iceberg-rust-it"),
        "props={:?}",
        md.properties
    );
    assert_eq!(
        md.properties.get("custom.tier").map(String::as_str),
        Some("gold"),
        "props={:?}",
        md.properties
    );

    drop_table(stack, "t_props").await;
}

/// Upstream: `testTransformSingularForm`. Singular partition transform names
/// (`year`, `month`, `day`, `hour`) are accepted by the parser.
async fn step_transform_singular_form(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_singular (ts TIMESTAMP, n INT) USING ICEBERG \
             PARTITIONED BY (year(ts))"
        ),
    )
    .await;

    let md = load_table_metadata(stack, "t_singular").await;
    let spec = md.default_partition_spec().unwrap();
    assert_eq!(spec.fields().len(), 1);
    assert!(
        matches!(spec.fields()[0].transform(), Transform::Year),
        "expected Year transform, got {:?}",
        spec.fields()[0].transform()
    );

    drop_table(stack, "t_singular").await;
}

/// Upstream: `testTransformPluralForm`. Plural partition transform names
/// (`years`, `months`, …) are also accepted and lower to the same Transform.
async fn step_transform_plural_form(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_plural (ts TIMESTAMP, n INT) USING ICEBERG \
             PARTITIONED BY (months(ts))"
        ),
    )
    .await;

    let md = load_table_metadata(stack, "t_plural").await;
    let spec = md.default_partition_spec().unwrap();
    assert_eq!(spec.fields().len(), 1);
    assert!(
        matches!(spec.fields()[0].transform(), Transform::Month),
        "expected Month transform, got {:?}",
        spec.fields()[0].transform()
    );

    drop_table(stack, "t_plural").await;
}
