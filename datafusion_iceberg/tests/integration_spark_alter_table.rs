//! Ports the portable subset of upstream `TestAlterTable` to the Spark
//! integration fixture. Schema-evolution operations are exercised end-to-end
//! by issuing `ALTER TABLE …` via spark-sql and inspecting the table metadata
//! through the REST catalog.

#[path = "spark_common/mod.rs"]
mod spark_common;

use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::table_metadata::TableMetadata;
use iceberg_rust::spec::types::{PrimitiveType, Type};

use spark_common::{boot_spark_stack, spark_sql_ok, SparkStack};

const TEST_NS: &str = "ns_alter_table";

fn ident(name: &str) -> Identifier {
    Identifier::try_new(&[TEST_NS.to_string(), name.to_string()], None).unwrap()
}

async fn load_md(stack: &SparkStack, name: &str) -> TableMetadata {
    let tabular = stack
        .catalog
        .clone()
        .load_tabular(&ident(name))
        .await
        .expect("load_tabular");
    match tabular {
        Tabular::Table(t) => t.metadata().clone(),
        _ => panic!("expected `{name}` to be a Table"),
    }
}

#[tokio::test]
async fn integration_spark_alter_table_suite() {
    let stack = boot_spark_stack().await;
    spark_sql_ok(
        &stack,
        &format!("CREATE NAMESPACE IF NOT EXISTS demo.{TEST_NS}"),
    )
    .await;

    step_add_column(&stack).await;
    step_add_column_with_array(&stack).await;
    step_drop_column(&stack).await;
    step_rename_column(&stack).await;
    step_alter_column_comment(&stack).await;
    step_alter_column_type(&stack).await;
    step_alter_column_position_first(&stack).await;
    step_alter_column_position_after(&stack).await;

    spark_sql_ok(&stack, &format!("DROP NAMESPACE demo.{TEST_NS}")).await;
}

/// Upstream: `testAddColumn`. ALTER TABLE … ADD COLUMN adds an optional
/// column to the end of the schema.
async fn step_add_column(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_add (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_add ADD COLUMN data STRING"),
    )
    .await;

    let md = load_md(stack, "t_add").await;
    let fields = md.current_schema(None).unwrap().fields();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[1].name, "data");
    assert!(!fields[1].required, "added column should be optional");
    assert_eq!(fields[1].field_type, Type::Primitive(PrimitiveType::String));

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_add")).await;
}

/// Upstream: `testAddColumnWithArray`. ALTER TABLE … ADD COLUMN with an
/// ARRAY<STRING> nested type lands a List column.
async fn step_add_column_with_array(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_addarr (id BIGINT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_addarr ADD COLUMN tags ARRAY<STRING>"),
    )
    .await;

    let md = load_md(stack, "t_addarr").await;
    let fields = md.current_schema(None).unwrap().fields();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[1].name, "tags");
    match &fields[1].field_type {
        Type::List(list) => {
            assert_eq!(&*list.element, &Type::Primitive(PrimitiveType::String));
        }
        other => panic!("expected List, got {other:?}"),
    }

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_addarr")).await;
}

/// Upstream: `testDropColumn`. ALTER TABLE … DROP COLUMN removes the column
/// from the schema.
async fn step_drop_column(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_drop_col (id BIGINT, data STRING, extra INT) \
             USING ICEBERG"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_drop_col DROP COLUMN extra"),
    )
    .await;

    let md = load_md(stack, "t_drop_col").await;
    let fields = md.current_schema(None).unwrap().fields();
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["id", "data"]);

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_drop_col")).await;
}

/// Upstream: `testRenameColumn`. ALTER TABLE … RENAME COLUMN changes the
/// field name while preserving the field id.
async fn step_rename_column(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_rename (id BIGINT, data STRING) USING ICEBERG"),
    )
    .await;
    let before = load_md(stack, "t_rename").await;
    let before_id = before.current_schema(None).unwrap().fields()[1].id;

    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_rename RENAME COLUMN data TO payload"),
    )
    .await;

    let after = load_md(stack, "t_rename").await;
    let after_fields = after.current_schema(None).unwrap().fields();
    assert_eq!(after_fields[1].name, "payload");
    assert_eq!(
        after_fields[1].id, before_id,
        "field id should be preserved across rename"
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_rename")).await;
}

/// Upstream: `testAlterColumnComment`. ALTER TABLE … ALTER COLUMN … COMMENT
/// updates the column doc string.
async fn step_alter_column_comment(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_comment (id BIGINT, data STRING) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_comment ALTER COLUMN data COMMENT 'payload doc'"),
    )
    .await;

    let md = load_md(stack, "t_comment").await;
    let fields = md.current_schema(None).unwrap().fields();
    assert_eq!(fields[1].doc.as_deref(), Some("payload doc"));

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_comment")).await;
}

/// Upstream: `testAlterColumnType`. ALTER TABLE … ALTER COLUMN … TYPE
/// widens an INT column to BIGINT.
async fn step_alter_column_type(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!("CREATE TABLE demo.{TEST_NS}.t_type (id BIGINT, amount INT) USING ICEBERG"),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_type ALTER COLUMN amount TYPE BIGINT"),
    )
    .await;

    let md = load_md(stack, "t_type").await;
    let fields = md.current_schema(None).unwrap().fields();
    assert_eq!(fields[1].name, "amount");
    assert_eq!(
        fields[1].field_type,
        Type::Primitive(PrimitiveType::Long),
        "amount should now be Long"
    );

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_type")).await;
}

/// Upstream: `testAlterColumnPositionFirst`. ALTER COLUMN … FIRST reorders
/// the column to position 0.
async fn step_alter_column_position_first(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_pos_first (id BIGINT, data STRING, n INT) \
             USING ICEBERG"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_pos_first ALTER COLUMN n FIRST"),
    )
    .await;

    let md = load_md(stack, "t_pos_first").await;
    let names: Vec<&str> = md
        .current_schema(None)
        .unwrap()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, vec!["n", "id", "data"]);

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_pos_first")).await;
}

/// Upstream: `testAlterColumnPositionAfter`. ALTER COLUMN … AFTER reorders
/// the column to immediately follow the named sibling.
async fn step_alter_column_position_after(stack: &SparkStack) {
    spark_sql_ok(
        stack,
        &format!(
            "CREATE TABLE demo.{TEST_NS}.t_pos_after (id BIGINT, data STRING, n INT) \
             USING ICEBERG"
        ),
    )
    .await;
    spark_sql_ok(
        stack,
        &format!("ALTER TABLE demo.{TEST_NS}.t_pos_after ALTER COLUMN n AFTER id"),
    )
    .await;

    let md = load_md(stack, "t_pos_after").await;
    let names: Vec<&str> = md
        .current_schema(None)
        .unwrap()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, vec!["id", "n", "data"]);

    spark_sql_ok(stack, &format!("DROP TABLE demo.{TEST_NS}.t_pos_after")).await;
}
