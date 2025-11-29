use datafusion::{
    arrow::{error::ArrowError, record_batch::RecordBatch},
    assert_batches_eq,
    prelude::SessionContext,
};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use duckdb::Connection;
use futures::stream;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::namespace::Namespace;
use iceberg_rust::{
    arrow::write::write_equality_deletes_parquet_partitioned,
    catalog::Catalog,
    object_store::ObjectStoreBuilder,
    spec::{
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, Type},
    },
    table::Table,
};
use iceberg_sql_catalog::SqlCatalog;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tempfile::TempDir;

/// Convert DuckDB's Arrow RecordBatch to DataFusion's Arrow RecordBatch
/// using Arrow IPC format as an interchange format.
/// This allows compatibility between different Arrow versions.
fn convert_duckdb_batch_to_datafusion(
    duckdb_batch: duckdb::arrow::record_batch::RecordBatch,
) -> RecordBatch {
    use arrow_ipc::writer::StreamWriter as DuckDBWriter;
    use datafusion::arrow::ipc::reader::StreamReader as DataFusionReader;

    let mut buffer = Vec::new();
    let mut writer = DuckDBWriter::try_new(&mut buffer, &duckdb_batch.schema()).unwrap();
    writer.write(&duckdb_batch).unwrap();
    writer.finish().unwrap();
    drop(writer);

    let mut reader = DataFusionReader::try_new(std::io::Cursor::new(buffer), None).unwrap();
    reader.next().unwrap().unwrap()
}

#[tokio::test]
pub async fn test_equality_delete() {
    let temp_dir = TempDir::new().unwrap();
    let table_dir = format!("{}/test/orders", temp_dir.path().to_str().unwrap());
    let object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));

    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store.clone())
            .await
            .unwrap(),
    );

    catalog
        .create_namespace(&Namespace::try_new(&["test".to_string()]).unwrap(), None)
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "customer_id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
        })
        .with_struct_field(StructField {
            id: 3,
            name: "product_id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
        })
        .with_struct_field(StructField {
            id: 4,
            name: "date".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Date),
            doc: None,
        })
        .with_struct_field(StructField {
            id: 5,
            name: "amount".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Int),
            doc: None,
        })
        .build()
        .unwrap();

    let partition_spec = PartitionSpec::builder()
        .with_partition_field(PartitionField::new(4, 1000, "date_day", Transform::Day))
        .build()
        .expect("Failed to create partition spec");

    let table = Table::builder()
        .with_name("orders")
        .with_location(&table_dir)
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("Failed to create table");

    let ctx = SessionContext::new();

    let datafusion_catalog = Arc::new(IcebergCatalog::new(catalog.clone(), None).await.unwrap());

    ctx.register_catalog("warehouse", datafusion_catalog);

    ctx.sql(
        "INSERT INTO warehouse.test.orders (id, customer_id, product_id, date, amount) VALUES 
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
    .await
    .expect("Failed to insert values into table");

    let batches = ctx
        .sql("select * from warehouse.test.orders order by id")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    let expected = [
        "+----+-------------+------------+------------+--------+",
        "| id | customer_id | product_id | date       | amount |",
        "+----+-------------+------------+------------+--------+",
        "| 1  | 1           | 1          | 2020-01-01 | 1      |",
        "| 2  | 2           | 1          | 2020-01-01 | 1      |",
        "| 3  | 3           | 1          | 2020-01-01 | 3      |",
        "| 4  | 1           | 2          | 2020-02-02 | 1      |",
        "| 5  | 1           | 1          | 2020-02-02 | 2      |",
        "| 6  | 3           | 3          | 2020-02-02 | 3      |",
        "+----+-------------+------------+------------+--------+",
    ];
    assert_batches_eq!(expected, &batches);

    let batches = ctx
        .sql(
            "SELECT id, customer_id, product_id, date FROM warehouse.test.orders WHERE customer_id = 1 order by id",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

    let expected = [
        "+----+-------------+------------+------------+",
        "| id | customer_id | product_id | date       |",
        "+----+-------------+------------+------------+",
        "| 1  | 1           | 1          | 2020-01-01 |",
        "| 4  | 1           | 2          | 2020-02-02 |",
        "| 5  | 1           | 1          | 2020-02-02 |",
        "+----+-------------+------------+------------+",
    ];
    assert_batches_eq!(expected, &batches);

    let files = write_equality_deletes_parquet_partitioned(
        &table,
        stream::iter(batches.into_iter().map(Ok::<_, ArrowError>)),
        None,
        &[1, 2, 3, 4],
    )
    .await
    .unwrap();

    // Load the latest table version, which includes the inserted rows
    let Tabular::Table(mut table) = catalog
        .clone()
        .load_tabular(&Identifier::new(&["test".to_string()], "orders"))
        .await
        .unwrap()
    else {
        panic!("Tabular should be a table");
    };

    table
        .new_transaction(None)
        .append_delete(files)
        .commit()
        .await
        .unwrap();

    let batches = ctx
        .sql("select * from warehouse.test.orders order by id")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    let expected = [
        "+----+-------------+------------+------------+--------+",
        "| id | customer_id | product_id | date       | amount |",
        "+----+-------------+------------+------------+--------+",
        "| 2  | 2           | 1          | 2020-01-01 | 1      |",
        "| 3  | 3           | 1          | 2020-01-01 | 3      |",
        "| 6  | 3           | 3          | 2020-02-02 | 3      |",
        "+----+-------------+------------+------------+--------+",
    ];
    assert_batches_eq!(expected, &batches);

    let conn = Connection::open_in_memory().unwrap();
    conn.execute("install iceberg", []).unwrap();
    conn.execute("load iceberg", []).unwrap();

    let duckdb_batches: Vec<RecordBatch> = conn
        .prepare("select * from iceberg_scan(?) order by id")
        .unwrap()
        .query_arrow([table_dir])
        .unwrap()
        .map(|batch| convert_duckdb_batch_to_datafusion(batch))
        .collect();
    assert_batches_eq!(expected, &duckdb_batches);

    // Test that projecting a column that is not included in equality deletes works
    ctx.sql(
        "INSERT INTO warehouse.test.orders (id, customer_id, product_id, date, amount) VALUES
                (7, 3, 2, '2020-01-01', 2),
                (8, 2, 1, '2020-02-02', 3),
                (9, 1, 3, '2020-01-01', 1);",
    )
    .await
    .expect("Failed to create query plan for insert")
    .collect()
    .await
    .expect("Failed to insert values into table");

    let batches = ctx
        .sql("select sum(amount) from warehouse.test.orders")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    let expected = [
        "+-----------------------------------+",
        "| sum(warehouse.test.orders.amount) |",
        "+-----------------------------------+",
        "| 13                                |",
        "+-----------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}
