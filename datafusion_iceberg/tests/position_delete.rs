use std::{collections::HashMap, fs::File, sync::Arc};

use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema as ArrowSchema},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    assert_batches_eq,
    parquet::{
        arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY},
        file::properties::WriterProperties,
    },
    prelude::SessionContext,
};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use futures::{stream, TryStreamExt};
use iceberg_rust::{
    arrow::write::write_equality_deletes_parquet_partitioned,
    catalog::{identifier::Identifier, tabular::Tabular, Catalog},
    object_store::ObjectStoreBuilder,
    spec::{
        manifest::{Content, DataFile, FileFormat, Status},
        namespace::Namespace,
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, Type},
        values::Struct,
    },
    table::Table,
};
use iceberg_sql_catalog::SqlCatalog;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

const FILE_PATH_FIELD_ID: i32 = i32::MAX - 101;
const POS_FIELD_ID: i32 = i32::MAX - 102;

async fn run_query(query: &str, ctx: &SessionContext) -> Vec<RecordBatch> {
    ctx.sql(query)
        .await
        .expect("query planning failed")
        .collect()
        .await
        .expect("query execution failed")
}

fn write_position_delete_file(
    path: &str,
    data_file_path: &str,
    partition: Struct,
    positions: &[i64],
) -> DataFile {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            FILE_PATH_FIELD_ID.to_string(),
        )])),
        Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            POS_FIELD_ID.to_string(),
        )])),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![data_file_path; positions.len()])),
            Arc::new(Int64Array::from(positions.to_vec())),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build())).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    let file_size = std::fs::metadata(path).unwrap().len();

    DataFile::builder()
        .with_content(Content::PositionDeletes)
        .with_file_path(path.to_string())
        .with_file_format(FileFormat::Parquet)
        .with_partition(partition)
        .with_record_count(metadata.file_metadata().num_rows())
        .with_file_size_in_bytes(i64::try_from(file_size).unwrap())
        .with_column_sizes(None)
        .with_value_counts(None)
        .with_null_value_counts(None)
        .with_nan_value_counts(None)
        .with_distinct_counts(None)
        .with_lower_bounds(None)
        .with_upper_bounds(None)
        .build()
        .unwrap()
}

#[tokio::test]
async fn applies_v2_position_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let table_dir = format!("{}/test/orders", temp_dir.path().display());
    let object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store)
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
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "payload".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 3,
            name: "__data_file_path".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 4,
            name: "__iceberg_file_row_position".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 5,
            name: "__iceberg_data_sequence_number".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 6,
            name: "category".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .build()
        .unwrap();

    let partition_spec = PartitionSpec::builder()
        .with_partition_field(PartitionField::new(
            6,
            1000,
            "category",
            Transform::Identity,
        ))
        .build()
        .unwrap();

    Table::builder()
        .with_name("orders")
        .with_location(&table_dir)
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog.clone())
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "warehouse",
        Arc::new(IcebergCatalog::new(catalog.clone(), None).await.unwrap()),
    );

    run_query(
        "INSERT INTO warehouse.test.orders VALUES
            (1, 'one', 101, 'row-one', 'seq-one', 'a'),
            (2, 'two', 102, 'row-two', 'seq-two', 'a'),
            (3, 'three', 103, 'row-three', 'seq-three', 'a'),
            (4, 'four', 104, 'row-four', 'seq-four', 'a'),
            (5, 'five', 105, 'row-five', 'seq-five', 'a'),
            (6, 'six', 106, 'row-six', 'seq-six', 'a')",
        &ctx,
    )
    .await;

    let identifier = Identifier::new(&["test".to_string()], "orders");
    let Tabular::Table(mut table) = catalog.clone().load_tabular(&identifier).await.unwrap() else {
        panic!("orders should be an Iceberg table");
    };
    let manifests = table.manifests(None, None).await.unwrap();
    let data_files = table
        .datafiles(&manifests, None, (None, None))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let (_, data_manifest_entry) = data_files
        .iter()
        .find(|(_, entry)| {
            entry.status() != &Status::Deleted && entry.data_file().content() == &Content::Data
        })
        .unwrap();
    let data_file_path = data_manifest_entry.data_file().file_path().clone();
    let partition = data_manifest_entry.data_file().partition().clone();

    let delete_dir = format!("{table_dir}/data");
    std::fs::create_dir_all(&delete_dir).unwrap();
    let delete_files = vec![
        write_position_delete_file(
            &format!("{delete_dir}/position-delete-1.parquet"),
            &data_file_path,
            partition.clone(),
            &[1, 4],
        ),
        write_position_delete_file(
            &format!("{delete_dir}/position-delete-2.parquet"),
            &data_file_path,
            partition,
            &[4, 5],
        ),
    ];

    table
        .new_transaction(None)
        .append_delete(delete_files)
        .commit()
        .await
        .unwrap();

    let batches = run_query(
        "SELECT id, payload FROM warehouse.test.orders ORDER BY id",
        &ctx,
    )
    .await;
    assert_batches_eq!(
        [
            "+----+---------+",
            "| id | payload |",
            "+----+---------+",
            "| 1  | one     |",
            "| 3  | three   |",
            "| 4  | four    |",
            "+----+---------+",
        ],
        &batches
    );

    let batches = run_query(
        "SELECT __iceberg_data_sequence_number, id, __data_file_path,
                __iceberg_file_row_position
         FROM warehouse.test.orders ORDER BY id",
        &ctx,
    )
    .await;
    assert_batches_eq!(
        [
            "+--------------------------------+----+------------------+-----------------------------+",
            "| __iceberg_data_sequence_number | id | __data_file_path | __iceberg_file_row_position |",
            "+--------------------------------+----+------------------+-----------------------------+",
            "| seq-one                        | 1  | 101              | row-one                     |",
            "| seq-three                      | 3  | 103              | row-three                   |",
            "| seq-four                       | 4  | 104              | row-four                    |",
            "+--------------------------------+----+------------------+-----------------------------+",
        ],
        &batches
    );

    let batches = run_query(
        "SELECT id FROM warehouse.test.orders WHERE id IN (2, 3, 5) ORDER BY id",
        &ctx,
    )
    .await;
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 3  |", "+----+",],
        &batches
    );

    run_query(
        "INSERT INTO warehouse.test.orders VALUES
            (7, 'seven', 107, 'row-seven', 'seq-seven', 'a'),
            (8, 'eight', 108, 'row-eight', 'seq-eight', 'a')",
        &ctx,
    )
    .await;
    let batches = run_query(
        "SELECT id FROM warehouse.test.orders WHERE id >= 5 ORDER BY id",
        &ctx,
    )
    .await;
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 7  |", "| 8  |", "+----+",],
        &batches
    );

    let equality_rows = run_query(
        "SELECT id, category FROM warehouse.test.orders WHERE id IN (3, 7)",
        &ctx,
    )
    .await;
    let equality_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        Field::new("category", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "6".to_string(),
        )])),
    ]));
    let equality_rows = equality_rows
        .into_iter()
        .map(|batch| RecordBatch::try_new(equality_schema.clone(), batch.columns().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let Tabular::Table(mut table) = catalog.clone().load_tabular(&identifier).await.unwrap() else {
        panic!("orders should be an Iceberg table");
    };
    let equality_files = write_equality_deletes_parquet_partitioned(
        &table,
        stream::iter(equality_rows.into_iter().map(Ok::<_, ArrowError>)),
        None,
        &[1, 6],
    )
    .await
    .unwrap();
    table
        .new_transaction(None)
        .append_delete(equality_files)
        .commit()
        .await
        .unwrap();

    let batches = run_query("SELECT id FROM warehouse.test.orders ORDER BY id", &ctx).await;
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 1  |", "| 4  |", "| 8  |", "+----+",],
        &batches
    );
}
