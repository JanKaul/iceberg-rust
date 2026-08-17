//! Declared sort orders end to end: a table declares its default sort order,
//! sorted writes attest it per file (Parquet footer + manifest entry), and
//! unsorted writes stay honest by not attesting anything.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::stream;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::file::metadata::SortingColumn;
use parquet::file::reader::{FileReader, SerializedFileReader};

use iceberg_rust::arrow::write::{write_parquet_partitioned, write_sorted_parquet_partitioned};
use iceberg_rust::catalog::Catalog;
use iceberg_rust::file_format::parquet::{attested_sort_order_id, ICEBERG_SORT_ORDER_ID_META_KEY};
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::manifest::DataFile;
use iceberg_rust::table::Table;
use iceberg_rust_spec::spec::partition::{PartitionField, PartitionSpec, Transform};
use iceberg_rust_spec::spec::schema::Schema;
use iceberg_rust_spec::spec::sort::{
    NullOrder, SortDirection, SortField, SortOrder, SortOrderBuilder,
};
use iceberg_rust_spec::spec::types::{PrimitiveType, StructField, Type};
use iceberg_rust_spec::util::strip_prefix;
use iceberg_sql_catalog::SqlCatalog;

const SORT_ORDER_ID: i32 = 1;

fn schema() -> Schema {
    let mut builder = Schema::builder();
    builder
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
            name: "region".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 3,
            name: "value".to_string(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: None,
            initial_default: None,
            write_default: None,
        });
    builder.build().unwrap()
}

fn region_partition_spec() -> PartitionSpec {
    PartitionSpec::builder()
        .with_partition_field(PartitionField::new(2, 1000, "region", Transform::Identity))
        .build()
        .unwrap()
}

/// `(id ASC NULLS FIRST, value DESC NULLS LAST)`.
fn sort_order() -> SortOrder {
    SortOrderBuilder::default()
        .with_order_id(SORT_ORDER_ID)
        .with_sort_field(SortField {
            source_id: 1,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: 3,
            transform: Transform::Identity,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        })
        .build()
        .unwrap()
}

/// The Parquet rendering of [`sort_order`]: leaf indices in the file schema.
fn expected_sorting_columns() -> Vec<SortingColumn> {
    vec![
        SortingColumn {
            column_idx: 0,
            descending: false,
            nulls_first: true,
        },
        SortingColumn {
            column_idx: 2,
            descending: true,
            nulls_first: false,
        },
    ]
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ])
}

/// Rows sorted by [`sort_order`], spanning two partitions.
fn sorted_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "us-east", "us-west", "us-east", "us-west",
            ])),
            Arc::new(Int64Array::from(vec![
                Some(100),
                Some(200),
                Some(300),
                None,
            ])),
        ],
    )
    .unwrap()
}

async fn catalog() -> Arc<dyn Catalog> {
    Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", ObjectStoreBuilder::memory())
            .await
            .unwrap(),
    )
}

async fn create_table(catalog: Arc<dyn Catalog>, name: &str, order: Option<SortOrder>) -> Table {
    let mut builder = Table::builder();
    builder
        .with_name(name)
        .with_location(format!("/test/{name}"))
        .with_schema(schema())
        .with_partition_spec(region_partition_spec());
    if let Some(order) = order {
        builder.with_sort_order(order);
    }
    builder
        .build(&["test".to_owned()], catalog)
        .await
        .expect("Failed to create table")
}

async fn footer(
    store: &Arc<dyn ObjectStore>,
    file: &DataFile,
) -> parquet::file::metadata::ParquetMetaData {
    let path = Path::from(strip_prefix(file.file_path()));
    let bytes: Bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    SerializedFileReader::new(bytes).unwrap().metadata().clone()
}

#[tokio::test]
async fn table_created_with_sort_order_declares_it_as_default() {
    let table = create_table(catalog().await, "declared", Some(sort_order())).await;

    let metadata = table.metadata();
    assert_eq!(metadata.default_sort_order_id, SORT_ORDER_ID);
    assert_eq!(metadata.default_sort_order().unwrap(), &sort_order());
}

#[tokio::test]
async fn sorted_write_attests_the_order_on_every_file() {
    let table = create_table(catalog().await, "sorted", Some(sort_order())).await;

    let files =
        write_sorted_parquet_partitioned(&table, stream::iter(vec![Ok(sorted_batch())]), None)
            .await
            .expect("sorted write");
    assert_eq!(files.len(), 2, "one file per region partition");

    let store = table.object_store();
    for file in &files {
        assert_eq!(
            *file.sort_order_id(),
            Some(SORT_ORDER_ID),
            "manifest entry must carry the attested sort order id"
        );

        let metadata = footer(&store, file).await;
        assert_eq!(attested_sort_order_id(&metadata), Some(SORT_ORDER_ID));
        let key_value = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == ICEBERG_SORT_ORDER_ID_META_KEY)
            })
            .expect("footer records the sort order id");
        assert_eq!(key_value.value.as_deref(), Some("1"));
        assert!(!metadata.row_groups().is_empty());
        for row_group in metadata.row_groups() {
            assert_eq!(
                row_group.sorting_columns(),
                Some(&expected_sorting_columns()),
                "every row group declares the sort order's columns"
            );
        }
    }
}

#[tokio::test]
async fn unsorted_write_attests_nothing() {
    let table = create_table(catalog().await, "unsorted", Some(sort_order())).await;

    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(sorted_batch())]), None)
        .await
        .expect("plain write");
    assert!(!files.is_empty());

    let store = table.object_store();
    for file in &files {
        assert_eq!(*file.sort_order_id(), None);
        let metadata = footer(&store, file).await;
        assert_eq!(attested_sort_order_id(&metadata), None);
        for row_group in metadata.row_groups() {
            assert_eq!(row_group.sorting_columns(), None);
        }
    }
}

#[tokio::test]
async fn sorted_write_on_a_table_without_declared_order_attests_nothing() {
    let table = create_table(catalog().await, "undeclared", None).await;
    assert!(table
        .metadata()
        .default_sort_order()
        .unwrap()
        .fields
        .is_empty());

    let files =
        write_sorted_parquet_partitioned(&table, stream::iter(vec![Ok(sorted_batch())]), None)
            .await
            .expect("sorted write");
    for file in &files {
        assert_eq!(*file.sort_order_id(), None);
    }
}

#[tokio::test]
async fn replace_sort_order_declares_the_order_on_an_existing_table() {
    let catalog = catalog().await;
    let mut table = create_table(catalog.clone(), "upgraded", None).await;
    assert_eq!(table.metadata().default_sort_order_id, 0);

    table
        .new_transaction(None)
        .replace_sort_order(sort_order())
        .commit()
        .await
        .expect("replace sort order");

    let metadata = table.metadata();
    assert_eq!(metadata.default_sort_order_id, SORT_ORDER_ID);
    assert_eq!(metadata.default_sort_order().unwrap(), &sort_order());

    // Idempotent: declaring the same order again is a no-op on the metadata.
    table
        .new_transaction(None)
        .replace_sort_order(sort_order())
        .commit()
        .await
        .expect("replace sort order again");
    assert_eq!(table.metadata().default_sort_order_id, SORT_ORDER_ID);
    assert_eq!(
        table.metadata().sort_orders.len(),
        2,
        "unsorted order 0 plus the declared one"
    );

    // Files written after the declaration attest it.
    let files =
        write_sorted_parquet_partitioned(&table, stream::iter(vec![Ok(sorted_batch())]), None)
            .await
            .expect("sorted write");
    for file in &files {
        assert_eq!(*file.sort_order_id(), Some(SORT_ORDER_ID));
    }
}

#[tokio::test]
async fn replace_sort_order_rejects_the_reserved_unsorted_id() {
    let mut table = create_table(catalog().await, "reserved", None).await;
    let mut order = sort_order();
    order.order_id = 0;

    let result = table
        .new_transaction(None)
        .replace_sort_order(order)
        .commit()
        .await;
    assert!(result.is_err(), "order id 0 with fields must be rejected");
}
