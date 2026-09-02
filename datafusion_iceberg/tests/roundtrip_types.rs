//! Verifies `parquet_to_datafile`/`Value::try_from_bytes` decode Parquet
//! column statistics correctly across primitive types and `PhysicalTypeHint`s:
//! INT32/INT64-physical DECIMAL stats (native little-endian on disk; Iceberg
//! decimals are big-endian), Uuid (written as Arrow `Utf8`, so BYTE_ARRAY
//! stats hold its string form), and the remaining little-endian primitives.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::error::Error;
use iceberg_rust::file_format::parquet::parquet_to_datafile;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::decimal::decimal_from_i128_with_scale;
use iceberg_rust::spec::manifest::DataFile;
use iceberg_rust::spec::namespace::Namespace;
use iceberg_rust::spec::partition::{BoundPartitionField, PartitionField, Transform};
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{PrimitiveType, StructField, Type};
use iceberg_rust::spec::values::Value;
use iceberg_rust::table::Table;
use iceberg_sql_catalog::SqlCatalog;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use uuid::Uuid;

/// Build an in-memory catalog with a single `public.t(id INT, amount DECIMAL(18,2))`
/// table and write `unscaled_amounts` as one data file.
async fn setup(unscaled_amounts: Vec<i128>) -> SessionContext {
    let object_store = ObjectStoreBuilder::memory();
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "test", object_store)
            .await
            .unwrap(),
    );
    catalog
        .create_namespace(&Namespace::try_new(&["public".to_string()]).unwrap(), None)
        .await
        .unwrap();
    let identifier = Identifier::new(&["public".to_string()], "t");

    Table::builder()
        .with_name("t")
        .with_location("/t")
        .with_schema(
            Schema::builder()
                .with_struct_field(StructField {
                    id: 0,
                    name: "id".to_owned(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                })
                .with_struct_field(StructField {
                    id: 1,
                    name: "amount".to_owned(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::Decimal {
                        precision: 18,
                        scale: 2,
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                })
                .build()
                .unwrap(),
        )
        .build(identifier.namespace(), catalog.clone())
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "warehouse",
        Arc::new(IcebergCatalog::new(catalog, None).await.unwrap()),
    );

    let n = unscaled_amounts.len();
    let ids = Int32Array::from((0..n as i32).collect::<Vec<_>>());
    let amount = Decimal128Array::from(unscaled_amounts)
        .with_precision_and_scale(18, 2)
        .unwrap();
    let data = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("amount", Arc::new(amount) as ArrayRef),
    ])
    .unwrap();

    ctx.read_batch(data)
        .unwrap()
        .write_table("warehouse.public.t", DataFrameWriteOptions::default())
        .await
        .unwrap();

    ctx
}

async fn scalar_i128(ctx: &SessionContext, sql: &str) -> i128 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let b = batches.into_iter().find(|b| b.num_rows() > 0).unwrap();
    b.column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(0)
}

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    use datafusion::arrow::array::Int64Array;
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let b = batches.into_iter().find(|b| b.num_rows() > 0).unwrap();
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

/// Writes a single-row Parquet file for the given Arrow schema/batch and
/// returns the `DataFile` metadata `parquet_to_datafile` builds for it.
fn write_and_extract(
    arrow_schema: Arc<ArrowSchema>,
    batch: RecordBatch,
    schema: &Schema,
    partition_fields: &[BoundPartitionField<'_>],
) -> Result<DataFile, Error> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let file_size = buf.len() as u64;

    let reader = SerializedFileReader::new(bytes::Bytes::from(buf)).unwrap();
    let parquet_metadata = reader.metadata().clone();

    parquet_to_datafile(
        "/t/data/1.parquet",
        file_size,
        &parquet_metadata,
        schema,
        partition_fields,
        None,
        &HashMap::new(),
    )
}

/// A single-row file has exact min==max stats, so DataFusion may materialize
/// the column value from the statistic instead of the page.
#[tokio::test]
async fn single_row_decimal_value_is_not_byteswapped() {
    // 100000.00
    let ctx = setup(vec![10_000_000]).await;
    let got = scalar_i128(&ctx, "SELECT amount FROM warehouse.public.t").await;
    assert_eq!(got, 10_000_000, "single-row decimal value was byteswapped");
}

/// A multi-row file's data is read from the page, but a predicate still
/// depends on the manifest's min/max bounds for pruning.
#[tokio::test]
async fn multi_row_decimal_pruning_uses_correct_bounds() {
    // 100.00, 200.00, 300.00
    let ctx = setup(vec![10_000, 20_000, 30_000]).await;

    let total = count(&ctx, "SELECT count(*) FROM warehouse.public.t").await;
    assert_eq!(total, 3);

    let matched = count(
        &ctx,
        "SELECT count(*) FROM warehouse.public.t WHERE amount = CAST(200.00 AS DECIMAL(18,2))",
    )
    .await;
    assert_eq!(
        matched, 1,
        "predicate matching a real row was wrongly pruned (byteswapped bound)"
    );
}

#[test]
fn parquet_stats_and_partition_value_decode_correctly() {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("amount", DataType::Decimal128(18, 2), false),
        Field::new("int_col", DataType::Int32, false),
        Field::new("long_col", DataType::Int64, false),
        Field::new("float_col", DataType::Float32, false),
        Field::new("double_col", DataType::Float64, false),
        Field::new("date_col", DataType::Date32, false),
        Field::new("time_col", DataType::Time64(TimeUnit::Microsecond), false),
        Field::new(
            "ts_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "tstz_col",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            false,
        ),
        Field::new("uuid_col", DataType::Utf8, false),
    ]));

    let amount_val: i128 = 10_000_000; // 100000.00
    let int_val: i32 = 0x0102_0304;
    let long_val: i64 = 0x0102_0304_0506_0708;
    let float_val: f32 = 1.1;
    let double_val: f64 = 2.2;
    let date_val: i32 = 19_723; // 2023-12-04
    let time_val: i64 = 45_296_123_456; // 12:34:56.123456
    let ts_val: i64 = 1_700_000_000_123_456;
    let tstz_val: i64 = 1_700_000_012_345_678;
    let uuid_str = "550e8400-e29b-41d4-a716-446655440000";

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(
                Decimal128Array::from(vec![amount_val])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            Arc::new(Int32Array::from(vec![int_val])),
            Arc::new(Int64Array::from(vec![long_val])),
            Arc::new(Float32Array::from(vec![float_val])),
            Arc::new(Float64Array::from(vec![double_val])),
            Arc::new(Date32Array::from(vec![date_val])),
            Arc::new(Time64MicrosecondArray::from(vec![time_val])),
            Arc::new(TimestampMicrosecondArray::from(vec![ts_val])),
            Arc::new(TimestampMicrosecondArray::from(vec![tstz_val]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec![uuid_str])),
        ],
    )
    .unwrap();

    let mut schema_builder = Schema::builder();
    for (id, (name, field_type)) in [
        (
            "amount",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            }),
        ),
        ("int_col", Type::Primitive(PrimitiveType::Int)),
        ("long_col", Type::Primitive(PrimitiveType::Long)),
        ("float_col", Type::Primitive(PrimitiveType::Float)),
        ("double_col", Type::Primitive(PrimitiveType::Double)),
        ("date_col", Type::Primitive(PrimitiveType::Date)),
        ("time_col", Type::Primitive(PrimitiveType::Time)),
        ("ts_col", Type::Primitive(PrimitiveType::Timestamp)),
        ("tstz_col", Type::Primitive(PrimitiveType::Timestamptz)),
        ("uuid_col", Type::Primitive(PrimitiveType::Uuid)),
    ]
    .into_iter()
    .enumerate()
    {
        schema_builder.with_struct_field(StructField {
            id: id as i32,
            name: name.to_owned(),
            required: true,
            field_type,
            doc: None,
            initial_default: None,
            write_default: None,
        });
    }
    let schema = schema_builder.build().unwrap();

    let partition_field = PartitionField::new(0, 1000, "amount", Transform::Identity);
    let struct_field = schema.fields().get(0).unwrap().clone();
    let bound_field = BoundPartitionField::new(&partition_field, &struct_field);

    let data_file = write_and_extract(arrow_schema, batch, &schema, &[bound_field])
        .expect("stats decode and partition value inference should succeed");

    let partition_value = data_file
        .partition()
        .get("amount")
        .cloned()
        .flatten()
        .expect("partition value should have been inferred from stats");

    let amount = Value::Decimal(decimal_from_i128_with_scale(amount_val, 2));
    assert_eq!(partition_value, amount);

    let uuid_val = Value::UUID(Uuid::parse_str(uuid_str).unwrap());
    let lower = data_file.lower_bounds().as_ref().unwrap();
    let upper = data_file.upper_bounds().as_ref().unwrap();
    for bounds in [lower, upper] {
        assert_eq!(bounds[&0], amount);
        assert_eq!(bounds[&1], Value::Int(int_val));
        assert_eq!(bounds[&2], Value::LongInt(long_val));
        assert_eq!(bounds[&3], Value::Float(float_val.into()));
        assert_eq!(bounds[&4], Value::Double(double_val.into()));
        assert_eq!(bounds[&5], Value::Date(date_val));
        assert_eq!(bounds[&6], Value::Time(time_val));
        assert_eq!(bounds[&7], Value::Timestamp(ts_val));
        assert_eq!(bounds[&8], Value::TimestampTZ(tstz_val));
        assert_eq!(bounds[&9], uuid_val);
    }
}
