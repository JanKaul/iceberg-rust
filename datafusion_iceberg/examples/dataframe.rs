use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{StructField, StructType};
use iceberg_rust::table::Table;
use iceberg_sql_catalog::SqlCatalog;
use object_store::memory::InMemory;
use object_store::ObjectStore;

use std::sync::Arc;

#[tokio::main]
pub(crate) async fn main() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "test", object_store.clone())
            .await
            .unwrap(),
    );

    let identifier = Identifier::new(&["public".to_string()], "bank_account");

    // Create iceberg table in the catalog
    Table::builder()
        .with_name("bank_account")
        .with_location("/bank_account")
        .with_schema(
            Schema::builder()
                .with_fields(
                    StructType::builder()
                        .with_struct_field(StructField {
                            id: 0,
                            name: "id".to_owned(),
                            required: true,
                            field_type: iceberg_rust::spec::types::Type::Primitive(
                                iceberg_rust::spec::types::PrimitiveType::Int,
                            ),
                            doc: None,
                        })
                        .with_struct_field(StructField {
                            id: 1,
                            name: "bank_account".to_owned(),
                            required: false,
                            field_type: iceberg_rust::spec::types::Type::Primitive(
                                iceberg_rust::spec::types::PrimitiveType::Int,
                            ),
                            doc: None,
                        })
                        .build()
                        .unwrap(),
                )
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

    let data = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "bank_account",
            Arc::new(Int32Array::from(vec![9000, 8000, 7000])),
        ),
    ])
    .unwrap();

    let input = ctx.read_batch(data).unwrap();

    // Write the data from the input dataframe to the table
    input
        .write_table(
            "warehouse.public.bank_account",
            DataFrameWriteOptions::default(),
        )
        .await
        .unwrap();

    let df = ctx
        .sql("Select sum(bank_account) from warehouse.public.bank_account")
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
        .downcast_ref::<Int64Array>()
        .expect("Failed to get values from batch.");

    assert_eq!(values.value(0), 24000);
}
