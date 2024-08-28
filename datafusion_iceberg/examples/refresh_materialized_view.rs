use datafusion::{arrow::array::Int64Array, prelude::SessionContext};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use datafusion_iceberg::materialized_view::refresh_materialized_view;
use iceberg_rust::catalog::CatalogList;
use iceberg_rust::materialized_view::MaterializedView;
use iceberg_rust::spec::partition::PartitionSpec;
use iceberg_rust::spec::view_metadata::{Version, ViewRepresentation};
use iceberg_rust::spec::{
    partition::{PartitionField, Transform},
    schema::Schema,
    types::{PrimitiveType, StructField, StructType, Type},
};
use iceberg_rust::table::Table;
use iceberg_sql_catalog::SqlCatalogList;
use object_store::memory::InMemory;
use object_store::ObjectStore;

use std::sync::Arc;
#[tokio::main]
pub(crate) async fn main() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let catalog_list = Arc::new(
        SqlCatalogList::new("sqlite://", object_store.clone())
            .await
            .unwrap(),
    );

    let catalog = catalog_list.catalog("iceberg").unwrap();

    let schema = Schema::builder()
        .with_fields(
            StructType::builder()
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
                .unwrap(),
        )
        .build()
        .unwrap();

    let partition_spec = PartitionSpec::builder()
        .with_partition_field(PartitionField::new(4, 1000, "day", Transform::Day))
        .build()
        .expect("Failed to create partition spec");

    Table::builder()
        .with_name("orders")
        .with_location("/test/orders")
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("Failed to create table");

    let matview_schema = Schema::builder()
        .with_fields(
            StructType::builder()
                .with_struct_field(StructField {
                    id: 1,
                    name: "product_id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Long),
                    doc: None,
                })
                .with_struct_field(StructField {
                    id: 2,
                    name: "amount".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                })
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let mut matview = MaterializedView::builder()
        .with_name("orders_view")
        .with_location("test/orders_view")
        .with_schema(matview_schema)
        .with_view_version(
            Version::builder()
                .with_representation(ViewRepresentation::sql(
                    "select product_id, amount from iceberg.test.orders where product_id < 3;",
                    None,
                ))
                .build()
                .unwrap(),
        )
        .build(&["test".to_owned()], catalog.clone())
        .await
        .expect("Failed to create materialized view");

    let total_matview_schema = Schema::builder()
        .with_fields(
            StructType::builder()
                .with_struct_field(StructField {
                    id: 1,
                    name: "product_id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Long),
                    doc: None,
                })
                .with_struct_field(StructField {
                    id: 2,
                    name: "amount".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Long),
                    doc: None,
                })
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let mut total_matview = MaterializedView::builder()
            .with_name("total_orders")
            .with_location("test/total_orders")
            .with_schema(total_matview_schema)
            .with_view_version(
                Version::builder()
                    .with_representation(ViewRepresentation::sql(
                        "select product_id, sum(amount) from iceberg.test.orders_view group by product_id;",
                        None,
                    ))
                    .build()
                    .unwrap(),
            )
            .build(&["test".to_owned()], catalog.clone())
            .await
            .expect("Failed to create materialized view");

    // Datafusion

    let datafusion_catalog = Arc::new(
        IcebergCatalog::new(catalog, None)
            .await
            .expect("Failed to create datafusion catalog"),
    );

    let ctx = SessionContext::new();

    ctx.register_catalog("iceberg", datafusion_catalog);

    ctx.sql(
        "INSERT INTO iceberg.test.orders (id, customer_id, product_id, date, amount) VALUES 
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

    refresh_materialized_view(&mut matview, catalog_list.clone(), None)
        .await
        .expect("Failed to refresh materialized view");

    let batches = ctx
        .sql("select product_id, sum(amount) from iceberg.test.orders_view group by product_id;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    for batch in batches {
        if batch.num_rows() != 0 {
            let (order_ids, amounts) = (
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            );
            for (order_id, amount) in order_ids.iter().zip(amounts) {
                if order_id.unwrap() == 1 {
                    assert_eq!(amount.unwrap(), 7)
                } else if order_id.unwrap() == 2 {
                    assert_eq!(amount.unwrap(), 1)
                } else {
                    panic!("Unexpected order id")
                }
            }
        }
    }

    ctx.sql(
        "INSERT INTO iceberg.test.orders (id, customer_id, product_id, date, amount) VALUES 
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1);",
    )
    .await
    .expect("Failed to create query plan for insert")
    .collect()
    .await
    .expect("Failed to insert values into table");

    refresh_materialized_view(&mut matview, catalog_list.clone(), None)
        .await
        .expect("Failed to refresh materialized view");

    let batches = ctx
        .sql("select product_id, sum(amount) from iceberg.test.orders_view group by product_id;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    for batch in batches {
        if batch.num_rows() != 0 {
            let (order_ids, amounts) = (
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            );
            for (order_id, amount) in order_ids.iter().zip(amounts) {
                if order_id.unwrap() == 1 {
                    assert_eq!(amount.unwrap(), 9)
                } else if order_id.unwrap() == 2 {
                    assert_eq!(amount.unwrap(), 2)
                } else {
                    panic!("Unexpected order id")
                }
            }
        }
    }

    refresh_materialized_view(&mut total_matview, catalog_list.clone(), None)
        .await
        .expect("Failed to refresh materialized view");

    let batches = ctx
        .sql("select product_id, amount from iceberg.test.total_orders;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    for batch in batches {
        if batch.num_rows() != 0 {
            let (order_ids, amounts) = (
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
            );
            for (order_id, amount) in order_ids.iter().zip(amounts) {
                if order_id.unwrap() == 1 {
                    assert_eq!(amount.unwrap(), 9)
                } else if order_id.unwrap() == 2 {
                    assert_eq!(amount.unwrap(), 2)
                } else {
                    panic!("Unexpected order id")
                }
            }
        }
    }
}
