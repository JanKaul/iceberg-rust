# Rust implementation of [Apache Iceberg](https://iceberg.apache.org)

Apache Iceberg is Open Table Format that brings ACID quarantees to large analytic datasets. 
This repository contains a Rust implementation of Apache Iceberg that focuses on the interoperability with the Arrow ecosystem.
It provides an Iceberg integration for the [Datafusion](https://arrow.apache.org/datafusion/) query engine.

[![Crates.io][crates-badge]][crates-url]
[![Apache V2.0 licensed][apache-badge]][apache-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/iceberg-rust
[crates-url]: https://crates.io/crates/iceberg-rust
[apache-badge]: https://img.shields.io/badge/License-Apache_2.0-blue.svg
[apache-url]: https://github.com/JanKaul/iceberg-rust/blob/main/LICENSE
[actions-badge]: https://github.com/JanKaul/iceberg-rust/actions/workflows/rust.yml/badge.svg?branch=main
[actions-url]: https://github.com/JanKaul/iceberg-rust/actions/workflows/rust.yml

## Features

### Iceberg tables

| Feature | Status |
| --- | --- |
| Read | :white_check_mark: |
| Read partitioned | :white_check_mark: |
| Insert | :white_check_mark: |
| Insert partitioned | :white_check_mark: |
| Equality deletes | :white_check_mark: |
| Positional deletes | |

### Table Maintenance

| Feature | Status |
| --- | --- |
| Expire snapshots | :white_check_mark: |
| Orphan file cleanup | :white_check_mark: |

### Iceberg Views

| Feature | Status |
| --- | --- |
| Read | :white_check_mark: |

### Iceberg Materialized Views

| Feature | Status |
| --- | --- |
| Read | :white_check_mark: |
| Full refresh | :white_check_mark: |
| Incremental refresh | :white_check_mark: |

### Catalogs

- REST
- S3Tables
- Filesystem
- Glue
- RDBMS (Postgres, MySQL)

### File formats

- parquet

### Integrations

- [Datafusion](https://arrow.apache.org/datafusion/)

## Example

Check out the [datafusion examples](datafusion_iceberg/examples).

### Basic Table Operations

```rust
use datafusion::{arrow::array::Int64Array, prelude::SessionContext};
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::{
    catalog::Catalog,
    spec::{
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, StructType, Type},
    },
    table::Table,
};
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

    let table = Table::builder()
        .with_name("orders")
        .with_location("/test/orders")
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .build(&["test".to_owned()], catalog)
        .await
        .expect("Failed to create table");

    let table = Arc::new(DataFusionTable::from(table));

    let ctx = SessionContext::new();

    ctx.register_table("orders", table).unwrap();

    ctx.sql(
        "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
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
        .sql("select product_id, sum(amount) from orders group by product_id;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    for batch in batches {
        if batch.num_rows() != 0 {
            let (product_ids, amounts) = (
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
            for (product_id, amount) in product_ids.iter().zip(amounts) {
                if product_id.unwrap() == 1 {
                    assert_eq!(amount.unwrap(), 7)
                } else if product_id.unwrap() == 2 {
                    assert_eq!(amount.unwrap(), 1)
                } else if product_id.unwrap() == 3 {
                    assert_eq!(amount.unwrap(), 3)
                } else {
                    panic!("Unexpected product id")
                }
            }
        }
    }

    ctx.sql(
        "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
        (7, 1, 3, '2020-01-03', 1),
        (8, 2, 1, '2020-01-03', 2),
        (9, 2, 2, '2020-01-03', 1);",
    )
    .await
    .expect("Failed to create query plan for insert")
    .collect()
    .await
    .expect("Failed to insert values into table");

    let batches = ctx
        .sql("select product_id, sum(amount) from orders group by product_id;")
        .await
        .expect("Failed to create plan for select")
        .collect()
        .await
        .expect("Failed to execute select query");

    for batch in batches {
        if batch.num_rows() != 0 {
            let (product_ids, amounts) = (
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
            for (product_id, amount) in product_ids.iter().zip(amounts) {
                if product_id.unwrap() == 1 {
                    assert_eq!(amount.unwrap(), 9)
                } else if product_id.unwrap() == 2 {
                    assert_eq!(amount.unwrap(), 2)
                } else if product_id.unwrap() == 3 {
                    assert_eq!(amount.unwrap(), 4)
                } else {
                    panic!("Unexpected product id")
                }
            }
        }
    }
}
```
