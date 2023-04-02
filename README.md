# Rust implementation of [Apache Iceberg](https://iceberg.apache.org)

## [Iceberg-rust](iceberg-rust/README.md)

Low level rust implementation of the Apache iceberg specification. Can be used to access iceberg catalogs and create and load iceberg tables.

## [Datafusion_iceberg](datafusion_iceberg/README.md)

Use apache iceberg with datafusion. Provides implementations of `TableProvider`, `SchemaProvider` and `CatalogProvider`. Currently only supports reading iceberg tables.

## [Iceberg REST Catalog](iceberg_catalog_rest_client_rust/README.md)

Is a http client for the Iceberg REST Catalog API that implements the rust Iceberg catalog trait.

## [Iceberg-c](iceberg-c/README.md)

C bindings for iceberg-rust

# Example

```rust
use std::sync::Arc;

use datafusion::{
    arrow::array::{Float32Array, RecordBatch},
    prelude::SessionContext,
};
use iceberg_rust::table::Table;
use object_store::{local::LocalFileSystem, ObjectStore};

use datafusion_iceberg::DataFusionTable;

fn main() {
    let object_store: Arc<dyn ObjectStore> =
    Arc::new(LocalFileSystem::new_with_prefix("./tests").unwrap());

    let table = Arc::new(DataFusionTable::from(
        Table::load_file_system_table("/home/iceberg/warehouse/nyc/taxis", &object_store)
            .await
            .unwrap(),
    ));

    let ctx = SessionContext::new();

    ctx.register_table("nyc_taxis", table).unwrap();

    let df = ctx
        .sql("SELECT vendor_id, MIN(trip_distance) FROM nyc_taxis GROUP BY vendor_id")
        .await
        .unwrap();

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await.expect("Failed to execute query plan.");

    let batch = results
        .into_iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("All record batches are empty");

    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Failed to get values from batch.");

    // Value can either be 0.9 or 1.8
    assert!(((1.35 - values.value(0)).abs() - 0.45).abs() < 0.001)
}
```
