use datafusion::arrow::array::Float32Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_expr::{col, min};
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::Catalog;
use iceberg_sql_catalog::SqlCatalog;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use std::sync::Arc;

#[tokio::main]
pub(crate) async fn main() {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix("iceberg-tests/nyc_taxis").unwrap());

    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "test", object_store.clone())
            .await
            .unwrap(),
    );
    let identifier = Identifier::parse("test.table1").unwrap();

    let table = catalog.clone().register_table(identifier.clone(), "/home/iceberg/warehouse/nyc/taxis/metadata/fb072c92-a02b-11e9-ae9c-1bb7bc9eca94.metadata.json").await.expect("Failed to register table.");

    let ctx = SessionContext::new();

    let df = ctx
        .read_table(Arc::new(DataFusionTable::from(table)))
        .expect("Failed to read table")
        .select(vec![col("vendor_id"), col("trip_distance")])
        .unwrap()
        .aggregate(vec![col("vendor_id")], vec![min(col("trip_distance"))])
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
