/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use std::ops::DerefMut;

use anyhow::Result;

use datafusion::{datasource::ViewTable, prelude::SessionContext};

use crate::{model::view_metadata::Representation, view::View};

/// Iceberg table for datafusion
pub struct DataFusionView(View);

impl core::ops::Deref for DataFusionView {
    type Target = View;

    fn deref(self: &'_ DataFusionView) -> &'_ Self::Target {
        &self.0
    }
}

impl DerefMut for DataFusionView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<View> for DataFusionView {
    fn from(value: View) -> Self {
        DataFusionView(value)
    }
}

impl DataFusionView {
    /// Get DataFusion View Table that implements TableProvider.
    pub fn to_view_table(&self, ctx: &SessionContext) -> Result<ViewTable> {
        match self.metadata().representation() {
            Representation::Sql { sql, .. } => {
                let logical_plan = ctx.create_logical_plan(sql)?;
                ViewTable::try_new(logical_plan, Some(sql.to_string()))
            }
        }
        .map_err(anyhow::Error::msg)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use datafusion::{
        arrow::{array::Float32Array, record_batch::RecordBatch},
        prelude::SessionContext,
    };
    use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};

    use crate::{
        datafusion::DataFusionTable,
        model::schema::{AllType, PrimitiveType, SchemaStruct, SchemaV2, StructField},
        table::Table,
        view::view_builder::ViewBuilder,
    };

    use super::*;

    #[tokio::test]
    pub async fn test_datafusion_view_scan() {
        let local_object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("./tests").unwrap());
        let memory_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let table = Arc::new(DataFusionTable::from(
            Table::load_file_system_table("/home/iceberg/warehouse/nyc/taxis", &local_object_store)
                .await
                .unwrap(),
        ));

        let ctx = SessionContext::new();

        ctx.register_table("nyc_taxis", table).unwrap();

        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1]),
            name_mapping: None,
            struct_fields: SchemaStruct {
                fields: vec![StructField {
                    id: 1,
                    name: "trip_distance".to_string(),
                    required: false,
                    field_type: AllType::Primitive(PrimitiveType::Float),
                    doc: None,
                }],
            },
        };

        let view = Arc::new(
            DataFusionView::from(
                ViewBuilder::new_filesystem_view(
                    "SELECT trip_distance FROM nyc_taxis",
                    "test/view1",
                    schema,
                    Arc::clone(&memory_object_store),
                )
                .unwrap()
                .commit()
                .await
                .unwrap(),
            )
            .to_view_table(&ctx)
            .unwrap(),
        );

        ctx.register_table("nyc_taxis_trip_distance", view).unwrap();

        let df = ctx
            .sql("SELECT MIN(trip_distance) FROM nyc_taxis_trip_distance")
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
            .downcast_ref::<Float32Array>()
            .expect("Failed to get values from batch.");

        // Value can either be 0.9 or 1.8
        assert!(((1.35 - values.value(0)).abs() - 0.45).abs() < 0.001)
    }
}
