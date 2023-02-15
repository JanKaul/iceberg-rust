/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use anyhow::Result;
use chrono::{naive::NaiveDateTime, DateTime, Utc};
use object_store::ObjectMeta;
use std::{any::Any, collections::HashMap, ops::DerefMut, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef},
    common::DataFusionError,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        TableProvider, ViewTable,
    },
    execution::context::SessionState,
    logical_expr::TableType,
    optimizer::utils::conjunction,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{file_format::FileScanConfig, ExecutionPlan},
    prelude::Expr,
    scalar::ScalarValue,
    sql::parser::DFParser,
};
use url::Url;

use crate::pruning_statistics::{PruneDataFiles, PruneManifests};

use iceberg_rust::{
    arrow::schema::iceberg_to_arrow_schema, catalog::relation::Relation,
    model::view_metadata::Representation, table::Table, util, view::View,
};
// mod value;

/// Iceberg table for datafusion
pub struct DataFusionTable(pub Relation);

impl core::ops::Deref for DataFusionTable {
    type Target = Relation;

    fn deref(self: &'_ DataFusionTable) -> &'_ Self::Target {
        &self.0
    }
}

impl DerefMut for DataFusionTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Relation> for DataFusionTable {
    fn from(value: Relation) -> Self {
        DataFusionTable(value)
    }
}

impl From<Table> for DataFusionTable {
    fn from(value: Table) -> Self {
        DataFusionTable(Relation::Table(value))
    }
}

impl From<View> for DataFusionTable {
    fn from(value: View) -> Self {
        DataFusionTable(Relation::View(value))
    }
}

#[async_trait::async_trait]
impl TableProvider for DataFusionTable {
    fn as_any(&self) -> &dyn Any {
        match &self.0 {
            Relation::Table(table) => table,
            Relation::View(view) => view,
        }
    }
    fn schema(&self) -> SchemaRef {
        let schema = match &self.0 {
            Relation::Table(table) => table.schema(),
            Relation::View(view) => view.schema().unwrap(),
        };
        Arc::new(iceberg_to_arrow_schema(schema).unwrap())
    }
    fn table_type(&self) -> TableType {
        match &self.0 {
            Relation::Table(_) => TableType::Base,
            Relation::View(_) => TableType::View,
        }
    }
    async fn scan(
        &self,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match &self.0 {
            Relation::View(view) => {
                let sql = match view.metadata().representation() {
                    Representation::Sql { sql, .. } => sql,
                };
                let statement = DFParser::new(sql)?.parse_statement()?;
                let logical_plan = session.statement_to_plan(statement).await?;
                ViewTable::try_new(logical_plan, Some(sql.clone()))?
                    .scan(session, projection, filters, limit)
                    .await
            }
            Relation::Table(table) => {
                let schema = self.schema();

                // Create a unique URI for this particular object store
                let object_store_url = ObjectStoreUrl::parse(
                    "iceberg://".to_owned()
                        + &util::strip_prefix(table.metadata().location()).replace('/', "-"),
                )?;
                let url: &Url = object_store_url.as_ref();
                session.runtime_env().register_object_store(
                    url.scheme(),
                    url.host_str().unwrap_or_default(),
                    table.object_store(),
                );

                // All files have to be grouped according to their partition values. This is done by using a HashMap with the partition values as the key.
                // This way data files with the same partition value are mapped to the same vector.
                let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> =
                    HashMap::new();
                // If there is a filter expression the manifests to read are pruned based on the pruning statistics available in the manifest_list file.
                if let Some(Some(predicate)) =
                    (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
                {
                    let pruning_predicate = PruningPredicate::try_new(predicate, schema.clone())?;
                    let manifests_to_prune =
                        pruning_predicate.prune(&PruneManifests::from(table))?;
                    let files = table
                        .files(Some(manifests_to_prune))
                        .await
                        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
                    // After the first pruning stage the data_files are pruned again based on the pruning statistics in the manifest files.
                    let files_to_prune =
                        pruning_predicate.prune(&PruneDataFiles::new(table, &files))?;
                    files.into_iter().zip(files_to_prune.into_iter()).for_each(
                        |(manifest, prune_file)| {
                            if !prune_file {
                                let partition_values = manifest
                                    .partition_values()
                                    .iter()
                                    .map(|value| match value {
                                        Some(v) => ScalarValue::Utf8(Some(
                                            serde_json::to_string(v).unwrap(),
                                        )),
                                        None => ScalarValue::Null,
                                    })
                                    .collect::<Vec<ScalarValue>>();
                                let object_meta = ObjectMeta {
                                    location: util::strip_prefix(manifest.file_path()).into(),
                                    size: manifest.file_size_in_bytes() as usize,
                                    last_modified: {
                                        let last_updated_ms = table.metadata().last_updated_ms();
                                        let secs = last_updated_ms / 1000;
                                        let nsecs = (last_updated_ms % 1000) as u32 * 1000000;
                                        DateTime::from_utc(
                                            NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap(),
                                            Utc,
                                        )
                                    },
                                };
                                let file = PartitionedFile {
                                    object_meta,
                                    partition_values,
                                    range: None,
                                    extensions: None,
                                };
                                file_groups
                                    .entry(file.partition_values.clone())
                                    .or_default()
                                    .push(file);
                            };
                        },
                    );
                } else {
                    let files = table
                        .files(None)
                        .await
                        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
                    files.into_iter().for_each(|manifest| {
                        let partition_values = manifest
                            .partition_values()
                            .iter()
                            .map(|value| match value {
                                Some(v) => {
                                    ScalarValue::Utf8(Some(serde_json::to_string(v).unwrap()))
                                }
                                None => ScalarValue::Null,
                            })
                            .collect::<Vec<ScalarValue>>();
                        let object_meta = ObjectMeta {
                            location: util::strip_prefix(manifest.file_path()).into(),
                            size: manifest.file_size_in_bytes() as usize,
                            last_modified: {
                                let last_updated_ms = table.metadata().last_updated_ms();
                                let secs = last_updated_ms / 1000;
                                let nsecs = (last_updated_ms % 1000) as u32 * 1000000;
                                DateTime::from_utc(
                                    NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap(),
                                    Utc,
                                )
                            },
                        };
                        let file = PartitionedFile {
                            object_meta,
                            partition_values,
                            range: None,
                            extensions: None,
                        };
                        file_groups
                            .entry(file.partition_values.clone())
                            .or_default()
                            .push(file);
                    });
                };

                let statistics = self
                    .statistics()
                    .await
                    .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;

                // Get all partition columns
                let table_partition_cols: Vec<(String, DataType)> = table
                    .metadata()
                    .default_spec()
                    .iter()
                    .map(|field| {
                        (
                            field.name.clone(),
                            schema.field(field.field_id as usize).data_type().clone(),
                        )
                    })
                    .collect();

                // Remove the partition columns from the schema. The values for the partition column are stored in the partition values
                let file_schema = Arc::new(ArrowSchema::new(
                    schema
                        .fields()
                        .iter()
                        .filter(|field| {
                            !table_partition_cols
                                .contains(&(field.name().clone(), field.data_type().clone()))
                        })
                        .cloned()
                        .collect(),
                ));

                // Get the ids of the partition columns
                let partition_ids: Vec<usize> = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, field)| {
                        if table_partition_cols
                            .contains(&(field.name().clone(), field.data_type().clone()))
                        {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                    .collect();

                // Change the projection according to the previous schema change
                let projection = projection.clone().map(|projection| {
                    projection
                        .iter()
                        .map(|idx| {
                            if partition_ids.contains(idx) {
                                file_schema.fields.len()
                                    + partition_ids.iter().position(|x| x == idx).unwrap()
                            } else {
                                partition_ids
                                    .iter()
                                    .fold(*idx, |acc, x| if idx > x { acc - 1 } else { acc })
                            }
                        })
                        .collect()
                });

                let file_scan_config = FileScanConfig {
                    object_store_url,
                    file_schema,
                    file_groups: file_groups.into_values().collect(),
                    statistics,
                    projection,
                    limit,
                    table_partition_cols,
                    output_ordering: None,
                    infinite_source: false,
                };
                ParquetFormat::default()
                    .create_physical_plan(session, file_scan_config, filters)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use datafusion::{
        arrow::{array::Float32Array, record_batch::RecordBatch},
        prelude::SessionContext,
    };
    use iceberg_rust::{
        model::{
            data_types::{PrimitiveType, StructField, StructType, Type},
            schema::SchemaV2,
        },
        table::Table,
        view::view_builder::ViewBuilder,
    };
    use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};

    use crate::DataFusionTable;

    #[tokio::test]
    pub async fn test_datafusion_table_scan() {
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

    #[tokio::test]
    pub async fn test_datafusion_view_scan() {
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("./tests").unwrap());

        let memory_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let table = Arc::new(DataFusionTable::from(
            Table::load_file_system_table("/home/iceberg/warehouse/nyc/taxis", &object_store)
                .await
                .unwrap(),
        ));

        let ctx = SessionContext::new();

        ctx.register_table("nyc_taxis", table).unwrap();

        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            fields: StructType {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "vendor_id".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::Int),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "min_trip_distance".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::Float),
                        doc: None,
                    },
                ],
            },
        };
        let view = Arc::new(DataFusionTable::from(
            ViewBuilder::new_filesystem_view(
                "SELECT vendor_id, MIN(trip_distance) FROM nyc_taxis GROUP BY vendor_id",
                "test/nyc_taxis_view",
                schema,
                Arc::clone(&memory_object_store),
            )
            .expect("Failed to create filesystem view builder.")
            .commit()
            .await
            .expect("Failed to create filesystem view"),
        ));

        ctx.register_table("nyc_taxis_view", view).unwrap();

        let df = ctx
            .sql("SELECT vendor_id, min_trip_distance FROM nyc_taxis_view")
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
}
