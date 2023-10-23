/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use anyhow::Result;
use chrono::{naive::NaiveDateTime, DateTime, Utc};
use object_store::ObjectMeta;
use std::{any::Any, collections::HashMap, ops::DerefMut, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::DataFusionError,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::FileScanConfig,
        TableProvider, ViewTable,
    },
    execution::context::SessionState,
    logical_expr::{TableSource, TableType},
    optimizer::utils::conjunction,
    physical_expr::create_physical_expr,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{ExecutionPlan, Statistics},
    prelude::Expr,
    scalar::ScalarValue,
    sql::parser::DFParser,
};
use url::Url;

use crate::pruning_statistics::{PruneDataFiles, PruneManifests};

use iceberg_rust::{
    catalog::relation::Relation,
    materialized_view::MaterializedView,
    spec::{types::StructField, view_metadata::ViewRepresentation},
    table::Table,
    util,
    view::View,
};
// mod value;

/// Iceberg table for datafusion
pub struct DataFusionTable {
    pub snapshot_range: (Option<i64>, Option<i64>),
    pub tabular: Relation,
}

impl core::ops::Deref for DataFusionTable {
    type Target = Relation;

    fn deref(self: &'_ DataFusionTable) -> &'_ Self::Target {
        &self.tabular
    }
}

impl DerefMut for DataFusionTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tabular
    }
}

impl From<Relation> for DataFusionTable {
    fn from(value: Relation) -> Self {
        DataFusionTable {
            tabular: value,
            snapshot_range: (None, None),
        }
    }
}

impl From<Table> for DataFusionTable {
    fn from(value: Table) -> Self {
        DataFusionTable {
            tabular: Relation::Table(value),
            snapshot_range: (None, None),
        }
    }
}

impl From<View> for DataFusionTable {
    fn from(value: View) -> Self {
        DataFusionTable {
            tabular: Relation::View(value),
            snapshot_range: (None, None),
        }
    }
}

impl From<MaterializedView> for DataFusionTable {
    fn from(value: MaterializedView) -> Self {
        DataFusionTable {
            tabular: Relation::MaterializedView(value),
            snapshot_range: (None, None),
        }
    }
}

impl DataFusionTable {
    pub fn new_table(table: Table, start: Option<i64>, end: Option<i64>) -> Self {
        DataFusionTable {
            tabular: Relation::Table(table),
            snapshot_range: (start, end),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for DataFusionTable {
    fn as_any(&self) -> &dyn Any {
        match &self.tabular {
            Relation::Table(table) => table,
            Relation::View(view) => view,
            Relation::MaterializedView(mv) => mv,
        }
    }
    fn schema(&self) -> SchemaRef {
        match &self.tabular {
            Relation::Table(table) => {
                let mut schema = table.schema().unwrap().clone();
                // Add the partition columns to the table schema
                for partition_field in &table.metadata().default_partition_spec().unwrap().fields {
                    schema.fields.fields.push(StructField {
                        id: partition_field.field_id,
                        name: partition_field.name.clone(),
                        field_type: schema
                            .fields
                            .get(partition_field.source_id as usize)
                            .unwrap()
                            .field_type
                            .tranform(&partition_field.transform)
                            .unwrap(),
                        required: true,
                        doc: None,
                    })
                }
                Arc::new((&schema.fields).try_into().unwrap())
            }
            Relation::View(view) => Arc::new((&view.schema().unwrap().fields).try_into().unwrap()),
            Relation::MaterializedView(mv) => {
                let table = mv.storage_table();
                let mut schema = table.schema().unwrap().clone();
                // Add the partition columns to the table schema
                for partition_field in &table.metadata().default_partition_spec().unwrap().fields {
                    schema.fields.fields.push(StructField {
                        id: partition_field.field_id,
                        name: partition_field.name.clone(),
                        field_type: schema
                            .fields
                            .get(partition_field.source_id as usize)
                            .unwrap()
                            .field_type
                            .tranform(&partition_field.transform)
                            .unwrap(),
                        required: true,
                        doc: None,
                    })
                }
                Arc::new((&schema.fields).try_into().unwrap())
            }
        }
    }
    fn table_type(&self) -> TableType {
        match &self.tabular {
            Relation::Table(_) => TableType::Base,
            Relation::View(_) => TableType::View,
            Relation::MaterializedView(_) => TableType::Base,
        }
    }
    async fn scan(
        &self,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match &self.tabular {
            Relation::View(view) => {
                let sql = match &view
                    .metadata()
                    .current_version()
                    .map_err(|err| DataFusionError::Internal(format!("{}", err)))?
                    .representations[0]
                {
                    ViewRepresentation::Sql { sql, .. } => sql,
                };
                let statement = DFParser::new(sql)?.parse_statement()?;
                let logical_plan = session.statement_to_plan(statement).await?;
                ViewTable::try_new(logical_plan, Some(sql.clone()))?
                    .scan(session, projection, filters, limit)
                    .await
            }
            Relation::Table(table) => {
                let schema = self.schema();
                let statistics = self
                    .statistics()
                    .await
                    .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
                table_scan(
                    table,
                    &self.snapshot_range,
                    schema,
                    statistics,
                    session,
                    projection,
                    filters,
                    limit,
                )
                .await
            }
            Relation::MaterializedView(mv) => {
                let table = mv.storage_table();
                let schema = self.schema();
                let statistics = self
                    .statistics()
                    .await
                    .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
                table_scan(
                    table,
                    &self.snapshot_range,
                    schema,
                    statistics,
                    session,
                    projection,
                    filters,
                    limit,
                )
                .await
            }
        }
    }
}
async fn table_scan(
    table: &Table,
    snapshot_range: &(Option<i64>, Option<i64>),
    schema: SchemaRef,
    statistics: Statistics,
    session: &SessionState,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // Create a unique URI for this particular object store
    let object_store_url = ObjectStoreUrl::parse(
        "iceberg://".to_owned() + &util::strip_prefix(&table.metadata().location).replace('/', "-"),
    )?;
    let url: &Url = object_store_url.as_ref();
    session
        .runtime_env()
        .register_object_store(url, table.object_store());

    // All files have to be grouped according to their partition values. This is done by using a HashMap with the partition values as the key.
    // This way data files with the same partition value are mapped to the same vector.
    let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
    // If there is a filter expression the manifests to read are pruned based on the pruning statistics available in the manifest_list file.
    let physical_predicate = if let Some(predicate) = conjunction(filters.iter().cloned()) {
        Some(create_physical_expr(
            &predicate,
            &schema.as_ref().clone().try_into()?,
            &schema,
            session.execution_props(),
        )?)
    } else {
        None
    };
    if let Some(physical_predicate) = physical_predicate.clone() {
        let pruning_predicate = PruningPredicate::try_new(physical_predicate, schema.clone())?;
        let manifests = table
            .manifests(snapshot_range.0, snapshot_range.1)
            .await
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        let manifests_to_prune =
            pruning_predicate.prune(&PruneManifests::new(table, &manifests))?;
        let data_files = table
            .datafiles(&manifests, Some(manifests_to_prune))
            .await
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        // After the first pruning stage the data_files are pruned again based on the pruning statistics in the manifest files.
        let files_to_prune = pruning_predicate.prune(&PruneDataFiles::new(table, &data_files))?;
        data_files
            .into_iter()
            .zip(files_to_prune.into_iter())
            .for_each(|(manifest, prune_file)| {
                if !prune_file {
                    let partition_values = manifest
                        .data_file
                        .partition
                        .iter()
                        .map(|value| match value {
                            Some(v) => ScalarValue::Utf8(Some(serde_json::to_string(v).unwrap())),
                            None => ScalarValue::Null,
                        })
                        .collect::<Vec<ScalarValue>>();
                    let object_meta = ObjectMeta {
                        location: util::strip_prefix(&manifest.data_file.file_path).into(),
                        size: manifest.data_file.file_size_in_bytes as usize,
                        last_modified: {
                            let last_updated_ms = table.metadata().last_updated_ms;
                            let secs = last_updated_ms / 1000;
                            let nsecs = (last_updated_ms % 1000) as u32 * 1000000;
                            DateTime::from_naive_utc_and_offset(
                                NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap(),
                                Utc,
                            )
                        },
                        e_tag: None,
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
            });
    } else {
        let manifests = table
            .manifests(snapshot_range.0, snapshot_range.1)
            .await
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        let data_files = table
            .datafiles(&manifests, None)
            .await
            .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        data_files.into_iter().for_each(|manifest| {
            let partition_values = manifest
                .data_file
                .partition
                .iter()
                .map(|value| match value {
                    Some(v) => ScalarValue::Utf8(Some(serde_json::to_string(v).unwrap())),
                    None => ScalarValue::Null,
                })
                .collect::<Vec<ScalarValue>>();
            let object_meta = ObjectMeta {
                location: util::strip_prefix(&manifest.data_file.file_path).into(),
                size: manifest.data_file.file_size_in_bytes as usize,
                last_modified: {
                    let last_updated_ms = table.metadata().last_updated_ms;
                    let secs = last_updated_ms / 1000;
                    let nsecs = (last_updated_ms % 1000) as u32 * 1000000;
                    DateTime::from_naive_utc_and_offset(
                        NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap(),
                        Utc,
                    )
                },
                e_tag: None,
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

    // Get all partition columns
    let table_partition_cols: Vec<(String, DataType)> = table
        .metadata()
        .default_partition_spec()
        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?
        .fields
        .iter()
        .map(|field| {
            Ok((
                field.name.clone(),
                (&table
                    .schema()?
                    .fields
                    .get(field.source_id as usize)
                    .unwrap()
                    .field_type
                    .tranform(&field.transform)?)
                    .try_into()?,
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;

    let file_scan_config = FileScanConfig {
        object_store_url,
        file_schema: schema,
        file_groups: file_groups.into_values().collect(),
        statistics,
        projection: projection.cloned(),
        limit,
        table_partition_cols,
        output_ordering: vec![],
        infinite_source: false,
    };

    ParquetFormat::default()
        .create_physical_plan(session, file_scan_config, physical_predicate.as_ref())
        .await
}

pub(crate) struct IcebergTableSource(DataFusionTable);

impl TableSource for IcebergTableSource {
    fn as_any(&self) -> &dyn Any {
        self.0.as_any()
    }
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

impl DataFusionTable {
    pub(crate) fn into_table_source(self) -> IcebergTableSource {
        IcebergTableSource(self)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use anyhow::anyhow;
    use datafusion::{
        arrow::{array::Float32Array, record_batch::RecordBatch},
        prelude::SessionContext,
    };
    use iceberg_rust::{
        catalog::{identifier::Identifier, memory::MemoryCatalog, relation::Relation, Catalog},
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        view::view_builder::ViewBuilder,
    };
    use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};

    use crate::DataFusionTable;

    #[tokio::test]
    pub async fn test_datafusion_table_scan() {
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("../iceberg-tests/nyc_taxis").unwrap());

        let catalog: Arc<dyn Catalog> =
            Arc::new(MemoryCatalog::new("test", object_store.clone()).unwrap());
        let identifier = Identifier::parse("test.table1").unwrap();

        catalog.clone().register_table(identifier.clone(), "/home/iceberg/warehouse/nyc/taxis/metadata/fb072c92-a02b-11e9-ae9c-1bb7bc9eca94.metadata.json").await.expect("Failed to register table.");

        let table = if let Relation::Table(table) = catalog
            .load_table(&identifier)
            .await
            .expect("Failed to load table")
        {
            Ok(Arc::new(DataFusionTable::from(table)))
        } else {
            Err(anyhow!("Relation must be a table"))
        }
        .unwrap();

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
            Arc::new(LocalFileSystem::new_with_prefix("../iceberg-tests/nyc_taxis").unwrap());

        let memory_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let catalog: Arc<dyn Catalog> =
            Arc::new(MemoryCatalog::new("test", object_store.clone()).unwrap());
        let memory_catalog: Arc<dyn Catalog> =
            Arc::new(MemoryCatalog::new("test", memory_object_store.clone()).unwrap());
        let identifier = Identifier::parse("test.table1").unwrap();

        catalog.clone().register_table(identifier.clone(), "/home/iceberg/warehouse/nyc/taxis/metadata/fb072c92-a02b-11e9-ae9c-1bb7bc9eca94.metadata.json").await.expect("Failed to register table.");

        let table = if let Relation::Table(table) = catalog
            .clone()
            .load_table(&identifier)
            .await
            .expect("Failed to load table")
        {
            Ok(Arc::new(DataFusionTable::from(table)))
        } else {
            Err(anyhow!("Relation must be a table"))
        }
        .unwrap();

        let ctx = SessionContext::new();

        ctx.register_table("nyc_taxis", table).unwrap();

        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            fields: StructType::new(vec![
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
            ]),
        };
        let view_identifier = Identifier::parse("test.view1").unwrap();

        let mut builder = ViewBuilder::new(
            "SELECT vendor_id, MIN(trip_distance) FROM nyc_taxis GROUP BY vendor_id",
            schema,
            view_identifier,
            memory_catalog,
        )
        .expect("Failed to create filesystem view builder.");
        builder.location("test/nyc_taxis_view");

        let view = Arc::new(DataFusionTable::from(
            builder
                .build()
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
