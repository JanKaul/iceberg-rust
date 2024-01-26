/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use async_trait::async_trait;
use chrono::{naive::NaiveDateTime, DateTime, Utc};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::{RwLock, RwLockWriteGuard};

use datafusion::{
    arrow::datatypes::{Field, SchemaRef},
    common::{not_impl_err, plan_err, DataFusionError, SchemaExt},
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::FileScanConfig,
        TableProvider, ViewTable,
    },
    execution::{context::SessionState, TaskContext},
    logical_expr::{TableProviderFilterPushDown, TableType},
    optimizer::utils::conjunction,
    physical_expr::create_physical_expr,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        insert::{DataSink, FileSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
    prelude::Expr,
    scalar::ScalarValue,
    sql::parser::DFParser,
};

use crate::{
    error::Error,
    pruning_statistics::{PruneDataFiles, PruneManifests},
};

use iceberg_rust::{
    arrow::write::write_parquet_partitioned, catalog::tabular::Tabular,
    materialized_view::MaterializedView, table::Table, view::View,
};
use iceberg_rust_spec::spec::{types::StructField, view_metadata::ViewRepresentation};
use iceberg_rust_spec::util;
// mod value;

#[derive(Debug, Clone)]
/// Iceberg table for datafusion
pub struct DataFusionTable {
    pub tabular: Arc<RwLock<Tabular>>,
    pub schema: SchemaRef,
    pub snapshot_range: (Option<i64>, Option<i64>),
    pub branch: Option<String>,
}

impl From<Tabular> for DataFusionTable {
    fn from(value: Tabular) -> Self {
        Self::new(value, None, None, None)
    }
}

impl From<Table> for DataFusionTable {
    fn from(value: Table) -> Self {
        Self::new(Tabular::Table(value), None, None, None)
    }
}

impl From<View> for DataFusionTable {
    fn from(value: View) -> Self {
        Self::new(Tabular::View(value), None, None, None)
    }
}

impl From<MaterializedView> for DataFusionTable {
    fn from(value: MaterializedView) -> Self {
        Self::new(Tabular::MaterializedView(value), None, None, None)
    }
}

impl DataFusionTable {
    pub fn new(
        tabular: Tabular,
        start: Option<i64>,
        end: Option<i64>,
        branch: Option<&str>,
    ) -> Self {
        let schema = match &tabular {
            Tabular::Table(table) => {
                let schema = end
                    .and_then(|snapshot_id| table.metadata().schema(snapshot_id).ok().cloned())
                    .unwrap_or_else(|| table.current_schema(None).unwrap().clone());
                Arc::new((&schema.fields).try_into().unwrap())
            }
            Tabular::View(view) => {
                let schema = end
                    .and_then(|version_id| view.metadata().schema(version_id).ok().cloned())
                    .unwrap_or_else(|| view.current_schema(None).unwrap().clone());
                Arc::new((&schema.fields).try_into().unwrap())
            }
            Tabular::MaterializedView(matview) => {
                let schema = end
                    .and_then(|version_id| matview.metadata().schema(version_id).ok().cloned())
                    .unwrap_or_else(|| matview.current_schema(None).unwrap().clone());
                Arc::new((&schema.fields).try_into().unwrap())
            }
        };
        DataFusionTable {
            tabular: Arc::new(RwLock::new(tabular)),
            snapshot_range: (start, end),
            schema,
            branch: branch.map(ToOwned::to_owned),
        }
    }
    #[inline]
    pub fn new_table(
        table: Table,
        start: Option<i64>,
        end: Option<i64>,
        branch: Option<&str>,
    ) -> Self {
        Self::new(Tabular::Table(table), start, end, branch)
    }

    pub async fn inner_mut(&self) -> RwLockWriteGuard<'_, Tabular> {
        self.tabular.write().await
    }
}

#[async_trait]
impl TableProvider for DataFusionTable {
    fn as_any(&self) -> &dyn Any {
        &self.tabular
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match self.tabular.read().await.deref() {
            Tabular::View(view) => {
                let metadata = view.metadata();
                let version = self
                    .snapshot_range
                    .1
                    .and_then(|version_id| metadata.versions.get(&version_id))
                    .unwrap_or(metadata.current_version(None).map_err(Error::from)?);
                let sql = match &version.representations[0] {
                    ViewRepresentation::Sql { sql, .. } => sql,
                };
                let statement = DFParser::new(sql)?.parse_statement()?;
                let logical_plan = session.statement_to_plan(statement).await?;
                ViewTable::try_new(logical_plan, Some(sql.clone()))?
                    .scan(session, projection, filters, limit)
                    .await
            }
            Tabular::Table(table) => {
                let schema = self.schema();
                let statistics = self.statistics().await.map_err(Into::<Error>::into)?;
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
            Tabular::MaterializedView(mv) => {
                let table = mv.storage_table(None).await.map_err(Error::from)?;
                let schema = self.schema();
                let statistics = self.statistics().await.map_err(Into::<Error>::into)?;
                table_scan(
                    &table,
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
    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        if !self.schema().equivalent_names_and_types(&input.schema()) {
            return plan_err!("Inserting query must have the same schema with the table.");
        }
        if overwrite {
            return not_impl_err!("Overwrite not implemented for MemoryTable yet");
        }
        Ok(Arc::new(FileSinkExec::new(
            input,
            Arc::new(self.clone().into_data_sink()),
            self.schema.clone(),
            None,
        )))
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

#[allow(clippy::too_many_arguments)]
async fn table_scan(
    table: &Table,
    snapshot_range: &(Option<i64>, Option<i64>),
    arrow_schema: SchemaRef,
    statistics: Statistics,
    session: &SessionState,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let schema = snapshot_range
        .1
        .and_then(|snapshot_id| table.metadata().schema(snapshot_id).ok().cloned())
        .unwrap_or_else(|| table.current_schema(None).unwrap().clone());

    // Create a unique URI for this particular object store
    let object_store_url = ObjectStoreUrl::parse(
        "iceberg://".to_owned() + &util::strip_prefix(&table.metadata().location).replace('/', "-"),
    )?;
    session
        .runtime_env()
        .register_object_store(object_store_url.as_ref(), table.object_store());

    // All files have to be grouped according to their partition values. This is done by using a HashMap with the partition values as the key.
    // This way data files with the same partition value are mapped to the same vector.
    let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

    let partition_column_names = table
        .metadata()
        .default_partition_spec()
        .map_err(Error::from)?
        .fields
        .iter()
        .map(|x| {
            Ok(schema
                .fields
                .get(x.source_id as usize)
                .ok_or(Error::NotFound(
                    "Field".to_string(),
                    x.source_id.to_string(),
                ))?
                .name
                .clone())
        })
        .collect::<Result<HashSet<_>, Error>>()?;

    // If there is a filter expression the manifests to read are pruned based on the pruning statistics available in the manifest_list file.
    let physical_predicate = if let Some(predicate) = conjunction(filters.iter().cloned()) {
        Some(create_physical_expr(
            &predicate,
            &arrow_schema.as_ref().clone().try_into()?,
            &arrow_schema,
            session.execution_props(),
        )?)
    } else {
        None
    };
    if let Some(physical_predicate) = physical_predicate.clone() {
        let partition_predicates = conjunction(
            filters
                .iter()
                .filter(|expr| {
                    if let Ok(set) = expr.to_columns() {
                        let set: HashSet<String> =
                            set.into_iter().map(|x| x.name.clone()).collect();
                        set.is_subset(&partition_column_names)
                    } else {
                        false
                    }
                })
                .cloned(),
        );

        let manifests = table
            .manifests(snapshot_range.0, snapshot_range.1)
            .await
            .map_err(Into::<Error>::into)?;

        // If there is a filter expression on the partition column, the manifest files to read are pruned.
        let data_files = if let Some(predicate) = partition_predicates {
            let physical_partition_predicate = create_physical_expr(
                &predicate,
                &arrow_schema.as_ref().clone().try_into()?,
                &arrow_schema,
                session.execution_props(),
            )?;
            let pruning_predicate =
                PruningPredicate::try_new(physical_partition_predicate, arrow_schema.clone())?;
            let manifests_to_prune =
                pruning_predicate.prune(&PruneManifests::new(table, &manifests))?;

            table
                .datafiles(&manifests, Some(manifests_to_prune))
                .await
                .map_err(Into::<Error>::into)?
        } else {
            table
                .datafiles(&manifests, None)
                .await
                .map_err(Into::<Error>::into)?
        };

        let pruning_predicate =
            PruningPredicate::try_new(physical_predicate, arrow_schema.clone())?;
        // After the first pruning stage the data_files are pruned again based on the pruning statistics in the manifest files.
        let files_to_prune = pruning_predicate.prune(&PruneDataFiles::new(table, &data_files))?;

        data_files
            .into_iter()
            .zip(files_to_prune.into_iter())
            .for_each(|(manifest, prune_file)| {
                if prune_file {
                    let partition_values = manifest
                        .data_file()
                        .partition()
                        .iter()
                        .map(|value| match value {
                            Some(v) => ScalarValue::Utf8(Some(serde_json::to_string(v).unwrap())),
                            None => ScalarValue::Null,
                        })
                        .collect::<Vec<ScalarValue>>();
                    let object_meta = ObjectMeta {
                        location: util::strip_prefix(manifest.data_file().file_path()).into(),
                        size: *manifest.data_file().file_size_in_bytes() as usize,
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
            .map_err(Into::<Error>::into)?;
        let data_files = table
            .datafiles(&manifests, None)
            .await
            .map_err(Into::<Error>::into)?;
        data_files.into_iter().for_each(|manifest| {
            let partition_values = manifest
                .data_file()
                .partition()
                .iter()
                .map(|value| match value {
                    Some(v) => ScalarValue::Utf8(Some(serde_json::to_string(v).unwrap())),
                    None => ScalarValue::Null,
                })
                .collect::<Vec<ScalarValue>>();
            let object_meta = ObjectMeta {
                location: util::strip_prefix(manifest.data_file().file_path()).into(),
                size: *manifest.data_file().file_size_in_bytes() as usize,
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
    let table_partition_cols: Vec<Field> = table
        .metadata()
        .default_partition_spec()
        .map_err(Into::<Error>::into)?
        .fields
        .iter()
        .map(|field| {
            let struct_field = schema.fields.get(field.source_id as usize).unwrap();
            Ok(Field::new(
                field.name.clone(),
                (&struct_field
                    .field_type
                    .tranform(&field.transform)
                    .map_err(Into::<Error>::into)?)
                    .try_into()
                    .map_err(Into::<Error>::into)?,
                !struct_field.required,
            ))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()
        .map_err(Into::<Error>::into)?;

    // Add the partition columns to the table schema
    let mut file_schema = schema.clone();
    for partition_field in &table.metadata().default_partition_spec().unwrap().fields {
        file_schema.fields.fields.push(StructField {
            id: partition_field.field_id,
            name: partition_field.name.clone(),
            field_type: file_schema
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
    let file_schema: SchemaRef = Arc::new((&file_schema.fields).try_into().unwrap());

    let file_scan_config = FileScanConfig {
        object_store_url,
        file_schema,
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

impl DisplayAs for DataFusionTable {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergTable")
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct IcebergDataSink(DataFusionTable);

impl DataFusionTable {
    pub(crate) fn into_data_sink(self) -> IcebergDataSink {
        IcebergDataSink(self)
    }
}

impl DisplayAs for IcebergDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(t, f)
    }
}

#[async_trait]
impl DataSink for IcebergDataSink {
    fn as_any(&self) -> &dyn Any {
        self.0.as_any()
    }
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64, DataFusionError> {
        let mut lock = self.0.tabular.write().await;
        let table = if let Tabular::Table(table) = lock.deref_mut() {
            Ok(table)
        } else {
            Err(Error::InvalidFormat("database entity".to_string()))
        }
        .map_err(Into::<Error>::into)?;

        let metadata_files =
            write_parquet_partitioned(table, data.map_err(Into::into), self.0.branch.as_deref())
                .await?;

        table
            .new_transaction(self.0.branch.as_deref())
            .append(metadata_files)
            .commit()
            .await
            .map_err(Into::<Error>::into)?;

        Ok(0)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }
}

#[cfg(test)]
mod tests {

    use datafusion::{
        arrow::{
            array::{Float32Array, Int64Array},
            record_batch::RecordBatch,
        },
        prelude::SessionContext,
    };
    use iceberg_catalog_sql::SqlCatalog;
    use iceberg_rust::{
        catalog::{identifier::Identifier, tabular::Tabular, Catalog},
        table::table_builder::TableBuilder,
        view::view_builder::ViewBuilder,
    };
    use iceberg_rust_spec::spec::{
        partition::{PartitionField, PartitionSpecBuilder, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, StructTypeBuilder, Type},
    };
    use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
    use std::sync::Arc;

    use crate::{catalog::catalog::IcebergCatalog, error::Error, DataFusionTable};

    #[tokio::test]
    pub async fn test_datafusion_table_scan() {
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("../iceberg-tests/nyc_taxis").unwrap());

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store.clone())
                .await
                .unwrap(),
        );
        let identifier = Identifier::parse("test.table1").unwrap();

        catalog.clone().register_table(identifier.clone(), "/home/iceberg/warehouse/nyc/taxis/metadata/fb072c92-a02b-11e9-ae9c-1bb7bc9eca94.metadata.json").await.expect("Failed to register table.");

        let table = if let Tabular::Table(table) = catalog
            .load_table(&identifier)
            .await
            .expect("Failed to load table")
        {
            Ok(Arc::new(DataFusionTable::from(table)))
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
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
    pub async fn test_datafusion_table_insert() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store.clone())
                .await
                .unwrap(),
        );

        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: None,
            fields: StructTypeBuilder::default()
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
        };
        let partition_spec = PartitionSpecBuilder::default()
            .with_spec_id(1)
            .with_partition_field(PartitionField {
                source_id: 4,
                field_id: 1000,
                name: "day".to_string(),
                transform: Transform::Day,
            })
            .build()
            .expect("Failed to create partition spec");

        let mut builder =
            TableBuilder::new("test.orders", catalog).expect("Failed to create table builder");
        builder
            .location("/test/orders")
            .with_schema((1, schema))
            .current_schema_id(1)
            .with_partition_spec((1, partition_spec))
            .default_spec_id(1);
        let table = Arc::new(DataFusionTable::from(
            builder.build().await.expect("Failed to create table."),
        ));

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
                    } else if order_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 3)
                    } else {
                        panic!("Unexpected order id")
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
                    } else if order_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 4)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }
    }

    #[tokio::test]
    pub async fn test_datafusion_table_branch_insert() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "iceberg", object_store.clone())
                .await
                .unwrap(),
        );

        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: None,
            fields: StructTypeBuilder::default()
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
        };
        let partition_spec = PartitionSpecBuilder::default()
            .with_spec_id(1)
            .with_partition_field(PartitionField {
                source_id: 4,
                field_id: 1000,
                name: "day".to_string(),
                transform: Transform::Day,
            })
            .build()
            .expect("Failed to create partition spec");

        let mut builder = TableBuilder::new("test.orders", catalog.clone())
            .expect("Failed to create table builder");
        builder
            .location("/test/orders")
            .with_schema((1, schema.clone()))
            .current_schema_id(1)
            .with_partition_spec((1, partition_spec))
            .default_spec_id(1);

        builder.build().await.expect("Failed to create table.");

        // Datafusion

        let datafusion_catalog = Arc::new(
            IcebergCatalog::new(catalog.clone(), Some("dev"))
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

        let batches = ctx
            .sql("select product_id, sum(amount) from iceberg.test.orders group by product_id;")
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
                    } else if order_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 3)
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

        let batches = ctx
            .sql("select product_id, sum(amount) from iceberg.test.orders group by product_id;")
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
                    } else if order_id.unwrap() == 3 {
                        assert_eq!(amount.unwrap(), 4)
                    } else {
                        panic!("Unexpected order id")
                    }
                }
            }
        }
    }

    #[tokio::test]
    pub async fn test_datafusion_view_scan() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store.clone())
                .await
                .unwrap(),
        );

        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: None,
            fields: StructTypeBuilder::default()
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
        };
        let partition_spec = PartitionSpecBuilder::default()
            .with_spec_id(1)
            .with_partition_field(PartitionField {
                source_id: 4,
                field_id: 1000,
                name: "day".to_string(),
                transform: Transform::Day,
            })
            .build()
            .expect("Failed to create partition spec");

        let mut builder = TableBuilder::new("test.orders", catalog.clone())
            .expect("Failed to create table builder");
        builder
            .location("/test/orders")
            .with_schema((1, schema.clone()))
            .current_schema_id(1)
            .with_partition_spec((1, partition_spec))
            .default_spec_id(1);
        let table = Arc::new(DataFusionTable::from(
            builder.build().await.expect("Failed to create table."),
        ));

        let ctx = SessionContext::new();

        ctx.register_table("orders", table).unwrap();

        ctx.sql(
            "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
                (1, 1, 1, '2020-01-01', 1),
                (2, 2, 1, '2020-01-01', 1),
                (3, 3, 1, '2020-01-01', 3),
                (4, 1, 2, '2020-02-02', 1),
                (5, 1, 1, '2020-02-02', 2),
                (6, 3, 3, '2020-02-02', 3),
                (7, 1, 3, '2020-01-03', 1),
                (8, 2, 1, '2020-01-03', 2),
                (9, 2, 2, '2020-01-03', 1);",
        )
        .await
        .expect("Failed to create query plan for insert")
        .collect()
        .await
        .expect("Failed to insert values into table");

        let view_schema = Schema {
            schema_id: 1,
            identifier_field_ids: None,
            fields: StructTypeBuilder::default()
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
        };

        let mut builder = ViewBuilder::new(
            "select product_id, amount from orders where product_id < 3;",
            "test.orders_view",
            view_schema,
            catalog,
        )
        .expect("Failed to create filesystem view builder.");
        builder.location("test/orders_view");

        let view = Arc::new(DataFusionTable::from(
            builder
                .build()
                .await
                .expect("Failed to create filesystem view"),
        ));

        ctx.register_table("orders_view", view).unwrap();

        let batches = ctx
            .sql("select product_id, sum(amount) from orders_view group by product_id;")
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
}
