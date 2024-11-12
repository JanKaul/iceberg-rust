use std::{fmt::Debug, hash::Hash, sync::Arc};

use async_trait::async_trait;
use datafusion_expr::{ColumnarValue, CreateView, ScalarUDFImpl, Signature, Volatility};
use regex::Regex;

use crate::{catalog::catalog::IcebergCatalog, materialized_view::refresh_materialized_view};
use datafusion::{
    arrow::datatypes::{DataType, Schema as ArrowSchema},
    common::tree_node::Transformed,
    error::DataFusionError,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{
        CreateExternalTable, DdlStatement, Extension, LogicalPlan, UserDefinedLogicalNode,
    },
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    scalar::ScalarValue,
    sql::TableReference,
};
use iceberg_rust::{
    catalog::{tabular::Tabular, CatalogList},
    error::Error,
    materialized_view::MaterializedView,
    spec::{
        identifier::Identifier,
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        types::StructType,
        view_metadata::{Version, ViewRepresentation},
    },
    table::Table,
};

#[derive(Debug)]
pub struct IcebergQueryPlanner {}

#[async_trait]
impl QueryPlanner for IcebergQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(CreateIcebergTablePlanner {}),
            Arc::new(CreateIcebergViewPlanner {}),
        ]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

pub fn iceberg_transform(node: LogicalPlan) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    match node {
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(table)) => {
            if table.file_type == "ICEBERG" {
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(CreateIcebergTable(table)),
                })))
            } else {
                Ok(Transformed::no(LogicalPlan::Ddl(
                    DdlStatement::CreateExternalTable(table),
                )))
            }
        }
        LogicalPlan::Ddl(DdlStatement::CreateView(view)) => {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(CreateIcebergView(view)),
            })))
        }
        _ => Ok(Transformed::no(node)),
    }
}

pub struct CreateIcebergTablePlanner {}

#[async_trait]
impl ExtensionPlanner for CreateIcebergTablePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let Some(node) = node.as_any().downcast_ref::<CreateIcebergTable>() else {
            return Ok(None);
        };

        let table_ref = &node.0.name.to_string();

        let identifier = TableReference::parse_str(table_ref).resolve("datafusion", "public");

        let catalog_list = session_state.catalog_list();
        let catalog_name = &identifier.catalog;
        let namespace_name = &identifier.schema;
        let table_name: &str = &identifier.table;
        let datafusion_catalog =
            catalog_list
                .catalog(catalog_name)
                .ok_or(DataFusionError::Plan(format!(
                    "Catalog {catalog_name} does not exist."
                )))?;
        let Some(iceberg_catalog) = datafusion_catalog.as_any().downcast_ref::<IcebergCatalog>()
        else {
            return Err(DataFusionError::Plan(format!(
                "Catalog {catalog_name} is not an Iceberg catalog."
            )));
        };

        let catalog = iceberg_catalog.catalog();

        let schema = StructType::try_from(node.0.schema.as_arrow())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let pacrtition_spec = node
            .0
            .table_partition_cols
            .iter()
            .enumerate()
            .map(|(i, x)| {
                let (column, transform) = parse_transform(x)?;
                let name = if let Transform::Identity = &transform {
                    column.clone()
                } else {
                    column.clone() + "_" + &transform.to_string()
                };
                Ok::<_, Error>(PartitionField::new(
                    schema
                        .get_name(&column)
                        .ok_or(Error::NotFound("Column".to_owned(), column.clone()))?
                        .id,
                    1000 + i as i32,
                    &name,
                    transform,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let partition_spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_fields(pacrtition_spec)
            .build()
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Table::builder()
            .with_name(table_name)
            .with_location(&node.0.location)
            .with_schema(
                Schema::builder()
                    .with_fields(schema)
                    .build()
                    .map_err(|err| DataFusionError::External(Box::new(err)))?,
            )
            .with_partition_spec(partition_spec)
            .with_properties(node.0.options.clone())
            .build(&[namespace_name.as_ref().to_owned()], catalog)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Some(Arc::new(EmptyExec::new(Arc::new(
            ArrowSchema::empty(),
        )))))
    }
}

pub struct CreateIcebergViewPlanner {}

#[async_trait]
impl ExtensionPlanner for CreateIcebergViewPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let Some(node) = node.as_any().downcast_ref::<CreateIcebergView>() else {
            return Ok(None);
        };

        let table_ref = &node.0.name.to_string();

        let identifier = TableReference::parse_str(table_ref).resolve("datafusion", "public");

        let catalog_list = session_state.catalog_list();
        let catalog_name = &identifier.catalog;
        let namespace_name = &identifier.schema;
        let table_name: &str = &identifier.table;
        let datafusion_catalog =
            catalog_list
                .catalog(catalog_name)
                .ok_or(DataFusionError::Plan(format!(
                    "Catalog {catalog_name} does not exist."
                )))?;
        let Some(iceberg_catalog) = datafusion_catalog.as_any().downcast_ref::<IcebergCatalog>()
        else {
            return Err(DataFusionError::Plan(format!(
                "Catalog {catalog_name} is not an Iceberg catalog."
            )));
        };

        let catalog = iceberg_catalog.catalog();

        let schema = StructType::try_from(node.0.input.schema().as_arrow())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let lowercase = node.0.definition.as_ref().unwrap().to_lowercase();
        let definition = lowercase.split_once(" as ").unwrap().1;

        #[cfg(test)]
        let location = catalog_name.to_string() + "/" + namespace_name + "/" + table_name;

        #[cfg(not(test))]
        let location = "s3://".to_string() + catalog_name + "/" + namespace_name + "/" + table_name;

        MaterializedView::builder()
            .with_name(table_name)
            .with_location(location)
            .with_schema(
                Schema::builder()
                    .with_fields(schema)
                    .build()
                    .map_err(|err| DataFusionError::External(Box::new(err)))?,
            )
            .with_view_version(
                Version::builder()
                    .with_representation(ViewRepresentation::sql(definition, None))
                    .build()
                    .map_err(|err| DataFusionError::External(Box::new(err)))?,
            )
            .build(&[namespace_name.as_ref().to_owned()], catalog)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Some(Arc::new(EmptyExec::new(Arc::new(
            ArrowSchema::empty(),
        )))))
    }
}

#[derive(Clone)]
pub struct CreateIcebergTable(pub CreateExternalTable);

impl Debug for CreateIcebergTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = &self.0.name;
        let constraints = &self.0.constraints;
        write!(f, "CreateIcebergTable: {name:?}{constraints}")
    }
}

impl UserDefinedLogicalNode for CreateIcebergTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "CreateIcebergTable"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.0.schema
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = &self.0.name;
        let constraints = &self.0.constraints;
        write!(f, "CreateIcebergTable: {name:?}{constraints}")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        _inputs: Vec<datafusion::logical_expr::LogicalPlan>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(self.clone()))
    }

    fn dyn_hash(&self, mut state: &mut dyn std::hash::Hasher) {
        self.0.hash(&mut state)
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<CreateIcebergTable>() {
            self.0.eq(&other.0)
        } else {
            false
        }
    }

    fn dyn_ord(&self, _other: &dyn UserDefinedLogicalNode) -> Option<std::cmp::Ordering> {
        None
    }
}

#[derive(Clone)]
pub struct CreateIcebergView(pub CreateView);

impl Debug for CreateIcebergView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = &self.0.name;
        write!(f, "CreateIcebergView: {name:?}")
    }
}

impl UserDefinedLogicalNode for CreateIcebergView {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "CreateIcebergView"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.0.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = &self.0.name;
        write!(f, "CreateIcebergView: {name:?}")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        _inputs: Vec<datafusion::logical_expr::LogicalPlan>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(self.clone()))
    }

    fn dyn_hash(&self, mut state: &mut dyn std::hash::Hasher) {
        self.0.hash(&mut state)
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<CreateIcebergView>() {
            self.0.eq(&other.0)
        } else {
            false
        }
    }

    fn dyn_ord(&self, _other: &dyn UserDefinedLogicalNode) -> Option<std::cmp::Ordering> {
        None
    }
}

#[derive(Debug)]
pub struct RefreshMaterializedView {
    pub catalog_list: Arc<dyn CatalogList>,
    pub signature: Signature,
}

impl RefreshMaterializedView {
    pub fn new(catalog_list: Arc<dyn CatalogList>) -> Self {
        Self {
            catalog_list,
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for RefreshMaterializedView {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "refresh_materialized_view"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(
        &self,
        args: &[datafusion_expr::ColumnarValue],
    ) -> datafusion::error::Result<datafusion_expr::ColumnarValue> {
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) = &args[0] else {
            return Err(DataFusionError::Execution(
                "Refresh function only takes a scalar string input.".to_string(),
            ));
        };

        let identifier = TableReference::parse_str(name).resolve("datafusion", "public");

        let catalog_list = self.catalog_list.clone();

        tokio::task::spawn(async move {
            let catalog_name = &identifier.catalog;
            let catalog = catalog_list
                .catalog(catalog_name)
                .ok_or(DataFusionError::Execution(format!(
                    "Catalog {catalog_name} not found."
                )))
                .unwrap();

            let Tabular::MaterializedView(mut matview) = catalog
                .load_tabular(&Identifier::new(
                    &[identifier.schema.to_string()],
                    &identifier.table,
                ))
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))
                .unwrap()
            else {
                panic!("Failed to load table {identifier}");
            };

            refresh_materialized_view(&mut matview, catalog_list, None)
                .await
                .unwrap();
        });

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "Refresh successful".to_string(),
        ))))
    }
}

fn parse_transform(input: &str) -> Result<(String, Transform), Error> {
    let short = Regex::new(r"(\w+)").unwrap();
    let full = Regex::new(r"(\w+)\((.*)\)").unwrap();

    let (transform_name, column, arg) = if let Some(caps) = short.captures(input) {
        let column = caps
            .get(1)
            .ok_or(Error::InvalidFormat("Partition column".to_owned()))?
            .as_str()
            .to_string();
        ("identity".to_owned(), column, None)
    } else {
        let caps = full
            .captures(input)
            .ok_or(Error::InvalidFormat("Partition transform".to_owned()))?;
        let transform_name = caps
            .get(1)
            .ok_or(Error::InvalidFormat("Partition transform".to_owned()))?
            .as_str()
            .to_string();
        let args = caps
            .get(2)
            .ok_or(Error::InvalidFormat("Partition column".to_owned()))?
            .as_str();
        let mut args = args.split(',').map(|s| s.to_string());
        let column = args
            .next()
            .ok_or(Error::InvalidFormat("Partition column".to_owned()))?;
        let arg = args.next();
        (transform_name, column, arg)
    };
    match (transform_name.as_str(), column, arg) {
        ("identity", column, None) => Ok((column, Transform::Identity)),
        ("void", column, None) => Ok((column, Transform::Void)),
        ("year", column, None) => Ok((column, Transform::Year)),
        ("month", column, None) => Ok((column, Transform::Month)),
        ("day", column, None) => Ok((column, Transform::Day)),
        ("hour", column, None) => Ok((column, Transform::Hour)),
        ("bucket", column, Some(m)) => Ok((column, Transform::Bucket(m.parse()?))),
        ("truncate", column, Some(m)) => Ok((column, Transform::Truncate(m.parse()?))),
        _ => Err(Error::InvalidFormat("Partition transform".to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use datafusion::{
        arrow::array::{Int32Array, Int64Array},
        common::tree_node::{TransformedResult, TreeNode},
        execution::{context::SessionContext, SessionStateBuilder},
    };
    use datafusion_expr::ScalarUDF;
    use iceberg_sql_catalog::SqlCatalogList;
    use object_store::{memory::InMemory, ObjectStore};
    use tokio::time::sleep;

    use crate::{
        catalog::catalog_list::IcebergCatalogList,
        planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView},
    };

    #[tokio::test]
    async fn test_planner() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let iceberg_catalog_list = Arc::new(
            SqlCatalogList::new("sqlite://", object_store.clone())
                .await
                .unwrap(),
        );

        let catalog_list = {
            Arc::new(
                IcebergCatalogList::new(iceberg_catalog_list.clone())
                    .await
                    .unwrap(),
            )
        };

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_catalog_list(catalog_list)
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .build();

        let ctx = SessionContext::new_with_state(state);

        ctx.register_udf(ScalarUDF::from(RefreshMaterializedView::new(
            iceberg_catalog_list,
        )));

        let sql = "CREATE EXTERNAL TABLE iceberg.public.orders (
      id BIGINT NOT NULL,
      order_date DATE NOT NULL,
      customer_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL
)
STORED AS ICEBERG
LOCATION '/path/to/'
OPTIONS ('has_header' 'true');";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        ctx.sql(
            "INSERT INTO iceberg.public.orders (id, customer_id, product_id, order_date, quantity) VALUES 
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

        let sql = "CREATE VIEW iceberg.public.quantities_by_product AS select product_id, sum(quantity) from iceberg.public.orders group by product_id;";

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

        let batches = ctx
            .sql("select * from iceberg.public.quantities_by_product;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        assert!(batches.iter().all(|batch| batch.num_rows() == 0));

        ctx.sql("select refresh_materialized_view('iceberg.public.quantities_by_product');")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        sleep(Duration::from_millis(1_000)).await;

        let batches = ctx
            .sql("select * from iceberg.public.quantities_by_product;")
            .await
            .expect("Failed to create plan for select")
            .collect()
            .await
            .expect("Failed to execute select query");

        let mut once = false;

        for batch in batches {
            if batch.num_rows() != 0 {
                let (product_ids, amounts) = (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
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
                        panic!("Unexpected order id")
                    }
                }
                once = true
            }
        }

        assert!(once);
    }
}
