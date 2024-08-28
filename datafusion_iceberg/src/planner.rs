use std::{fmt::Debug, hash::Hash, sync::Arc};

use async_trait::async_trait;

use crate::catalog::catalog::IcebergCatalog;
use datafusion::{
    arrow::datatypes::Schema as ArrowSchema,
    common::tree_node::Transformed,
    error::DataFusionError,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{
        CreateExternalTable, DdlStatement, Extension, LogicalPlan, UserDefinedLogicalNode,
    },
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use iceberg_rust::{
    spec::{schema::Schema, types::StructType, view_metadata::FullIdentifier},
    table::Table,
};

pub struct IcebergQueryPlanner {}

#[async_trait]
impl QueryPlanner for IcebergQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(IcebergPlanner {})]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

pub fn iceberg_transform(node: LogicalPlan) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(table)) = node {
        if table.file_type == "ICEBERG" {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(CreateIcebergTable(table)),
            })))
        } else {
            Ok(Transformed::no(LogicalPlan::Ddl(
                DdlStatement::CreateExternalTable(table),
            )))
        }
    } else {
        Ok(Transformed::no(node))
    }
}

pub struct IcebergPlanner {}

#[async_trait]
impl ExtensionPlanner for IcebergPlanner {
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

        let identifier = FullIdentifier::parse(&table_ref, None, None)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let catalog_list = session_state.catalog_list();
        let catalog_name = identifier.catalog();
        let namespace_name = identifier.namespace();
        let table_name = identifier.name();
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

        Table::builder()
            .with_name(table_name)
            .with_location(&node.0.location)
            .with_schema(
                Schema::builder()
                    .with_fields(schema)
                    .build()
                    .map_err(|err| DataFusionError::External(Box::new(err)))?,
            )
            .with_properties(node.0.options.clone())
            .build(&[namespace_name[0].to_owned()], catalog)
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
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        common::tree_node::{TransformedResult, TreeNode},
        execution::{
            config::SessionConfig,
            context::{SessionContext, SessionState},
            runtime_env::RuntimeEnv,
        },
    };
    use iceberg_sql_catalog::SqlCatalogList;
    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        catalog::catalog_list::IcebergCatalogList,
        planner::{iceberg_transform, IcebergQueryPlanner},
    };

    #[tokio::test]
    async fn test_planner() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let catalog_list = {
            Arc::new(
                IcebergCatalogList::new(Arc::new(
                    SqlCatalogList::new("sqlite://", object_store.clone())
                        .await
                        .unwrap(),
                ))
                .await
                .unwrap(),
            )
        };
        let session_config = SessionConfig::from_env()
            .unwrap()
            .with_create_default_catalog_and_schema(true)
            .with_information_schema(true);

        let runtime_env = Arc::new(RuntimeEnv::default());

        let state = SessionState::new_with_config_rt_and_catalog_list(
            session_config,
            runtime_env,
            catalog_list,
        )
        .with_query_planner(Arc::new(IcebergQueryPlanner {}));

        let ctx = SessionContext::new_with_state(state);
        let var_name = "CREATE EXTERNAL TABLE iceberg.public.test (
    c1  VARCHAR NOT NULL,
    c2  INT NOT NULL,
    c5  INT NOT NULL,
    c6  BIGINT NOT NULL,
    c8  INT NOT NULL,
    c9  BIGINT NOT NULL,
    c10 VARCHAR NOT NULL,
    c11 FLOAT NOT NULL,
    c12 DOUBLE NOT NULL,
    c13 VARCHAR NOT NULL
)
STORED AS ICEBERG
LOCATION '/path/to/'
OPTIONS ('has_header' 'true');";

        let plan = ctx.state().create_logical_plan(&var_name).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        let df = ctx.execute_logical_plan(transformed).await.unwrap();

        // execute the plan
        df.collect().await.expect("Failed to execute query plan.");
    }
}
