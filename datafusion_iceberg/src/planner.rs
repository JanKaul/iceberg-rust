use std::{fmt::Debug, hash::Hash, sync::Arc};

use async_trait::async_trait;
use datafusion_expr::{
    ColumnarValue, CreateCatalogSchema, CreateView, DropCatalogSchema, DropTable,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use itertools::Itertools;
use regex::Regex;

use crate::{
    catalog::{catalog::IcebergCatalog, schema::IcebergSchema},
    materialized_view::{delta_queries::fork_node::ForkNodePlanner, refresh_materialized_view},
};
use datafusion::{
    arrow::datatypes::{DataType, Schema as ArrowSchema},
    catalog::CatalogProvider,
    common::{tree_node::Transformed, SchemaReference},
    error::DataFusionError,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{
        CreateExternalTable, DdlStatement, Extension, InvariantLevel, LogicalPlan,
        UserDefinedLogicalNode,
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
        arrow::schema::new_fields_with_ids,
        identifier::Identifier,
        namespace::Namespace,
        partition::{PartitionField, PartitionSpec, Transform},
        schema::{Schema, DEFAULT_SCHEMA_ID},
        types::StructType,
        view_metadata::{Version, ViewRepresentation},
    },
    table::Table,
    view::View,
};

pub struct IcebergQueryPlanner(DefaultPhysicalPlanner);

impl Debug for IcebergQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IcebergQueryPlanner")
    }
}

impl Default for IcebergQueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl IcebergQueryPlanner {
    pub fn new() -> Self {
        IcebergQueryPlanner(DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(IcebergExtensionPlanner {}),
            Arc::new(ForkNodePlanner::new()),
        ]))
    }
}

#[async_trait]
impl QueryPlanner for IcebergQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.0
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

pub fn iceberg_transform(node: LogicalPlan) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    match node {
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(table)) => {
            if table.file_type.to_lowercase() == "iceberg" {
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
        LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(schema)) => {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(CreateIcebergNamespace(schema)),
            })))
        }
        LogicalPlan::Ddl(DdlStatement::DropTable(table)) => {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(DropIcebergTable(table)),
            })))
        }
        LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(schema)) => {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(DropIcebergNamespace(schema)),
            })))
        }
        _ => Ok(Transformed::no(node)),
    }
}

pub struct IcebergExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for IcebergExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(node) = node.as_any().downcast_ref::<CreateIcebergTable>() {
            plan_create_table(node, session_state).await
        } else if let Some(node) = node.as_any().downcast_ref::<CreateIcebergView>() {
            plan_create_view(node, session_state).await
        } else if let Some(node) = node.as_any().downcast_ref::<CreateIcebergNamespace>() {
            plan_create_namespace(node, session_state).await
        } else if let Some(node) = node.as_any().downcast_ref::<DropIcebergTable>() {
            plan_drop_table(node, session_state).await
        } else if let Some(node) = node.as_any().downcast_ref::<DropIcebergNamespace>() {
            plan_drop_namespace(node, session_state).await
        } else {
            return Ok(None);
        }
    }
}

async fn plan_create_table(
    node: &CreateIcebergTable,
    session_state: &SessionState,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let table_ref = &node.0.name.to_string();

    let identifier = TableReference::parse_str(table_ref).resolve("datafusion", "public");

    let catalog_list = session_state.catalog_list();
    let catalog_name = &identifier.catalog;
    let namespace_name = &identifier.schema;
    let table_name: &str = &identifier.table;
    let datafusion_catalog = catalog_list
        .catalog(catalog_name)
        .ok_or(DataFusionError::Plan(format!(
            "Catalog {catalog_name} does not exist."
        )))?;
    let Some(iceberg_catalog) = datafusion_catalog.as_any().downcast_ref::<IcebergCatalog>() else {
        return Err(DataFusionError::Plan(format!(
            "Catalog {catalog_name} is not an Iceberg catalog."
        )));
    };

    let catalog = iceberg_catalog.catalog();

    let schema = StructType::try_from(&new_fields_with_ids(
        node.0.schema.as_arrow().fields(),
        &mut 0,
    ))
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
                    .ok_or(Error::NotFound(format!("Column {column}")))?
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
        .with_schema(Schema::from_struct_type(schema, DEFAULT_SCHEMA_ID, None))
        .with_partition_spec(partition_spec)
        .with_properties(node.0.options.clone())
        .build(&[namespace_name.as_ref().to_owned()], catalog)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    Ok(Some(Arc::new(EmptyExec::new(Arc::new(
        ArrowSchema::empty(),
    )))))
}

async fn plan_create_view(
    node: &CreateIcebergView,
    session_state: &SessionState,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let table_ref = &node.0.name.to_string();

    let identifier = TableReference::parse_str(table_ref).resolve("datafusion", "public");

    let catalog_list = session_state.catalog_list();
    let catalog_name = &identifier.catalog;
    let namespace_name = &identifier.schema;
    let table_name: &str = &identifier.table;
    let datafusion_catalog = catalog_list
        .catalog(catalog_name)
        .ok_or(DataFusionError::Plan(format!(
            "Catalog {catalog_name} does not exist."
        )))?;
    let Some(iceberg_catalog) = datafusion_catalog.as_any().downcast_ref::<IcebergCatalog>() else {
        return Err(DataFusionError::Plan(format!(
            "Catalog {catalog_name} is not an Iceberg catalog."
        )));
    };

    let catalog = iceberg_catalog.catalog();

    let schema = StructType::try_from(&new_fields_with_ids(
        node.0.input.schema().as_arrow().fields(),
        &mut 0,
    ))
    .map_err(|err| DataFusionError::External(Box::new(err)))?;

    let definition = node.0.definition.as_ref().unwrap();
    let definition = match (definition.split_once(" as "), definition.split_once(" AS ")) {
        (Some(definition), None) => definition.1,
        (None, Some(definition)) => definition.1,
        _ => panic!("Something is wrong"),
    };

    #[cfg(test)]
    let location = "/tmp/".to_owned() + catalog_name + "/" + namespace_name + "/" + table_name;

    #[cfg(not(test))]
    let location = "s3://".to_string() + catalog_name + "/" + namespace_name + "/" + table_name;

    if node.0.temporary {
        MaterializedView::builder()
            .with_name(table_name)
            .with_location(location)
            .with_schema(Schema::from_struct_type(schema, DEFAULT_SCHEMA_ID, None))
            .with_view_version(
                Version::builder()
                    .with_representation(ViewRepresentation::sql(definition, None))
                    .build()
                    .map_err(|err| DataFusionError::External(Box::new(err)))?,
            )
            .build(&[namespace_name.as_ref().to_owned()], catalog)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
    } else {
        View::builder()
            .with_name(table_name)
            .with_location(location)
            .with_schema(Schema::from_struct_type(schema, DEFAULT_SCHEMA_ID, None))
            .with_view_version(
                Version::builder()
                    .with_representation(ViewRepresentation::sql(definition, None))
                    .build()
                    .map_err(|err| DataFusionError::External(Box::new(err)))?,
            )
            .build(&[namespace_name.as_ref().to_owned()], catalog)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
    }

    Ok(Some(Arc::new(EmptyExec::new(Arc::new(
        ArrowSchema::empty(),
    )))))
}

async fn plan_create_namespace(
    node: &CreateIcebergNamespace,
    session_state: &SessionState,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let (catalog_name, namespace_name) =
        node.0
            .schema_name
            .split('.')
            .collect_tuple()
            .ok_or(DataFusionError::Plan(format!(
                "Schema name {} has an invalid format.",
                &node.0.schema_name
            )))?;

    let catalog_list = session_state.catalog_list();
    let datafusion_catalog = catalog_list
        .catalog(catalog_name)
        .ok_or(DataFusionError::Plan(format!(
            "Catalog {catalog_name} does not exist."
        )))?;
    let Some(iceberg_catalog) = datafusion_catalog.as_any().downcast_ref::<IcebergCatalog>() else {
        return Err(DataFusionError::Plan(format!(
            "Catalog {catalog_name} is not an Iceberg catalog."
        )));
    };

    let catalog = iceberg_catalog.catalog();

    let namespace = Namespace::try_new(&[namespace_name.to_owned()])
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    catalog
        .create_namespace(&namespace, None)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    iceberg_catalog.register_schema(
        namespace_name,
        Arc::new(IcebergSchema::new(namespace, iceberg_catalog.mirror())),
    )?;

    Ok(Some(Arc::new(EmptyExec::new(Arc::new(
        ArrowSchema::empty(),
    )))))
}

async fn plan_drop_table(
    node: &DropIcebergTable,
    session_state: &SessionState,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let table_ref = &node.0.name.to_string();

    let identifier = TableReference::parse_str(table_ref).resolve("datafusion", "public");

    let catalog_list = session_state.catalog_list();
    let catalog_name = &identifier.catalog;
    let namespace_name = &identifier.schema;
    let table_name: &str = &identifier.table;
    let datafusion_catalog = catalog_list
        .catalog(catalog_name)
        .ok_or(DataFusionError::Plan(format!(
            "Catalog {catalog_name} does not exist."
        )))?;
    let Some(iceberg_catalog) = datafusion_catalog.as_any().downcast_ref::<IcebergCatalog>() else {
        return Err(DataFusionError::Plan(format!(
            "Catalog {catalog_name} is not an Iceberg catalog."
        )));
    };

    let catalog = iceberg_catalog.catalog();

    catalog
        .drop_table(
            &Identifier::try_new(&[namespace_name.to_string(), table_name.to_string()], None)
                .map_err(|err| DataFusionError::External(Box::new(err)))?,
        )
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    Ok(Some(Arc::new(EmptyExec::new(Arc::new(
        ArrowSchema::empty(),
    )))))
}

async fn plan_drop_namespace(
    node: &DropIcebergNamespace,
    session_state: &SessionState,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let (catalog_name, namespace_name) = match &node.0.name {
        SchemaReference::Bare { schema } => ("datafusion".to_owned(), schema.to_string()),
        SchemaReference::Full { schema, catalog } => (catalog.to_string(), schema.to_string()),
    };

    let catalog_list = session_state.catalog_list();
    let datafusion_catalog = catalog_list
        .catalog(&catalog_name)
        .ok_or(DataFusionError::Plan(format!(
            "Catalog {catalog_name} does not exist."
        )))?;
    let Some(iceberg_catalog) = datafusion_catalog.as_any().downcast_ref::<IcebergCatalog>() else {
        return Err(DataFusionError::Plan(format!(
            "Catalog {catalog_name} is not an Iceberg catalog."
        )));
    };

    let catalog = iceberg_catalog.catalog();

    let namespace = Namespace::try_new(&[namespace_name.to_owned()])
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    if catalog.load_namespace(&namespace).await.is_ok() {
        catalog
            .drop_namespace(&namespace)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
    }

    iceberg_catalog.deregister_schema(&namespace_name, false)?;

    Ok(Some(Arc::new(EmptyExec::new(Arc::new(
        ArrowSchema::empty(),
    )))))
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

    fn check_invariants(&self, _check: InvariantLevel) -> Result<(), DataFusionError> {
        Ok(())
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

    fn check_invariants(&self, _check: InvariantLevel) -> Result<(), DataFusionError> {
        Ok(())
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

#[derive(Clone)]
pub struct CreateIcebergNamespace(pub CreateCatalogSchema);

impl Debug for CreateIcebergNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = &self.0.schema_name;
        write!(f, "CreateIcebergNamespace: {name:?}")
    }
}

impl UserDefinedLogicalNode for CreateIcebergNamespace {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "CreateIcebergNamespace"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.0.schema
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<(), DataFusionError> {
        Ok(())
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = &self.0.schema_name;
        write!(f, "CreateIcebergNamespace: {name:?}")
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
        if let Some(other) = other.as_any().downcast_ref::<CreateIcebergNamespace>() {
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
pub struct DropIcebergTable(pub DropTable);

impl Debug for DropIcebergTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = &self.0.name;
        write!(f, "DropIcebergTable: {name:?}")
    }
}

impl UserDefinedLogicalNode for DropIcebergTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DropIcebergTable"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.0.schema
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<(), DataFusionError> {
        Ok(())
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = &self.0.name;
        write!(f, "DropIcebergTable: {name:?}")
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
        if let Some(other) = other.as_any().downcast_ref::<DropIcebergTable>() {
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
pub struct DropIcebergNamespace(pub DropCatalogSchema);

impl Debug for DropIcebergNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = &self.0.name;
        write!(f, "DropIcebergNamespace: {name:?}")
    }
}

impl UserDefinedLogicalNode for DropIcebergNamespace {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DropIcebergNamespace"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.0.schema
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<(), DataFusionError> {
        Ok(())
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = &self.0.name;
        write!(f, "DropIcebergNamespace: {name:?}")
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
        if let Some(other) = other.as_any().downcast_ref::<DropIcebergNamespace>() {
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

impl PartialEq for RefreshMaterializedView {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for RefreshMaterializedView {}

impl Hash for RefreshMaterializedView {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "RefreshMaterializedView".hash(state);
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

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion_expr::ColumnarValue> {
        let args = args.args;
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

    let (transform_name, column, arg) = if let Some(caps) = full.captures(input) {
        let transform_name = caps
            .get(1)
            .ok_or(Error::InvalidFormat("Partition transform".to_owned()))?
            .as_str()
            .to_string()
            .to_lowercase();
        let args = caps
            .get(2)
            .ok_or(Error::InvalidFormat("Partition column".to_owned()))?
            .as_str();
        let mut args = args.split(',').map(|s| s.to_string());
        let column = args
            .next()
            .ok_or(Error::InvalidFormat("Partition column".to_owned()))?
            .to_lowercase();
        let arg = args.next();
        (transform_name, column, arg)
    } else {
        let caps = short
            .captures(input)
            .ok_or(Error::InvalidFormat("Partition transform".to_owned()))?;

        let column = caps
            .get(1)
            .ok_or(Error::InvalidFormat("Partition column".to_owned()))?
            .as_str()
            .to_string()
            .to_lowercase();
        ("identity".to_owned(), column, None)
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
    use iceberg_rust::object_store::ObjectStoreBuilder;
    use iceberg_sql_catalog::SqlCatalogList;
    use tokio::time::sleep;

    use crate::{
        catalog::catalog_list::IcebergCatalogList,
        planner::{iceberg_transform, IcebergQueryPlanner, RefreshMaterializedView},
    };

    #[tokio::test]
    async fn test_planner() {
        let object_store = ObjectStoreBuilder::memory();
        let iceberg_catalog_list = Arc::new(
            SqlCatalogList::new("sqlite://", object_store)
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

        let state = SessionStateBuilder::default()
            .with_default_features()
            .with_catalog_list(catalog_list)
            .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
            .build();

        let ctx = SessionContext::new_with_state(state);

        ctx.register_udf(ScalarUDF::from(RefreshMaterializedView::new(
            iceberg_catalog_list,
        )));

        let sql = &"CREATE SCHEMA iceberg.public;".to_string();

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let transformed = plan.transform(iceberg_transform).data().unwrap();

        ctx.execute_logical_plan(transformed)
            .await
            .unwrap()
            .collect()
            .await
            .expect("Failed to execute query plan.");

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

        let sql = "CREATE TEMPORARY VIEW iceberg.public.quantities_by_product AS select product_id, sum(quantity) as total from iceberg.public.orders group by product_id;";

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
