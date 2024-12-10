use std::sync::Arc;

use arrow_schema::{FieldRef, Schema};
use datafusion_common::DataFusionError;
use datafusion_sql::{
    planner::SqlToRel,
    sqlparser::{dialect::GenericDialect, parser::Parser},
};
use iceberg_rust::{catalog::CatalogList, spec::types::StructType};

use crate::context::IcebergContext;

pub async fn get_schema(
    sql: &str,
    relations: &[(String, String, String)],
    catalogs: Arc<dyn CatalogList>,
    branch: Option<&str>,
) -> Result<StructType, DataFusionError> {
    let context = IcebergContext::new(relations, catalogs, branch).await?;
    let statement = Parser::parse_sql(&GenericDialect, sql)?
        .pop()
        .ok_or(DataFusionError::Internal("sql statement".to_string()))?;

    let planner = SqlToRel::new(&context);

    let logical_plan = planner.sql_statement_to_plan(statement)?;
    let fields: Vec<FieldRef> = logical_plan.schema().fields().iter().cloned().collect();
    let struct_type = StructType::try_from(&Schema::new(fields))
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    Ok(struct_type)
}
