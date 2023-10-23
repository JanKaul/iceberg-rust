use std::sync::Arc;

use anyhow::anyhow;
use datafusion::{
    arrow::datatypes::{FieldRef, Schema as ArrowSchema},
    sql::{
        planner::SqlToRel,
        sqlparser::{dialect::GenericDialect, parser::Parser},
    },
};
use iceberg_rust::{catalog::Catalog, spec::types::StructType};

use crate::catalog::context::IcebergContext;

pub async fn get_schema(
    sql: &str,
    relations: Vec<String>,
    catalog: Arc<dyn Catalog>,
) -> Result<StructType, anyhow::Error> {
    let context = IcebergContext::new(relations, catalog).await?;
    let statement = Parser::parse_sql(&GenericDialect, sql)?
        .pop()
        .ok_or(anyhow!("No sql statement found.".to_string()))?;

    let planner = SqlToRel::new(&context);

    let logical_plan = planner.sql_statement_to_plan(statement)?;
    let fields: Vec<FieldRef> = logical_plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.field())
        .cloned()
        .collect();
    let struct_type = StructType::try_from(&ArrowSchema::new(fields))?;
    Ok(struct_type)
}
