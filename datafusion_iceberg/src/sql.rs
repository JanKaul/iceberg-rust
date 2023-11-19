use std::{ops::ControlFlow, sync::Arc};

use datafusion::{
    arrow::datatypes::{FieldRef, Schema as ArrowSchema},
    sql::{
        planner::SqlToRel,
        sqlparser::{
            ast::{visit_relations_mut, Ident},
            dialect::GenericDialect,
            parser::Parser,
        },
    },
};
use iceberg_rust::catalog::Catalog;
use iceberg_rust_spec::spec::types::StructType;
use itertools::Itertools;

use crate::{catalog::context::IcebergContext, error::Error};

pub async fn get_schema(
    sql: &str,
    relations: Vec<String>,
    catalog: Arc<dyn Catalog>,
) -> Result<StructType, Error> {
    let context = IcebergContext::new(relations, catalog).await?;
    let statement = Parser::parse_sql(&GenericDialect, sql)?
        .pop()
        .ok_or(Error::InvalidFormat("sql statement".to_string()))?;

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

pub(crate) fn transform_name(input: &str) -> String {
    input.replace('.', "__")
}

pub(crate) fn transform_relations(sql: &str) -> Result<Vec<String>, Error> {
    let mut statements = Parser::parse_sql(&GenericDialect, sql)?;

    visit_relations_mut(&mut statements, |relation| {
        relation.0 = vec![Ident::new(transform_name(
            &Itertools::intersperse(relation.0.iter().skip(1).map(|x| x.value.as_str()), ".")
                .collect::<String>(),
        ))];
        relation.0[0].value = transform_name(&relation.0[0].value);
        ControlFlow::<()>::Continue(())
    });

    Ok(statements
        .into_iter()
        .map(|statement| statement.to_string())
        .collect())
}
