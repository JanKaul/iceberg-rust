/*!
Utility functions for SQL
*/

use std::ops::ControlFlow;

use sqlparser::{ast::visit_relations, dialect::GenericDialect, parser::Parser};

pub(crate) fn find_relations(sql: &str) -> Result<Vec<String>, anyhow::Error> {
    let statements = Parser::parse_sql(&GenericDialect, sql)?;
    let mut visited = Vec::new();

    visit_relations(&statements, |relation| {
        visited.push(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    Ok(visited)
}
