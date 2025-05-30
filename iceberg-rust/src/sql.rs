/*!
Utility functions for SQL
*/

use std::ops::ControlFlow;

use sqlparser::{ast::visit_relations, dialect::GenericDialect, parser::Parser};

use crate::error::Error;

/// Find all table references in a SQL query
pub fn find_relations(sql: &str) -> Result<Vec<String>, Error> {
    let statements = Parser::parse_sql(&GenericDialect, sql)?;
    let mut visited = Vec::new();

    let _ = visit_relations(&statements, |relation| {
        visited.push(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    Ok(visited)
}
