/*!
Utility functions for SQL
*/

use std::ops::ControlFlow;

use sqlparser::{ast::visit_relations, dialect::GenericDialect, parser::Parser};

use crate::error::Error;

pub fn find_relations(sql: &str) -> Result<Vec<String>, Error> {
    let statements = Parser::parse_sql(&GenericDialect, sql)?;
    let mut visited = Vec::new();

    visit_relations(&statements, |relation| {
        visited.push(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    Ok(visited)
}
