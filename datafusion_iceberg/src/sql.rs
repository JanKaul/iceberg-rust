use std::ops::ControlFlow;

use datafusion::sql::sqlparser::{
    ast::{visit_relations_mut, Ident},
    dialect::GenericDialect,
    parser::Parser,
};
use itertools::Itertools;

use crate::error::Error;

pub(crate) fn transform_name(input: &str) -> String {
    input.replace('.', "__")
}

pub(crate) fn transform_relations(sql: &str) -> Result<Vec<String>, Error> {
    let mut statements = Parser::parse_sql(&GenericDialect, sql)?;

    visit_relations_mut(&mut statements, |relation| {
        relation.0 = vec![Ident::new(transform_name(
            &Itertools::intersperse(relation.0.iter().map(|x| x.value.as_str()), ".")
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
