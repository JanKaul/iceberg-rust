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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_returns_single_relation() {
        let relations = find_relations("SELECT id FROM orders").unwrap();
        assert_eq!(relations, vec!["orders".to_string()]);
    }

    #[test]
    fn test_qualified_relation_preserves_namespace_dots() {
        let relations = find_relations("SELECT * FROM analytics.daily.orders").unwrap();
        assert_eq!(relations, vec!["analytics.daily.orders".to_string()]);
    }

    #[test]
    fn test_join_returns_both_relations() {
        let relations = find_relations(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
        )
        .unwrap();
        assert_eq!(
            relations,
            vec!["orders".to_string(), "customers".to_string()],
        );
    }

    #[test]
    fn test_subquery_in_from_surfaces_inner_relation() {
        let relations =
            find_relations("SELECT total FROM (SELECT SUM(amount) AS total FROM payments) p")
                .unwrap();
        assert_eq!(relations, vec!["payments".to_string()]);
    }

    #[test]
    fn test_cte_includes_underlying_table_and_alias_reference() {
        // CTEs are themselves visited as relations: both the underlying table
        // and the CTE name referenced from the outer SELECT appear, in the
        // order the AST walker visits them.
        let relations = find_relations(
            "WITH recent AS (SELECT * FROM events WHERE ts > now() - INTERVAL '7 day') \
             SELECT * FROM recent",
        )
        .unwrap();
        assert!(
            relations.contains(&"events".to_string()),
            "underlying table missing, got {relations:?}",
        );
        assert!(
            relations.contains(&"recent".to_string()),
            "CTE reference missing, got {relations:?}",
        );
    }

    #[test]
    fn test_union_walks_both_branches() {
        let relations =
            find_relations("SELECT id FROM eu_users UNION ALL SELECT id FROM us_users").unwrap();
        assert_eq!(
            relations,
            vec!["eu_users".to_string(), "us_users".to_string()],
        );
    }

    #[test]
    fn test_self_join_returns_same_relation_twice() {
        let relations = find_relations(
            "SELECT a.id, b.id FROM employees a JOIN employees b ON a.manager_id = b.id",
        )
        .unwrap();
        assert_eq!(
            relations,
            vec!["employees".to_string(), "employees".to_string()],
        );
    }

    #[test]
    fn test_multiple_statements_aggregate_relations() {
        let relations = find_relations(
            "INSERT INTO sink SELECT * FROM source_a; \
             INSERT INTO sink SELECT * FROM source_b;",
        )
        .unwrap();
        // Each statement contributes its target + its source, in order.
        assert_eq!(
            relations,
            vec![
                "sink".to_string(),
                "source_a".to_string(),
                "sink".to_string(),
                "source_b".to_string(),
            ],
        );
    }

    #[test]
    fn test_malformed_sql_propagates_parser_error() {
        let err = find_relations("SELECT FROM").err().unwrap();
        assert!(matches!(err, Error::SQLParser(_)), "got {err:?}");
    }

    #[test]
    fn test_empty_string_returns_empty_vec() {
        // sqlparser treats an empty input as zero statements rather than an
        // error, so the walker visits nothing.
        let relations = find_relations("").unwrap();
        assert!(relations.is_empty(), "got {relations:?}");
    }
}
