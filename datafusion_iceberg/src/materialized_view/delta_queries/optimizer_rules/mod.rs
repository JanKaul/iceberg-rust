use std::{collections::HashMap, sync::Arc};

use datafusion::{
    catalog::TableProvider,
    datasource::{empty::EmptyTable, DefaultTableSource},
    error::DataFusionError,
    optimizer::AnalyzerRule,
    sql::TableReference,
};
use datafusion_expr::{Filter, Join, LogicalPlan, Projection, Union};
use iceberg_rust::materialized_view::SourceTableState;

use crate::DataFusionTable;

use super::delta_node::PosDeltaNode;

#[derive(Debug)]
pub struct PosDelta {
    source_table_state: HashMap<TableReference, SourceTableState>,
}

impl PosDelta {
    pub fn new(source_table_state: HashMap<TableReference, SourceTableState>) -> Self {
        Self { source_table_state }
    }
}

impl AnalyzerRule for PosDelta {
    fn name(&self) -> &str {
        "PosDelta"
    }
    fn analyze(
        &self,
        plan: LogicalPlan,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<LogicalPlan> {
        match &plan {
            LogicalPlan::Extension(ext) => {
                if ext.node.name() == "PosDelta" {
                    match ext.node.inputs()[0] {
                        LogicalPlan::Projection(proj) => {
                            let input = self
                                .analyze(
                                    PosDeltaNode {
                                        input: proj.input.clone(),
                                    }
                                    .into_logical_plan(),
                                    config,
                                )
                                .map(|x| Arc::new(x))?;
                            Ok(LogicalPlan::Projection(Projection::try_new(
                                proj.expr.clone(),
                                input,
                            )?))
                        }
                        LogicalPlan::Filter(filter) => {
                            let input = self
                                .analyze(
                                    PosDeltaNode {
                                        input: filter.input.clone(),
                                    }
                                    .into_logical_plan(),
                                    config,
                                )
                                .map(|x| Arc::new(x))?;
                            Ok(LogicalPlan::Filter(Filter::try_new(
                                filter.predicate.clone(),
                                input,
                            )?))
                        }
                        LogicalPlan::Join(join) => {
                            let delta_left = self
                                .analyze(
                                    PosDeltaNode {
                                        input: join.left.clone(),
                                    }
                                    .into_logical_plan(),
                                    config,
                                )
                                .map(|x| Arc::new(x))?;
                            let delta_right = self
                                .analyze(
                                    PosDeltaNode {
                                        input: join.right.clone(),
                                    }
                                    .into_logical_plan(),
                                    config,
                                )
                                .map(|x| Arc::new(x))?;
                            let delta_delta = LogicalPlan::Join(Join {
                                left: delta_left.clone(),
                                right: delta_right.clone(),
                                schema: join.schema.clone(),
                                on: join.on.clone(),
                                filter: join.filter.clone(),
                                join_type: join.join_type.clone(),
                                join_constraint: join.join_constraint.clone(),
                                null_equals_null: join.null_equals_null.clone(),
                            });
                            let left_delta = LogicalPlan::Join(Join {
                                left: join.left.clone(),
                                right: delta_right.clone(),
                                schema: join.schema.clone(),
                                on: join.on.clone(),
                                filter: join.filter.clone(),
                                join_type: join.join_type.clone(),
                                join_constraint: join.join_constraint.clone(),
                                null_equals_null: join.null_equals_null.clone(),
                            });
                            let right_delta = LogicalPlan::Join(Join {
                                left: delta_left.clone(),
                                right: join.right.clone(),
                                schema: join.schema.clone(),
                                on: join.on.clone(),
                                filter: join.filter.clone(),
                                join_type: join.join_type.clone(),
                                join_constraint: join.join_constraint.clone(),
                                null_equals_null: join.null_equals_null.clone(),
                            });
                            Ok(LogicalPlan::Union(Union {
                                inputs: vec![
                                    Arc::new(delta_delta),
                                    Arc::new(left_delta),
                                    Arc::new(right_delta),
                                ],
                                schema: join.schema.clone(),
                            }))
                        }
                        LogicalPlan::Union(union) => {
                            let inputs = union
                                .inputs
                                .iter()
                                .map(|input| {
                                    Ok(self
                                        .analyze(
                                            PosDeltaNode {
                                                input: input.clone(),
                                            }
                                            .into_logical_plan(),
                                            config,
                                        )
                                        .map(|x| Arc::new(x))?)
                                })
                                .collect::<datafusion::common::Result<_>>()?;
                            Ok(LogicalPlan::Union(Union {
                                inputs,
                                schema: union.schema.clone(),
                            }))
                        }
                        LogicalPlan::TableScan(scan) => {
                            let mut scan = scan.clone();
                            let mut table = scan
                                .source
                                .as_any()
                                .downcast_ref::<DefaultTableSource>()
                                .ok_or(DataFusionError::Plan(format!(
                                    "Table scan {} doesn't target a Datafusion DefaultTableSource.",
                                    scan.table_name
                                )))?
                                .table_provider
                                .as_any()
                                .downcast_ref::<DataFusionTable>()
                                .ok_or(DataFusionError::Plan(format!(
                                    "Table scan {} doesn't reference an Iceberg table.",
                                    scan.table_name
                                )))?
                                .clone();
                            let table_provider: Arc<dyn TableProvider> =
                                match self.source_table_state.get(&scan.table_name).unwrap() {
                                    SourceTableState::Fresh => {
                                        Arc::new(EmptyTable::new(table.schema))
                                    }
                                    SourceTableState::Outdated(id) => {
                                        table.snapshot_range = (Some(*id), None);
                                        Arc::new(table)
                                    }
                                    SourceTableState::Invalid => Arc::new(table),
                                };
                            scan.source = Arc::new(DefaultTableSource::new(table_provider));
                            Ok(LogicalPlan::TableScan(scan))
                        }
                        x => Ok(x.clone()),
                    }
                } else {
                    Ok(plan)
                }
            }
            LogicalPlan::TableScan(scan) => {
                let mut scan = scan.clone();
                let mut table = scan
                    .source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                    .ok_or(DataFusionError::Plan(format!(
                        "Table scan {} doesn't target a Datafusion DefaultTableSource.",
                        scan.table_name
                    )))?
                    .table_provider
                    .as_any()
                    .downcast_ref::<DataFusionTable>()
                    .ok_or(DataFusionError::Plan(format!(
                        "Table scan {} doesn't reference an Iceberg table.",
                        scan.table_name
                    )))?
                    .clone();
                let table_provider: Arc<dyn TableProvider> =
                    match self.source_table_state.get(&scan.table_name).unwrap() {
                        SourceTableState::Fresh => Arc::new(table),
                        SourceTableState::Outdated(id) => {
                            table.snapshot_range = (None, Some(*id));
                            Arc::new(table)
                        }
                        SourceTableState::Invalid => Arc::new(EmptyTable::new(table.schema)),
                    };
                scan.source = Arc::new(DefaultTableSource::new(table_provider));
                Ok(LogicalPlan::TableScan(scan))
            }
            _ => Ok(plan),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::{ops::Deref, sync::Arc};

    use datafusion::config::ConfigOptions;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use datafusion_expr::LogicalPlan;
    use iceberg_rust::catalog::Catalog;
    use iceberg_rust::materialized_view::SourceTableState;
    use iceberg_rust::object_store::ObjectStoreBuilder;
    use iceberg_rust::spec::schema::Schema;
    use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};
    use iceberg_rust::table::Table;
    use iceberg_sql_catalog::SqlCatalog;

    use crate::materialized_view::delta_queries::{
        delta_node::PosDeltaNode, optimizer_rules::PosDelta,
    };
    use crate::DataFusionTable;

    #[tokio::test]
    async fn test_projection() {
        let ctx = SessionContext::new();

        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "name".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let table = Table::builder()
            .with_name("users")
            .with_location("test/users")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog)
            .await
            .expect("Failed to build view");

        let table = Arc::new(DataFusionTable::from(table));

        ctx.register_table("public.users", table).unwrap();

        let sql = "select id, name from public.users;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let analyzer = Analyzer::with_rules(vec![Arc::new(PosDelta {
            source_table_state: HashMap::from_iter(vec![(
                TableReference::parse_str("public.users"),
                SourceTableState::Fresh,
            )]),
        })]);

        let output = analyzer
            .execute_and_check(delta_plan, &ConfigOptions::default(), |_x, _y| ())
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::TableScan(table) = proj.input.deref() {
                assert_eq!(table.table_name.table(), "users")
            } else {
                panic!("Node is not a table scan.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }

    #[tokio::test]
    async fn test_filter() {
        let ctx = SessionContext::new();

        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "name".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let table = Table::builder()
            .with_name("users")
            .with_location("test/users")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog)
            .await
            .expect("Failed to build view");

        let table = Arc::new(DataFusionTable::from(table));

        ctx.register_table("public.users", table).unwrap();

        let sql = "select * from public.users where id = 1;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let analyzer = Analyzer::with_rules(vec![Arc::new(PosDelta {
            source_table_state: HashMap::from_iter(vec![(
                TableReference::parse_str("public.users"),
                SourceTableState::Fresh,
            )]),
        })]);

        let output = analyzer
            .execute_and_check(delta_plan, &ConfigOptions::default(), |_x, _y| ())
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Filter(filter) = proj.input.deref() {
                if let LogicalPlan::TableScan(table) = filter.input.deref() {
                    assert_eq!(table.table_name.table(), "users")
                } else {
                    panic!("Node is not a table scan.")
                }
            } else {
                panic!("Node is not a filter.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }

    #[tokio::test]
    async fn test_join() {
        let ctx = SessionContext::new();

        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "name".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 3,
                        name: "address".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let table = Table::builder()
            .with_name("users")
            .with_location("test/users")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog.clone())
            .await
            .expect("Failed to build view");

        let table = Arc::new(DataFusionTable::from(table));

        ctx.register_table("public.users", table).unwrap();

        let schema = Schema::builder()
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "size".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "address".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let table = Table::builder()
            .with_name("homes")
            .with_location("test/homes")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog)
            .await
            .expect("Failed to build view");

        let table = Arc::new(DataFusionTable::from(table));

        ctx.register_table("public.homes", table).unwrap();

        let sql = "select users.name, homes.size from public.users join public.homes on users.address = homes.address;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let analyzer = Analyzer::with_rules(vec![Arc::new(PosDelta {
            source_table_state: HashMap::from_iter(vec![
                (
                    TableReference::parse_str("public.users"),
                    SourceTableState::Fresh,
                ),
                (
                    TableReference::parse_str("public.homes"),
                    SourceTableState::Fresh,
                ),
            ]),
        })]);

        let output = analyzer
            .execute_and_check(delta_plan, &ConfigOptions::default(), |_x, _y| ())
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Union(union) = proj.input.deref() {
                if let LogicalPlan::Join(join) = union.inputs[0].deref() {
                    if let (LogicalPlan::TableScan(left), LogicalPlan::TableScan(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.table_name.table(), "users");
                        assert_eq!(right.table_name.table(), "homes")
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[1].deref() {
                    if let (LogicalPlan::TableScan(left), LogicalPlan::TableScan(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.table_name.table(), "users");
                        assert_eq!(right.table_name.table(), "homes")
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[2].deref() {
                    if let (LogicalPlan::TableScan(left), LogicalPlan::TableScan(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.table_name.table(), "users");
                        assert_eq!(right.table_name.table(), "homes")
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
            } else {
                panic!("Node is not a filter.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }

    #[tokio::test]
    async fn test_union() {
        let ctx = SessionContext::new();

        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        let schema = Schema::builder()
            .with_fields(
                StructType::builder()
                    .with_struct_field(StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .with_struct_field(StructField {
                        id: 2,
                        name: "name".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let table = Table::builder()
            .with_name("users")
            .with_location("test/users")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog.clone())
            .await
            .expect("Failed to build view");

        let table1 = Arc::new(DataFusionTable::from(table.clone()));

        ctx.register_table("public.users1", table1).unwrap();

        let table2 = Arc::new(DataFusionTable::from(table));

        ctx.register_table("public.users2", table2).unwrap();

        let sql =
            "select id, name from public.users1 union all select id, name from public.users2;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let analyzer = Analyzer::with_rules(vec![Arc::new(PosDelta {
            source_table_state: HashMap::from_iter(vec![
                (
                    TableReference::parse_str("public.users1"),
                    SourceTableState::Fresh,
                ),
                (
                    TableReference::parse_str("public.users2"),
                    SourceTableState::Fresh,
                ),
            ]),
        })]);

        let output = analyzer
            .execute_and_check(delta_plan, &ConfigOptions::default(), |_x, _y| ())
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Union(union) = output {
            if let LogicalPlan::Projection(proj) = union.inputs[0].deref() {
                if let LogicalPlan::TableScan(table) = proj.input.deref() {
                    assert_eq!(table.table_name.table(), "users1")
                } else {
                    panic!("Node is not a table scan.")
                }
            } else {
                panic!("Node is not a projection.")
            }
            if let LogicalPlan::Projection(proj) = union.inputs[1].deref() {
                if let LogicalPlan::TableScan(table) = proj.input.deref() {
                    assert_eq!(table.table_name.table(), "users2")
                } else {
                    panic!("Node is not a table scan.")
                }
            } else {
                panic!("Node is not a projection.")
            }
        } else {
            panic!("Node is not a filter.")
        }
    }
}