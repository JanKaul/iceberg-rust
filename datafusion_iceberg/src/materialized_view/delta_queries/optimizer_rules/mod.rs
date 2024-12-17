use std::sync::Arc;

use datafusion::{common::tree_node::Transformed, optimizer::OptimizerRule, sql::TableReference};
use datafusion_expr::{Filter, Join, LogicalPlan, Projection, Union};

use super::delta_node::PosDeltaNode;

#[derive(Debug)]
pub struct PosDelta {}

impl OptimizerRule for PosDelta {
    fn name(&self) -> &str {
        "PosDelta"
    }
    fn rewrite(
        &self,
        plan: datafusion_expr::LogicalPlan,
        config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<datafusion_expr::LogicalPlan>> {
        if let LogicalPlan::Extension(ext) = &plan {
            if ext.node.name() == "PosDelta" {
                match ext.node.inputs()[0] {
                    LogicalPlan::Projection(proj) => {
                        let input = self
                            .rewrite(
                                PosDeltaNode {
                                    input: proj.input.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )
                            .map(|x| Arc::new(x.data))?;
                        Ok(Transformed::yes(LogicalPlan::Projection(
                            Projection::try_new(proj.expr.clone(), input)?,
                        )))
                    }
                    LogicalPlan::Filter(filter) => {
                        let input = self
                            .rewrite(
                                PosDeltaNode {
                                    input: filter.input.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )
                            .map(|x| Arc::new(x.data))?;
                        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                            filter.predicate.clone(),
                            input,
                        )?)))
                    }
                    LogicalPlan::Join(join) => {
                        let delta_left = self
                            .rewrite(
                                PosDeltaNode {
                                    input: join.left.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )
                            .map(|x| Arc::new(x.data))?;
                        let delta_right = self
                            .rewrite(
                                PosDeltaNode {
                                    input: join.right.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )
                            .map(|x| Arc::new(x.data))?;
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
                        Ok(Transformed::yes(LogicalPlan::Union(Union {
                            inputs: vec![
                                Arc::new(delta_delta),
                                Arc::new(left_delta),
                                Arc::new(right_delta),
                            ],
                            schema: join.schema.clone(),
                        })))
                    }
                    LogicalPlan::Union(union) => {
                        let inputs = union
                            .inputs
                            .iter()
                            .map(|input| {
                                Ok(self
                                    .rewrite(
                                        PosDeltaNode {
                                            input: input.clone(),
                                        }
                                        .into_logical_plan(),
                                        config,
                                    )
                                    .map(|x| Arc::new(x.data))?)
                            })
                            .collect::<datafusion::common::Result<_>>()?;
                        Ok(Transformed::yes(LogicalPlan::Union(Union {
                            inputs,
                            schema: union.schema.clone(),
                        })))
                    }
                    LogicalPlan::TableScan(scan) => {
                        let mut scan = scan.clone();
                        scan.table_name = match scan.table_name {
                            TableReference::Bare { table } => TableReference::Bare {
                                table: (table.to_string() + "__pos__delta__").into(),
                            },
                            TableReference::Partial { table, schema } => TableReference::Partial {
                                table: (table.to_string() + "__pos__delta__").into(),
                                schema,
                            },
                            TableReference::Full {
                                table,
                                schema,
                                catalog,
                            } => TableReference::Full {
                                table: (table.to_string() + "__pos__delta__").into(),
                                schema,
                                catalog,
                            },
                        };
                        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
                    }
                    x => Ok(Transformed::no(x.clone())),
                }
            } else {
                Ok(Transformed::no(plan))
            }
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::optimizer::{optimizer::Optimizer, OptimizerContext};
    use datafusion::{datasource::MemTable, prelude::SessionContext};
    use datafusion_expr::LogicalPlan;

    use crate::materialized_view::delta_queries::{
        delta_node::PosDeltaNode, optimizer_rules::PosDelta,
    };

    #[tokio::test]
    async fn test_projection() {
        let ctx = SessionContext::new();

        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let table = Arc::new(MemTable::try_new(users_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users", table).unwrap();

        let sql = "select id, name from public.users;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::TableScan(table) = proj.input.deref() {
                assert_eq!(table.table_name.table(), "users__pos__delta__")
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

        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let table = Arc::new(MemTable::try_new(users_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users", table).unwrap();

        let sql = "select * from public.users where id = 1;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Filter(filter) = proj.input.deref() {
                if let LogicalPlan::TableScan(table) = filter.input.deref() {
                    assert_eq!(table.table_name.table(), "users__pos__delta__")
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

        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("address", DataType::Utf8, false),
        ]));

        let homes_schema = Arc::new(Schema::new(vec![
            Field::new("address", DataType::Utf8, false),
            Field::new("size", DataType::Int32, true),
        ]));

        let users_table = Arc::new(MemTable::try_new(users_schema, vec![vec![]]).unwrap());
        let homes_table = Arc::new(MemTable::try_new(homes_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users", users_table).unwrap();
        ctx.register_table("public.homes", homes_table).unwrap();

        let sql = "select users.name, homes.size from public.users join public.homes on users.address = homes.address;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Union(union) = proj.input.deref() {
                if let LogicalPlan::Join(join) = union.inputs[0].deref() {
                    if let (LogicalPlan::TableScan(left), LogicalPlan::TableScan(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.table_name.table(), "users__pos__delta__");
                        assert_eq!(right.table_name.table(), "homes__pos__delta__")
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
                        assert_eq!(right.table_name.table(), "homes__pos__delta__")
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
                        assert_eq!(left.table_name.table(), "users__pos__delta__");
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

        let users1_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let users2_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let table1 = Arc::new(MemTable::try_new(users1_schema, vec![vec![]]).unwrap());
        let table2 = Arc::new(MemTable::try_new(users2_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users1", table1).unwrap();
        ctx.register_table("public.users2", table2).unwrap();

        let sql =
            "select id, name from public.users1 union all select id, name from public.users2;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Union(union) = output {
            if let LogicalPlan::Projection(proj) = union.inputs[0].deref() {
                if let LogicalPlan::TableScan(table) = proj.input.deref() {
                    assert_eq!(table.table_name.table(), "users1__pos__delta__")
                } else {
                    panic!("Node is not a table scan.")
                }
            } else {
                panic!("Node is not a projection.")
            }
            if let LogicalPlan::Projection(proj) = union.inputs[1].deref() {
                if let LogicalPlan::TableScan(table) = proj.input.deref() {
                    assert_eq!(table.table_name.table(), "users2__pos__delta__")
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
