use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    sync::Arc,
};

use datafusion::{
    catalog::TableProvider,
    common::{tree_node::Transformed, Column},
    datasource::{empty::EmptyTable, DefaultTableSource},
    error::DataFusionError,
    sql::TableReference,
};
use datafusion_expr::{
    build_join_schema, Aggregate, Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan,
    Projection, SubqueryAlias, TableScan, Union,
};
use iceberg_rust::error::Error;

use crate::DataFusionTable;

use super::{
    aggregate_functions::incremental_aggregate_function,
    channel_node::channel_nodes,
    delta_node::{NegDeltaNode, PosDeltaNode},
};

pub(crate) fn delta_transform_down(
    plan: LogicalPlan,
    source_table_state: &HashMap<
        TableReference,
        (
            Option<Arc<dyn TableProvider>>,
            Option<Arc<dyn TableProvider>>,
        ),
    >,
    storage_table: Arc<dyn TableProvider>,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    let storage_table_reference = TableReference::parse_str("storage_table");

    let storage_table_scan = Arc::new(LogicalPlan::TableScan(TableScan::try_new(
        storage_table_reference.clone(),
        Arc::new(DefaultTableSource::new(storage_table)),
        None,
        Vec::new(),
        None,
    )?));

    let storage_table_schema = storage_table_scan.schema().clone();
    match &plan {
        LogicalPlan::Extension(ext) => {
            if ext.node.name() == "PosDelta" {
                let (storage_table_scan_one, storage_table_scan_two) =
                    channel_nodes(storage_table_scan);
                let node = ext.node.as_any().downcast_ref::<PosDeltaNode>().unwrap();
                match ext.node.inputs()[0] {
                    LogicalPlan::Filter(filter) => {
                        let input = Arc::new(PosDeltaNode::new(filter.input.clone()).into());
                        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                            filter.predicate.clone(),
                            input,
                        )?)))
                    }
                    LogicalPlan::Projection(proj) => {
                        let mut aliases = BTreeMap::new();
                        for expr in proj.expr.iter() {
                            if let Expr::Alias(alias) = expr {
                                aliases.insert(alias.expr.name_for_alias()?, alias.name.clone());
                            }
                        }
                        let input = Arc::new(
                            PosDeltaNode::new_with_aliases(proj.input.clone(), aliases).into(),
                        );
                        Ok(Transformed::yes(LogicalPlan::Projection(
                            Projection::try_new(proj.expr.clone(), input)?,
                        )))
                    }
                    LogicalPlan::Join(join) => {
                        let inputs = transform_join(join)?.0;

                        let data = LogicalPlan::Union(Union {
                            inputs,
                            schema: join.schema.clone(),
                        });
                        Ok(Transformed::yes(data))
                    }
                    LogicalPlan::Union(union) => {
                        let inputs = union
                            .inputs
                            .iter()
                            .map(|input| Ok(Arc::new(PosDeltaNode::new(input.clone()).into())))
                            .collect::<datafusion::common::Result<_>>()?;
                        Ok(Transformed::yes(LogicalPlan::Union(Union {
                            inputs,
                            schema: union.schema.clone(),
                        })))
                    }
                    LogicalPlan::SubqueryAlias(alias) => {
                        let input = Arc::new(PosDeltaNode::new(alias.input.clone()).into());
                        Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                            SubqueryAlias::try_new(input, alias.alias.clone())?,
                        )))
                    }
                    LogicalPlan::Aggregate(aggregate) => {
                        let delta = Arc::new(PosDeltaNode::new(aggregate.input.clone()).into());
                        let delta_aggregate =
                            Arc::new(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                                delta,
                                aggregate.group_expr.clone(),
                                aggregate.aggr_expr.clone(),
                                aggregate.schema.clone(),
                            )?));

                        let delta_aggregate_schema = delta_aggregate.schema().clone();

                        let (delta_aggregate_one, delta_aggregate_two) =
                            channel_nodes(delta_aggregate);

                        let join_schema = Arc::new(build_join_schema(
                            &delta_aggregate_schema,
                            &storage_table_schema,
                            &JoinType::Inner,
                        )?);

                        let storage_table_group_exprs = storage_table_expressions(
                            &aggregate.group_expr,
                            &storage_table_reference,
                            &node.aliases,
                        )?;

                        let join_on = aggregate
                            .group_expr
                            .clone()
                            .into_iter()
                            .zip(storage_table_group_exprs)
                            .collect::<Vec<_>>();

                        let join = Arc::new(LogicalPlan::Join(Join {
                            left: Arc::new(delta_aggregate_one.into()),
                            right: Arc::new(storage_table_scan_one.into()),
                            schema: join_schema,
                            on: join_on.clone(),
                            filter: None,
                            join_type: JoinType::Inner,
                            join_constraint: JoinConstraint::On,
                            null_equals_null: false,
                        }));

                        let storage_table_aggregate_exprs = storage_table_expressions(
                            &aggregate.aggr_expr,
                            &storage_table_reference,
                            &node.aliases,
                        )?;

                        let mut aggregation_exprs = aggregate.group_expr.clone();

                        aggregation_exprs.extend(
                            aggregate
                                .aggr_expr
                                .clone()
                                .into_iter()
                                .zip(storage_table_aggregate_exprs.into_iter())
                                .map(|(x, y)| incremental_aggregate_function(&vec![x], &vec![y]))
                                .collect::<Result<Vec<_>, _>>()?,
                        );

                        let aggregate_projection = Arc::new(LogicalPlan::Projection(
                            Projection::try_new(aggregation_exprs, join)?,
                        ));

                        let anti_join_schema = Arc::new(build_join_schema(
                            &delta_aggregate_schema,
                            &storage_table_schema,
                            &JoinType::LeftAnti,
                        )?);

                        let anti_join = Arc::new(LogicalPlan::Join(Join {
                            left: Arc::new(delta_aggregate_two.into()),
                            right: Arc::new(storage_table_scan_two.into()),
                            schema: anti_join_schema,
                            on: join_on,
                            filter: None,
                            join_type: JoinType::LeftAnti,
                            join_constraint: JoinConstraint::On,
                            null_equals_null: false,
                        }));

                        Ok(Transformed::yes(LogicalPlan::Union(Union {
                            inputs: vec![aggregate_projection, anti_join],
                            schema: aggregate.schema.clone(),
                        })))
                    }
                    LogicalPlan::TableScan(scan) => {
                        let mut scan = scan.clone();
                        let table_provider = source_table_state.get(&scan.table_name).unwrap();
                        scan.source = Arc::new(DefaultTableSource::new(
                            table_provider
                                .1
                                .as_ref()
                                .map(Clone::clone)
                                .unwrap_or(Arc::new(EmptyTable::new(Arc::new(
                                    scan.projected_schema.as_arrow().clone(),
                                )))),
                        ));
                        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
                    }
                    x => Err(DataFusionError::External(Box::new(Error::NotSupported(
                        format!("Logical plan {x}"),
                    )))),
                }
            } else if ext.node.name() == "NegDelta" {
                let node = ext.node.as_any().downcast_ref::<NegDeltaNode>().unwrap();
                match ext.node.inputs()[0] {
                    LogicalPlan::Projection(proj) => {
                        let mut aliases = BTreeMap::new();
                        for expr in proj.expr.iter() {
                            if let Expr::Alias(alias) = expr {
                                aliases.insert(alias.expr.name_for_alias()?, alias.name.clone());
                            }
                        }
                        let expr = proj
                            .expr
                            .clone()
                            .into_iter()
                            .filter(|x| matches!(x, Expr::Column(_)))
                            .collect();
                        let input = Arc::new(
                            NegDeltaNode::new_with_aliases(proj.input.clone(), aliases).into(),
                        );
                        Ok(Transformed::yes(LogicalPlan::Projection(
                            Projection::try_new(expr, input)?,
                        )))
                    }
                    LogicalPlan::Filter(filter) => {
                        let input = Arc::new(NegDeltaNode::new(filter.input.clone()).into());
                        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                            filter.predicate.clone(),
                            input,
                        )?)))
                    }
                    LogicalPlan::Join(join) => {
                        let delta_left: Arc<LogicalPlan> =
                            Arc::new(NegDeltaNode::new(join.left.clone()).into());
                        let delta_right: Arc<LogicalPlan> =
                            Arc::new(NegDeltaNode::new(join.right.clone()).into());
                        let delta_delta = LogicalPlan::Join(Join {
                            left: delta_left.clone(),
                            right: delta_right.clone(),
                            schema: join.schema.clone(),
                            on: join.on.clone(),
                            filter: join.filter.clone(),
                            join_type: join.join_type,
                            join_constraint: join.join_constraint,
                            null_equals_null: join.null_equals_null,
                        });
                        let left_delta = LogicalPlan::Join(Join {
                            left: join.left.clone(),
                            right: delta_right.clone(),
                            schema: join.schema.clone(),
                            on: join.on.clone(),
                            filter: join.filter.clone(),
                            join_type: join.join_type,
                            join_constraint: join.join_constraint,
                            null_equals_null: join.null_equals_null,
                        });
                        let right_delta = LogicalPlan::Join(Join {
                            left: delta_left.clone(),
                            right: join.right.clone(),
                            schema: join.schema.clone(),
                            on: join.on.clone(),
                            filter: join.filter.clone(),
                            join_type: join.join_type,
                            join_constraint: join.join_constraint,
                            null_equals_null: join.null_equals_null,
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
                            .map(|input| Ok(Arc::new(NegDeltaNode::new(input.clone()).into())))
                            .collect::<datafusion::common::Result<_>>()?;
                        Ok(Transformed::yes(LogicalPlan::Union(Union {
                            inputs,
                            schema: union.schema.clone(),
                        })))
                    }
                    LogicalPlan::Aggregate(aggregate) => {
                        let delta = Arc::new(PosDeltaNode::new(aggregate.input.clone()).into());
                        let delta_aggregate =
                            Arc::new(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                                delta,
                                aggregate.group_expr.clone(),
                                aggregate.aggr_expr.clone(),
                                aggregate.schema.clone(),
                            )?));

                        let join_schema = Arc::new(build_join_schema(
                            delta_aggregate.schema(),
                            &storage_table_schema,
                            &JoinType::Inner,
                        )?);

                        let storage_table_group_exprs = storage_table_expressions(
                            &aggregate.group_expr,
                            &storage_table_reference,
                            &node.aliases,
                        )?;

                        let join_on = aggregate
                            .group_expr
                            .clone()
                            .into_iter()
                            .zip(storage_table_group_exprs)
                            .collect::<Vec<_>>();

                        let join = Arc::new(LogicalPlan::Join(Join {
                            left: delta_aggregate.clone(),
                            right: storage_table_scan,
                            schema: join_schema,
                            on: join_on.clone(),
                            filter: None,
                            join_type: JoinType::Inner,
                            join_constraint: JoinConstraint::On,
                            null_equals_null: false,
                        }));

                        Ok(Transformed::yes(LogicalPlan::Projection(
                            Projection::try_new(aggregate.group_expr.clone(), join)?,
                        )))
                    }
                    LogicalPlan::SubqueryAlias(alias) => {
                        let input = Arc::new(NegDeltaNode::new(alias.input.clone()).into());
                        Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                            SubqueryAlias::try_new(input, alias.alias.clone())?,
                        )))
                    }
                    LogicalPlan::TableScan(scan) => {
                        let mut scan = scan.clone();
                        let table = scan
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
                        let table_provider = Arc::new(EmptyTable::new(table.schema));
                        scan.source = Arc::new(DefaultTableSource::new(table_provider));
                        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
                    }
                    x => Err(DataFusionError::External(Box::new(Error::NotSupported(
                        format!("Logical plan {x}"),
                    )))),
                }
            } else {
                Ok(Transformed::no(plan))
            }
        }
        LogicalPlan::TableScan(scan) => {
            if scan.table_name.to_string() == "storage_table" {
                return Ok(Transformed::no(plan));
            }
            let mut scan = scan.clone();
            let table_provider = source_table_state.get(&scan.table_name).unwrap();
            scan.source = Arc::new(DefaultTableSource::new(
                table_provider
                    .0
                    .as_ref()
                    .map(Clone::clone)
                    .unwrap_or(Arc::new(EmptyTable::new(Arc::new(
                        scan.projected_schema.as_arrow().clone(),
                    )))),
            ));
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn transform_join(
    join: &Join,
) -> Result<(Vec<Arc<LogicalPlan>>, Arc<LogicalPlan>), DataFusionError> {
    let (delta_left_vec, left, delta_right_vec, right): (
        Vec<Arc<LogicalPlan>>,
        Arc<LogicalPlan>,
        Vec<Arc<LogicalPlan>>,
        Arc<LogicalPlan>,
    ) = match (join.left.deref(), join.right.deref()) {
        (LogicalPlan::Join(_), LogicalPlan::Join(_)) => Err(DataFusionError::External(Box::new(
            Error::NotSupported("Materialzied views cannot join two joins.".to_owned()),
        ))),
        (LogicalPlan::Join(left), _) => {
            let (delta_x, x) = transform_join(left)?;
            Ok((
                delta_x,
                x,
                vec![Arc::new(PosDeltaNode::new(join.right.clone()).into())],
                join.right.clone(),
            ))
        }
        (_, LogicalPlan::Join(right)) => {
            let (delta_x, x) = transform_join(right)?;
            Ok((
                vec![Arc::new(PosDeltaNode::new(join.left.clone()).into())],
                join.left.clone(),
                delta_x,
                x,
            ))
        }
        _ => Ok((
            vec![Arc::new(PosDeltaNode::new(join.left.clone()).into())],
            join.left.clone(),
            vec![Arc::new(PosDeltaNode::new(join.right.clone()).into())],
            join.right.clone(),
        )),
    }?;
    let mut inputs = Vec::new();
    for delta_left in delta_left_vec.iter() {
        for delta_right in delta_right_vec.iter() {
            let (delta_left_one, delta_left_two) = channel_nodes(delta_left.clone());
            let (delta_right_one, delta_right_two) = channel_nodes(delta_right.clone());
            let delta_delta = LogicalPlan::Join(Join {
                left: Arc::new(delta_left_one.into()),
                right: Arc::new(delta_right_one.into()),
                schema: join.schema.clone(),
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_type: join.join_type,
                join_constraint: join.join_constraint,
                null_equals_null: join.null_equals_null,
            });
            let left_delta = LogicalPlan::Join(Join {
                left: left.clone(),
                right: Arc::new(delta_right_two.into()),
                schema: join.schema.clone(),
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_type: join.join_type,
                join_constraint: join.join_constraint,
                null_equals_null: join.null_equals_null,
            });
            let right_delta = LogicalPlan::Join(Join {
                left: Arc::new(delta_left_two.into()),
                right: right.clone(),
                schema: join.schema.clone(),
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_type: join.join_type,
                join_constraint: join.join_constraint,
                null_equals_null: join.null_equals_null,
            });
            inputs.push(Arc::new(delta_delta));
            inputs.push(Arc::new(left_delta));
            inputs.push(Arc::new(right_delta));
        }
    }
    let left_right = LogicalPlan::Join(Join {
        left,
        right,
        schema: join.schema.clone(),
        on: join.on.clone(),
        filter: join.filter.clone(),
        join_type: join.join_type,
        join_constraint: join.join_constraint,
        null_equals_null: join.null_equals_null,
    });
    Ok((inputs, Arc::new(left_right)))
}

fn storage_table_expressions(
    exprs: &[Expr],
    storage_table_reference: &TableReference,
    aliases: &BTreeMap<String, String>,
) -> Result<Vec<Expr>, DataFusionError> {
    exprs
        .iter()
        .map(|x| match x {
            Expr::Column(column) => Ok(Expr::Column(Column::new(
                Some(storage_table_reference.clone()),
                column.name.clone(),
            ))),
            x => {
                let mut name = x.name_for_alias()?;
                if let Some(alias) = aliases.get(&name) {
                    name = alias.clone();
                }
                Ok(Expr::Column(Column::new(
                    Some(storage_table_reference.clone()),
                    name,
                )))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::collections::HashMap;
    use std::{ops::Deref, sync::Arc};

    use datafusion::common::tree_node::TreeNode;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use datafusion_expr::LogicalPlan;
    use iceberg_rust::catalog::Catalog;
    use iceberg_rust::object_store::ObjectStoreBuilder;
    use iceberg_rust::spec::schema::Schema;
    use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};
    use iceberg_rust::table::Table;
    use iceberg_sql_catalog::SqlCatalog;

    use crate::materialized_view::delta_queries::channel_node::{ReceiverNode, SenderNode};
    use crate::materialized_view::delta_queries::delta_node::{NegDeltaNode, PosDeltaNode};
    use crate::materialized_view::delta_queries::transform::delta_transform_down;
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

        let source_table_state = HashMap::from_iter(vec![(
            TableReference::parse_str("public.users"),
            (None, None),
        )]);

        let storage_table = Arc::new(EmptyTable::new(Arc::new(
            logical_plan.schema().as_arrow().clone(),
        )));

        let delta_plan: LogicalPlan = PosDeltaNode::new(logical_plan.into()).into();
        let output = delta_plan
            .transform_down(|plan| {
                delta_transform_down(plan, &source_table_state, storage_table.clone())
            })
            .unwrap()
            .data;

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

        let source_table_state = HashMap::from_iter(vec![(
            TableReference::parse_str("public.users"),
            (None, None),
        )]);
        let storage_table = Arc::new(EmptyTable::new(Arc::new(
            logical_plan.schema().as_arrow().clone(),
        )));

        let delta_plan: LogicalPlan = PosDeltaNode::new(logical_plan.into()).into();

        let output = delta_plan
            .transform_down(|plan| {
                delta_transform_down(plan, &source_table_state, storage_table.clone())
            })
            .unwrap()
            .data;

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

        let source_table_state = HashMap::from_iter(vec![
            (TableReference::parse_str("public.users"), (None, None)),
            (TableReference::parse_str("public.homes"), (None, None)),
        ]);
        let storage_table = Arc::new(EmptyTable::new(Arc::new(
            logical_plan.schema().as_arrow().clone(),
        )));

        let delta_plan: LogicalPlan = PosDeltaNode::new(logical_plan.into()).into();
        let output = delta_plan
            .transform_down(|plan| {
                delta_transform_down(plan, &source_table_state, storage_table.clone())
            })
            .unwrap()
            .data;

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Union(union) = proj.input.deref() {
                if let LogicalPlan::Join(join) = union.inputs[0].deref() {
                    if let LogicalPlan::Extension(ext) = join.left.deref() {
                        if let Some(ext) = ext.node.as_any().downcast_ref::<SenderNode>() {
                            if let LogicalPlan::TableScan(table) = ext.input.deref() {
                                assert_eq!(table.table_name.table(), "users")
                            } else {
                                panic!("Node is not a table scan.")
                            }
                        } else {
                            panic!("Node is not a SenderNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                    if let LogicalPlan::Extension(ext) = join.right.deref() {
                        if let Some(ext) = ext.node.as_any().downcast_ref::<SenderNode>() {
                            if let LogicalPlan::TableScan(table) = ext.input.deref() {
                                assert_eq!(table.table_name.table(), "homes")
                            } else {
                                panic!("Node is not a table scan.")
                            }
                        } else {
                            panic!("Node is not a SenderNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[1].deref() {
                    if let LogicalPlan::TableScan(table) = join.left.deref() {
                        assert_eq!(table.table_name.table(), "users")
                    } else {
                        panic!("Node is not a table scan.")
                    }
                    if let LogicalPlan::Extension(ext) = join.right.deref() {
                        if let Some(_) = ext.node.as_any().downcast_ref::<ReceiverNode>() {
                        } else {
                            panic!("Node is not a ReceiverNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[2].deref() {
                    if let LogicalPlan::Extension(ext) = join.left.deref() {
                        if let Some(_) = ext.node.as_any().downcast_ref::<ReceiverNode>() {
                        } else {
                            panic!("Node is not a RecveiverNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                    if let LogicalPlan::TableScan(table) = join.right.deref() {
                        assert_eq!(table.table_name.table(), "homes")
                    } else {
                        panic!("Node is not a table scan.")
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

    async fn test_multi_join() {
        let ctx = SessionContext::new();

        let object_store = ObjectStoreBuilder::memory();

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalog::new("sqlite://", "test", object_store)
                .await
                .unwrap(),
        );

        // Users table schema
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

        let users_table = Table::builder()
            .with_name("users")
            .with_location("test/users")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog.clone())
            .await
            .expect("Failed to build users table");

        let users_table = Arc::new(DataFusionTable::from(users_table));

        ctx.register_table("public.users", users_table).unwrap();

        // Homes table schema
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
                    .with_struct_field(StructField {
                        id: 3,
                        name: "city_id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Long),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let homes_table = Table::builder()
            .with_name("homes")
            .with_location("test/homes")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog.clone())
            .await
            .expect("Failed to build homes table");

        let homes_table = Arc::new(DataFusionTable::from(homes_table));

        ctx.register_table("public.homes", homes_table).unwrap();

        // Cities table schema
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
                        name: "city_name".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let cities_table = Table::builder()
            .with_name("cities")
            .with_location("test/cities")
            .with_schema(schema)
            .build(&["public".to_owned()], catalog)
            .await
            .expect("Failed to build cities table");

        let cities_table = Arc::new(DataFusionTable::from(cities_table));

        ctx.register_table("public.cities", cities_table).unwrap();

        let sql = "select users.name, homes.size, cities.city_name \
               from public.users \
               join public.homes on users.address = homes.address \
               join public.cities on homes.city_id = cities.id;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let source_table_state = HashMap::from_iter(vec![
            (TableReference::parse_str("public.users"), (None, None)),
            (TableReference::parse_str("public.homes"), (None, None)),
            (TableReference::parse_str("public.cities"), (None, None)),
        ]);

        let storage_table = Arc::new(EmptyTable::new(Arc::new(
            logical_plan.schema().as_arrow().clone(),
        )));

        let delta_plan: LogicalPlan = PosDeltaNode::new(logical_plan.into()).into();
        let output = delta_plan
            .transform_down(|plan| {
                delta_transform_down(plan, &source_table_state, storage_table.clone())
            })
            .unwrap()
            .data;

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Union(union) = proj.input.deref() {
                if let LogicalPlan::Join(join) = union.inputs[0].deref() {
                    if let LogicalPlan::Extension(ext) = join.left.deref() {
                        if let Some(ext) = ext.node.as_any().downcast_ref::<SenderNode>() {
                            if let LogicalPlan::TableScan(table) = ext.input.deref() {
                                assert_eq!(table.table_name.table(), "users")
                            } else {
                                panic!("Node is not a table scan.")
                            }
                        } else {
                            panic!("Node is not a SenderNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                    if let LogicalPlan::Extension(ext) = join.right.deref() {
                        if let Some(ext) = ext.node.as_any().downcast_ref::<SenderNode>() {
                            if let LogicalPlan::TableScan(table) = ext.input.deref() {
                                assert_eq!(table.table_name.table(), "homes")
                            } else {
                                panic!("Node is not a table scan.")
                            }
                        } else {
                            panic!("Node is not a SenderNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[1].deref() {
                    if let LogicalPlan::TableScan(table) = join.left.deref() {
                        assert_eq!(table.table_name.table(), "users")
                    } else {
                        panic!("Node is not a table scan.")
                    }
                    if let LogicalPlan::Extension(ext) = join.right.deref() {
                        if let Some(_) = ext.node.as_any().downcast_ref::<ReceiverNode>() {
                        } else {
                            panic!("Node is not a ReceiverNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[2].deref() {
                    if let LogicalPlan::Extension(ext) = join.left.deref() {
                        if let Some(_) = ext.node.as_any().downcast_ref::<ReceiverNode>() {
                        } else {
                            panic!("Node is not a RecveiverNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                    if let LogicalPlan::TableScan(table) = join.right.deref() {
                        assert_eq!(table.table_name.table(), "homes")
                    } else {
                        panic!("Node is not a table scan.")
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

        let source_table_state = HashMap::from_iter(vec![
            (TableReference::parse_str("public.users1"), (None, None)),
            (TableReference::parse_str("public.users2"), (None, None)),
        ]);

        let storage_table = Arc::new(EmptyTable::new(Arc::new(
            logical_plan.schema().as_arrow().clone(),
        )));

        let delta_plan: LogicalPlan = PosDeltaNode::new(logical_plan.into()).into();
        let output = delta_plan
            .transform_down(|plan| {
                delta_transform_down(plan, &source_table_state, storage_table.clone())
            })
            .unwrap()
            .data;

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

    #[tokio::test]
    async fn test_aggregate() {
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

        let sql = "select sum(id) as total, name from public.users group by name;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let source_table_state = HashMap::from_iter(vec![(
            TableReference::parse_str("public.users"),
            (None, None),
        )]);

        let storage_table = Arc::new(EmptyTable::new(Arc::new(
            logical_plan.schema().as_arrow().clone(),
        )));

        let delta_plan: LogicalPlan = PosDeltaNode::new(logical_plan.into()).into();
        let output = delta_plan
            .transform_down(|plan| {
                delta_transform_down(plan, &source_table_state, storage_table.clone())
            })
            .unwrap()
            .data;

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Union(union) = proj.input.deref() {
                if let LogicalPlan::Projection(proj) = union.inputs[0].deref() {
                    if let LogicalPlan::Join(join) = proj.input.deref() {
                        if let LogicalPlan::Extension(ext) = join.left.deref() {
                            if let Some(ext) = ext.node.as_any().downcast_ref::<SenderNode>() {
                                if let LogicalPlan::Aggregate(aggregate) = ext.input.deref() {
                                    if let LogicalPlan::TableScan(table) = aggregate.input.deref() {
                                        assert_eq!(table.table_name.table(), "users")
                                    } else {
                                        panic!("Node is not a table scan.")
                                    }
                                } else {
                                    panic!("Node is not an aggregate.")
                                }
                            } else {
                                panic!("Node is not a SenderNode")
                            }
                        } else {
                            panic!("Node is not an extension")
                        }
                        if let LogicalPlan::Extension(ext) = join.right.deref() {
                            if let Some(ext) = ext.node.as_any().downcast_ref::<SenderNode>() {
                                if let LogicalPlan::TableScan(table) = ext.input.deref() {
                                    assert_eq!(table.table_name.table(), "storage_table")
                                } else {
                                    panic!("Node is not a table scan.")
                                }
                            } else {
                                panic!("Node is not a SenderNode")
                            }
                        } else {
                            panic!("Node is not an extension")
                        }
                    } else {
                        panic!("Node is not a CrossJoin.")
                    }
                } else {
                    panic!("Node is not a projection.")
                }
                if let LogicalPlan::Join(join) = union.inputs[1].deref() {
                    if let LogicalPlan::Extension(ext) = join.left.deref() {
                        if let Some(_) = ext.node.as_any().downcast_ref::<ReceiverNode>() {
                        } else {
                            panic!("Node is not a ReceiverNode")
                        }
                    } else {
                        panic!("Node is not an extension")
                    }
                    if let LogicalPlan::Extension(ext) = join.right.deref() {
                        if let Some(_) = ext.node.as_any().downcast_ref::<ReceiverNode>() {
                        } else {
                            panic!("Node is not a ReceiverNode")
                        }
                    } else {
                        panic!("Node is not an extension")
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
    async fn test_aggregate_neg() {
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

        let sql = "select sum(id) as total, name from public.users group by name;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let source_table_state = HashMap::from_iter(vec![(
            TableReference::parse_str("public.users"),
            (None, None),
        )]);

        let storage_table = Arc::new(EmptyTable::new(Arc::new(
            logical_plan.schema().as_arrow().clone(),
        )));

        let delta_plan: LogicalPlan = NegDeltaNode::new(logical_plan.into()).into();
        let output = delta_plan
            .transform_down(|plan| {
                delta_transform_down(plan, &source_table_state, storage_table.clone())
            })
            .unwrap()
            .data;

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Projection(proj) = proj.input.deref() {
                if let LogicalPlan::Join(join) = proj.input.deref() {
                    if let LogicalPlan::Aggregate(aggregate) = join.left.deref() {
                        if let LogicalPlan::TableScan(table) = aggregate.input.deref() {
                            assert_eq!(table.table_name.table(), "users")
                        } else {
                            panic!("Node is not a table scan.")
                        }
                    } else {
                        panic!("Node is not an aggregate.")
                    }
                    if let LogicalPlan::TableScan(table) = join.right.deref() {
                        assert_eq!(table.table_name.table(), "storage_table")
                    } else {
                        panic!("Node is not a table scan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
            } else {
                panic!("Node is not a projection.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }
}
