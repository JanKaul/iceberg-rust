use datafusion::{common::Column, error::DataFusionError, scalar::ScalarValue};
use datafusion_expr::{BinaryExpr, Case, Expr, Operator};
use iceberg_rust::error::Error;

pub fn incremental_aggregate_function(
    left: &[Expr],
    right: &[Expr],
) -> Result<Expr, DataFusionError> {
    if left.len() == 1 {
        let func_name = if let Expr::AggregateFunction(left) = &left[0] {
            Ok(left.func.name().to_string())
        } else {
            Err(DataFusionError::External(Box::new(Error::InvalidFormat(
                format!(
                    "The expression {:?}, {:?} are not aggregate expressions",
                    left[0], right[0]
                ),
            ))))
        }?;
        match func_name.as_str() {
            "count" => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::Column(Column::new(
                    None::<String>,
                    left[0].name_for_alias()?,
                ))),
                Operator::Plus,
                Box::new(right[0].clone()),
            ))
            .alias(left[0].name_for_alias()?)),
            "sum" => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(Expr::Column(Column::new(
                    None::<String>,
                    left[0].name_for_alias()?,
                ))),
                Operator::Plus,
                Box::new(right[0].clone()),
            ))
            .alias(left[0].name_for_alias()?)),
            "min" => Ok(Expr::Case(Case::new(
                Some(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::Column(Column::new(
                        None::<String>,
                        left[0].name_for_alias()?,
                    ))),
                    Operator::Lt,
                    Box::new(right[0].clone()),
                )))),
                vec![
                    (
                        Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                        Box::new(Expr::Column(Column::new(
                            None::<String>,
                            left[0].name_for_alias()?,
                        ))),
                    ),
                    (
                        Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)))),
                        Box::new(right[0].clone()),
                    ),
                ],
                None,
            ))),
            "max" => Ok(Expr::Case(Case::new(
                Some(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::Column(Column::new(
                        None::<String>,
                        left[0].name_for_alias()?,
                    ))),
                    Operator::Gt,
                    Box::new(right[0].clone()),
                )))),
                vec![
                    (
                        Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                        Box::new(Expr::Column(Column::new(
                            None::<String>,
                            left[0].name_for_alias()?,
                        ))),
                    ),
                    (
                        Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)))),
                        Box::new(right[0].clone()),
                    ),
                ],
                None,
            ))),
            x => Err(DataFusionError::External(Box::new(Error::NotSupported(
                format!("Aggregate function {x} is not supported for incremental refreshs."),
            )))),
        }
    } else {
        Err(DataFusionError::External(Box::new(Error::NotSupported(
            format!("Aggregate functions with more than one arguments"),
        ))))
    }
}
