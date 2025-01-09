use datafusion::{error::DataFusionError, scalar::ScalarValue};
use datafusion_expr::{BinaryExpr, Case, Expr, Operator};
use iceberg_rust::error::Error;

pub fn incremental_aggregate_function(
    func_name: &str,
    args: &[Expr],
) -> Result<Expr, DataFusionError> {
    match func_name {
        "count" => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(args[0].clone()),
            Operator::Plus,
            Box::new(args[1].clone()),
        ))),
        "sum" => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(args[0].clone()),
            Operator::Plus,
            Box::new(args[1].clone()),
        ))),
        "min" => Ok(Expr::Case(Case::new(
            Some(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(args[0].clone()),
                Operator::Lt,
                Box::new(args[1].clone()),
            )))),
            vec![
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                    Box::new(args[0].clone()),
                ),
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)))),
                    Box::new(args[1].clone()),
                ),
            ],
            None,
        ))),
        "max" => Ok(Expr::Case(Case::new(
            Some(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(args[0].clone()),
                Operator::Gt,
                Box::new(args[1].clone()),
            )))),
            vec![
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                    Box::new(args[0].clone()),
                ),
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)))),
                    Box::new(args[1].clone()),
                ),
            ],
            None,
        ))),
        x => Err(DataFusionError::External(Box::new(Error::NotSupported(
            format!("Aggregate function {x} is not supported for incremental refreshs."),
        )))),
    }
}
