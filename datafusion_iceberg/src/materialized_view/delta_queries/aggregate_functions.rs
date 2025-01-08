use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    physical_plan::{
        expressions::{BinaryExpr, CaseExpr, Literal},
        PhysicalExpr,
    },
};
use datafusion_expr::Operator;
use iceberg_rust::error::Error;

pub fn incremental_aggregate_function(
    func_name: &str,
    args: &[Arc<dyn PhysicalExpr>],
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    match func_name {
        "count" => Ok(Arc::new(BinaryExpr::new(
            args[0].clone(),
            Operator::Plus,
            args[1].clone(),
        ))),
        "sum" => Ok(Arc::new(BinaryExpr::new(
            args[0].clone(),
            Operator::Plus,
            args[1].clone(),
        ))),
        "min" => Ok(Arc::new(CaseExpr::try_new(
            Some(Arc::new(BinaryExpr::new(
                args[0].clone(),
                Operator::Lt,
                args[1].clone(),
            ))),
            vec![
                (
                    Arc::new(Literal::new(datafusion::scalar::ScalarValue::Boolean(
                        Some(true),
                    ))),
                    args[0].clone(),
                ),
                (
                    Arc::new(Literal::new(datafusion::scalar::ScalarValue::Boolean(
                        Some(false),
                    ))),
                    args[1].clone(),
                ),
            ],
            None,
        )?)),
        "max" => Ok(Arc::new(CaseExpr::try_new(
            Some(Arc::new(BinaryExpr::new(
                args[0].clone(),
                Operator::Gt,
                args[1].clone(),
            ))),
            vec![
                (
                    Arc::new(Literal::new(datafusion::scalar::ScalarValue::Boolean(
                        Some(true),
                    ))),
                    args[0].clone(),
                ),
                (
                    Arc::new(Literal::new(datafusion::scalar::ScalarValue::Boolean(
                        Some(false),
                    ))),
                    args[1].clone(),
                ),
            ],
            None,
        )?)),
        x => Err(DataFusionError::External(Box::new(Error::NotSupported(
            format!("Aggregate function {x} is not supported for incremental refreshs."),
        )))),
    }
}
