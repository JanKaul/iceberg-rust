use std::sync::Arc;

use datafusion::{common::stats::Precision, datasource::DefaultTableSource};
use datafusion_expr::LogicalPlan;

use crate::error::Error;
use iceberg_rust::error::Error as IcebergError;

pub trait CardinalityEstimate {
    fn cardinality(plan: &LogicalPlan) -> Option<usize> {
        estimate_cardinality(plan).ok()
    }
}

fn estimate_cardinality(plan: &LogicalPlan) -> Result<usize, Error> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let input_cardinality = estimate_cardinality(&filter.input)?;
            Ok((0.1 * input_cardinality as f64) as usize)
        }
        LogicalPlan::TableScan(scan) => {
            if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
                let statistics = source
                    .table_provider
                    .statistics()
                    .ok_or(IcebergError::NotFound("Table statistics".to_owned()))?;
                if let Precision::Exact(num_rows) | Precision::Inexact(num_rows) =
                    statistics.num_rows
                {
                    Ok(num_rows)
                } else {
                    Err(IcebergError::InvalidFormat("Num rows".to_owned()).into())
                }
            } else {
                Err(IcebergError::InvalidFormat("Table source".to_owned()).into())
            }
        }
        x => {
            let inputs = x.inputs();
            if inputs.len() == 1 {
                estimate_cardinality(inputs[0])
            } else {
                Err(IcebergError::InvalidFormat("LogicalPlan".to_owned()).into())
            }
        }
    }
}
