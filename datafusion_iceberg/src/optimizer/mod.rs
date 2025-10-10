use datafusion::common::Statistics;
use datafusion_expr::LogicalPlan;

pub(crate) mod cardinality;
pub(crate) mod precedence_graph;
pub(crate) mod query_graph;
pub(crate) mod selectivity;

trait LogicalStatisticsProvider {
    fn statistics(&self) -> Statistics;
}

impl LogicalStatisticsProvider for LogicalPlan {
    fn statistics(&self) -> Statistics {
        todo!()
    }
}
