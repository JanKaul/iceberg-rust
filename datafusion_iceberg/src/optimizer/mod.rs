use datafusion::common::Statistics;
use datafusion_expr::LogicalPlan;

pub(crate) mod precedence_graph;
pub(crate) mod query_graph;

trait LogicalStatisticsProvider {
    fn statistics(&self) -> Statistics;
}

impl LogicalStatisticsProvider for LogicalPlan {
    fn statistics(&self) -> Statistics {
        todo!()
    }
}
