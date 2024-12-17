use core::fmt;
use std::{fmt::Debug, sync::Arc};

use datafusion::common::DFSchemaRef;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(PartialEq, Eq, Hash, PartialOrd)]
pub struct PosDeltaNode {
    pub input: Arc<LogicalPlan>,
}

impl Debug for PosDeltaNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for PosDeltaNode {
    fn name(&self) -> &str {
        "PosDelta"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PosDelta")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Self {
            input: Arc::new(inputs[0].clone()),
        }
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self {
            input: Arc::new(inputs[0].clone()),
        })
    }
}

impl PosDeltaNode {
    pub fn new(plan: LogicalPlan) -> Self {
        Self {
            input: Arc::new(plan),
        }
    }

    pub(crate) fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

#[derive(PartialEq, Eq, PartialOrd, Hash)]
pub struct PosDeltaScanNode {
    pub input: LogicalPlan,
}

impl Debug for PosDeltaScanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for PosDeltaScanNode {
    fn name(&self) -> &str {
        "PosDeltaScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PosDeltaScan")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Self {
            input: inputs[0].clone(),
        }
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self {
            input: inputs[0].clone(),
        })
    }
}

impl PosDeltaScanNode {
    pub(crate) fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}
