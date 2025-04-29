use core::fmt;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use datafusion::common::DFSchemaRef;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(PartialEq, Eq, Hash, PartialOrd)]
pub struct PosDeltaNode {
    pub input: Arc<LogicalPlan>,
    pub aliases: BTreeMap<String, String>,
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

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self {
            input: Arc::new(inputs[0].clone()),
            aliases: BTreeMap::new(),
        })
    }
}

impl PosDeltaNode {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self {
            input: plan,
            aliases: BTreeMap::new(),
        }
    }

    pub fn new_with_aliases(plan: Arc<LogicalPlan>, aliases: BTreeMap<String, String>) -> Self {
        Self {
            input: plan,
            aliases,
        }
    }
}

impl From<PosDeltaNode> for LogicalPlan {
    fn from(value: PosDeltaNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(value),
        })
    }
}

#[derive(PartialEq, Eq, Hash, PartialOrd)]
pub struct NegDeltaNode {
    pub input: Arc<LogicalPlan>,
    pub aliases: BTreeMap<String, String>,
}

impl Debug for NegDeltaNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for NegDeltaNode {
    fn name(&self) -> &str {
        "NegDelta"
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
        write!(f, "NegDelta")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            input: Arc::new(inputs[0].clone()),
            aliases: BTreeMap::new(),
        })
    }
}

impl NegDeltaNode {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self {
            input: plan,
            aliases: BTreeMap::new(),
        }
    }

    pub fn new_with_aliases(plan: Arc<LogicalPlan>, aliases: BTreeMap<String, String>) -> Self {
        Self {
            input: plan,
            aliases,
        }
    }
}

impl From<NegDeltaNode> for LogicalPlan {
    fn from(value: NegDeltaNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(value),
        })
    }
}
