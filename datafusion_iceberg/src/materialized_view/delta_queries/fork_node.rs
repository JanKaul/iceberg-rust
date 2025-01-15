use core::panic;
use std::{
    fmt::{self, Debug},
    hash::Hash,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use datafusion::{
    arrow::array::RecordBatch,
    common::DFSchemaRef,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, SessionState},
    physical_plan::{stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    SinkExt, TryStreamExt,
};

pub fn channel(plan: Arc<LogicalPlan>) -> (SenderNode, ReceiverNode) {
    let (left_sender, left_reciever) = unbounded();
    (
        SenderNode {
            sender: left_sender,
            input: plan.clone(),
        },
        ReceiverNode {
            receiver: Arc::new(Mutex::new(Some(left_reciever))),
            input: plan,
        },
    )
}

pub struct SenderNode {
    input: Arc<LogicalPlan>,
    sender: UnboundedSender<Result<RecordBatch, DataFusionError>>,
}

impl PartialEq for SenderNode {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input)
    }
}

impl Eq for SenderNode {}

impl PartialOrd for SenderNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl Hash for SenderNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "SenderNode".hash(state);
        self.input.hash(state);
    }
}

impl fmt::Debug for SenderNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for SenderNode {
    fn name(&self) -> &str {
        "SenderNode"
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
        write!(f, "SenderNode")?;
        Debug::fmt(&self.input, f)
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        panic!("Creating fork node from template is not allowed");
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self {
            input: Arc::new(inputs.pop().unwrap()),
            sender: self.sender.clone(),
        })
    }
}

impl SenderNode {
    pub(crate) fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

pub struct ReceiverNode {
    input: Arc<LogicalPlan>,
    receiver: Arc<Mutex<Option<UnboundedReceiver<Result<RecordBatch, DataFusionError>>>>>,
}

impl PartialEq for ReceiverNode {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input)
    }
}

impl Eq for ReceiverNode {}

impl PartialOrd for ReceiverNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl Hash for ReceiverNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "ReceiverNode".hash(state);
        self.input.hash(state);
    }
}

impl fmt::Debug for ReceiverNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for ReceiverNode {
    fn name(&self) -> &str {
        "ReceiverNode"
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
        write!(f, "ReceiverNode")?;
        Debug::fmt(&self.input, f)
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        panic!("Creating fork node from template is not allowed");
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self {
            input: Arc::new(inputs.pop().unwrap()),
            receiver: self.receiver.clone(),
        })
    }
}

impl ReceiverNode {
    pub(crate) fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

pub(crate) struct PhysicalSenderNode {
    input: Arc<dyn ExecutionPlan>,
    sender: UnboundedSender<Result<RecordBatch, DataFusionError>>,
}

impl Debug for PhysicalSenderNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PhysicalSenderNode")?;
        self.input.fmt(f)
    }
}

impl DisplayAs for PhysicalSenderNode {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        self.fmt(f)
    }
}

impl ExecutionPlan for PhysicalSenderNode {
    fn name(&self) -> &str {
        "PhysicalReceiverNode"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(PhysicalSenderNode {
            input: children.pop().unwrap(),
            sender: self.sender.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let pin = self.input.clone().execute(partition, context.clone())?;
        let schema = self.schema().clone();
        let unbounded_sender = self.sender.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            pin.and_then(move |batch| {
                let mut unbounded_sender = unbounded_sender.clone();
                async move {
                    unbounded_sender
                        .send(Ok(batch.clone()))
                        .await
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    Ok(batch)
                }
            }),
        )))
    }
}

pub(crate) struct PhysicalReceiverNode {
    input: Arc<dyn ExecutionPlan>,
    reciever: Arc<Mutex<Option<UnboundedReceiver<Result<RecordBatch, DataFusionError>>>>>,
}

impl Debug for PhysicalReceiverNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PhysicalReceiverNode")?;
        self.input.fmt(f)
    }
}

impl DisplayAs for PhysicalReceiverNode {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        self.fmt(f)
    }
}

impl ExecutionPlan for PhysicalReceiverNode {
    fn name(&self) -> &str {
        "PhysicalReceiverNode"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(PhysicalReceiverNode {
            input: children.pop().unwrap(),
            reciever: self.reciever.clone(),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let reciever = {
            let mut lock = self.reciever.lock().unwrap();
            lock.take()
        }
        .ok_or(DataFusionError::Internal(
            "Fork node can only be executed once.".to_string(),
        ))
        .unwrap();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema().clone(),
            reciever,
        )))
    }
}

pub struct ChannelNodePlanner {}

impl ChannelNodePlanner {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for ChannelNodePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(fork_node) = node.as_any().downcast_ref::<SenderNode>() {
            Ok(Some(Arc::new(PhysicalSenderNode {
                input: physical_inputs[0].clone(),
                sender: fork_node.sender.clone(),
            })))
        } else if let Some(fork_node) = node.as_any().downcast_ref::<ReceiverNode>() {
            Ok(Some(Arc::new(PhysicalReceiverNode {
                input: physical_inputs[0].clone(),
                reciever: fork_node.receiver.clone(),
            })))
        } else {
            Ok(None)
        }
    }
}
