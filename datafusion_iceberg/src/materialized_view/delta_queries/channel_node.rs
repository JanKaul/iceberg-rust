use core::panic;
use std::{
    cmp::min,
    fmt::{self, Debug},
    hash::Hash,
    iter,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    task::{Context, Poll},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::DFSchemaRef,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream, SessionState},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PlanProperties,
    },
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use futures::{
    channel::mpsc::{channel, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender},
    SinkExt, Stream, StreamExt, TryStreamExt,
};
use pin_project_lite::pin_project;

pub fn channel_nodes(plan: Arc<LogicalPlan>) -> (SenderNode, ReceiverNode) {
    let (left_sender, left_reciever) = channel(1);
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
    pub(crate) input: Arc<LogicalPlan>,
    sender: Sender<(
        PlanProperties,
        Vec<Arc<Mutex<Option<UnboundedReceiver<Result<RecordBatch, DataFusionError>>>>>>,
    )>,
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
        writeln!(f, "SenderNode")?;
        write!(f, "{}", self.input)
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

impl From<SenderNode> for LogicalPlan {
    fn from(value: SenderNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(value),
        })
    }
}

pub struct ReceiverNode {
    input: Arc<LogicalPlan>,
    receiver: Arc<
        Mutex<
            Option<
                Receiver<(
                    PlanProperties,
                    Vec<
                        Arc<Mutex<Option<UnboundedReceiver<Result<RecordBatch, DataFusionError>>>>>,
                    >,
                )>,
            >,
        >,
    >,
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
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "ReceiverNode")
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        panic!("Creating fork node from template is not allowed");
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self {
            input: self.input.clone(),
            receiver: self.receiver.clone(),
        })
    }
}

impl From<ReceiverNode> for LogicalPlan {
    fn from(value: ReceiverNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(value),
        })
    }
}

pub(crate) struct PhysicalSenderNode {
    input: Arc<dyn ExecutionPlan>,
    sender: Vec<UnboundedSender<Result<RecordBatch, DataFusionError>>>,
    closed_sender: Vec<Arc<AtomicUsize>>,
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
        "PhysicalSenderNode"
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
        let closed_sender = iter::repeat_n((), self.closed_sender.len())
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        Ok(Arc::new(PhysicalSenderNode {
            input: children.pop().unwrap(),
            sender: self.sender.clone(),
            closed_sender,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let pin = self.input.clone().execute(partition, context.clone())?;
        let schema = pin.schema().clone();
        let n_partitions = self.properties().partitioning.partition_count();
        let n_receiver_partitions = self.closed_sender.len();
        let index = partition.rem_euclid(n_receiver_partitions);
        let mut count = n_partitions / n_receiver_partitions;
        if index < n_partitions % n_receiver_partitions {
            count += 1;
        }
        let unbounded_sender = self.sender[index].clone();
        Ok(Box::pin(RecordBatchStreamSender::new(
            schema,
            unbounded_sender.clone(),
            self.closed_sender[index].clone(),
            count,
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
    properties: PlanProperties,
    receiver: Vec<Arc<Mutex<Option<UnboundedReceiver<Result<RecordBatch, DataFusionError>>>>>>,
}

impl Debug for PhysicalReceiverNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PhysicalReceiverNode")
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
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 0);
        Ok(Arc::new(PhysicalReceiverNode {
            receiver: self.receiver.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let reciever = {
            let mut lock = self.receiver[partition].lock().unwrap();
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
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(fork_node) = node.as_any().downcast_ref::<SenderNode>() {
            assert_eq!(physical_inputs.len(), 1);
            assert_eq!(logical_inputs.len(), 1);
            assert!(fork_node
                .input
                .schema()
                .matches_arrow_schema(&physical_inputs[0].schema()));
            let parallelism = std::thread::available_parallelism().unwrap().get();
            let n_partitions = physical_inputs[0].output_partitioning().partition_count();
            let n_receiver_partitions = min(n_partitions, parallelism);
            let (sender, receiver): (
                Vec<UnboundedSender<Result<RecordBatch, DataFusionError>>>,
                Vec<_>,
            ) = iter::repeat_n((), n_receiver_partitions)
                .map(|_| {
                    let (sender, receiver) = unbounded();
                    (sender, Arc::new(Mutex::new(Some(receiver))))
                })
                .unzip();
            let closed_sender = iter::repeat_n((), n_receiver_partitions)
                .map(|_| Arc::new(AtomicUsize::new(0)))
                .collect();
            let properties = physical_inputs[0].properties().clone();
            let mut s = fork_node.sender.clone();
            s.send((
                properties
                    .with_partitioning(Partitioning::UnknownPartitioning(n_receiver_partitions)),
                receiver,
            ))
            .await
            .unwrap();
            s.close_channel();
            Ok(Some(Arc::new(PhysicalSenderNode {
                input: physical_inputs[0].clone(),
                sender,
                closed_sender,
            })))
        } else if let Some(fork_node) = node.as_any().downcast_ref::<ReceiverNode>() {
            assert_eq!(physical_inputs.len(), 0);
            assert_eq!(logical_inputs.len(), 0);
            let mut receiver = {
                let mut lock = fork_node.receiver.lock().unwrap();
                lock.take()
            }
            .ok_or(DataFusionError::Internal(
                "Fork node can only be executed once.".to_string(),
            ))
            .unwrap();
            let (properties, receiver) = receiver
                .next()
                .await
                .ok_or(DataFusionError::Internal(
                    "Fork node can only be executed once.".to_string(),
                ))
                .unwrap();
            Ok(Some(Arc::new(PhysicalReceiverNode {
                receiver,
                properties,
            })))
        } else {
            Ok(None)
        }
    }
}

pin_project! {
    pub struct RecordBatchStreamSender<S> {
        schema: SchemaRef,
        sender: UnboundedSender<Result<RecordBatch, DataFusionError>>,
        n_closed: Arc<AtomicUsize>,
        count: usize,

        #[pin]
        stream: S,
    }
}

impl<S> RecordBatchStreamSender<S> {
    pub fn new(
        schema: SchemaRef,
        sender: UnboundedSender<Result<RecordBatch, DataFusionError>>,
        n_closed: Arc<AtomicUsize>,
        count: usize,
        stream: S,
    ) -> Self {
        Self {
            schema,
            sender,
            n_closed,
            count,
            stream,
        }
    }
}

impl<S> std::fmt::Debug for RecordBatchStreamSender<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordBatchStreamSender")
            .field("schema", &self.schema)
            .finish()
    }
}

impl<S> Stream for RecordBatchStreamSender<S>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>>,
{
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let project = self.project();
        match project.stream.poll_next(cx) {
            Poll::Ready(None) => {
                let n_closed = project
                    .n_closed
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n_closed + 1 == *project.count {
                    let unbounded_sender = project.sender.clone();
                    unbounded_sender.close_channel();
                }
                Poll::Ready(None)
            }
            x => x,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> RecordBatchStream for RecordBatchStreamSender<S>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>>,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
