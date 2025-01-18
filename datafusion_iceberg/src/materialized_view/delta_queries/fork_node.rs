use core::panic;
use std::{
    fmt::{self, Debug},
    hash::Hash,
    iter,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::DFSchemaRef,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream, SessionState},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan, Partitioning, PlanProperties,
    },
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    stream, SinkExt, Stream, StreamExt, TryStreamExt,
};
use pin_project_lite::pin_project;

pub fn fork_node(plan: Arc<LogicalPlan>) -> (ForkNode, ForkNode) {
    let parallelism = std::thread::available_parallelism().unwrap().get();
    let (left_sender, (left_receiver, (right_sender, right_receiver))): (
        Vec<_>,
        (Vec<_>, (Vec<_>, Vec<_>)),
    ) = iter::repeat_n((), parallelism)
        .map(|_| {
            let (left_sender, left_receiver) = channel(8);
            let (right_sender, right_receiver) = channel(8);
            (
                left_sender,
                (
                    Arc::new(Mutex::new(Some(left_receiver))),
                    (right_sender, Arc::new(Mutex::new(Some(right_receiver)))),
                ),
            )
        })
        .unzip();
    let executed: Vec<Arc<AtomicBool>> = iter::repeat_n((), parallelism)
        .map(|_| Arc::new(AtomicBool::new(false)))
        .collect();
    (
        ForkNode {
            input: plan.clone(),
            sender: right_sender.clone(),
            receiver: left_receiver,
            own_sender: left_sender.clone(),
            executed: executed.clone(),
        },
        ForkNode {
            input: plan,
            sender: left_sender,
            receiver: right_receiver,
            own_sender: right_sender,
            executed,
        },
    )
}

pub struct ForkNode {
    pub(crate) input: Arc<LogicalPlan>,
    sender: Vec<Sender<Result<RecordBatch, DataFusionError>>>,
    receiver: Vec<Arc<Mutex<Option<Receiver<Result<RecordBatch, DataFusionError>>>>>>,
    own_sender: Vec<Sender<Result<RecordBatch, DataFusionError>>>,
    executed: Vec<Arc<AtomicBool>>,
}

impl PartialEq for ForkNode {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input)
    }
}

impl Eq for ForkNode {}

impl PartialOrd for ForkNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl Hash for ForkNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "ForkNode".hash(state);
        self.input.hash(state);
    }
}

impl fmt::Debug for ForkNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for ForkNode {
    fn name(&self) -> &str {
        "ForkNode"
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
        writeln!(f, "ForkNode")?;
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
            receiver: self.receiver.clone(),
            own_sender: self.own_sender.clone(),
            executed: self.executed.clone(),
        })
    }
}

impl From<ForkNode> for LogicalPlan {
    fn from(value: ForkNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(value),
        })
    }
}

pub(crate) struct PhysicalForkNode {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    sender: Vec<Sender<Result<RecordBatch, DataFusionError>>>,
    receiver: Vec<Arc<Mutex<Option<Receiver<Result<RecordBatch, DataFusionError>>>>>>,
    own_sender: Vec<Sender<Result<RecordBatch, DataFusionError>>>,
    executed: Vec<Arc<AtomicBool>>,
}

impl Debug for PhysicalForkNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PhysicalForkNode")?;
        self.input.fmt(f)
    }
}

impl DisplayAs for PhysicalForkNode {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        self.fmt(f)
    }
}

impl ExecutionPlan for PhysicalForkNode {
    fn name(&self) -> &str {
        "PhysicalForkNode"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        let properties = children[0]
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(self.executed.len()));
        Ok(Arc::new(PhysicalForkNode {
            input: children.pop().unwrap(),
            properties,
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            own_sender: self.own_sender.clone(),
            executed: self.executed.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input_partitions = self.input.properties().partitioning.partition_count();
        let n_partitions = self.executed.len();

        let executed = self.executed[partition]
            .compare_exchange(false, true, Ordering::Release, Ordering::Acquire)
            .is_err();

        if executed == true {
            let receiver = {
                let mut lock = self.receiver[partition].lock().unwrap();
                lock.take()
            }
            .ok_or(DataFusionError::Internal(
                "Fork node can only be executed once.".to_string(),
            ))
            .unwrap();

            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema().clone(),
                receiver,
            )));
        }

        self.own_sender[partition].clone().close_channel();

        let schema = self.schema().clone();
        let sender = self.sender[partition].clone();

        if partition >= input_partitions {
            return Ok(Box::pin(RecordBatchStreamSender::new(
                schema,
                sender,
                stream::empty(),
            )));
        }

        // If there are more input_partitions then partitions, some partitions have to execute multiple input partitions
        let mut count = input_partitions / n_partitions;
        if partition < input_partitions % n_partitions {
            count += 1;
        }

        // If multiple input_partitions have to be executed, chain their batch streams together
        let stream = (1..count).try_fold(
            self.input.clone().execute(partition, context.clone())?,
            |acc, x| {
                let partition = x * n_partitions + partition;
                Ok::<_, DataFusionError>(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema().clone(),
                    acc.chain(self.input.clone().execute(partition, context.clone())?),
                )) as SendableRecordBatchStream)
            },
        )?;

        Ok(Box::pin(RecordBatchStreamSender::new(
            schema,
            sender.clone(),
            stream.and_then(move |batch| {
                let mut sender = sender.clone();
                async move {
                    sender
                        .send(Ok(batch.clone()))
                        .await
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    Ok(batch)
                }
            }),
        )))
    }
}

pub struct ForkNodePlanner {}

impl ForkNodePlanner {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for ForkNodePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(fork_node) = node.as_any().downcast_ref::<ForkNode>() {
            assert_eq!(physical_inputs.len(), 1);
            assert_eq!(logical_inputs.len(), 1);

            let len = fork_node.sender.len();
            let properties = physical_inputs[0]
                .properties()
                .clone()
                .with_partitioning(Partitioning::UnknownPartitioning(len));
            Ok(Some(Arc::new(PhysicalForkNode {
                input: physical_inputs[0].clone(),
                properties,
                sender: fork_node.sender.clone(),
                receiver: fork_node.receiver.clone(),
                own_sender: fork_node.own_sender.clone(),
                executed: fork_node.executed.clone(),
            })))
        } else {
            Ok(None)
        }
    }
}

pin_project! {
    pub struct RecordBatchStreamSender<S> {
        schema: SchemaRef,
        sender: Sender<Result<RecordBatch, DataFusionError>>,

        #[pin]
        stream: S,
    }
}

impl<S> RecordBatchStreamSender<S> {
    fn new(
        schema: SchemaRef,
        sender: Sender<Result<RecordBatch, DataFusionError>>,
        stream: S,
    ) -> Self {
        RecordBatchStreamSender {
            schema,
            sender,
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
                project.sender.close_channel();
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
