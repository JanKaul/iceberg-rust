//! Custom DataFusion `ExecutionPlan` that applies Iceberg deletion vectors to
//! the output of a parquet scan.
//!
//! `IcebergDvExec` wraps a parquet plan that emits two extra columns: a
//! `__data_file_path` partition column and a Parquet row-number virtual column
//! (`RowNumber`) carrying each row's absolute position within its data file.
//! For each batch it groups rows by the path column, looks up the matching
//! [`DeletionVector`] in an `Arc<HashMap<String, DeletionVector>>`, and clears
//! the keep-mask bit for any row whose row number is present in the vector. The
//! mask is applied via [`arrow::compute::filter_record_batch`], and the
//! internal columns are stripped from the output — the path column is kept only
//! when the user opted in via `DataFusionTableConfig::enable_data_file_path_column`.
//!
//! Because positions come from the Parquet reader's row number — not a running
//! per-stream counter — filtering is correct regardless of how the scan is
//! partitioned, whether row groups are pruned by predicate pushdown, or the
//! order in which batches arrive. Distinct paths in the batch are extracted via
//! the Arrow shift-and-`distinct` idiom from
//! `iceberg_rust::arrow::partition::distinct_values_string`, so each DV is
//! looked up once per file rather than once per row.

use std::{
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{
        array::{Array, BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch, StringArray},
        compute::{filter_record_batch, kernels::cmp::eq},
        datatypes::{Schema as ArrowSchema, SchemaRef},
    },
    common::{tree_node::TreeNodeRecursion, DataFusionError},
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_expr::equivalence::ProjectionMapping,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties},
};
use futures::{Stream, StreamExt};
use iceberg_rust::arrow::partition::distinct_values_string;
use iceberg_rust::spec::{deletion_vector::DeletionVector, util};

/// Wraps a parquet scan that emits a `__data_file_path` partition column and a
/// Parquet row-number virtual column, and applies a path-keyed
/// [`DeletionVector`] to filter out deleted rows.
#[derive(Debug)]
pub(crate) struct IcebergDvExec {
    input: Arc<dyn ExecutionPlan>,
    /// DVs keyed by the normalized data-file path (output of
    /// `iceberg_rust_spec::util::strip_prefix`).
    dvs: Arc<HashMap<String, DeletionVector>>,
    /// Index of the path column in `input.schema()`.
    path_col_idx: usize,
    /// Index of the row-number virtual column in `input.schema()`.
    row_number_col_idx: usize,
    /// True when the path column was force-injected by the scan wiring and
    /// must be stripped from `IcebergDvExec`'s output (the user did not opt
    /// in to it via `DataFusionTableConfig::enable_data_file_path_column`).
    strip_path_col: bool,
    /// Sorted indices of the internal columns removed from the output: always
    /// the row-number column, plus the path column when `strip_path_col`.
    strip_indices: Vec<usize>,
    /// Output schema — `input.schema()` minus `strip_indices`.
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IcebergDvExec {
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        dvs: Arc<HashMap<String, DeletionVector>>,
        path_col_name: &str,
        row_number_col_name: &str,
        strip_path_col: bool,
    ) -> Result<Self, DataFusionError> {
        let input_schema = input.schema();
        let resolve = |name: &str| {
            input_schema.index_of(name).map_err(|_| {
                DataFusionError::Internal(format!(
                    "IcebergDvExec: column {name} not present in child schema"
                ))
            })
        };
        let path_col_idx = resolve(path_col_name)?;
        let row_number_col_idx = resolve(row_number_col_name)?;

        // The row-number column is always internal; the path column is stripped
        // only when it was force-injected.
        let mut strip_indices = vec![row_number_col_idx];
        if strip_path_col {
            strip_indices.push(path_col_idx);
        }
        strip_indices.sort_unstable();
        strip_indices.dedup();

        let retained_indices: Vec<_> = (0..input_schema.fields().len())
            .filter(|i| !strip_indices.contains(i))
            .collect();
        let fields: Vec<_> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| !strip_indices.contains(i))
            .map(|(_, f)| f.clone())
            .collect();
        let output_schema = Arc::new(ArrowSchema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));

        // Project schema-dependent properties across the removed internal
        // columns. Cloning the child's properties would retain its wider
        // schema and fail DataFusion's physical-plan validation.
        let input_properties = input.properties();
        let projection = ProjectionMapping::from_indices(&retained_indices, &input_schema)?;
        let eq_properties = input_properties
            .equivalence_properties()
            .project(&projection, output_schema.clone());
        let partitioning = input_properties
            .output_partitioning()
            .project(&projection, input_properties.equivalence_properties());
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                partitioning,
                input_properties.emission_type,
                input_properties.boundedness,
            )
            .with_evaluation_type(input_properties.evaluation_type)
            .with_scheduling_type(input_properties.scheduling_type),
        );
        Ok(Self {
            input,
            dvs,
            path_col_idx,
            row_number_col_idx,
            strip_path_col,
            strip_indices,
            output_schema,
            properties,
        })
    }
}

impl DisplayAs for IcebergDvExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IcebergDvExec: dv_files={}, strip_path_col={}",
            self.dvs.len(),
            self.strip_path_col
        )
    }
}

impl ExecutionPlan for IcebergDvExec {
    fn name(&self) -> &str {
        "IcebergDvExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // We do cheap per-batch work — more input parallelism is fine.
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergDvExec expects exactly one child, got {}",
                children.len()
            )));
        }
        let input = children.pop().unwrap();
        // The internal-column indices must still resolve in the new child's schema.
        let path_col_name = self.input.schema().field(self.path_col_idx).name().clone();
        let row_number_col_name = self
            .input
            .schema()
            .field(self.row_number_col_idx)
            .name()
            .clone();
        Ok(Arc::new(Self::try_new(
            input,
            self.dvs.clone(),
            &path_col_name,
            &row_number_col_name,
            self.strip_path_col,
        )?))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &datafusion::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        // Forward repartitioning to the child. Each row carries both its file
        // identity (`__data_file_path`) and its absolute file row number, so
        // reshuffling files *or* splitting a single file's row groups across
        // output partitions is safe — the DV lookup depends only on the
        // per-row values, not on stream order or completeness.
        let Some(new_input) = self.input.repartitioned(target_partitions, config)? else {
            return Ok(None);
        };
        let path_col_name = self.input.schema().field(self.path_col_idx).name().clone();
        let row_number_col_name = self
            .input
            .schema()
            .field(self.row_number_col_idx)
            .name()
            .clone();
        Ok(Some(Arc::new(Self::try_new(
            new_input,
            self.dvs.clone(),
            &path_col_name,
            &row_number_col_name,
            self.strip_path_col,
        )?)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(DvFilterStream {
            inner: stream,
            dvs: self.dvs.clone(),
            output_schema: self.output_schema.clone(),
            path_col_idx: self.path_col_idx,
            row_number_col_idx: self.row_number_col_idx,
            strip_indices: self.strip_indices.clone(),
        }))
    }
}

struct DvFilterStream {
    inner: SendableRecordBatchStream,
    dvs: Arc<HashMap<String, DeletionVector>>,
    output_schema: SchemaRef,
    path_col_idx: usize,
    row_number_col_idx: usize,
    /// Sorted indices of the internal columns removed from the output.
    strip_indices: Vec<usize>,
}

impl Stream for DvFilterStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let batch = match self.inner.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => batch,
        };
        Poll::Ready(Some(self.process(batch)))
    }
}

impl DvFilterStream {
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        if batch.num_rows() == 0 {
            return self.finalize(batch);
        }
        let path_col = batch
            .column(self.path_col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "IcebergDvExec: __data_file_path column must be Utf8".to_string(),
                )
            })?
            .clone();
        let row_numbers = batch
            .column(self.row_number_col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "IcebergDvExec: row-number column must be Int64".to_string(),
                )
            })?
            .clone();

        let mut keep = BooleanBufferBuilder::new(batch.num_rows());
        keep.append_n(batch.num_rows(), true);

        // Group rows by data file so each DV is looked up once, then probe the
        // bitmap at every row's true (absolute) file position. Positions come
        // from the Parquet row number, so this does not assume any batch
        // ordering or that the whole file is present in this stream.
        let distinct = distinct_values_string(Arc::new(path_col.clone()))?;
        for path in &distinct {
            let normalized = util::strip_prefix(path);
            let Some(dv) = self.dvs.get(&normalized) else {
                continue;
            };
            let mask_p = eq(&StringArray::new_scalar(path), &path_col)?;
            for i in collect_true_indices(&mask_p) {
                if dv.is_deleted(row_numbers.value(i) as u64) {
                    keep.set_bit(i, false);
                }
            }
        }

        let keep_array = BooleanArray::new(keep.finish(), None);
        let filtered = filter_record_batch(&batch, &keep_array)?;
        self.finalize(filtered)
    }

    fn finalize(&self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        // The row-number column is always internal, so there is always at least
        // one column to strip.
        let keep_cols: Vec<usize> = (0..batch.num_columns())
            .filter(|i| !self.strip_indices.contains(i))
            .collect();
        // `RecordBatch::project` rebuilds the schema; replace it with our
        // pre-computed `output_schema` so consumers see the canonical fields
        // (preserving any metadata we set up in `try_new`).
        let projected = batch.project(&keep_cols)?;
        Ok(RecordBatch::try_new(
            self.output_schema.clone(),
            projected.columns().to_vec(),
        )?)
    }
}

impl RecordBatchStream for DvFilterStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

/// Indices of `true` bits in `mask`, in order — the batch row indices that
/// belong to one data file.
fn collect_true_indices(mask: &BooleanArray) -> Vec<usize> {
    let mut out = Vec::with_capacity(mask.true_count());
    for i in 0..mask.len() {
        // Nulls in mask are treated as false (a path column should never be
        // null in our wiring, but defensively bypass them).
        if mask.is_valid(i) && mask.value(i) {
            out.push(i);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use roaring::RoaringTreemap;

    use super::*;

    const PATH_COL: &str = "__data_file_path";
    const ROW_NUMBER_COL: &str = "__iceberg_file_row_position";

    fn dv_with(positions: &[u64]) -> DeletionVector {
        let mut tm = RoaringTreemap::new();
        for p in positions {
            tm.insert(*p);
        }
        DeletionVector::from(tm)
    }

    // --- Stream-level tests via a hand-built input plan ----------------------

    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::{
        common::DataFusionError,
        execution::SendableRecordBatchStream,
        physical_plan::{
            execution_plan::{Boundedness, EmissionType},
            stream::RecordBatchStreamAdapter,
            PlanProperties,
        },
    };
    use futures::stream;

    /// Tiny ExecutionPlan that yields a fixed list of RecordBatches.
    #[derive(Debug)]
    struct MockBatches {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        properties: Arc<PlanProperties>,
    }

    impl MockBatches {
        fn new(batches: Vec<RecordBatch>) -> Arc<Self> {
            let schema = batches[0].schema();
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Arc::new(Self {
                schema,
                batches,
                properties,
            })
        }
    }

    impl DisplayAs for MockBatches {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "MockBatches")
        }
    }

    impl ExecutionPlan for MockBatches {
        fn name(&self) -> &str {
            "MockBatches"
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &Arc<dyn PhysicalExpr>,
            ) -> Result<TreeNodeRecursion, DataFusionError>,
        ) -> Result<TreeNodeRecursion, DataFusionError> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            let stream = stream::iter(self.batches.clone().into_iter().map(Ok));
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )))
        }
    }

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new(PATH_COL, DataType::Utf8, false),
            Field::new(ROW_NUMBER_COL, DataType::Int64, false),
        ]))
    }

    /// A batch of one file: `rows` are the user values, `positions` are the
    /// absolute Parquet row numbers (need not be contiguous or zero-based).
    fn make_batch(rows: &[i64], path: &str, positions: &[i64]) -> RecordBatch {
        assert_eq!(rows.len(), positions.len());
        let path_arr = StringArray::from(vec![path; rows.len()]);
        RecordBatch::try_new(
            make_schema(),
            vec![
                Arc::new(Int64Array::from(rows.to_vec())),
                Arc::new(path_arr),
                Arc::new(Int64Array::from(positions.to_vec())),
            ],
        )
        .unwrap()
    }

    /// A batch interleaving multiple files: each row is `(value, path, position)`.
    fn make_batch_interleaved(rows: &[(i64, &str, i64)]) -> RecordBatch {
        let values: Vec<i64> = rows.iter().map(|(v, _, _)| *v).collect();
        let paths: Vec<&str> = rows.iter().map(|(_, p, _)| *p).collect();
        let positions: Vec<i64> = rows.iter().map(|(_, _, pos)| *pos).collect();
        RecordBatch::try_new(
            make_schema(),
            vec![
                Arc::new(Int64Array::from(values)),
                Arc::new(StringArray::from(paths)),
                Arc::new(Int64Array::from(positions)),
            ],
        )
        .unwrap()
    }

    fn rows_of(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect()
    }

    async fn run(exec: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
        let task_ctx = Arc::new(datafusion::execution::TaskContext::default());
        let mut stream = exec.execute(0, task_ctx).unwrap();
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            out.push(batch.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn stream_single_file_with_dv_drops_marked_rows() {
        let mut dvs = HashMap::new();
        dvs.insert("/data/a.parquet".to_string(), dv_with(&[1, 3]));
        let input = MockBatches::new(vec![make_batch(
            &[10, 11, 12, 13, 14],
            "/data/a.parquet",
            &[0, 1, 2, 3, 4],
        )]);
        let exec = Arc::new(
            IcebergDvExec::try_new(
                input,
                Arc::new(dvs),
                PATH_COL,
                ROW_NUMBER_COL,
                /* strip_path_col */ true,
            )
            .unwrap(),
        );
        let out = run(exec).await;
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].num_columns(),
            1,
            "path and row-number columns should be stripped"
        );
        assert_eq!(rows_of(&out[0]), vec![10, 12, 14]);
    }

    #[tokio::test]
    async fn stream_non_contiguous_positions_from_skipped_row_group() {
        // Simulate a scan where an earlier row group was pruned: the surviving
        // rows carry non-contiguous, non-zero-based row numbers. The cursor
        // approach could not have handled this — the true position must come
        // from the row-number column.
        let mut dvs = HashMap::new();
        dvs.insert("/data/a.parquet".to_string(), dv_with(&[6, 1000]));
        let input = MockBatches::new(vec![make_batch(
            &[10, 11, 12, 13],
            "/data/a.parquet",
            &[5, 6, 1000, 1001],
        )]);
        let exec = Arc::new(
            IcebergDvExec::try_new(input, Arc::new(dvs), PATH_COL, ROW_NUMBER_COL, true).unwrap(),
        );
        let out = run(exec).await;
        // Positions 6 and 1000 are deleted → drop values 11 and 12.
        assert_eq!(rows_of(&out[0]), vec![10, 13]);
    }

    #[tokio::test]
    async fn stream_split_file_across_out_of_order_batches() {
        // The same file arrives split across two batches, each starting at a
        // non-zero position and out of order — as can happen under
        // `repartition_file_scans`. Correctness must not depend on arrival
        // order or a running cursor.
        let mut dvs = HashMap::new();
        dvs.insert("/data/a.parquet".to_string(), dv_with(&[5, 6]));
        let b1 = make_batch(&[100, 101], "/data/a.parquet", &[1001, 5]);
        let b2 = make_batch(&[200, 201], "/data/a.parquet", &[6, 1000]);
        let input = MockBatches::new(vec![b1, b2]);
        let exec = Arc::new(
            IcebergDvExec::try_new(input, Arc::new(dvs), PATH_COL, ROW_NUMBER_COL, true).unwrap(),
        );
        let out = run(exec).await;
        // b1: position 5 deleted → drop 101, keep 100.
        assert_eq!(rows_of(&out[0]), vec![100]);
        // b2: position 6 deleted → drop 200, keep 201 (position 1000).
        assert_eq!(rows_of(&out[1]), vec![201]);
    }

    #[tokio::test]
    async fn stream_interleaved_files_only_filters_dv_owner() {
        let mut dvs = HashMap::new();
        // p1's DV deletes absolute positions 0 and 7; p2 has no DV.
        dvs.insert("/data/p1.parquet".to_string(), dv_with(&[0, 7]));
        let batch = make_batch_interleaved(&[
            (100, "/data/p1.parquet", 0),
            (200, "/data/p2.parquet", 0),
            (101, "/data/p1.parquet", 2),
            (201, "/data/p2.parquet", 1),
            (102, "/data/p1.parquet", 7),
            (202, "/data/p2.parquet", 2),
        ]);
        let input = MockBatches::new(vec![batch]);
        let exec = Arc::new(
            IcebergDvExec::try_new(input, Arc::new(dvs), PATH_COL, ROW_NUMBER_COL, true).unwrap(),
        );
        let out = run(exec).await;
        // p1 positions 0 and 7 dropped (values 100, 102); p2 untouched.
        assert_eq!(rows_of(&out[0]), vec![200, 101, 201, 202]);
    }

    #[tokio::test]
    async fn stream_strips_row_number_but_keeps_path_when_user_opted_in() {
        let mut dvs = HashMap::new();
        dvs.insert("/data/a.parquet".to_string(), dv_with(&[]));
        let input = MockBatches::new(vec![make_batch(&[1, 2, 3], "/data/a.parquet", &[0, 1, 2])]);
        let exec = Arc::new(
            IcebergDvExec::try_new(input, Arc::new(dvs), PATH_COL, ROW_NUMBER_COL, false).unwrap(),
        );
        let out = run(exec).await;
        // Row-number column is always internal; the path column is kept.
        assert_eq!(out[0].num_columns(), 2);
        let schema = out[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["v", PATH_COL]);
    }
}
