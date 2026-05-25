//! Custom DataFusion `ExecutionPlan` that applies Iceberg deletion vectors to
//! the output of a parquet scan.
//!
//! `IcebergDvExec` wraps a parquet plan that emits an extra
//! `__data_file_path` partition column. For each batch it groups rows by the
//! path column, looks up the matching [`DeletionVector`] in an
//! `Arc<HashMap<String, DeletionVector>>`, and clears bits in a keep-mask for
//! the deleted positions. The mask is then applied via
//! [`arrow::compute::filter_record_batch`], and the path column is stripped
//! from the output unless the user explicitly requested it.
//!
//! Two design choices make this efficient for sparse deletion vectors:
//!
//! - Distinct paths in the batch are extracted via the Arrow shift-and-
//!   `distinct` idiom from `iceberg_rust::arrow::partition::distinct_values_string`,
//!   so the common single-file-batch case takes one vectorized scan and no
//!   per-row Rust loop.
//! - For each file with a DV, the deletions within this batch's row range are
//!   enumerated through [`RoaringTreemap::rank`] + [`RoaringTreemap::select`]
//!   (both O(log)), so the per-batch cost scales with the number of
//!   *deletions* in the batch, not the number of rows.

use std::{
    any::Any,
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{
        array::{Array, BooleanArray, BooleanBufferBuilder, RecordBatch, StringArray},
        compute::{filter_record_batch, kernels::cmp::eq},
        datatypes::{Schema as ArrowSchema, SchemaRef},
        error::ArrowError,
    },
    common::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};
use futures::{Stream, StreamExt};
use iceberg_rust::arrow::partition::distinct_values_string;
use iceberg_rust::spec::{deletion_vector::DeletionVector, util};
use roaring::RoaringTreemap;

/// Wraps a parquet scan that emits a `__data_file_path` partition column and
/// applies a path-keyed [`DeletionVector`] to filter out deleted rows.
#[derive(Debug)]
pub(crate) struct IcebergDvExec {
    input: Arc<dyn ExecutionPlan>,
    /// DVs keyed by the normalized data-file path (output of
    /// `iceberg_rust_spec::util::strip_prefix`).
    dvs: Arc<HashMap<String, DeletionVector>>,
    /// Index of the path column in `input.schema()`.
    path_col_idx: usize,
    /// True when the path column was force-injected by the scan wiring and
    /// must be stripped from `IcebergDvExec`'s output (the user did not opt
    /// in to it via `DataFusionTableConfig::enable_data_file_path_column`).
    strip_path_col: bool,
    /// Output schema — `input.schema()` minus the path column iff
    /// `strip_path_col`.
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IcebergDvExec {
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        dvs: Arc<HashMap<String, DeletionVector>>,
        path_col_name: &str,
        strip_path_col: bool,
    ) -> Result<Self, DataFusionError> {
        let input_schema = input.schema();
        let path_col_idx = input_schema.index_of(path_col_name).map_err(|_| {
            DataFusionError::Internal(format!(
                "IcebergDvExec: column {path_col_name} not present in child schema"
            ))
        })?;
        let output_schema = if strip_path_col {
            let fields: Vec<_> = input_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != path_col_idx)
                .map(|(_, f)| f.clone())
                .collect();
            Arc::new(ArrowSchema::new_with_metadata(
                fields,
                input_schema.metadata().clone(),
            ))
        } else {
            input_schema.clone()
        };
        // We preserve partitioning, ordering, and bounded/unbounded
        // characteristics from the input — only rows are dropped.
        let properties = Arc::new(input.properties().as_ref().clone());
        Ok(Self {
            input,
            dvs,
            path_col_idx,
            strip_path_col,
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

    fn as_any(&self) -> &dyn Any {
        self
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
        // The path-column index must still resolve in the new child's schema.
        let path_col_name = self.input.schema().field(self.path_col_idx).name().clone();
        Ok(Arc::new(Self::try_new(
            input,
            self.dvs.clone(),
            &path_col_name,
            self.strip_path_col,
        )?))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &datafusion::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        // Forward repartitioning to the child. File identity travels in the
        // batch (`__data_file_path`), so reshuffling files between output
        // partitions is safe. Intra-file row-group splitting is *not* — see
        // module doc — but DataFusion 53's `ParquetSource` only splits at
        // row-group boundaries when `repartition_file_scans` is on, and we
        // cannot suppress that from here. Documented limitation.
        let Some(new_input) = self.input.repartitioned(target_partitions, config)? else {
            return Ok(None);
        };
        let path_col_name = self.input.schema().field(self.path_col_idx).name().clone();
        Ok(Some(Arc::new(Self::try_new(
            new_input,
            self.dvs.clone(),
            &path_col_name,
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
            strip_path_col: self.strip_path_col,
            offsets: HashMap::new(),
        }))
    }
}

struct DvFilterStream {
    inner: SendableRecordBatchStream,
    dvs: Arc<HashMap<String, DeletionVector>>,
    output_schema: SchemaRef,
    path_col_idx: usize,
    strip_path_col: bool,
    /// Absolute file-row position cursor, per file path this stream has seen.
    offsets: HashMap<String, u64>,
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

        let mut keep = BooleanBufferBuilder::new(batch.num_rows());
        keep.append_n(batch.num_rows(), true);

        // Common case: one file per batch — single distinct value, no need to
        // build a per-file index since `true_indices[r] == r`.
        let distinct = distinct_values_string(Arc::new(path_col.clone()))?;
        for path in &distinct {
            let normalized = util::strip_prefix(path);
            let dv = match self.dvs.get(&normalized) {
                Some(dv) => dv,
                None => {
                    // Still need to advance the offset for this file so a
                    // later DV that hits the same path lines up. (Today
                    // missing-DV means "no deletes for this file", but a
                    // future state where a DV is added mid-scan should not
                    // mis-align.)
                    let n_p = count_for_path(&path_col, path)?;
                    *self.offsets.entry(normalized).or_insert(0) += n_p as u64;
                    continue;
                }
            };
            let mask_p = eq(&StringArray::new_scalar(path), &path_col)?;
            let n_p = mask_p.true_count();
            if n_p == 0 {
                continue;
            }
            let offset = *self.offsets.entry(normalized.clone()).or_insert(0);
            apply_dv_for_file_run(
                dv.as_treemap(),
                offset,
                n_p,
                if n_p == batch.num_rows() {
                    None
                } else {
                    Some(collect_true_indices(&mask_p))
                }
                .as_deref(),
                &mut keep,
            );
            *self.offsets.get_mut(&normalized).unwrap() += n_p as u64;
        }

        let keep_array = BooleanArray::new(keep.finish(), None);
        let filtered = filter_record_batch(&batch, &keep_array)?;
        self.finalize(filtered)
    }

    fn finalize(&self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        if !self.strip_path_col {
            return Ok(batch);
        }
        let keep_cols: Vec<usize> = (0..batch.num_columns())
            .filter(|i| *i != self.path_col_idx)
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

/// Enumerate the deletions that fall in `[offset, offset + n_p)` and clear
/// their bits in `keep`. Sparse — work scales with deletion count, not n_p.
///
/// `true_indices`: if `Some`, maps "rank within file" (0..n_p) to the batch
/// row index that holds that rank. `None` is shorthand for the identity
/// mapping (used when the whole batch is one file).
fn apply_dv_for_file_run(
    tm: &RoaringTreemap,
    offset: u64,
    n_p: usize,
    true_indices: Option<&[usize]>,
    keep: &mut BooleanBufferBuilder,
) {
    let first_k = if offset == 0 { 0 } else { tm.rank(offset - 1) };
    let last = offset + n_p as u64 - 1;
    let end_k = tm.rank(last);
    if first_k == end_k {
        return; // no deletions in this batch's slice of file
    }
    for k in first_k..end_k {
        let deleted_pos = tm
            .select(k)
            .expect("rank() guarantees this rank exists in the bitmap");
        let rank_in_file = (deleted_pos - offset) as usize;
        let batch_idx = match true_indices {
            None => rank_in_file,
            Some(idx) => idx[rank_in_file],
        };
        keep.set_bit(batch_idx, false);
    }
}

/// Indices of `true` bits in `mask`, in order. Used to translate "rank within
/// a file's rows" to "row index within the batch" when a batch interleaves
/// multiple files.
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

/// Cheap count of rows in `path_col` equal to `path`. Used on the no-DV
/// branch where we don't need the mask itself.
fn count_for_path(path_col: &StringArray, path: &str) -> Result<usize, ArrowError> {
    let mask = eq(&StringArray::new_scalar(path), path_col)?;
    Ok(mask.true_count())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::{BooleanBufferBuilder, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use roaring::RoaringTreemap;

    use super::*;

    fn dv_with(positions: &[u64]) -> DeletionVector {
        let mut tm = RoaringTreemap::new();
        for p in positions {
            tm.insert(*p);
        }
        DeletionVector::from(tm)
    }

    fn keep_to_vec(keep: &BooleanBufferBuilder, len: usize) -> Vec<bool> {
        // BooleanBufferBuilder lacks an inspect; finish() consumes it, so the
        // tests below clone via reading bits directly.
        (0..len).map(|i| keep.get_bit(i)).collect()
    }

    #[test]
    fn apply_dv_drops_marked_rows_identity_mapping() {
        let tm = dv_with(&[1, 3]).into_inner();
        let mut keep = BooleanBufferBuilder::new(5);
        keep.append_n(5, true);
        apply_dv_for_file_run(&tm, 0, 5, None, &mut keep);
        assert_eq!(
            keep_to_vec(&keep, 5),
            vec![true, false, true, false, true]
        );
    }

    #[test]
    fn apply_dv_respects_start_offset() {
        let tm = dv_with(&[102]).into_inner();
        let mut keep = BooleanBufferBuilder::new(5);
        keep.append_n(5, true);
        apply_dv_for_file_run(&tm, 100, 5, None, &mut keep);
        assert_eq!(
            keep_to_vec(&keep, 5),
            vec![true, true, false, true, true]
        );
    }

    #[test]
    fn apply_dv_uses_true_indices_for_interleaved_files() {
        // Two files in one batch of 6 rows. File p1 owns rows [0, 2, 4]
        // (n_p == 3). The DV for p1 deletes file-rows 0 and 2 (which map
        // back to batch rows 0 and 4).
        let tm = dv_with(&[0, 2]).into_inner();
        let true_indices = vec![0usize, 2, 4];
        let mut keep = BooleanBufferBuilder::new(6);
        keep.append_n(6, true);
        apply_dv_for_file_run(&tm, 0, 3, Some(&true_indices), &mut keep);
        assert_eq!(
            keep_to_vec(&keep, 6),
            vec![false, true, true, true, false, true]
        );
    }

    #[test]
    fn apply_dv_fast_path_when_rank_window_is_empty() {
        // DV outside this batch's range — first_k == end_k, no per-row work.
        let tm = dv_with(&[1_000_000]).into_inner();
        let mut keep = BooleanBufferBuilder::new(8);
        keep.append_n(8, true);
        apply_dv_for_file_run(&tm, 0, 8, None, &mut keep);
        assert_eq!(keep_to_vec(&keep, 8), vec![true; 8]);
    }

    #[test]
    fn apply_dv_sparse_only_visits_actual_deletions() {
        // A DV with three deletes spread across a huge file. We process a
        // batch covering the whole file — only the three deletions should
        // hit the `keep.set_bit(_, false)` path. We can't observe internals
        // directly, but the keep mask must be correct without iterating
        // every row.
        let tm = dv_with(&[5, 5_000_000, 9_999_999]).into_inner();
        let n = 10_000_000;
        let mut keep = BooleanBufferBuilder::new(n);
        keep.append_n(n, true);
        apply_dv_for_file_run(&tm, 0, n, None, &mut keep);
        assert!(!keep.get_bit(5));
        assert!(!keep.get_bit(5_000_000));
        assert!(!keep.get_bit(9_999_999));
        // Spot-check a few non-deleted positions.
        assert!(keep.get_bit(0));
        assert!(keep.get_bit(4_999_999));
        assert!(keep.get_bit(5_000_001));
    }

    // --- Stream-level tests via a hand-built input plan ----------------------

    use datafusion::{
        common::DataFusionError,
        execution::SendableRecordBatchStream,
        physical_plan::{
            execution_plan::{Boundedness, EmissionType},
            stream::RecordBatchStreamAdapter,
            PlanProperties,
        },
    };
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
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
        fn as_any(&self) -> &dyn Any {
            self
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

    fn make_schema_with_path() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("__data_file_path", DataType::Utf8, false),
        ]))
    }

    fn make_batch(rows: &[i64], path: &str) -> RecordBatch {
        let schema = make_schema_with_path();
        let path_arr = StringArray::from(vec![path; rows.len()]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(rows.to_vec())), Arc::new(path_arr)],
        )
        .unwrap()
    }

    fn make_batch_interleaved(rows: &[(i64, &str)]) -> RecordBatch {
        let schema = make_schema_with_path();
        let values: Vec<i64> = rows.iter().map(|(v, _)| *v).collect();
        let paths: Vec<&str> = rows.iter().map(|(_, p)| *p).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(values)),
                Arc::new(StringArray::from(paths)),
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
        let input = MockBatches::new(vec![make_batch(&[10, 11, 12, 13, 14], "/data/a.parquet")]);
        let exec = Arc::new(
            IcebergDvExec::try_new(
                input,
                Arc::new(dvs),
                "__data_file_path",
                /* strip_path_col */ true,
            )
            .unwrap(),
        );
        let out = run(exec).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_columns(), 1, "path column should be stripped");
        assert_eq!(rows_of(&out[0]), vec![10, 12, 14]);
    }

    #[tokio::test]
    async fn stream_interleaved_files_only_filters_dv_owner() {
        let mut dvs = HashMap::new();
        // p1 deletes its file-rows 0 and 2 (batch positions 0 and 4).
        dvs.insert("/data/p1.parquet".to_string(), dv_with(&[0, 2]));
        // p2 has no DV.
        let batch = make_batch_interleaved(&[
            (100, "/data/p1.parquet"),
            (200, "/data/p2.parquet"),
            (101, "/data/p1.parquet"),
            (201, "/data/p2.parquet"),
            (102, "/data/p1.parquet"),
            (202, "/data/p2.parquet"),
        ]);
        let input = MockBatches::new(vec![batch]);
        let exec = Arc::new(
            IcebergDvExec::try_new(input, Arc::new(dvs), "__data_file_path", true).unwrap(),
        );
        let out = run(exec).await;
        // p1 rows 0, 2 dropped (values 100, 102); p2 rows pass through.
        assert_eq!(rows_of(&out[0]), vec![200, 101, 201, 202]);
    }

    #[tokio::test]
    async fn stream_cursor_advances_across_batches() {
        let mut dvs = HashMap::new();
        // Delete file-row 7 only.
        dvs.insert("/data/a.parquet".to_string(), dv_with(&[7]));
        // Two consecutive batches from the same file: rows 0..5 and rows 5..10.
        let b1 = make_batch(&[0, 1, 2, 3, 4], "/data/a.parquet");
        let b2 = make_batch(&[5, 6, 7, 8, 9], "/data/a.parquet");
        let input = MockBatches::new(vec![b1, b2]);
        let exec = Arc::new(
            IcebergDvExec::try_new(input, Arc::new(dvs), "__data_file_path", true).unwrap(),
        );
        let out = run(exec).await;
        // Batch 1: rows 0..5 — none deleted.
        assert_eq!(rows_of(&out[0]), vec![0, 1, 2, 3, 4]);
        // Batch 2: rows 5..10 — file-row 7 is at batch position 2 (value 7).
        assert_eq!(rows_of(&out[1]), vec![5, 6, 8, 9]);
    }

    #[tokio::test]
    async fn stream_preserves_path_column_when_user_opted_in() {
        let mut dvs = HashMap::new();
        dvs.insert("/data/a.parquet".to_string(), dv_with(&[]));
        let input = MockBatches::new(vec![make_batch(&[1, 2, 3], "/data/a.parquet")]);
        let exec = Arc::new(
            IcebergDvExec::try_new(input, Arc::new(dvs), "__data_file_path", false).unwrap(),
        );
        let out = run(exec).await;
        assert_eq!(out[0].num_columns(), 2, "path column should be kept");
    }
}
