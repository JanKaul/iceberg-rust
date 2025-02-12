/*!
 * Functions to partition arrow record batches according to a partitoin spec
*/

use std::{
    collections::{HashSet, VecDeque},
    hash::Hash,
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{
    array::{
        as_primitive_array, as_string_array, ArrayIter, ArrayRef, BooleanArray,
        BooleanBufferBuilder, PrimitiveArray, StringArray,
    },
    compute::kernels::cmp::eq,
    compute::{and, filter_record_batch},
    datatypes::{ArrowPrimitiveType, DataType, Int32Type, Int64Type},
    error::ArrowError,
    record_batch::RecordBatch,
};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    Stream,
};
use itertools::{iproduct, Itertools};

use iceberg_rust_spec::{partition::BoundPartitionField, spec::values::Value};
use once_map::OnceMap;
use pin_project_lite::pin_project;

use crate::error::Error;

use super::transform::transform_arrow;

type RecordBatchSender = Sender<Result<RecordBatch, ArrowError>>;
type RecordBatchReceiver = Receiver<Result<RecordBatch, ArrowError>>;

/// A stream that partitions Arrow record batches according to partition field specifications
///
/// This struct implements Stream to process record batches asynchronously, splitting them into
/// separate streams based on partition values. It maintains internal state to:
/// * Track active partition streams
/// * Buffer pending record batches
/// * Manage channel senders/receivers for each partition
///
/// # Type Parameters
/// * `'a` - Lifetime of the partition field specifications
pin_project! {
    pub(crate) struct PartitionStream<'a> {
        #[pin]
        record_batches: Pin<Box<dyn Stream<Item = Result<RecordBatch, ArrowError>> + Send>>,
        partition_fields: &'a [BoundPartitionField<'a>],
        partition_streams: OnceMap<Vec<Value>, RecordBatchSender>,
        queue: VecDeque<Result<(Vec<Value>, RecordBatchReceiver), Error>>,
        sends: Vec<(RecordBatchSender, RecordBatch)>,
    }
}

impl<'a> PartitionStream<'a> {
    pub(crate) fn new(
        record_batches: Pin<Box<dyn Stream<Item = Result<RecordBatch, ArrowError>> + Send>>,
        partition_fields: &'a [BoundPartitionField<'a>],
    ) -> Self {
        Self {
            record_batches,
            partition_fields,
            partition_streams: OnceMap::new(),
            queue: VecDeque::new(),
            sends: Vec::new(),
        }
    }
}

impl Stream for PartitionStream<'_> {
    type Item = Result<(Vec<Value>, RecordBatchReceiver), Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if let Some(result) = this.queue.pop_front() {
                break Poll::Ready(Some(result));
            }

            if !this.sends.is_empty() {
                let mut new_sends = Vec::with_capacity(this.sends.len());
                while let Some((mut sender, batch)) = this.sends.pop() {
                    match sender.poll_ready(cx) {
                        Poll::Pending => {
                            new_sends.push((sender, batch));
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                        Poll::Ready(Ok(())) => {
                            sender.start_send(Ok(batch))?;
                        }
                    }
                }
                *this.sends = new_sends;

                if !this.sends.is_empty() {
                    break Poll::Pending;
                }
            }

            match this.record_batches.as_mut().poll_next(cx) {
                Poll::Pending => {
                    break Poll::Pending;
                }
                Poll::Ready(None) => {
                    for sender in this.partition_streams.read_only_view().values() {
                        sender.clone().close_channel();
                    }
                    break Poll::Ready(None);
                }
                Poll::Ready(Some(Err(err))) => {
                    break Poll::Ready(Some(Err(err.into())));
                }
                Poll::Ready(Some(Ok(batch))) => {
                    for result in partition_record_batch(&batch, this.partition_fields)? {
                        let (partition_values, batch) = result?;

                        let sender = if let Some(sender) =
                            this.partition_streams.get_cloned(&partition_values)
                        {
                            sender
                        } else {
                            this.partition_streams
                                .insert_cloned(partition_values, |key| {
                                    let (sender, reciever) = channel(1);
                                    this.queue.push_back(Ok((key.clone(), reciever)));
                                    sender
                                })
                        };

                        this.sends.push((sender, batch));
                    }
                }
            }
        }
    }
}

/// Partitions a record batch according to the given partition fields.
///
/// This function takes a record batch and partition field specifications, then splits the batch into
/// multiple record batches based on unique combinations of partition values.
///
/// # Arguments
/// * `record_batch` - The input record batch to partition
/// * `partition_fields` - The partition field specifications that define how to split the data
///
/// # Returns
/// An iterator over results containing:
/// * A vector of partition values that identify the partition
/// * The record batch containing only rows matching those partition values
///
/// # Errors
/// Returns an ArrowError if:
/// * Required columns are missing from the record batch
/// * Transformation operations fail
/// * Data type conversions fail
fn partition_record_batch<'a>(
    record_batch: &'a RecordBatch,
    partition_fields: &[BoundPartitionField<'_>],
) -> Result<impl Iterator<Item = Result<(Vec<Value>, RecordBatch), ArrowError>> + 'a, ArrowError> {
    let partition_columns: Vec<ArrayRef> = partition_fields
        .iter()
        .map(|field| {
            let array = record_batch
                .column_by_name(field.source_name())
                .ok_or(ArrowError::SchemaError("Column doesn't exist".to_string()))?;
            transform_arrow(array.clone(), field.transform())
        })
        .collect::<Result<_, ArrowError>>()?;
    let distinct_values: Vec<DistinctValues> = partition_columns
        .iter()
        .map(|x| distinct_values(x.clone()))
        .collect::<Result<Vec<_>, ArrowError>>()?;
    let mut true_buffer = BooleanBufferBuilder::new(record_batch.num_rows());
    true_buffer.append_n(record_batch.num_rows(), true);
    let predicates = distinct_values
        .into_iter()
        .zip(partition_columns.iter())
        .map(|(distinct, value)| match distinct {
            DistinctValues::Int(set) => set
                .into_iter()
                .map(|x| {
                    Ok((
                        Value::Int(x),
                        eq(&PrimitiveArray::<Int32Type>::new_scalar(x), value)?,
                    ))
                })
                .collect::<Result<Vec<_>, ArrowError>>(),
            DistinctValues::Long(set) => set
                .into_iter()
                .map(|x| {
                    Ok((
                        Value::LongInt(x),
                        eq(&PrimitiveArray::<Int64Type>::new_scalar(x), value)?,
                    ))
                })
                .collect::<Result<Vec<_>, ArrowError>>(),
            DistinctValues::String(set) => set
                .into_iter()
                .map(|x| {
                    let res = eq(&StringArray::new_scalar(&x), value)?;
                    Ok((Value::String(x), res))
                })
                .collect::<Result<Vec<_>, ArrowError>>(),
        })
        .try_fold(
            vec![(vec![], BooleanArray::new(true_buffer.finish(), None))],
            |acc, predicates| {
                iproduct!(acc, predicates?.iter())
                    .map(|((mut values, x), (value, y))| {
                        values.push(value.clone());
                        Ok((values, and(&x, y)?))
                    })
                    .filter_ok(|x| x.1.true_count() != 0)
                    .collect::<Result<Vec<(Vec<Value>, _)>, ArrowError>>()
            },
        )?;
    Ok(predicates.into_iter().map(move |(values, predicate)| {
        Ok((values, filter_record_batch(record_batch, &predicate)?))
    }))
}

/// Extracts distinct values from an Arrow array into a DistinctValues enum
///
/// # Arguments
/// * `array` - The Arrow array to extract distinct values from
///
/// # Returns
/// * `Ok(DistinctValues)` - An enum containing a HashSet of the distinct values
/// * `Err(ArrowError)` - If the array's data type is not supported
///
/// # Supported Data Types
/// * Int32 - Converted to DistinctValues::Int
/// * Int64 - Converted to DistinctValues::Long
/// * Utf8 - Converted to DistinctValues::String
fn distinct_values(array: ArrayRef) -> Result<DistinctValues, ArrowError> {
    match array.data_type() {
        DataType::Int32 => Ok(DistinctValues::Int(distinct_values_primitive::<
            i32,
            Int32Type,
        >(array))),
        DataType::Int64 => Ok(DistinctValues::Long(distinct_values_primitive::<
            i64,
            Int64Type,
        >(array))),
        DataType::Utf8 => Ok(DistinctValues::String(distinct_values_string(array))),
        _ => Err(ArrowError::ComputeError(
            "Datatype not supported for transform.".to_string(),
        )),
    }
}

/// Extracts distinct primitive values from an Arrow array into a HashSet
///
/// # Type Parameters
/// * `T` - The Rust native type that implements Eq + Hash
/// * `P` - The Arrow primitive type corresponding to T
///
/// # Arguments
/// * `array` - The Arrow array to extract distinct values from
///
/// # Returns
/// A HashSet containing all unique values from the array
fn distinct_values_primitive<T: Eq + Hash, P: ArrowPrimitiveType<Native = T>>(
    array: ArrayRef,
) -> HashSet<P::Native> {
    let mut set = HashSet::new();
    let array = as_primitive_array::<P>(&array);
    for value in ArrayIter::new(array).flatten() {
        if !set.contains(&value) {
            set.insert(value);
        }
    }
    set
}

/// Extracts distinct string values from an Arrow array into a HashSet
///
/// # Arguments
/// * `array` - The Arrow array to extract distinct values from
///
/// # Returns
/// A HashSet containing all unique string values from the array
fn distinct_values_string(array: ArrayRef) -> HashSet<String> {
    let mut set = HashSet::new();
    let array = as_string_array(&array);
    for value in ArrayIter::new(array).flatten() {
        if !set.contains(value) {
            set.insert(value.to_owned());
        }
    }
    set
}

/// Represents distinct values found in Arrow arrays during partitioning
///
/// This enum stores unique values from different Arrow array types:
/// * `Int` - Distinct 32-bit integer values
/// * `Long` - Distinct 64-bit integer values  
/// * `String` - Distinct string values
enum DistinctValues {
    Int(HashSet<i32>),
    Long(HashSet<i64>),
    String(HashSet<String>),
}

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt};
    use std::sync::Arc;
    use tokio::task::JoinSet;

    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        error::ArrowError,
        record_batch::RecordBatch,
    };

    use iceberg_rust_spec::{
        partition::BoundPartitionField,
        spec::{
            partition::{PartitionField, PartitionSpec, Transform},
            schema::Schema,
            types::{PrimitiveType, StructField, Type},
        },
    };

    use crate::{arrow::partition::PartitionStream, error::Error};

    #[tokio::test]
    async fn test_partition() {
        let batch1 = RecordBatch::try_from_iter(vec![
            (
                "x",
                Arc::new(Int64Array::from(vec![1, 1, 1, 1, 2, 3])) as ArrayRef,
            ),
            (
                "y",
                Arc::new(Int64Array::from(vec![1, 2, 2, 2, 1, 1])) as ArrayRef,
            ),
            (
                "z",
                Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E", "F"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![
            (
                "x",
                Arc::new(Int64Array::from(vec![1, 1, 2, 2, 2, 3])) as ArrayRef,
            ),
            (
                "y",
                Arc::new(Int64Array::from(vec![1, 2, 2, 2, 1, 1])) as ArrayRef,
            ),
            (
                "z",
                Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E", "F"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let record_batches = stream::iter(
            vec![Ok::<_, ArrowError>(batch1), Ok::<_, ArrowError>(batch2)].into_iter(),
        );

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_string(),
                field_type: Type::Primitive(PrimitiveType::Int),
                required: true,
                doc: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "y".to_string(),
                field_type: Type::Primitive(PrimitiveType::Int),
                required: true,
                doc: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "z".to_string(),
                field_type: Type::Primitive(PrimitiveType::String),
                required: true,
                doc: None,
            })
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1001, "x", Transform::Identity))
            .build()
            .unwrap();
        let partition_fields = partition_spec
            .fields()
            .iter()
            .map(|partition_field| {
                let field =
                    schema
                        .get(*partition_field.source_id() as usize)
                        .ok_or(Error::NotFound(format!(
                            "Schema field with id {}",
                            partition_field.source_id(),
                        )))?;
                Ok(BoundPartitionField::new(partition_field, field))
            })
            .collect::<Result<Vec<_>, Error>>()
            .unwrap();
        let mut streams = PartitionStream::new(Box::pin(record_batches), &partition_fields);
        let mut set = JoinSet::new();
        while let Some(Ok((_, receiver))) = streams.next().await {
            set.spawn(async move { receiver.collect::<Vec<_>>().await });
        }
        let output = set.join_all().await;

        for x in output {
            for y in x {
                y.unwrap();
            }
        }
    }
}
