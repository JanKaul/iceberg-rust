/*!
 * Functions to partition arrow record batches according to a partitoin spec
*/

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::Hash,
    pin::Pin,
    sync::Arc,
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
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    lock::Mutex,
    stream, SinkExt, Stream, StreamExt, TryStreamExt,
};
use itertools::{iproduct, Itertools};

use iceberg_rust_spec::{partition::BoundPartitionField, spec::values::Value};

use super::transform::transform_arrow;

type SendableRecordBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, ArrowError>> + Send>>;

type RecordBatchSender = UnboundedSender<Result<RecordBatch, ArrowError>>;

/// Partition stream of record batches according to partition spec
pub async fn partition_record_batches(
    record_batches: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    partition_fields: &[BoundPartitionField<'_>],
) -> Result<
    impl Stream<
        Item = (
            Vec<Value>,
            impl Stream<Item = Result<RecordBatch, ArrowError>> + Send,
        ),
    >,
    ArrowError,
> {
    #[allow(clippy::type_complexity)]
    let (partition_sender, partition_receiver): (
        UnboundedSender<(Vec<Value>, SendableRecordBatchStream)>,
        UnboundedReceiver<(Vec<Value>, SendableRecordBatchStream)>,
    ) = unbounded();
    let partition_streams: Arc<Mutex<HashMap<Vec<Value>, RecordBatchSender>>> =
        Arc::new(Mutex::new(HashMap::new()));
    record_batches
        .try_for_each_concurrent(None, |record_batch| {
            let partition_streams = partition_streams.clone();
            let partition_sender = partition_sender.clone();
            async move {
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
                stream::iter(predicates.into_iter())
                    .map(|(values, predicate)| {
                        Ok((values, filter_record_batch(&record_batch, &predicate)?))
                    })
                    .try_for_each_concurrent(None, |(values, batch)| {
                        let partition_streams = partition_streams.clone();
                        let mut partition_sender = partition_sender.clone();
                        async move {
                            let mut sender = {
                                let mut partition_streams = partition_streams.lock().await;
                                let entry = partition_streams.entry(values.clone());
                                match entry {
                                    Entry::Occupied(entry) => entry.get().clone(),
                                    Entry::Vacant(entry) => {
                                        let (sender, reciever) = unbounded();
                                        entry.insert(sender.clone());
                                        partition_sender.send((values,Box::pin(reciever))).await.map_err(
                                            |err| ArrowError::ExternalError(Box::new(err)),
                                        )?;
                                        sender
                                    }
                                }
                            };
                            sender
                                .send(Ok(batch))
                                .await
                                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
                            Ok::<_, ArrowError>(())
                        }
                    })
                    .await?;
                Ok(())
            }
        })
        .await?;
    Arc::into_inner(partition_streams)
        .ok_or(ArrowError::MemoryError(
            "Couldn't close partition streams.".to_string(),
        ))?
        .into_inner()
        .into_values()
        .for_each(|sender| sender.close_channel());
    partition_sender.close_channel();
    Ok(partition_receiver)
}

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

enum DistinctValues {
    Int(HashSet<i32>),
    Long(HashSet<i64>),
    String(HashSet<String>),
}

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt};
    use std::sync::Arc;

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
            types::{PrimitiveType, StructField, StructType, Type},
        },
    };

    use crate::error::Error;

    use super::partition_record_batches;

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
            .with_fields(
                StructType::builder()
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
                    .unwrap(),
            )
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
                        .ok_or(Error::NotFound(
                            "Field".to_owned(),
                            partition_field.source_id().to_string(),
                        ))?;
                Ok(BoundPartitionField::new(partition_field, field))
            })
            .collect::<Result<Vec<_>, Error>>()
            .unwrap();
        let streams = partition_record_batches(record_batches, &partition_fields)
            .await
            .unwrap();
        let output = streams
            .then(|s| async move { s.1.collect::<Vec<_>>().await })
            .collect::<Vec<_>>()
            .await;

        for x in output {
            for y in x {
                y.unwrap();
            }
        }
    }
}
