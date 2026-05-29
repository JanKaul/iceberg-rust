//! Arrow-based partitioning implementation for Iceberg tables
//!
//! This module provides functionality to partition Arrow record batches according to Iceberg partition
//! specifications. It includes:
//!
//! * Streaming partition implementation that processes record batches asynchronously
//! * Support for different partition transforms (identity, bucket, truncate)
//! * Efficient handling of distinct partition values
//! * Automatic management of partition streams and channels

use std::{collections::HashSet, hash::Hash};

use arrow::{
    array::{
        as_primitive_array, as_string_array, ArrayRef, BooleanArray, BooleanBufferBuilder,
        PrimitiveArray, StringArray,
    },
    compute::{
        and, filter, filter_record_batch,
        kernels::cmp::{distinct, eq},
    },
    datatypes::{ArrowPrimitiveType, DataType, Int32Type, Int64Type},
    error::ArrowError,
    record_batch::RecordBatch,
};
use itertools::{iproduct, Itertools};

use iceberg_rust_spec::{partition::BoundPartitionField, spec::values::Value};

use super::transform::transform_arrow;

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
pub fn partition_record_batch<'a>(
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
        >(array)?)),
        DataType::Int64 => Ok(DistinctValues::Long(distinct_values_primitive::<
            i64,
            Int64Type,
        >(array)?)),
        DataType::Utf8 => Ok(DistinctValues::String(distinct_values_string(array)?)),
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
) -> Result<HashSet<P::Native>, ArrowError> {
    let array = as_primitive_array::<P>(&array);

    let first = array.value(0);

    let slice_len = array.len() - 1;

    if slice_len == 0 {
        return Ok(HashSet::from_iter([first]));
    }

    let v1 = array.slice(0, slice_len);
    let v2 = array.slice(1, slice_len);

    // Which consecutive entries are different
    let mask = distinct(&v1, &v2)?;

    let unique = filter(&v2, &mask)?;

    let unique = as_primitive_array::<P>(&unique);

    let set = unique
        .iter()
        .fold(HashSet::from_iter([first]), |mut acc, x| {
            if let Some(x) = x {
                acc.insert(x);
            }
            acc
        });
    Ok(set)
}

/// Extracts distinct string values from an Arrow array into a HashSet
///
/// # Arguments
/// * `array` - The Arrow array to extract distinct values from
///
/// # Returns
/// A HashSet containing all unique string values from the array
fn distinct_values_string(array: ArrayRef) -> Result<HashSet<String>, ArrowError> {
    let slice_len = array.len() - 1;

    let array = as_string_array(&array);

    let first = array.value(0).to_owned();

    if slice_len == 0 {
        return Ok(HashSet::from_iter([first]));
    }

    let v1 = array.slice(0, slice_len);
    let v2 = array.slice(1, slice_len);

    // Which consecutive entries are different
    let mask = distinct(&v1, &v2)?;

    let unique = filter(&v2, &mask)?;

    let unique = as_string_array(&unique);

    let set = unique
        .iter()
        .fold(HashSet::from_iter([first]), |mut acc, x| {
            if let Some(x) = x {
                acc.insert(x.to_owned());
            }
            acc
        });
    Ok(set)
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
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use iceberg_rust_spec::spec::partition::{BoundPartitionField, PartitionField, Transform};
    use iceberg_rust_spec::spec::types::{PrimitiveType, StructField, Type};
    use iceberg_rust_spec::spec::values::Value;

    use super::*;

    /// Build a `BoundPartitionField` and the two backing structs it borrows
    /// from. The caller pins the returned tuple on the stack and then borrows
    /// the first element for `partition_fields`.
    fn make_field(
        source_id: i32,
        field_id: i32,
        source_name: &str,
        partition_name: &str,
        primitive: PrimitiveType,
        transform: Transform,
        required: bool,
    ) -> (PartitionField, StructField) {
        let part = PartitionField::new(source_id, field_id, partition_name, transform);
        let source = StructField::new(
            source_id,
            source_name,
            required,
            Type::Primitive(primitive),
            None,
        );
        (part, source)
    }

    fn arrow_schema(fields: &[(&str, DataType)]) -> Arc<ArrowSchema> {
        let arrow_fields: Vec<Field> = fields
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), false))
            .collect();
        Arc::new(ArrowSchema::new(arrow_fields))
    }

    /// Sort the partition tuples so assertions don't depend on HashSet
    /// iteration order.
    fn sort_partitions(
        mut groups: Vec<(Vec<Value>, RecordBatch)>,
    ) -> Vec<(Vec<Value>, RecordBatch)> {
        groups.sort_by_key(|(values, _)| {
            values
                .iter()
                .map(|v| format!("{v:?}"))
                .collect::<Vec<_>>()
                .join("|")
        });
        groups
    }

    #[test]
    fn test_partition_identity_int_splits_by_distinct_values() {
        let schema = arrow_schema(&[("region_id", DataType::Int32), ("sales", DataType::Int32)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 10, 20, 20, 30])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();
        let (part_field, source_field) = make_field(
            1,
            1000,
            "region_id",
            "region_id",
            PrimitiveType::Int,
            Transform::Identity,
            true,
        );
        let bound = vec![BoundPartitionField::new(&part_field, &source_field)];

        let groups = sort_partitions(
            partition_record_batch(&batch, &bound)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        );

        assert_eq!(groups.len(), 3, "three distinct region ids");
        assert_eq!(groups[0].0, vec![Value::Int(10)]);
        assert_eq!(groups[0].1.num_rows(), 2);
        assert_eq!(groups[1].0, vec![Value::Int(20)]);
        assert_eq!(groups[1].1.num_rows(), 2);
        assert_eq!(groups[2].0, vec![Value::Int(30)]);
        assert_eq!(groups[2].1.num_rows(), 1);
    }

    #[test]
    fn test_partition_identity_string_groups_distinct_strings() {
        let schema = arrow_schema(&[("country", DataType::Utf8), ("amount", DataType::Int64)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["NL", "DE", "NL", "DE", "FR"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();
        let (part_field, source_field) = make_field(
            1,
            1000,
            "country",
            "country",
            PrimitiveType::String,
            Transform::Identity,
            true,
        );
        let bound = vec![BoundPartitionField::new(&part_field, &source_field)];

        let groups = sort_partitions(
            partition_record_batch(&batch, &bound)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        );

        let labels: Vec<String> = groups
            .iter()
            .map(|(values, _)| match &values[0] {
                Value::String(s) => s.clone(),
                other => panic!("expected string partition value, got {other:?}"),
            })
            .collect();
        assert_eq!(labels, vec!["DE", "FR", "NL"]);
        // Two rows for NL and DE, one for FR.
        let row_counts: Vec<usize> = groups.iter().map(|(_, b)| b.num_rows()).collect();
        assert_eq!(row_counts, vec![2, 1, 2]);
    }

    #[test]
    fn test_partition_two_fields_take_cartesian_product_of_distinct_values() {
        let schema = arrow_schema(&[
            ("region", DataType::Int32),
            ("category", DataType::Utf8),
            ("amount", DataType::Int64),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 1])),
                Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        let (r_part, r_src) = make_field(
            1,
            1000,
            "region",
            "region",
            PrimitiveType::Int,
            Transform::Identity,
            true,
        );
        let (c_part, c_src) = make_field(
            2,
            1001,
            "category",
            "category",
            PrimitiveType::String,
            Transform::Identity,
            true,
        );
        let bound = vec![
            BoundPartitionField::new(&r_part, &r_src),
            BoundPartitionField::new(&c_part, &c_src),
        ];

        let groups = partition_record_batch(&batch, &bound)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Region {1, 2} × category {A, B} = 4 possible groups, but no row has
        // region=2 / category=A only — wait, the data does have (2,A). All 4
        // tuples appear in the input, so all 4 groups should be produced and
        // contain at least one row each.
        assert_eq!(groups.len(), 4);
        for (_, batch) in &groups {
            assert!(batch.num_rows() >= 1);
        }
        let total: usize = groups.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total, 5, "every input row lands in exactly one group");
    }

    #[test]
    fn test_partition_truncate_groups_ints_into_step_buckets() {
        let schema = arrow_schema(&[("amount", DataType::Int32)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![5, 12, 17, 99, 100, 234]))],
        )
        .unwrap();
        let (part_field, source_field) = make_field(
            1,
            1000,
            "amount",
            "amount_bucket",
            PrimitiveType::Int,
            Transform::Truncate(100),
            true,
        );
        let bound = vec![BoundPartitionField::new(&part_field, &source_field)];

        let groups = sort_partitions(
            partition_record_batch(&batch, &bound)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        );

        // Truncate(100): 5/12/17/99 -> 0, 100 -> 100, 234 -> 200.
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].0, vec![Value::Int(0)]);
        assert_eq!(groups[0].1.num_rows(), 4);
        assert_eq!(groups[1].0, vec![Value::Int(100)]);
        assert_eq!(groups[1].1.num_rows(), 1);
        assert_eq!(groups[2].0, vec![Value::Int(200)]);
        assert_eq!(groups[2].1.num_rows(), 1);
    }

    #[test]
    fn test_partition_bucket_string_produces_int_partition_values() {
        let schema = arrow_schema(&[("name", DataType::Utf8)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "alpha", "beta", "gamma", "delta",
            ]))],
        )
        .unwrap();
        let (part_field, source_field) = make_field(
            1,
            1000,
            "name",
            "name_bucket",
            PrimitiveType::String,
            Transform::Bucket(4),
            true,
        );
        let bound = vec![BoundPartitionField::new(&part_field, &source_field)];

        let groups = partition_record_batch(&batch, &bound)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Every partition value is a non-negative bucket id < 4.
        for (values, batch) in &groups {
            match &values[0] {
                Value::Int(b) => {
                    assert!((0..4).contains(b), "bucket id should be in [0, 4), got {b}",)
                }
                other => panic!("expected bucket Int partition value, got {other:?}"),
            }
            assert!(batch.num_rows() >= 1);
        }
        let total: usize = groups.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_partition_missing_source_column_returns_schema_error() {
        let schema = arrow_schema(&[("present", DataType::Int32)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let (part_field, source_field) = make_field(
            1,
            1000,
            "absent",
            "absent_partition",
            PrimitiveType::Int,
            Transform::Identity,
            true,
        );
        let bound = vec![BoundPartitionField::new(&part_field, &source_field)];

        let err = partition_record_batch(&batch, &bound).err().unwrap();
        assert!(matches!(err, ArrowError::SchemaError(_)), "got {err:?}");
    }

    #[test]
    fn test_partition_unsupported_source_datatype_returns_compute_error() {
        // distinct_values only handles Int32, Int64, and Utf8 today; Float64
        // should surface as a ComputeError rather than silently grouping.
        let schema = arrow_schema(&[("score", DataType::Float64)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))])
                .unwrap();
        let (part_field, source_field) = make_field(
            1,
            1000,
            "score",
            "score",
            PrimitiveType::Double,
            Transform::Identity,
            true,
        );
        let bound = vec![BoundPartitionField::new(&part_field, &source_field)];

        let err = partition_record_batch(&batch, &bound).err().unwrap();
        assert!(matches!(err, ArrowError::ComputeError(_)), "got {err:?}");
    }
}
