/*!
 * Helpers for parquet files
*/

use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Sub,
};

use iceberg_rust_spec::{
    partition::BoundPartitionField,
    spec::{
        manifest::{AvroMap, Content, DataFile, FileFormat},
        partition::PartitionField,
        schema::Schema,
        types::Type,
        values::{Struct, Value},
    },
};
use parquet::file::{metadata::ParquetMetaData, writer::TrackedWrite};
use thrift::protocol::{TCompactOutputProtocol, TSerializable};
use tracing::instrument;

use crate::error::Error;

/// Read datafile statistics from parquetfile
#[instrument(name = "iceberg_rust::file_format::parquet::parquet_to_datafile", level = "debug", skip(file_metadata, schema, partition_fields), fields(
    location = location,
    file_size = file_size,
    partition_field_count = partition_fields.len(),
    has_equality_ids = equality_ids.is_some()
))]
pub fn parquet_to_datafile(
    location: &str,
    file_size: u64,
    file_metadata: &ParquetMetaData,
    schema: &Schema,
    partition_fields: &[BoundPartitionField<'_>],
    equality_ids: Option<&[i32]>,
) -> Result<DataFile, Error> {
    let mut partition = partition_fields
        .iter()
        .map(|field| Ok((field.name().to_owned(), None)))
        .collect::<Result<Struct, Error>>()?;
    let partition_fields = partition_fields
        .iter()
        .map(|field| {
            Ok((
                field.source_name().to_owned(),
                field.partition_field().clone(),
            ))
        })
        .collect::<Result<HashMap<String, PartitionField>, Error>>()?;
    let _parquet_schema = file_metadata.file_metadata().schema_descr_ptr();

    let mut column_sizes = AvroMap(HashMap::new());
    let mut value_counts = AvroMap(HashMap::new());
    let mut null_value_counts = AvroMap(HashMap::new());
    let mut distinct_counts = AvroMap(HashMap::new());
    let mut lower_bounds: HashMap<i32, Value> = HashMap::new();
    let mut upper_bounds: HashMap<i32, Value> = HashMap::new();

    for row_group in file_metadata.row_groups() {
        for column in row_group.columns() {
            let column_name = column.column_descr().name();
            let id = schema
                .get_name(&column.column_path().parts().join("."))
                .ok_or_else(|| Error::Schema(column_name.to_string(), "".to_string()))?
                .id;
            column_sizes
                .entry(id)
                .and_modify(|x| *x += column.compressed_size())
                .or_insert(column.compressed_size());
            value_counts
                .entry(id)
                .and_modify(|x| *x += row_group.num_rows())
                .or_insert(row_group.num_rows());

            if let Some(statistics) = column.statistics() {
                if let Some(null_count) = statistics.null_count_opt() {
                    null_value_counts
                        .entry(id)
                        .and_modify(|x| *x += null_count as i64)
                        .or_insert(null_count as i64);
                }

                let data_type = &schema
                    .fields()
                    .get(id as usize)
                    .ok_or_else(|| Error::Schema(column_name.to_string(), "".to_string()))?
                    .field_type;

                if let (Some(distinct_count), Some(min_bytes), Some(max_bytes)) = (
                    statistics.distinct_count_opt(),
                    statistics.min_bytes_opt(),
                    statistics.max_bytes_opt(),
                ) {
                    let min = Value::try_from_bytes(min_bytes, data_type)?;
                    let max = Value::try_from_bytes(max_bytes, data_type)?;
                    let current_min = lower_bounds.get(&id);
                    let current_max = upper_bounds.get(&id);
                    match (min, max, current_min, current_max) {
                        (
                            Value::Int(min),
                            Value::Int(max),
                            Some(Value::Int(current_min)),
                            Some(Value::Int(current_max)),
                        ) => {
                            distinct_counts
                                .entry(id)
                                .and_modify(|x| {
                                    *x += estimate_distinct_count(
                                        &[current_min, current_max],
                                        &[&min, &max],
                                        *x,
                                        distinct_count as i64,
                                    );
                                })
                                .or_insert(distinct_count as i64);
                        }
                        (
                            Value::LongInt(min),
                            Value::LongInt(max),
                            Some(Value::LongInt(current_min)),
                            Some(Value::LongInt(current_max)),
                        ) => {
                            distinct_counts
                                .entry(id)
                                .and_modify(|x| {
                                    *x += estimate_distinct_count(
                                        &[current_min, current_max],
                                        &[&min, &max],
                                        *x,
                                        distinct_count as i64,
                                    );
                                })
                                .or_insert(distinct_count as i64);
                        }
                        (_, _, None, None) => {
                            distinct_counts.entry(id).or_insert(distinct_count as i64);
                        }
                        _ => (),
                    }
                }

                if let Some(min_bytes) = statistics.min_bytes_opt() {
                    if let Type::Primitive(_) = &data_type {
                        let new = Value::try_from_bytes(min_bytes, data_type)?;
                        match lower_bounds.entry(id) {
                            Entry::Occupied(mut entry) => {
                                let entry = entry.get_mut();
                                match (&entry, &new) {
                                    (Value::Int(current), Value::Int(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::LongInt(current), Value::LongInt(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Float(current), Value::Float(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Double(current), Value::Double(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Date(current), Value::Date(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Time(current), Value::Time(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Timestamp(current), Value::Timestamp(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::TimestampTZ(current), Value::TimestampTZ(new_val)) => {
                                        if *current > *new_val {
                                            *entry = new
                                        }
                                    }
                                    _ => (),
                                }
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(new);
                            }
                        }
                    }
                }
                if let Some(max_bytes) = statistics.max_bytes_opt() {
                    if let Type::Primitive(_) = &data_type {
                        let new = Value::try_from_bytes(max_bytes, data_type)?;
                        match upper_bounds.entry(id) {
                            Entry::Occupied(mut entry) => {
                                let entry = entry.get_mut();
                                match (&entry, &new) {
                                    (Value::Int(current), Value::Int(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::LongInt(current), Value::LongInt(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Float(current), Value::Float(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Double(current), Value::Double(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Date(current), Value::Date(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Time(current), Value::Time(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::Timestamp(current), Value::Timestamp(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    (Value::TimestampTZ(current), Value::TimestampTZ(new_val)) => {
                                        if *current < *new_val {
                                            *entry = new
                                        }
                                    }
                                    _ => (),
                                }
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(new);
                            }
                        }
                    }
                }

                if let Some(partition_field) = partition_fields.get(column_name) {
                    if let Some(partition_value) = partition.get_mut(partition_field.name()) {
                        if partition_value.is_none() {
                            let partition_field = partition_fields
                                .get(column_name)
                                .ok_or_else(|| Error::InvalidFormat("transform".to_string()))?;
                            if let (Some(min_bytes), Some(max_bytes)) =
                                (statistics.min_bytes_opt(), statistics.max_bytes_opt())
                            {
                                let min = Value::try_from_bytes(min_bytes, data_type)?
                                    .transform(partition_field.transform())?;
                                let max = Value::try_from_bytes(max_bytes, data_type)?
                                    .transform(partition_field.transform())?;
                                if min == max {
                                    *partition_value = Some(min)
                                } else {
                                    return Err(Error::InvalidFormat(
                                        "Partition value of data file".to_owned(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let mut builder = DataFile::builder();
    builder
        .with_content(if equality_ids.is_none() {
            Content::Data
        } else {
            Content::EqualityDeletes
        })
        .with_file_path(location.to_string())
        .with_file_format(FileFormat::Parquet)
        .with_partition(partition)
        .with_record_count(file_metadata.file_metadata().num_rows())
        .with_file_size_in_bytes(file_size as i64)
        .with_column_sizes(Some(column_sizes))
        .with_value_counts(Some(value_counts))
        .with_null_value_counts(Some(null_value_counts))
        .with_nan_value_counts(None)
        .with_lower_bounds(Some(lower_bounds))
        .with_upper_bounds(Some(upper_bounds));

    if let Some(equality_ids) = equality_ids {
        builder.with_equality_ids(Some(equality_ids.to_vec()));
    }

    let content = builder.build()?;
    Ok(content)
}

/// Get parquet metadata size
pub fn thrift_size<T: TSerializable>(metadata: &T) -> Result<usize, Error> {
    let mut buffer = TrackedWrite::new(Vec::<u8>::new());
    let mut protocol = TCompactOutputProtocol::new(&mut buffer);
    metadata.write_to_out_protocol(&mut protocol)?;
    Ok(buffer.bytes_written())
}

fn range_overlap<T: Ord + Sub + Copy>(
    old_range: &[&T; 2],
    new_range: &[&T; 2],
) -> <T as Sub>::Output {
    let overlap_start = (*old_range[0]).max(*new_range[0]);
    let overlap_end = (*old_range[1]).min(*new_range[1]);
    overlap_end - overlap_start
}

/// Helper trait to convert numeric types to f64 for statistical calculations.
///
/// This trait provides a uniform interface for converting integer types to f64,
/// which is necessary for the statistical estimation algorithms. The conversion
/// may be lossy for very large i64 values (beyond 2^53), but this is acceptable
/// for statistical approximations.
pub trait ToF64 {
    /// Converts the value to f64.
    ///
    /// # Note
    ///
    /// For i64 values larger than 2^53, precision may be lost in the conversion.
    /// This is acceptable for statistical calculations where exact precision is
    /// not required.
    fn to_f64(self) -> f64;
}

impl ToF64 for i32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

/// Estimates the number of new distinct values when merging two sets of statistics.
///
/// This function assumes uniform distribution of distinct values within their respective ranges
/// and uses an independence approximation to estimate overlap probability.
///
/// # Algorithm
///
/// The estimation is split into two parts:
/// 1. **Non-overlapping region**: All values in the new range that fall outside the old range
///    are guaranteed to be new.
/// 2. **Overlapping region**: Uses the independence approximation:
///    - P(specific value not covered) = ((R-1)/R)^k
///    - where R is the overlap size and k is the expected number of old values in the overlap
///    - Expected new values = n2_overlap × P(not covered)
///
/// # Parameters
///
/// * `old_range` - [min, max] of the existing value range
/// * `new_range` - [min, max] of the new value range
/// * `old_distinct_count` - Number of distinct values in the old range
/// * `new_distinct_count` - Number of distinct values in the new range
///
/// # Returns
///
/// Estimated number of new distinct values to add to the running total
///
/// # Example
///
/// ```ignore
/// // Old range [0, 1000] with 100 distinct values
/// // New range [500, 1500] with 50 distinct values
/// let new_count = estimate_distinct_count(&[&0, &1000], &[&500, &1500], 100, 50);
/// ```
pub fn estimate_distinct_count<T>(
    old_range: &[&T; 2],
    new_range: &[&T; 2],
    old_distinct_count: i64,
    new_distinct_count: i64,
) -> i64
where
    T: Ord + Sub<Output = T> + Copy + Default + ToF64,
{
    let new_range_size = (*new_range[1] - *new_range[0]).to_f64();
    let current_range_size = (*old_range[1] - *old_range[0]).to_f64();
    let overlap = range_overlap(old_range, new_range);
    let overlap_size: f64 = if overlap >= T::default() {
        overlap.to_f64()
    } else {
        0.0
    };
    let n2 = new_distinct_count as f64;
    let n1 = old_distinct_count as f64;

    // Values outside overlap are definitely new
    let outside_overlap = ((new_range_size - overlap_size) / new_range_size * n2).max(0.0);

    // For overlap region: estimate how many new values exist
    // using independence approximation: P(value not covered) = ((R-1)/R)^k
    // Expected new values in overlap = n2_overlap * ((R-1)/R)^(n1_overlap)
    let n2_overlap = (overlap_size / new_range_size * n2).max(0.0);
    let expected_n1_in_overlap = (overlap_size / current_range_size * n1).max(0.0);

    let new_in_overlap = if overlap_size > 0.0 {
        let prob_not_covered = ((overlap_size - 1.0) / overlap_size).powf(expected_n1_in_overlap);
        n2_overlap * prob_not_covered
    } else {
        0.0
    };

    (outside_overlap + new_in_overlap).round() as i64
}
