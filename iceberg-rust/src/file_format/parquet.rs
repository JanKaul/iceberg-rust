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
        values::{PhysicalTypeHint, Struct, Value},
    },
    table_metadata::WRITE_METADATA_METRICS_DISTINCT_COUNTS_ENABLED,
};
use parquet::file::{metadata::ParquetMetaData, writer::TrackedWrite};
use thrift::protocol::{TCompactOutputProtocol, TSerializable};
use tracing::instrument;

use crate::error::Error;
use crate::file_format::metrics::{
    truncate_lower_bound, truncate_upper_bound, MetricsConfig, MetricsMode,
};

/// Parquet file-level KV metadata key that opts in to HLL-based `distinct_count`
/// estimation for `Int64` columns. A patched arrow-rs writer reads this entry
/// via `WriterProperties::key_value_metadata()` and, when the value is `"true"`,
/// runs an HLL sketch over each `Int64` column chunk and stores the estimate in
/// the chunk's `distinct_count` statistic. Setting it has no effect against
/// upstream parquet — the key is ignored.
pub const ICEBERG_ESTIMATE_INT64_DISTINCT_COUNT_META_KEY: &str =
    "iceberg.estimate-int64-distinct-count";

/// Read datafile statistics from parquetfile
#[instrument(name = "iceberg_rust::file_format::parquet::parquet_to_datafile", level = "debug", skip(file_metadata, schema, partition_fields, table_properties), fields(
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
    table_properties: &HashMap<String, String>,
) -> Result<DataFile, Error> {
    let write_distinct_counts = table_properties
        .get(WRITE_METADATA_METRICS_DISTINCT_COUNTS_ENABLED)
        .is_some_and(|x| x == "true");
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

    let metrics_config = MetricsConfig::from_table_properties(table_properties);
    // `max-inferred-column-defaults` counts top-level columns, so a nested
    // column inherits the position of the field it hangs off.
    let top_level_indices: HashMap<&str, usize> = schema
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name.as_str(), index))
        .collect();

    let mut column_sizes = AvroMap(HashMap::new());
    let mut value_counts = AvroMap(HashMap::new());
    let mut null_value_counts = AvroMap(HashMap::new());
    let mut distinct_counts = write_distinct_counts.then(|| AvroMap(HashMap::new()));
    let mut lower_bounds: HashMap<i32, Value> = HashMap::new();
    let mut upper_bounds: HashMap<i32, Value> = HashMap::new();
    // Which mode produced each column's bounds, so truncation can be applied
    // once at the end rather than per row group.
    let mut column_metrics_modes: HashMap<i32, MetricsMode> = HashMap::new();

    for row_group in file_metadata.row_groups() {
        for column in row_group.columns() {
            let column_name = column.column_descr().name();
            let column_path = column.column_path().parts().join(".");
            let id = schema
                .get_name(&column_path)
                .ok_or_else(|| Error::Schema(column_name.to_string(), "".to_string()))?
                .id;

            let top_level_index = column_path
                .split('.')
                .next()
                .and_then(|root| top_level_indices.get(root).copied());
            let metrics_mode = metrics_config.mode_for(&column_path, top_level_index);
            column_metrics_modes.insert(id, metrics_mode);

            // Column sizes describe the file's physical layout rather than its
            // values, so they are collected regardless of the metrics mode.
            column_sizes
                .entry(id)
                .and_modify(|x| *x += column.compressed_size())
                .or_insert(column.compressed_size());
            if metrics_mode.records_counts() {
                value_counts
                    .entry(id)
                    .and_modify(|x| *x += row_group.num_rows())
                    .or_insert(row_group.num_rows());
            }

            if let Some(statistics) = column.statistics() {
                if let Some(null_count) = statistics
                    .null_count_opt()
                    .filter(|_| metrics_mode.records_counts())
                {
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

                // Parquet's physical type can encode a logical type differently than
                // the Iceberg spec: INT32/INT64 stats are native little-endian
                // (Decimal's spec encoding is big-endian), and BYTE_ARRAY stats hold
                // Uuid's UTF-8 string form (spec: 16-byte big-endian integer).
                let physical_type_hint = match column.column_descr().physical_type() {
                    parquet::basic::Type::INT32 | parquet::basic::Type::INT64 => {
                        Some(PhysicalTypeHint::NativeLittleEndian)
                    }
                    parquet::basic::Type::BYTE_ARRAY => Some(PhysicalTypeHint::ByteArray),
                    _ => None,
                };

                if let Some(distinct_counts) = distinct_counts
                    .as_mut()
                    .filter(|_| metrics_mode.records_counts())
                {
                    if let (Some(distinct_count), Some(min_bytes), Some(max_bytes)) = (
                        statistics.distinct_count_opt(),
                        statistics.min_bytes_opt(),
                        statistics.max_bytes_opt(),
                    ) {
                        let min = Value::try_from_bytes_with_hint(
                            min_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
                        let max = Value::try_from_bytes_with_hint(
                            max_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
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
                }

                if let Some(min_bytes) = statistics
                    .min_bytes_opt()
                    .filter(|_| metrics_mode.records_bounds())
                {
                    if let Type::Primitive(_) = &data_type {
                        let new = Value::try_from_bytes_with_hint(
                            min_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
                        match lower_bounds.entry(id) {
                            Entry::Occupied(mut entry) => {
                                let entry = entry.get_mut();
                                match (&entry, &new) {
                                    (Value::Int(current), Value::Int(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::LongInt(current), Value::LongInt(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Float(current), Value::Float(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Double(current), Value::Double(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Date(current), Value::Date(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Time(current), Value::Time(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Timestamp(current), Value::Timestamp(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::TimestampTZ(current), Value::TimestampTZ(new_val))
                                        if *current > *new_val =>
                                    {
                                        *entry = new
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
                if let Some(max_bytes) = statistics
                    .max_bytes_opt()
                    .filter(|_| metrics_mode.records_bounds())
                {
                    if let Type::Primitive(_) = &data_type {
                        let new = Value::try_from_bytes_with_hint(
                            max_bytes,
                            data_type,
                            physical_type_hint,
                        )?;
                        match upper_bounds.entry(id) {
                            Entry::Occupied(mut entry) => {
                                let entry = entry.get_mut();
                                match (&entry, &new) {
                                    (Value::Int(current), Value::Int(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::LongInt(current), Value::LongInt(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Float(current), Value::Float(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Double(current), Value::Double(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Date(current), Value::Date(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Time(current), Value::Time(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::Timestamp(current), Value::Timestamp(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
                                    }
                                    (Value::TimestampTZ(current), Value::TimestampTZ(new_val))
                                        if *current < *new_val =>
                                    {
                                        *entry = new
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
                                let min = Value::try_from_bytes_with_hint(
                                    min_bytes,
                                    data_type,
                                    physical_type_hint,
                                )?
                                .transform(partition_field.transform())?;
                                let max = Value::try_from_bytes_with_hint(
                                    max_bytes,
                                    data_type,
                                    physical_type_hint,
                                )?
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
    // Truncate once, after the bounds have been merged across row groups.
    // Truncating per row group and merging afterwards would repeatedly widen an
    // already-widened upper bound.
    let lower_bounds = lower_bounds
        .into_iter()
        .map(|(id, value)| match column_metrics_modes.get(&id) {
            Some(MetricsMode::Truncate(length)) => (id, truncate_lower_bound(value, *length)),
            _ => (id, value),
        })
        .collect::<HashMap<i32, Value>>();
    let upper_bounds = upper_bounds
        .into_iter()
        .filter_map(|(id, value)| match column_metrics_modes.get(&id) {
            // A bound that cannot be raised is dropped: an understated upper
            // bound would prune files that still hold matching rows.
            Some(MetricsMode::Truncate(length)) => {
                truncate_upper_bound(value, *length).map(|value| (id, value))
            }
            _ => Some((id, value)),
        })
        .collect::<HashMap<i32, Value>>();

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
        .with_distinct_counts(distinct_counts)
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

#[cfg(test)]
mod metrics_mode_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use iceberg_rust_spec::spec::schema::Schema;
    use iceberg_rust_spec::spec::types::{PrimitiveType, StructField, Type};
    use iceberg_rust_spec::spec::values::Value;
    use iceberg_rust_spec::table_metadata::{
        WRITE_METADATA_METRICS_COLUMN_PREFIX, WRITE_METADATA_METRICS_DEFAULT,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::parquet_to_datafile;

    /// A two-column table whose values are far longer than any sane bound.
    fn schema() -> Schema {
        Schema::builder()
            .with_struct_field(StructField {
                id: 0,
                name: "body".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 1,
                name: "trace_id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap()
    }

    /// Writes one Parquet file holding `body`/`trace_id` and returns the
    /// `DataFile` built from it under the given table properties.
    fn datafile_with(
        properties: HashMap<String, String>,
    ) -> iceberg_rust_spec::spec::manifest::DataFile {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("body", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, false),
        ]));

        let bodies: ArrayRef = Arc::new(StringArray::from(vec![
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa- first log line",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz- last log line",
        ]));
        let trace_ids: ArrayRef = Arc::new(StringArray::from(vec![
            "00000000000000000000000000000001",
            "ffffffffffffffffffffffffffffffff",
        ]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![bodies, trace_ids]).unwrap();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_size = buffer.len() as u64;

        let reader = SerializedFileReader::new(bytes::Bytes::from(buffer)).unwrap();
        let parquet_metadata = reader.metadata().clone();

        parquet_to_datafile(
            "/t/data/1.parquet",
            file_size,
            &parquet_metadata,
            &schema(),
            &[],
            None,
            &properties,
        )
        .unwrap()
    }

    fn bound(bounds: &Option<HashMap<i32, Value>>, id: i32) -> Option<String> {
        match bounds.as_ref()?.get(&id)? {
            Value::String(string) => Some(string.clone()),
            other => panic!("expected a string bound, got {other:?}"),
        }
    }

    /// A table that says nothing must still not carry a full-length bound for
    /// every column: the spec's inferred default is `truncate(16)`.
    #[test]
    fn bounds_are_truncated_to_sixteen_by_default() {
        let datafile = datafile_with(HashMap::new());

        let lower = bound(datafile.lower_bounds(), 0).expect("a lower bound for body");
        let upper = bound(datafile.upper_bounds(), 0).expect("an upper bound for body");

        assert_eq!(lower.chars().count(), 16, "lower bound was not truncated");
        assert!(upper.chars().count() <= 16, "upper bound was not truncated");

        // Truncation must not break the bounds' meaning.
        assert!(lower.as_str() <= "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa- first log line");
        assert!(upper.as_str() >= "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz- last log line");
    }

    /// `none` on a column drops its bounds and counts, which is the point for a
    /// JSON blob or a log body that no query ever prunes on.
    #[test]
    fn a_column_set_to_none_carries_no_metrics() {
        let properties = HashMap::from([(
            format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}body"),
            "none".to_string(),
        )]);
        let datafile = datafile_with(properties);

        assert_eq!(bound(datafile.lower_bounds(), 0), None);
        assert_eq!(bound(datafile.upper_bounds(), 0), None);
        assert_eq!(datafile.value_counts().as_ref().unwrap().get(&0), None);

        // The other column is untouched.
        assert!(bound(datafile.lower_bounds(), 1).is_some());
        assert!(datafile.value_counts().as_ref().unwrap().get(&1).is_some());

        // Column sizes describe layout, not values, and are always present.
        assert!(datafile.column_sizes().as_ref().unwrap().get(&0).is_some());
    }

    /// `full` keeps the untruncated bound, for the columns worth it.
    #[test]
    fn a_column_set_to_full_keeps_untruncated_bounds() {
        let properties = HashMap::from([
            (
                WRITE_METADATA_METRICS_DEFAULT.to_string(),
                "counts".to_string(),
            ),
            (
                format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}trace_id"),
                "full".to_string(),
            ),
        ]);
        let datafile = datafile_with(properties);

        assert_eq!(
            bound(datafile.lower_bounds(), 1).as_deref(),
            Some("00000000000000000000000000000001")
        );
        // `counts` gives the other column counts but no bounds.
        assert_eq!(bound(datafile.lower_bounds(), 0), None);
        assert!(datafile.value_counts().as_ref().unwrap().get(&0).is_some());
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    // -- Parquet end-to-end + schema util + read projection + writers (8+10+5+7+3=33) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[ignore = "TestParquet (8), TestParquetSchemaUtil (10), TestParquetReadProjection (5), TestParquetDataWriter (7), TestParquetDeleteWriters (3): direct-Parquet writer + reader + schema-bridge + delete-writer surface"]
    fn test_parquet_writer_reader_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet writer/reader suite");
    }

    // -- Parquet page version + CDH parquet statistics (9+1=10) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[ignore = "TestParquetPageVersion + TestCDHParquetStatistics: page-version and CDH-stats edge cases"]
    fn test_parquet_page_version_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet page version + CDH stats");
    }

    // -- Row group filters: bloom (50) + dictionary (38) + metrics (41) + metrics-types (1) = 130 --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(13)]
    #[case(14)]
    #[case(15)]
    #[case(16)]
    #[case(17)]
    #[case(18)]
    #[case(19)]
    #[case(20)]
    #[case(21)]
    #[case(22)]
    #[case(23)]
    #[case(24)]
    #[case(25)]
    #[case(26)]
    #[case(27)]
    #[case(28)]
    #[case(29)]
    #[case(30)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[case(34)]
    #[case(35)]
    #[case(36)]
    #[case(37)]
    #[case(38)]
    #[case(39)]
    #[case(40)]
    #[case(41)]
    #[case(42)]
    #[case(43)]
    #[case(44)]
    #[case(45)]
    #[case(46)]
    #[case(47)]
    #[case(48)]
    #[case(49)]
    #[case(50)]
    #[case(51)]
    #[case(52)]
    #[case(53)]
    #[case(54)]
    #[case(55)]
    #[case(56)]
    #[case(57)]
    #[case(58)]
    #[case(59)]
    #[case(60)]
    #[case(61)]
    #[case(62)]
    #[case(63)]
    #[case(64)]
    #[case(65)]
    #[case(66)]
    #[case(67)]
    #[case(68)]
    #[case(69)]
    #[case(70)]
    #[case(71)]
    #[case(72)]
    #[case(73)]
    #[case(74)]
    #[case(75)]
    #[case(76)]
    #[case(77)]
    #[case(78)]
    #[case(79)]
    #[case(80)]
    #[case(81)]
    #[case(82)]
    #[case(83)]
    #[case(84)]
    #[case(85)]
    #[case(86)]
    #[case(87)]
    #[case(88)]
    #[case(89)]
    #[case(90)]
    #[case(91)]
    #[case(92)]
    #[case(93)]
    #[case(94)]
    #[case(95)]
    #[case(96)]
    #[case(97)]
    #[case(98)]
    #[case(99)]
    #[case(100)]
    #[case(101)]
    #[case(102)]
    #[case(103)]
    #[case(104)]
    #[case(105)]
    #[case(106)]
    #[case(107)]
    #[case(108)]
    #[case(109)]
    #[case(110)]
    #[case(111)]
    #[case(112)]
    #[case(113)]
    #[case(114)]
    #[case(115)]
    #[case(116)]
    #[case(117)]
    #[case(118)]
    #[case(119)]
    #[case(120)]
    #[case(121)]
    #[case(122)]
    #[case(123)]
    #[case(124)]
    #[case(125)]
    #[case(126)]
    #[case(127)]
    #[case(128)]
    #[case(129)]
    #[case(130)]
    #[ignore = "no row-group filter pushdown surface (BloomRowGroupFilter, DictionaryRowGroupFilter, MetricsRowGroupFilter + Types)"]
    fn test_parquet_row_group_filters_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet row-group filters");
    }

    // -- TestParquetEncryption + WriteSupport + InputStreamAdapter (3+1+2=6) --
    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[ignore = "no Parquet encryption + range-readable input stream adapter"]
    fn test_parquet_encryption_scenarios(#[case] _scenario: usize) {
        unimplemented!("Parquet encryption");
    }
}
