/*!
 * Helpers for parquet files
*/

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
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
use parquet::{
    file::{metadata::RowGroupMetaData, writer::TrackedWrite},
    format::FileMetaData,
    schema::types::{from_thrift, SchemaDescriptor},
};
use thrift::protocol::{TCompactOutputProtocol, TSerializable};

use crate::error::Error;

/// Read datafile statistics from parquetfile
pub fn parquet_to_datafile(
    location: &str,
    file_size: usize,
    file_metadata: &FileMetaData,
    schema: &Schema,
    partition_fields: &[BoundPartitionField<'_>],
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
    let parquet_schema = Arc::new(SchemaDescriptor::new(from_thrift(&file_metadata.schema)?));

    let mut column_sizes = AvroMap(HashMap::new());
    let mut value_counts = AvroMap(HashMap::new());
    let mut null_value_counts = AvroMap(HashMap::new());
    let mut distinct_counts = AvroMap(HashMap::new());
    let mut lower_bounds: HashMap<i32, Value> = HashMap::new();
    let mut upper_bounds: HashMap<i32, Value> = HashMap::new();

    for row_group in &file_metadata.row_groups {
        let row_group = RowGroupMetaData::from_thrift(parquet_schema.clone(), row_group.clone())?;

        for column in row_group.columns() {
            let column_name = column.column_descr().name();
            let id = schema
                .get_name(column_name)
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
                if let Some(distinct_count) = statistics.distinct_count_opt() {
                    distinct_counts
                        .entry(id)
                        .and_modify(|x| *x += distinct_count as i64)
                        .or_insert(distinct_count as i64);
                }
                let data_type = &schema
                    .fields()
                    .get(id as usize)
                    .ok_or_else(|| Error::Schema(column_name.to_string(), "".to_string()))?
                    .field_type;

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
                    if let Some(max_bytes) = statistics.max_bytes_opt() {
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

                        if let Some(partition_field) = partition_fields.get(column_name) {
                            if let Some(partition_value) = partition.get_mut(partition_field.name())
                            {
                                if partition_value.is_none() {
                                    let partition_field =
                                        partition_fields.get(column_name).ok_or_else(|| {
                                            Error::InvalidFormat("transform".to_string())
                                        })?;
                                    if let (Some(min_bytes), Some(max_bytes)) =
                                        (statistics.min_bytes_opt(), statistics.max_bytes_opt())
                                    {
                                        let min = Value::try_from_bytes(min_bytes, data_type)?
                                            .tranform(partition_field.transform())?;
                                        let max = Value::try_from_bytes(max_bytes, data_type)?
                                            .tranform(partition_field.transform())?;
                                        if min == max {
                                            *partition_value = Some(min)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let content = DataFile::builder()
        .with_content(Content::Data)
        .with_file_path(location.to_string())
        .with_file_format(FileFormat::Parquet)
        .with_partition(partition)
        .with_record_count(file_metadata.num_rows)
        .with_file_size_in_bytes(file_size as i64)
        .with_column_sizes(Some(column_sizes))
        .with_value_counts(Some(value_counts))
        .with_null_value_counts(Some(null_value_counts))
        .with_nan_value_counts(None)
        .with_distinct_counts(Some(distinct_counts))
        .with_lower_bounds(Some(lower_bounds))
        .with_upper_bounds(Some(upper_bounds))
        .build()
        .map_err(iceberg_rust_spec::error::Error::from)?;
    Ok(content)
}

/// Get parquet metadata size
pub fn thrift_size<T: TSerializable>(metadata: &T) -> Result<usize, Error> {
    let mut buffer = TrackedWrite::new(Vec::<u8>::new());
    let mut protocol = TCompactOutputProtocol::new(&mut buffer);
    metadata.write_to_out_protocol(&mut protocol)?;
    Ok(buffer.bytes_written())
}
