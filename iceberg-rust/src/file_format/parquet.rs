/*!
 * Helpers for parquet files
*/

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};

use futures::TryFutureExt;
use object_store::{path::Path, ObjectMeta, ObjectStore};
use parquet::{
    arrow::async_reader::fetch_parquet_metadata,
    errors::ParquetError,
    file::metadata::{ParquetMetaData, RowGroupMetaData},
    format::FileMetaData,
    schema::types::{from_thrift, SchemaDescriptor},
};
use serde_bytes::ByteBuf;

use crate::model::{
    bytes::bytes_to_any,
    data_types::{PrimitiveType, StructType, Type},
    manifest::{AvroMap, Content, DataFileV2, FileFormat},
    partition::{PartitionField, Transform},
    values::{Struct, Value},
};

/// Get parquet metadata for a given path from the object_store
pub async fn parquet_metadata(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, ObjectMeta, ParquetMetaData)> {
    let path: Path = path.into();
    let file_metadata = object_store.head(&path).await.map_err(anyhow::Error::msg)?;
    let parquet_metadata = fetch_parquet_metadata(
        |range| {
            object_store
                .get_range(&path, range)
                .map_err(|err| ParquetError::General(err.to_string()))
        },
        file_metadata.size,
        None,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    Ok((path.to_string(), file_metadata, parquet_metadata))
}

/// Read datafile statistics from parquetfile
pub fn parquet_to_datafilev2(
    location: &str,
    file_metadata: &FileMetaData,
    schema: &StructType,
    partition_spec: &[PartitionField],
) -> Result<DataFileV2> {
    let mut partition = partition_spec
        .iter()
        .map(|x| {
            let field = schema
                .get(x.source_id as usize)
                .ok_or_else(|| anyhow!("Column with id {} is missing in schema.", x.source_id))?;
            Ok((field.name.clone(), None))
        })
        .collect::<Result<Struct>>()?;
    let transforms = partition_spec
        .iter()
        .map(|x| {
            let field = schema
                .get(x.source_id as usize)
                .ok_or_else(|| anyhow!("Column with id {} is missing in schema.", x.source_id))?;
            Ok((field.name.clone(), x.transform.clone()))
        })
        .collect::<Result<HashMap<String, Transform>>>()?;
    let parquet_schema = Arc::new(SchemaDescriptor::new(from_thrift(&file_metadata.schema)?));

    let mut file_size = 0;

    let mut column_sizes = Some(AvroMap(HashMap::new()));
    let mut value_counts = Some(AvroMap(HashMap::new()));
    let mut null_value_counts = Some(AvroMap(HashMap::new()));
    let mut distinct_counts = Some(AvroMap(HashMap::new()));
    let mut lower_bounds: Option<AvroMap<ByteBuf>> = Some(AvroMap(HashMap::new()));
    let mut upper_bounds: Option<AvroMap<ByteBuf>> = Some(AvroMap(HashMap::new()));

    for row_group in &file_metadata.row_groups {
        let row_group = RowGroupMetaData::from_thrift(parquet_schema.clone(), row_group.clone())?;

        file_size += row_group.compressed_size();

        for column in row_group.columns() {
            let column_name = column.column_descr().name();
            let id = schema
                    .get_name(column_name)
                    .ok_or_else(|| anyhow!("Error: Failed to add Parquet file to table. Colummn {} doesn't exist in schema.", column_name))?.id;
            if let Some(column_sizes) = &mut column_sizes {
                if let Some(entry) = column_sizes.0.get_mut(&id) {
                    *entry += column.compressed_size()
                }
            }
            if let Some(value_counts) = &mut value_counts {
                if let Some(entry) = value_counts.0.get_mut(&id) {
                    *entry += row_group.num_rows()
                }
            }
            if let Some(statistics) = column.statistics() {
                if let Some(null_value_counts) = &mut null_value_counts {
                    if let Some(entry) = null_value_counts.0.get_mut(&id) {
                        *entry += statistics.null_count() as i64
                    }
                }
                if let Some(distinct_count) = &mut distinct_counts {
                    if let (Some(entry), Some(distinct_count)) =
                        (distinct_count.0.get_mut(&id), statistics.distinct_count())
                    {
                        *entry += distinct_count as i64
                    }
                }
                if let Some(lower_bounds) = &mut lower_bounds {
                    if let Some(entry) = lower_bounds.0.get_mut(&id) {
                        let data_type = &schema.get(id as usize).ok_or_else(|| anyhow!("Error: Failed to add Parquet file to table. Colummn {} doesn't exist in schema.", column_name))?.field_type;
                        match data_type {
                            Type::Primitive(prim) => match prim {
                                PrimitiveType::Date => {
                                    if let Some(new) = change_new::<i32, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Double => {
                                    if let Some(new) = change_new::<f64, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Float => {
                                    if let Some(new) = change_new::<f32, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Int => {
                                    if let Some(new) = change_new::<i32, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Long => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Time => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Timestamp => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Timestampz => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.min_bytes(),
                                        data_type,
                                        |current, new| current > new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                _ => (),
                            },
                            _ => (),
                        }
                    }
                }
                if let Some(upper_bounds) = &mut upper_bounds {
                    if let Some(entry) = upper_bounds.0.get_mut(&id) {
                        let data_type = &schema.get(id as usize).ok_or_else(|| anyhow!("Error: Failed to add Parquet file to table. Colummn {} doesn't exist in schema.", column_name))?.field_type;
                        match data_type {
                            Type::Primitive(prim) => match prim {
                                PrimitiveType::Date => {
                                    if let Some(new) = change_new::<i32, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Double => {
                                    if let Some(new) = change_new::<f64, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Float => {
                                    if let Some(new) = change_new::<f32, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Int => {
                                    if let Some(new) = change_new::<i32, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Long => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Time => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Timestamp => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                PrimitiveType::Timestampz => {
                                    if let Some(new) = change_new::<i64, _>(
                                        entry,
                                        statistics.max_bytes(),
                                        data_type,
                                        |current, new| current < new,
                                    )? {
                                        *entry = ByteBuf::from(new)
                                    }
                                }
                                _ => (),
                            },
                            _ => (),
                        }
                    }
                }
                if let Some(partition_value) = partition.get_mut(column_name) {
                    if partition_value.is_none() {
                        let data_type = &schema.get(id as usize).ok_or_else(|| anyhow!("Error: Failed to add Parquet file to table. Colummn {} doesn't exist in schema.", column_name))?.field_type;
                        match data_type {
                            Type::Primitive(prim) => match prim {
                                PrimitiveType::Date => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::Date(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<i32>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::Date(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<i32>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                PrimitiveType::Double => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::Double(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<f64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::Double(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<f64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                PrimitiveType::Float => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::Float(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<f32>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::Float(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<f32>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                PrimitiveType::Int => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::Int(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<i32>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::Int(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<i32>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                PrimitiveType::Long => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::LongInt(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::LongInt(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                PrimitiveType::Time => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::Time(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::Time(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                PrimitiveType::Timestamp => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::Timestamp(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::Timestamp(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                PrimitiveType::Timestampz => {
                                    let transform =
                                        transforms.get(column_name).ok_or_else(|| {
                                            anyhow!(
                                                "Transform for column {} doesn't exist",
                                                column_name
                                            )
                                        })?;
                                    let min = Value::TimestampTZ(
                                        *bytes_to_any(statistics.min_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    let max = Value::TimestampTZ(
                                        *bytes_to_any(statistics.max_bytes(), data_type)?
                                            .downcast::<i64>()
                                            .unwrap(),
                                    )
                                    .tranform(transform)?;
                                    if min == max {
                                        *partition_value = Some(min)
                                    }
                                }
                                _ => (),
                            },
                            _ => (),
                        }
                    }
                }
            }
        }
    }
    let content = DataFileV2 {
        content: Content::Data,
        file_path: location.to_string(),
        file_format: FileFormat::Parquet,
        partition,
        record_count: file_metadata.num_rows,
        file_size_in_bytes: file_size,
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts: None,
        distinct_counts,
        lower_bounds,
        upper_bounds,
        key_metadata: None,
        split_offsets: None,
        equality_ids: None,
        sort_order_id: None,
    };
    Ok(content)
}

// Downcasts the bytes to the data type and checks the predicate function
#[inline]
fn change_new<'bytes, T: 'static, F: FnOnce(Box<T>, Box<T>) -> bool>(
    first: &[u8],
    second: &'bytes [u8],
    data_type: &Type,
    f: F,
) -> Result<Option<&'bytes [u8]>> {
    let current = bytes_to_any(first, data_type)?.downcast::<T>().unwrap();
    let new = bytes_to_any(second, data_type)?.downcast::<T>().unwrap();
    if f(current, new) {
        Ok(Some(second))
    } else {
        Ok(None)
    }
}
