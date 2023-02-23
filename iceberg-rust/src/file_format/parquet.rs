/*!
 * Helpers for parquet files
*/

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};

use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use parquet::{
    arrow::async_reader::fetch_parquet_metadata, errors::ParquetError,
    file::metadata::ParquetMetaData,
};
use serde_bytes::ByteBuf;

use crate::model::{
    bytes::bytes_to_any,
    data_types::{PrimitiveType, StructType, Type},
    manifest::{AvroMap, Content, DataFileV2, FileFormat},
    values::Struct,
};

/// Get parquet metadata for a given path from the object_store
pub async fn parquet_metadata(
    paths: Vec<String>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<(String, ObjectMeta, ParquetMetaData)>> {
    stream::iter(paths.into_iter())
        .then(|x| {
            let object_store = object_store.clone();
            async move {
                let path: Path = x.as_str().into();
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
                Ok::<_, anyhow::Error>((x, file_metadata, parquet_metadata))
            }
        })
        .try_collect::<Vec<_>>()
        .await
}

/// Statistics of a datafile
pub struct DataFileContent {
    /// Full URI for the file with a FS scheme.
    pub file_path: String,
    /// String file format name, avro, orc or parquet
    pub file_format: FileFormat,
    /// Number of records in this file
    pub record_count: i64,
    /// Total file size in bytes
    pub file_size_in_bytes: i64,
    /// Map from column id to total size on disk
    pub column_sizes: Option<AvroMap<i64>>,
    /// Map from column id to number of values in the column (including null and NaN values)
    pub value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of null values
    pub null_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of NaN values
    pub nan_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of distinct values in the column.
    pub distinct_counts: Option<AvroMap<i64>>,
    /// Map from column id to lower bound in the column
    pub lower_bounds: Option<AvroMap<ByteBuf>>,
    /// Map from column id to upper bound in the column
    pub upper_bounds: Option<AvroMap<ByteBuf>>,
    /// Implementation specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
    /// Split offsets for the data file.
    pub split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files.
    pub equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file
    pub sort_order_id: Option<i32>,
}

/// Read datafile statistics from parquetfile
pub fn parquet_to_datafilev2(
    path: String,
    file_metadata: ObjectMeta,
    parquet_metadata: ParquetMetaData,
    partition: Struct,
    schema: &StructType,
) -> Result<DataFileV2> {
    let mut content = DataFileV2 {
        content: Content::Data,
        file_path: path,
        file_format: FileFormat::Parquet,
        partition,
        record_count: parquet_metadata.file_metadata().num_rows(),
        file_size_in_bytes: file_metadata.size as i64,
        column_sizes: Some(AvroMap(HashMap::new())),
        value_counts: Some(AvroMap(HashMap::new())),
        null_value_counts: Some(AvroMap(HashMap::new())),
        nan_value_counts: None,
        distinct_counts: Some(AvroMap(HashMap::new())),
        lower_bounds: Some(AvroMap(HashMap::new())),
        upper_bounds: Some(AvroMap(HashMap::new())),
        key_metadata: None,
        split_offsets: None,
        equality_ids: None,
        sort_order_id: None,
    };
    for row_group in parquet_metadata.row_groups() {
        for column in row_group.columns() {
            let column_name = column.column_descr().name();
            let id = schema
                    .get_name(column_name)
                    .ok_or_else(|| anyhow!("Error: Failed to add Parquet file to table. Colummn {} doesn't exist in schema.", column_name))?.id;
            if let Some(column_sizes) = &mut content.column_sizes {
                if let Some(entry) = column_sizes.0.get_mut(&id) {
                    *entry += column.compressed_size()
                }
            }
            if let Some(value_counts) = &mut content.value_counts {
                if let Some(entry) = value_counts.0.get_mut(&id) {
                    *entry += row_group.num_rows()
                }
            }
            if let Some(statistics) = column.statistics() {
                if let Some(null_value_counts) = &mut content.null_value_counts {
                    if let Some(entry) = null_value_counts.0.get_mut(&id) {
                        *entry += statistics.null_count() as i64
                    }
                }
                if let Some(distinct_count) = &mut content.distinct_counts {
                    if let (Some(entry), Some(distinct_count)) =
                        (distinct_count.0.get_mut(&id), statistics.distinct_count())
                    {
                        *entry += distinct_count as i64
                    }
                }
                if let Some(lower_bounds) = &mut content.lower_bounds {
                    if let Some(entry) = lower_bounds.0.get_mut(&id) {
                        let data_type = &schema.get(id as usize).ok_or_else(|| anyhow!("Error: Failed to add Parquet file to table. Colummn {} doesn't exist in schema.", column_name))?.field_type;
                        match data_type {
                            Type::Primitive(prim) => match prim {
                                PrimitiveType::Date => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i32>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i32>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Double => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<f64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<f64>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Float => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<f32>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<f32>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Int => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i32>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i32>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Long => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Time => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Timestamp => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Timestampz => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current > new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                _ => (),
                            },
                            _ => (),
                        }
                    }
                }
                if let Some(upper_bounds) = &mut content.upper_bounds {
                    if let Some(entry) = upper_bounds.0.get_mut(&id) {
                        let data_type = &schema.get(id as usize).ok_or_else(|| anyhow!("Error: Failed to add Parquet file to table. Colummn {} doesn't exist in schema.", column_name))?.field_type;
                        match data_type {
                            Type::Primitive(prim) => match prim {
                                PrimitiveType::Date => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i32>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i32>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Double => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<f64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<f64>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Float => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<f32>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<f32>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Int => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i32>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i32>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Long => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Time => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Timestamp => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
                                    }
                                }
                                PrimitiveType::Timestampz => {
                                    let current =
                                        bytes_to_any(entry, data_type)?.downcast::<i64>().unwrap();
                                    let new = bytes_to_any(statistics.min_bytes(), data_type)?
                                        .downcast::<i64>()
                                        .unwrap();
                                    if current < new {
                                        *entry = ByteBuf::from(statistics.min_bytes())
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
    Ok(content)
}
