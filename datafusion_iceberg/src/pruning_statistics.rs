/*!
 * Implement pruning statistics for Datafusion table
 *
 * Pruning is done on two levels:
 *
 * 1. Prune ManifestFiles based on information in Manifest_list_file
 * 2. Prune DataFiles based on information in Manifest_file
 *
 * For the first level the triat PruningStatistics is implemented for the DataFusionTable. It returns the pruning information for the manifest files
 * and not the final data files.
 *
 * For the second level the trait PruningStatistics is implemented for the ManifestFile
*/

use std::any::Any;

use datafusion::{
    arrow::{
        array::ArrayRef,
        datatypes::{DataType, Schema},
    },
    common::DataFusionError,
    physical_optimizer::pruning::PruningStatistics,
    prelude::Column,
    scalar::ScalarValue,
};

use iceberg_rust::{
    model::{manifest::ManifestEntry, values::Value},
    table::Table,
};

pub(crate) struct PruneManifests<'table>(&'table Table);

impl<'table> From<&'table Table> for PruneManifests<'table> {
    fn from(value: &'table Table) -> Self {
        PruneManifests(value)
    }
}

impl<'table> PruningStatistics for PruneManifests<'table> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let partition_spec = &self.0.metadata().default_partition_spec().ok()?.fields;
        let schema = self.0.schema();
        let (index, partition_field) = partition_spec
            .iter()
            .enumerate()
            .find(|(_, partition_field)| partition_field.name == column.name)?;
        let data_type = schema
            .get(partition_field.source_id as usize)
            .as_ref()?
            .field_type
            .tranform(&partition_field.transform)
            .ok()?
            .try_into()
            .ok()?;
        let min_values = self.0.manifests().iter().filter_map(|manifest| {
            manifest.partitions().as_ref().and_then(|partitions| {
                partitions[index]
                    .lower_bound
                    .as_ref()
                    .map(|min| Some(Value::from_bytes(min, &data_type).ok()?.into_any()))
            })
        });
        any_iter_to_array(min_values, &(&data_type).try_into().ok()?).ok()
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let partition_spec = self.0.metadata().default_partition_spec().ok()?;
        let schema = self.0.schema();
        let (index, partition_field) = partition_spec
            .fields
            .iter()
            .enumerate()
            .find(|(_, partition_field)| partition_field.name == column.name)?;
        let data_type = schema
            .get(partition_field.source_id as usize)
            .as_ref()?
            .field_type
            .tranform(&partition_field.transform)
            .ok()?
            .try_into()
            .ok()?;
        let max_values = self.0.manifests().iter().filter_map(|manifest| {
            manifest.partitions().as_ref().and_then(|partitions| {
                partitions[index]
                    .upper_bound
                    .as_ref()
                    .map(|max| Some(Value::from_bytes(max, &data_type).ok()?.into_any()))
            })
        });
        any_iter_to_array(max_values, &(&data_type).try_into().ok()?).ok()
    }
    fn num_containers(&self) -> usize {
        self.0.manifests().len()
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let partition_spec = self.0.metadata().default_partition_spec().ok()?;
        let (index, _) = partition_spec
            .fields
            .iter()
            .enumerate()
            .find(|(_, partition_field)| partition_field.name == column.name)?;
        let contains_null = self.0.manifests().iter().filter_map(|manifest| {
            manifest.partitions().as_ref().map(|partitions| {
                if !partitions[index].contains_null {
                    Some(0)
                } else {
                    None
                }
            })
        });
        ScalarValue::iter_to_array(contains_null.map(ScalarValue::Int32)).ok()
    }
}

pub(crate) struct PruneDataFiles<'table, 'manifests> {
    table: &'table Table,
    files: &'manifests [ManifestEntry],
}

impl<'table, 'manifests> PruneDataFiles<'table, 'manifests> {
    pub fn new(table: &'table Table, files: &'manifests [ManifestEntry]) -> Self {
        PruneDataFiles { table, files }
    }
}

impl<'table, 'manifests> PruningStatistics for PruneDataFiles<'table, 'manifests> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let schema: Schema = self.table.schema().try_into().ok()?;
        let column_id = schema.index_of(&column.name).ok()?;
        let datatype = schema.field_with_name(&column.name).ok()?.data_type();
        let min_values = self
            .files
            .iter()
            .map(|manifest| match &manifest.lower_bounds() {
                Some(map) => map.get(&(column_id as i32)).and_then(|value| {
                    Some(
                        Value::from_bytes(value, &datatype.try_into().ok()?)
                            .ok()?
                            .into_any(),
                    )
                }),
                None => None,
            });
        any_iter_to_array(min_values, datatype).ok()
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let schema: Schema = self.table.schema().try_into().ok()?;
        let column_id = schema.index_of(&column.name).ok()?;
        let datatype = schema.field_with_name(&column.name).ok()?.data_type();
        let max_values = self
            .files
            .iter()
            .map(|manifest| match &manifest.upper_bounds() {
                Some(map) => map.get(&(column_id as i32)).and_then(|value| {
                    Some(
                        Value::from_bytes(value, &datatype.try_into().ok()?)
                            .ok()?
                            .into_any(),
                    )
                }),
                None => None,
            });
        any_iter_to_array(max_values, datatype).ok()
    }
    fn num_containers(&self) -> usize {
        self.files.len()
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let schema: Schema = self.table.schema().try_into().ok()?;
        let column_id = schema.index_of(&column.name).ok()?;
        let null_counts = self
            .files
            .iter()
            .map(|manifest| match &manifest.null_value_counts() {
                Some(map) => map.get(&(column_id as i32)).copied(),
                None => None,
            });
        ScalarValue::iter_to_array(null_counts.map(ScalarValue::Int64)).ok()
    }
}

fn any_iter_to_array(
    iter: impl Iterator<Item = Option<Box<dyn Any>>>,
    datatype: &DataType,
) -> Result<ArrayRef, DataFusionError> {
    match datatype {
        DataType::Boolean => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Boolean(opt.and_then(|value| Some(*value.downcast::<bool>().ok()?)))
        })),
        DataType::Int32 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Int32(opt.and_then(|value| Some(*value.downcast::<i32>().ok()?)))
        })),
        DataType::Int64 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Int64(opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)))
        })),
        DataType::Float32 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Float32(opt.and_then(|value| Some(*value.downcast::<f32>().ok()?)))
        })),
        DataType::Float64 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Float64(opt.and_then(|value| Some(*value.downcast::<f64>().ok()?)))
        })),
        DataType::Date64 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Date64(opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)))
        })),
        DataType::Time64(_) => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Time32Millisecond(
                opt.and_then(|value| Some(*value.downcast::<i32>().ok()?)),
            )
        })),
        DataType::Timestamp(_, _) => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::TimestampMillisecond(
                opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)),
                None,
            )
        })),
        DataType::Utf8 => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Utf8(opt.and_then(|value| Some(*value.downcast::<String>().ok()?)))
        })),
        DataType::FixedSizeBinary(_) => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Binary(opt.and_then(|value| Some(*value.downcast::<Vec<u8>>().ok()?)))
        })),
        DataType::Binary => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::Binary(opt.and_then(|value| Some(*value.downcast::<Vec<u8>>().ok()?)))
        })),
        _ => Err(DataFusionError::Internal(
            "Arrow datatype not supported for pruning.".to_string(),
        )),
    }
}
