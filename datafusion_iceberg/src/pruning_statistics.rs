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
        datatypes::{DataType, Schema as ArrowSchema},
    },
    common::DataFusionError,
    physical_optimizer::pruning::PruningStatistics,
    prelude::Column,
    scalar::ScalarValue,
};
use iceberg_rust::spec::{
    manifest::ManifestEntry, manifest_list::ManifestListEntry, partition::PartitionSpec,
    schema::Schema,
};

pub(crate) struct PruneManifests<'table, 'manifests> {
    schema: &'table Schema,
    partition_spec: &'table PartitionSpec,
    files: &'manifests [ManifestListEntry],
}

impl<'table, 'manifests> PruneManifests<'table, 'manifests> {
    pub(crate) fn new(
        schema: &'table Schema,
        partition_spec: &'table PartitionSpec,
        files: &'manifests [ManifestListEntry],
    ) -> Self {
        Self {
            schema,
            partition_spec,
            files,
        }
    }
}

impl<'table, 'manifests> PruningStatistics for PruneManifests<'table, 'manifests> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column = self.schema.fields().get_name(&column.name)?;
        let (index, partition_field) = self
            .partition_spec
            .fields()
            .iter()
            .enumerate()
            .find(|(_, partition_field)| partition_field.source_id() == &column.id)?;
        let data_type = column
            .field_type
            .tranform(partition_field.transform())
            .ok()?;
        let min_values = self.files.iter().filter_map(|manifest| {
            manifest.partitions.as_ref().and_then(|partitions| {
                partitions[index]
                    .lower_bound
                    .as_ref()
                    .map(|min| Some(min.clone().into_any()))
            })
        });
        any_iter_to_array(min_values, &(&data_type).try_into().ok()?).ok()
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column = self.schema.fields().get_name(&column.name)?;
        let (index, partition_field) = self
            .partition_spec
            .fields()
            .iter()
            .enumerate()
            .find(|(_, partition_field)| partition_field.source_id() == &column.id)?;
        let data_type = column
            .field_type
            .tranform(partition_field.transform())
            .ok()?;
        let max_values = self.files.iter().filter_map(|manifest| {
            manifest.partitions.as_ref().and_then(|partitions| {
                partitions[index]
                    .upper_bound
                    .as_ref()
                    .map(|max| Some(max.clone().into_any()))
            })
        });
        any_iter_to_array(max_values, &(&data_type).try_into().ok()?).ok()
    }
    fn num_containers(&self) -> usize {
        self.files.len()
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column = self.schema.fields().get_name(&column.name)?;
        let (index, _) = self
            .partition_spec
            .fields()
            .iter()
            .enumerate()
            .find(|(_, partition_field)| partition_field.source_id() == &column.id)?;
        let contains_null = self.files.iter().filter_map(|manifest| {
            manifest.partitions.as_ref().map(|partitions| {
                if !partitions[index].contains_null {
                    Some(0)
                } else {
                    None
                }
            })
        });
        ScalarValue::iter_to_array(contains_null.map(ScalarValue::Int32)).ok()
    }
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<datafusion::arrow::array::BooleanArray> {
        None
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        ScalarValue::iter_to_array(
            self.files
                .iter()
                .map(|x| x.added_rows_count)
                .map(ScalarValue::Int64),
        )
        .ok()
    }
}

pub(crate) struct PruneDataFiles<'table, 'manifests> {
    schema: &'table Schema,
    arrow_schema: &'table ArrowSchema,
    files: &'manifests [ManifestEntry],
}

impl<'table, 'manifests> PruneDataFiles<'table, 'manifests> {
    pub(crate) fn new(
        schema: &'table Schema,
        arrow_schema: &'table ArrowSchema,
        files: &'manifests [ManifestEntry],
    ) -> Self {
        Self {
            schema,
            arrow_schema,
            files,
        }
    }
}

impl<'table, 'manifests> PruningStatistics for PruneDataFiles<'table, 'manifests> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.schema.fields().get_name(&column.name)?.id;
        let datatype = self
            .arrow_schema
            .field_with_name(&column.name)
            .ok()?
            .data_type();
        let min_values =
            self.files
                .iter()
                .map(|manifest| match &manifest.data_file().lower_bounds() {
                    Some(map) => map
                        .get(&(column_id as i32))
                        .map(|value| value.clone().into_any()),
                    None => None,
                });
        any_iter_to_array(min_values, datatype).ok()
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.schema.fields().get_name(&column.name)?.id;
        let datatype = self
            .arrow_schema
            .field_with_name(&column.name)
            .ok()?
            .data_type();
        let max_values =
            self.files
                .iter()
                .map(|manifest| match &manifest.data_file().upper_bounds() {
                    Some(map) => map
                        .get(&(column_id as i32))
                        .map(|value| value.clone().into_any()),
                    None => None,
                });
        any_iter_to_array(max_values, datatype).ok()
    }
    fn num_containers(&self) -> usize {
        self.files.len()
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.schema.fields().get_name(&column.name)?.id;
        let null_counts =
            self.files
                .iter()
                .map(|manifest| match &manifest.data_file().null_value_counts() {
                    Some(map) => map.get(&(column_id as i32)).copied(),
                    None => None,
                });
        ScalarValue::iter_to_array(null_counts.map(ScalarValue::Int64)).ok()
    }
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<datafusion::arrow::array::BooleanArray> {
        None
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.schema.fields().get_name(&column.name)?.id;
        let null_counts =
            self.files
                .iter()
                .map(|manifest| match &manifest.data_file().value_counts() {
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
            ScalarValue::Time64Microsecond(
                opt.and_then(|value| Some(*value.downcast::<i64>().ok()?)),
            )
        })),
        DataType::Timestamp(_, _) => ScalarValue::iter_to_array(iter.map(|opt| {
            ScalarValue::TimestampMicrosecond(
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
