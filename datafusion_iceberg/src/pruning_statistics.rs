/*!
 * Implement pruning statistics for Datafusion table
 *
 * Pruning is done on two levels:
 *
 * 1. Prune manifests based on information in manifests lists
 * 2. Prune data files based on information in manifests
 *
 * For the first level the trait [`PruningStatistics`] is implemented for the DataFusionTable. It returns the pruning information for the manifest files
 * and not the final data files.
 *
 * For the second level the trait PruningStatistics is implemented for the Manifest
*/

use std::{any::Any, sync::Arc};

use crate::error::Error as DatafusionIcebergError;
use datafusion::{
    arrow::{
        array::ArrayRef,
        datatypes::{DataType, Schema as ArrowSchema},
    },
    common::{
        tree_node::{Transformed, TreeNode},
        DataFusionError,
    },
    physical_optimizer::pruning::PruningStatistics,
    prelude::Column,
    scalar::ScalarValue,
};
use datafusion_expr::{
    expr::ScalarFunction, BinaryExpr, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use iceberg_rust::{
    arrow::transform::transform_arrow,
    error::Error,
    spec::{
        manifest::ManifestEntry,
        manifest_list::ManifestListEntry,
        partition::{BoundPartitionField, Transform},
        schema::Schema,
        values::Value,
    },
    table::ManifestPath,
};

pub(crate) struct PruneManifests<'table, 'manifests> {
    partition_fields: &'table [BoundPartitionField<'table>],
    files: &'manifests [ManifestListEntry],
}

impl<'table, 'manifests> PruneManifests<'table, 'manifests> {
    pub(crate) fn new(
        partition_fields: &'table [BoundPartitionField<'table>],
        files: &'manifests [ManifestListEntry],
    ) -> Self {
        Self {
            partition_fields,
            files,
        }
    }
}

impl PruningStatistics for PruneManifests<'_, '_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let (index, partition_field) = self
            .partition_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == column.name())?;
        let data_type = partition_field
            .field_type()
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
        let (index, partition_field) = self
            .partition_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == column.name())?;
        let data_type = partition_field
            .field_type()
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
        let (index, _) = self
            .partition_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.source_name() == column.name())?;
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
    files: &'manifests [(ManifestPath, ManifestEntry)],
}

impl<'table, 'manifests> PruneDataFiles<'table, 'manifests> {
    pub(crate) fn new(
        schema: &'table Schema,
        arrow_schema: &'table ArrowSchema,
        files: &'manifests [(ManifestPath, ManifestEntry)],
    ) -> Self {
        Self {
            schema,
            arrow_schema,
            files,
        }
    }
}

impl PruningStatistics for PruneDataFiles<'_, '_> {
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
                .map(|manifest| match &manifest.1.data_file().lower_bounds() {
                    Some(map) => map
                        .get(&{ column_id })
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
                .map(|manifest| match &manifest.1.data_file().upper_bounds() {
                    Some(map) => map
                        .get(&{ column_id })
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
            self.files.iter().map(
                |manifest| match &manifest.1.data_file().null_value_counts() {
                    Some(map) => map.get(&{ column_id }).copied(),
                    None => None,
                },
            );
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
                .map(|manifest| match &manifest.1.data_file().value_counts() {
                    Some(map) => map.get(&{ column_id }).copied(),
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

pub(crate) fn transform_predicate(
    expr: Expr,
    partition_fields: &[BoundPartitionField],
) -> Result<Expr, DataFusionError> {
    expr.transform_down(|expr| match expr {
        Expr::BinaryExpr(bin) => match (*bin.left, *bin.right) {
            (Expr::Column(column), right) => {
                let field = partition_fields
                    .iter()
                    .find(|x| x.source_name() == column.name())
                    .unwrap();
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::Column(Column::new(
                        column.relation,
                        field.name().to_owned(),
                    ))),
                    bin.op,
                    Box::new(transform_literal(right, field.transform())?),
                ))))
            }
            (left, Expr::Column(column)) => {
                let field = partition_fields
                    .iter()
                    .find(|x| x.source_name() == column.name())
                    .unwrap();
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::Column(Column::new(
                        column.relation,
                        field.name().to_owned(),
                    ))),
                    bin.op,
                    Box::new(transform_literal(left, field.transform())?),
                ))))
            }
            (left, right) => Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                bin.op,
                Box::new(right),
            )))),
        },
        x => Ok(Transformed::no(x)),
    })
    .map(|x| x.data)
}

fn transform_literal(expr: Expr, transform: &Transform) -> Result<Expr, DataFusionError> {
    match transform {
        Transform::Year => Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(DateTransform::new())),
            vec![Expr::Literal(ScalarValue::new_utf8("year"), None), expr],
        ))),
        Transform::Month => Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(DateTransform::new())),
            vec![Expr::Literal(ScalarValue::new_utf8("month"), None), expr],
        ))),
        Transform::Day => Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(DateTransform::new())),
            vec![Expr::Literal(ScalarValue::new_utf8("day"), None), expr],
        ))),
        Transform::Hour => Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(DateTransform::new())),
            vec![Expr::Literal(ScalarValue::new_utf8("hour"), None), expr],
        ))),
        _ => Ok(expr),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DateTransform {
    signature: Signature,
}

impl DateTransform {
    fn new() -> Self {
        let signature = Signature {
            type_signature: TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Date32]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                ]),
            ]),
            volatility: Volatility::Immutable,
            parameter_names: None,
        };
        Self { signature }
    }
}

impl ScalarUDFImpl for DateTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_transform"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        let transform = &args[0];
        let array = &args[1];
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(transform))) = transform else {
            return Err(DataFusionError::External(Box::new(Error::InvalidFormat(
                "Partition transform".to_owned(),
            ))));
        };
        let transform = match transform.as_str() {
            "year" => Ok(Transform::Year),
            "month" => Ok(Transform::Month),
            "day" => Ok(Transform::Day),
            "hour" => Ok(Transform::Hour),
            _ => Err(DataFusionError::External(Box::new(Error::InvalidFormat(
                "Partition transform".to_owned(),
            )))),
        }?;
        match array {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(transform_arrow(
                array.clone(),
                &transform,
            )?)),
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                value_to_scalarvalue(
                    scalarvalue_to_value(scalar)
                        .map_err(DatafusionIcebergError::from)?
                        .transform(&transform)
                        .map_err(DatafusionIcebergError::from)?,
                )
                .map_err(DatafusionIcebergError::from)?,
            )),
        }
    }
}

fn scalarvalue_to_value(scalar: &ScalarValue) -> Result<Value, Error> {
    match scalar {
        ScalarValue::Boolean(x) => Ok(Value::Boolean(x.ok_or(Error::InvalidFormat(
            "Value can't be null when converting to iceberg value".to_owned(),
        ))?)),
        ScalarValue::Int32(x) => Ok(Value::Int(x.ok_or(Error::InvalidFormat(
            "Value can't be null when converting to iceberg value".to_owned(),
        ))?)),
        ScalarValue::Int64(x) => Ok(Value::LongInt(x.ok_or(Error::InvalidFormat(
            "Value can't be null when converting to iceberg value".to_owned(),
        ))?)),
        ScalarValue::Date32(x) => Ok(Value::Date(x.ok_or(Error::InvalidFormat(
            "Value can't be null when converting to iceberg value".to_owned(),
        ))?)),
        ScalarValue::Time64Microsecond(x) => Ok(Value::Time(x.ok_or(Error::InvalidFormat(
            "Value can't be null when converting to iceberg value".to_owned(),
        ))?)),
        ScalarValue::TimestampMicrosecond(x, _) => Ok(Value::Timestamp(x.ok_or(
            Error::InvalidFormat("Value can't be null when converting to iceberg value".to_owned()),
        )?)),
        x => Err(Error::NotSupported(format!(
            "Transforming {x} to iceberg value"
        ))),
    }
}

fn value_to_scalarvalue(value: Value) -> Result<ScalarValue, Error> {
    match value {
        Value::Boolean(x) => Ok(ScalarValue::Boolean(Some(x))),
        Value::Int(x) => Ok(ScalarValue::Int32(Some(x))),
        Value::LongInt(x) => Ok(ScalarValue::Int64(Some(x))),
        Value::Date(x) => Ok(ScalarValue::Date32(Some(x))),
        Value::Time(x) => Ok(ScalarValue::Time64Microsecond(Some(x))),
        Value::Timestamp(x) => Ok(ScalarValue::TimestampMicrosecond(Some(x), None)),
        x => Err(Error::NotSupported(format!(
            "Transforming {x} to iceberg value"
        ))),
    }
}
