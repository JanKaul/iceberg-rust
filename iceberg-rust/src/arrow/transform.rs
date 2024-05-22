/*!
 * Functions to perform iceberg transforms to arrow record batches
*/

use std::sync::Arc;

use arrow::{
    array::{as_primitive_array, Array, ArrayRef},
    compute::{date_part, DatePart},
    datatypes::{DataType, Date32Type, TimeUnit, TimestampMicrosecondType},
    error::ArrowError,
};

use iceberg_rust_spec::spec::partition::Transform;

/// Perform iceberg transform on arrow array
pub fn transform_arrow(array: ArrayRef, transform: &Transform) -> Result<ArrayRef, ArrowError> {
    match (array.data_type(), transform) {
        (_, Transform::Identity) => Ok(array),
        (DataType::Date32, Transform::Day) => Ok(Arc::new(date_part(
            as_primitive_array::<Date32Type>(&array),
            DatePart::Day,
        )?) as Arc<dyn Array>),
        (DataType::Date32, Transform::Month) => Ok(Arc::new(date_part(
            as_primitive_array::<Date32Type>(&array),
            DatePart::Month,
        )?) as Arc<dyn Array>),
        (DataType::Date32, Transform::Year) => Ok(Arc::new(date_part(
            as_primitive_array::<Date32Type>(&array),
            DatePart::Year,
        )?) as Arc<dyn Array>),
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Hour) => {
            Ok(Arc::new(date_part(
                as_primitive_array::<TimestampMicrosecondType>(&array),
                DatePart::Hour,
            )?) as Arc<dyn Array>)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Day) => {
            Ok(Arc::new(date_part(
                as_primitive_array::<TimestampMicrosecondType>(&array),
                DatePart::Day,
            )?) as Arc<dyn Array>)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Month) => {
            Ok(Arc::new(date_part(
                as_primitive_array::<TimestampMicrosecondType>(&array),
                DatePart::Month,
            )?) as Arc<dyn Array>)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), Transform::Year) => {
            Ok(Arc::new(date_part(
                as_primitive_array::<TimestampMicrosecondType>(&array),
                DatePart::Year,
            )?) as Arc<dyn Array>)
        }
        _ => Err(ArrowError::ComputeError(
            "Failed to perform transform for datatype".to_string(),
        )),
    }
}
