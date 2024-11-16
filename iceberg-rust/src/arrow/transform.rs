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
#[cfg(test)]
mod tests {

    use super::*;
    use arrow::array::{ArrayRef, Date32Array, TimestampMicrosecondArray};

    fn create_date32_array() -> ArrayRef {
        Arc::new(Date32Array::from(vec![
            Some(19478), // 2023-05-01
            Some(19523), // 2023-06-15
            Some(19723), // 2024-01-01
            None,
        ])) as ArrayRef
    }

    fn create_timestamp_micro_array() -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1682937000000000),
            Some(1686840330000000),
            Some(1704067200000000),
            None,
        ])) as ArrayRef
    }

    #[test]
    fn test_identity_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array.clone(), &Transform::Identity).unwrap();
        assert_eq!(&array, &result);
    }

    #[test]
    fn test_date32_day_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array, &Transform::Day).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(1),
            Some(15),
            Some(1),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_date32_month_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array, &Transform::Month).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(5),
            Some(6),
            Some(1),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_date32_year_transform() {
        let array = create_date32_array();
        let result = transform_arrow(array, &Transform::Year).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(2023),
            Some(2023),
            Some(2024),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_hour_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, &Transform::Hour).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(10),
            Some(14),
            Some(0),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_day_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, &Transform::Day).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(1),
            Some(15),
            Some(1),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_month_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, &Transform::Month).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(5),
            Some(6),
            Some(1),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_timestamp_micro_year_transform() {
        let array = create_timestamp_micro_array();
        let result = transform_arrow(array, &Transform::Year).unwrap();
        let expected = Arc::new(arrow::array::Int32Array::from(vec![
            Some(2023),
            Some(2023),
            Some(2024),
            None,
        ])) as ArrayRef;
        assert_eq!(&expected, &result);
    }

    #[test]
    fn test_unsupported_transform() {
        let array = Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let result = transform_arrow(array, &Transform::Day);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Compute error: Failed to perform transform for datatype"
        );
    }
}
