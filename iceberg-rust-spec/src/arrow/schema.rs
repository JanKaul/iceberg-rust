/*!
 * Convert between datafusion and iceberg schema
*/

use std::{collections::HashMap, convert::TryInto, ops::Deref, sync::Arc};

use crate::{
    spec::types::{PrimitiveType, StructField, StructType, Type},
    types::ListType,
};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};

use crate::error::Error;

pub const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

impl TryInto<ArrowSchema> for &StructType {
    type Error = Error;

    fn try_into(self) -> Result<ArrowSchema, Self::Error> {
        let fields = self.try_into()?;
        let metadata = HashMap::new();
        Ok(ArrowSchema { fields, metadata })
    }
}

impl TryInto<Fields> for &StructType {
    type Error = Error;

    fn try_into(self) -> Result<Fields, Self::Error> {
        let fields = self
            .iter()
            .map(|field| {
                Ok(Field::new(
                    &field.name,
                    (&field.field_type).try_into()?,
                    !field.required,
                )
                .with_metadata(HashMap::from_iter(vec![(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    field.id.to_string(),
                )])))
            })
            .collect::<Result<_, Error>>()?;
        Ok(fields)
    }
}

impl TryFrom<&ArrowSchema> for StructType {
    type Error = Error;

    fn try_from(value: &ArrowSchema) -> Result<Self, Self::Error> {
        value.fields().try_into()
    }
}

impl TryFrom<&Fields> for StructType {
    type Error = Error;

    fn try_from(value: &Fields) -> Result<Self, Self::Error> {
        let fields = value
            .iter()
            .map(|field| {
                Ok(StructField {
                    id: get_field_id(field)?,
                    name: field.name().to_owned(),
                    required: !field.is_nullable(),
                    field_type: field.data_type().try_into()?,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                })
            })
            .collect::<Result<_, Error>>()?;
        Ok(StructType::new(fields))
    }
}

impl TryFrom<&Type> for DataType {
    type Error = Error;

    fn try_from(value: &Type) -> Result<Self, Self::Error> {
        match value {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Boolean => Ok(DataType::Boolean),
                PrimitiveType::Int => Ok(DataType::Int32),
                PrimitiveType::Long => Ok(DataType::Int64),
                PrimitiveType::Float => Ok(DataType::Float32),
                PrimitiveType::Double => Ok(DataType::Float64),
                PrimitiveType::Decimal { precision, scale } => {
                    Ok(DataType::Decimal128(*precision as u8, *scale as i8))
                }
                PrimitiveType::Date => Ok(DataType::Date32),
                PrimitiveType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
                PrimitiveType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
                PrimitiveType::Timestamptz => Ok(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(Arc::from("UTC")),
                )),
                PrimitiveType::TimestampNs => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                PrimitiveType::TimestamptzNs => Ok(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("UTC")),
                )),
                PrimitiveType::String => Ok(DataType::Utf8),
                PrimitiveType::Uuid => Ok(DataType::Utf8),
                PrimitiveType::Fixed(len) => Ok(DataType::FixedSizeBinary(*len as i32)),
                PrimitiveType::Binary => Ok(DataType::Binary),
                PrimitiveType::Variant
                | PrimitiveType::Geometry(_)
                | PrimitiveType::Geography(_, _) => Ok(DataType::Binary),
                PrimitiveType::Unknown => Ok(DataType::Null),
            },
            Type::List(list) => Ok(DataType::List(Arc::new(
                Field::new(
                    "item",
                    (&list.element as &Type).try_into()?,
                    !list.element_required,
                )
                .with_metadata(HashMap::from_iter(vec![(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    list.element_id.to_string(),
                )])),
            ))),
            Type::Struct(struc) => Ok(DataType::Struct(struc.try_into()?)),
            Type::Map(map) => Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", (&map.key as &Type).try_into()?, false).with_metadata(
                            HashMap::from_iter(vec![(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                map.key_id.to_string(),
                            )]),
                        ),
                        Field::new(
                            "value",
                            (&map.value as &Type).try_into()?,
                            !map.value_required,
                        )
                        .with_metadata(HashMap::from_iter(vec![(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            map.value_id.to_string(),
                        )])),
                    ])),
                    false,
                )),
                false,
            )),
        }
    }
}

impl TryFrom<&DataType> for Type {
    type Error = Error;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
            DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                Ok(Type::Primitive(PrimitiveType::Int))
            }
            DataType::Int64 => Ok(Type::Primitive(PrimitiveType::Long)),
            DataType::Float32 => Ok(Type::Primitive(PrimitiveType::Float)),
            DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
            DataType::Decimal128(precision, scale) => Ok(Type::Primitive(PrimitiveType::Decimal {
                precision: *precision as u32,
                scale: *scale as u32,
            })),
            DataType::Date32 => Ok(Type::Primitive(PrimitiveType::Date)),
            DataType::Time64(_) => Ok(Type::Primitive(PrimitiveType::Time)),
            DataType::Timestamp(_, None) => Ok(Type::Primitive(PrimitiveType::Timestamp)),
            DataType::Timestamp(_, Some(tz)) if tz.as_ref() == "UTC" || tz.as_ref() == "+00:00" => {
                Ok(Type::Primitive(PrimitiveType::Timestamptz))
            }
            DataType::Timestamp(_, Some(tz)) => Err(Error::NotSupported(format!(
                "Arrow timestamp with non-UTC timezone '{tz}' is not supported."
            ))),
            DataType::Utf8 => Ok(Type::Primitive(PrimitiveType::String)),
            DataType::Utf8View => Ok(Type::Primitive(PrimitiveType::String)),
            DataType::FixedSizeBinary(len) => {
                Ok(Type::Primitive(PrimitiveType::Fixed(*len as u64)))
            }
            DataType::Binary => Ok(Type::Primitive(PrimitiveType::Binary)),
            DataType::Struct(fields) => Ok(Type::Struct(fields.try_into()?)),
            DataType::List(field) => Ok(Type::List(ListType {
                element_id: get_field_id(field)?,
                element_required: !field.is_nullable(),
                element: Box::new(field.data_type().try_into()?),
            })),
            x => Err(Error::NotSupported(format!(
                "Arrow datatype {x} is not supported."
            ))),
        }
    }
}

fn get_field_id(field: &Field) -> Result<i32, Error> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or(Error::NotFound(format!(
            "Parquet field id of field {field}"
        )))
        .and_then(|x| x.parse().map_err(Error::from))
}

pub fn new_fields_with_ids(fields: &Fields, index: &mut i32) -> Fields {
    fields
        .into_iter()
        .map(|field| {
            *index += 1;
            match field.data_type() {
                DataType::Struct(fields) => {
                    let temp = *index;
                    Field::new(
                        field.name(),
                        DataType::Struct(new_fields_with_ids(fields, index)),
                        field.is_nullable(),
                    )
                    .with_metadata(HashMap::from_iter(vec![(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        temp.to_string(),
                    )]))
                }
                DataType::List(list_field) => {
                    let temp = *index;
                    *index += 1;
                    Field::new(
                        field.name(),
                        DataType::List(Arc::new(list_field.deref().clone().with_metadata(
                            HashMap::from_iter(vec![(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                index.to_string(),
                            )]),
                        ))),
                        field.is_nullable(),
                    )
                    .with_metadata(HashMap::from_iter(vec![(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        temp.to_string(),
                    )]))
                }
                _ => field
                    .deref()
                    .clone()
                    .with_metadata(HashMap::from_iter(vec![(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        index.to_string(),
                    )])),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::MapType;

    #[test]
    fn test_struct_type_to_arrow_schema_simple() {
        let struct_type = StructType::new(vec![
            StructField::new(1, "field1", true, Type::Primitive(PrimitiveType::Int), None),
            StructField::new(
                2,
                "field2",
                false,
                Type::Primitive(PrimitiveType::String),
                None,
            ),
        ]);

        let arrow_schema: ArrowSchema = (&struct_type).try_into().unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "field1");
        assert_eq!(get_field_id(arrow_schema.field(0)).unwrap(), 1);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(0).is_nullable());
        assert_eq!(arrow_schema.field(1).name(), "field2");
        assert_eq!(get_field_id(arrow_schema.field(1)).unwrap(), 2);
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(1).is_nullable());
    }

    #[test]
    fn test_struct_type_to_arrow_schema_nested() {
        let nested_struct = StructType::new(vec![
            StructField::new(
                3,
                "nested1",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ),
            StructField::new(
                4,
                "nested2",
                false,
                Type::Primitive(PrimitiveType::Boolean),
                None,
            ),
        ]);

        let struct_type = StructType::new(vec![
            StructField::new(1, "field1", true, Type::Primitive(PrimitiveType::Int), None),
            StructField::new(2, "field2", false, Type::Struct(nested_struct), None),
        ]);

        let arrow_schema: ArrowSchema = (&struct_type).try_into().unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "field1");
        assert_eq!(get_field_id(arrow_schema.field(0)).unwrap(), 1);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(0).is_nullable());

        let nested_field = arrow_schema.field(1);
        assert_eq!(nested_field.name(), "field2");
        assert_eq!(get_field_id(nested_field).unwrap(), 2);
        assert!(nested_field.is_nullable());

        if let DataType::Struct(nested_fields) = nested_field.data_type() {
            assert_eq!(nested_fields.len(), 2);
            assert_eq!(nested_fields[0].name(), "nested1");
            assert_eq!(get_field_id(&nested_fields[0]).unwrap(), 3);
            assert_eq!(nested_fields[0].data_type(), &DataType::Int64);
            assert!(!nested_fields[0].is_nullable());
            assert_eq!(nested_fields[1].name(), "nested2");
            assert_eq!(get_field_id(&nested_fields[1]).unwrap(), 4);
            assert_eq!(nested_fields[1].data_type(), &DataType::Boolean);
            assert!(nested_fields[1].is_nullable());
        } else {
            panic!("Expected nested field to be a struct");
        }
    }

    #[test]
    fn test_struct_type_to_arrow_schema_list() {
        let list_type = Type::List(ListType {
            element_id: 3,
            element_required: false,
            element: Box::new(Type::Primitive(PrimitiveType::Double)),
        });

        let struct_type = StructType::new(vec![
            StructField::new(1, "field1", true, Type::Primitive(PrimitiveType::Int), None),
            StructField::new(2, "field2", false, list_type, None),
        ]);

        let arrow_schema: ArrowSchema = (&struct_type).try_into().unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "field1");
        assert_eq!(get_field_id(arrow_schema.field(0)).unwrap(), 1);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(0).is_nullable());

        let list_field = arrow_schema.field(1);
        assert_eq!(list_field.name(), "field2");
        assert_eq!(get_field_id(list_field).unwrap(), 2);
        assert!(list_field.is_nullable());

        if let DataType::List(element_field) = list_field.data_type() {
            assert_eq!(element_field.data_type(), &DataType::Float64);
            assert_eq!(get_field_id(element_field).unwrap(), 3);
            assert!(element_field.is_nullable());
        } else {
            panic!("Expected list field");
        }
    }

    #[test]
    fn test_struct_type_to_arrow_schema_map() {
        let map_type = Type::Map(MapType {
            key_id: 3,
            value_id: 4,
            value_required: false,
            key: Box::new(Type::Primitive(PrimitiveType::String)),
            value: Box::new(Type::Primitive(PrimitiveType::Int)),
        });

        let struct_type = StructType::new(vec![
            StructField::new(1, "field1", true, Type::Primitive(PrimitiveType::Int), None),
            StructField::new(2, "field2", false, map_type, None),
        ]);

        let arrow_schema: ArrowSchema = (&struct_type).try_into().unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "field1");
        assert_eq!(get_field_id(arrow_schema.field(0)).unwrap(), 1);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(0).is_nullable());

        let map_field = arrow_schema.field(1);
        assert_eq!(map_field.name(), "field2");
        assert_eq!(get_field_id(map_field).unwrap(), 2);
        assert!(map_field.is_nullable());

        if let DataType::Map(entries_field, _) = map_field.data_type() {
            if let DataType::Struct(entry_fields) = entries_field.data_type() {
                assert_eq!(entry_fields.len(), 2);
                assert_eq!(entry_fields[0].name(), "key");
                assert_eq!(get_field_id(&entry_fields[0]).unwrap(), 3);
                assert_eq!(entry_fields[0].data_type(), &DataType::Utf8);
                assert!(!entry_fields[0].is_nullable());
                assert_eq!(entry_fields[1].name(), "value");
                assert_eq!(get_field_id(&entry_fields[1]).unwrap(), 4);
                assert_eq!(entry_fields[1].data_type(), &DataType::Int32);
                assert!(entry_fields[1].is_nullable());
            } else {
                panic!("Expected struct field for map entries");
            }
        } else {
            panic!("Expected map field");
        }
    }

    #[test]
    fn test_struct_type_to_arrow_schema_complex() {
        let nested_struct = StructType::new(vec![
            StructField::new(
                4,
                "nested1",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ),
            StructField::new(
                5,
                "nested2",
                false,
                Type::Primitive(PrimitiveType::Boolean),
                None,
            ),
        ]);

        let list_type = Type::List(ListType {
            element_id: 3,
            element_required: true,
            element: Box::new(Type::Struct(nested_struct)),
        });

        let map_type = Type::Map(MapType {
            key_id: 7,
            value_id: 8,
            value_required: false,
            key: Box::new(Type::Primitive(PrimitiveType::String)),
            value: Box::new(Type::Primitive(PrimitiveType::Date)),
        });

        let struct_type = StructType::new(vec![
            StructField::new(1, "field1", true, Type::Primitive(PrimitiveType::Int), None),
            StructField::new(2, "field2", false, list_type, None),
            StructField::new(6, "field3", true, map_type, None),
        ]);

        let arrow_schema: ArrowSchema = (&struct_type).try_into().unwrap();

        assert_eq!(arrow_schema.fields().len(), 3);
        // Assertions for field1 (simple int)
        assert_eq!(arrow_schema.field(0).name(), "field1");
        assert_eq!(get_field_id(arrow_schema.field(0)).unwrap(), 1);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(0).is_nullable());

        // Assertions for field2 (list of structs)
        let list_field = arrow_schema.field(1);
        assert_eq!(list_field.name(), "field2");
        assert_eq!(get_field_id(list_field).unwrap(), 2);
        assert!(list_field.is_nullable());
        if let DataType::List(element_field) = list_field.data_type() {
            if let DataType::Struct(nested_fields) = element_field.data_type() {
                assert_eq!(nested_fields.len(), 2);
                assert_eq!(nested_fields[0].name(), "nested1");
                assert_eq!(get_field_id(&nested_fields[0]).unwrap(), 4);
                assert_eq!(nested_fields[0].data_type(), &DataType::Int64);
                assert!(!nested_fields[0].is_nullable());
                assert_eq!(nested_fields[1].name(), "nested2");
                assert_eq!(get_field_id(&nested_fields[1]).unwrap(), 5);
                assert_eq!(nested_fields[1].data_type(), &DataType::Boolean);
                assert!(nested_fields[1].is_nullable());
            } else {
                panic!("Expected struct as list element");
            }
        } else {
            panic!("Expected list field");
        }

        // Assertions for field3 (map of string to list of structs)
        let map_field = arrow_schema.field(2);
        assert_eq!(map_field.name(), "field3");
        assert_eq!(get_field_id(map_field).unwrap(), 6);
        assert!(!map_field.is_nullable());
        if let DataType::Map(entries_field, _) = map_field.data_type() {
            if let DataType::Struct(entry_fields) = entries_field.data_type() {
                assert_eq!(entry_fields.len(), 2);
                assert_eq!(entry_fields[0].name(), "key");
                assert_eq!(get_field_id(&entry_fields[0]).unwrap(), 7);
                assert_eq!(entry_fields[0].data_type(), &DataType::Utf8);
                assert!(!entry_fields[0].is_nullable());

                // Check the value (list of structs)
                assert_eq!(entry_fields[1].name(), "value");
                assert_eq!(get_field_id(&entry_fields[1]).unwrap(), 8);
                assert!(entry_fields[1].is_nullable());
            } else {
                panic!("Expected struct field for map entries");
            }
        } else {
            panic!("Expected map field");
        }
    }

    #[test]
    fn test_struct_type_to_arrow_schema_empty() {
        let struct_type = StructType::new(vec![]);
        let arrow_schema: ArrowSchema = (&struct_type).try_into().unwrap();
        assert_eq!(arrow_schema.fields().len(), 0);
    }

    #[test]
    fn test_struct_type_to_arrow_schema_metadata() {
        let struct_type = StructType::new(vec![StructField::new(
            1,
            "field1",
            true,
            Type::Primitive(PrimitiveType::Int),
            None,
        )]);

        let arrow_schema: ArrowSchema = (&struct_type).try_into().unwrap();

        // Check that the PARQUET:field_id metadata is set correctly
        let field_metadata = arrow_schema.field(0).metadata();
        assert_eq!(
            field_metadata.get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );
    }

    use arrow_schema::DataType;
    use std::sync::Arc;

    #[test]
    fn test_arrow_schema_to_struct_type_simple() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("field1", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("field2", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("field3", DataType::Int16, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]);

        let struct_type: StructType = (&arrow_schema).try_into().unwrap();

        assert_eq!(struct_type[0].id, 1);
        assert_eq!(struct_type[0].name, "field1");
        assert!(struct_type[0].required);
        assert_eq!(
            struct_type[0].field_type,
            Type::Primitive(PrimitiveType::Int)
        );
        assert_eq!(struct_type[1].id, 2);
        assert_eq!(struct_type[1].name, "field2");
        assert!(!struct_type[1].required);
        assert_eq!(
            struct_type[1].field_type,
            Type::Primitive(PrimitiveType::String)
        );
        assert_eq!(struct_type[2].id, 3);
        assert_eq!(struct_type[2].name, "field3");
        assert!(!struct_type[2].required);
        assert_eq!(
            struct_type[2].field_type,
            Type::Primitive(PrimitiveType::Int)
        );
    }

    #[test]
    fn test_arrow_schema_to_struct_type_nested() {
        let nested_fields = Fields::from(vec![
            Field::new("nested1", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
            Field::new("nested2", DataType::Boolean, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
        ]);

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("field1", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("field2", DataType::Struct(nested_fields), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            ),
        ]);

        let struct_type: StructType = (&arrow_schema).try_into().unwrap();

        assert_eq!(struct_type[0].id, 1);
        assert_eq!(struct_type[0].name, "field1");
        assert!(struct_type[0].required);
        assert_eq!(
            struct_type[0].field_type,
            Type::Primitive(PrimitiveType::Int)
        );

        match &struct_type[1].field_type {
            Type::Struct(nested_struct) => {
                assert_eq!(nested_struct[0].id, 3);
                assert_eq!(nested_struct[0].name, "nested1");
                assert!(!nested_struct[0].required);
                assert_eq!(
                    nested_struct[0].field_type,
                    Type::Primitive(PrimitiveType::Long)
                );
                assert_eq!(nested_struct[1].id, 4);
                assert_eq!(nested_struct[1].name, "nested2");
                assert!(nested_struct[1].required);
                assert_eq!(
                    nested_struct[1].field_type,
                    Type::Primitive(PrimitiveType::Boolean)
                );
            }
            _ => panic!("Expected nested struct"),
        }
    }

    #[test]
    fn test_arrow_schema_to_struct_type_list() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("field1", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new(
                "field2",
                DataType::List(Arc::new(
                    Field::new("item", DataType::Float64, true).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "3".to_string(),
                    )])),
                )),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]);

        let struct_type: StructType = (&arrow_schema).try_into().unwrap();

        assert_eq!(struct_type[0].id, 1);
        assert_eq!(struct_type[0].name, "field1");
        assert!(struct_type[0].required);
        assert_eq!(
            struct_type[0].field_type,
            Type::Primitive(PrimitiveType::Int)
        );

        match &struct_type[1].field_type {
            Type::List(list_type) => {
                assert_eq!(list_type.element_id, 3);
                assert!(!list_type.element_required);
                assert_eq!(*list_type.element, Type::Primitive(PrimitiveType::Double));
            }
            _ => panic!("Expected list type"),
        }
    }

    // #[test]
    // fn test_arrow_schema_to_struct_type_map() {
    //     let arrow_schema = ArrowSchema::new(vec![
    //         Field::new("field1", DataType::Int32, false).with_metadata(HashMap::from([(
    //             PARQUET_FIELD_ID_META_KEY.to_string(),
    //             "1".to_string(),
    //         )])),
    //         Field::new(
    //             "field2",
    //             DataType::Map(
    //                 Arc::new(Field::new(
    //                     "entries",
    //                     DataType::Struct(Fields::from(vec![
    //                         Field::new("key", DataType::Utf8, false).with_metadata(HashMap::from(
    //                             [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
    //                         )),
    //                         Field::new("value", DataType::Int32, true).with_metadata(
    //                             HashMap::from([(
    //                                 PARQUET_FIELD_ID_META_KEY.to_string(),
    //                                 "4".to_string(),
    //                             )]),
    //                         ),
    //                     ])),
    //                     false,
    //                 )),
    //                 false,
    //             ),
    //             true,
    //         )
    //         .with_metadata(HashMap::from([(
    //             PARQUET_FIELD_ID_META_KEY.to_string(),
    //             "2".to_string(),
    //         )])),
    //     ]);

    //     let struct_type: StructType = (&arrow_schema).try_into().unwrap();

    //     assert_eq!(struct_type[0].id, 1);
    //     assert_eq!(struct_type[0].name, "field1");
    //     assert_eq!(struct_type[0].required, true);
    //     assert_eq!(
    //         struct_type[0].field_type,
    //         Type::Primitive(PrimitiveType::Int)
    //     );

    //     match &struct_type[1].field_type {
    //         Type::Map(map_type) => {
    //             assert_eq!(map_type.key_id, 3);
    //             assert_eq!(map_type.value_id, 4);
    //             assert_eq!(map_type.value_required, false);
    //             assert_eq!(*map_type.key, Type::Primitive(PrimitiveType::String));
    //             assert_eq!(*map_type.value, Type::Primitive(PrimitiveType::Int));
    //         }
    //         _ => panic!("Expected map type"),
    //     }
    // }

    // #[test]
    // fn test_arrow_schema_to_struct_type_complex() {
    //     let nested_fields = Fields::from(vec![
    //         Field::new("nested1", DataType::Int64, true).with_metadata(HashMap::from([(
    //             PARQUET_FIELD_ID_META_KEY.to_string(),
    //             "4".to_string(),
    //         )])),
    //         Field::new("nested2", DataType::Boolean, false).with_metadata(HashMap::from([(
    //             PARQUET_FIELD_ID_META_KEY.to_string(),
    //             "5".to_string(),
    //         )])),
    //     ]);

    //     let arrow_schema = ArrowSchema::new(vec![
    //         Field::new("field1", DataType::Int32, false).with_metadata(HashMap::from([(
    //             PARQUET_FIELD_ID_META_KEY.to_string(),
    //             "1".to_string(),
    //         )])),
    //         Field::new(
    //             "field2",
    //             DataType::List(Arc::new(
    //                 Field::new("item", DataType::Struct(nested_fields), false).with_metadata(
    //                     HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
    //                 ),
    //             )),
    //             true,
    //         )
    //         .with_metadata(HashMap::from([(
    //             PARQUET_FIELD_ID_META_KEY.to_string(),
    //             "2".to_string(),
    //         )])),
    //         Field::new(
    //             "field3",
    //             DataType::Map(
    //                 Arc::new(Field::new(
    //                     "entries",
    //                     DataType::Struct(Fields::from(vec![
    //                         Field::new("key", DataType::Utf8, false).with_metadata(HashMap::from(
    //                             [(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())],
    //                         )),
    //                         Field::new("value", DataType::Date32, true).with_metadata(
    //                             HashMap::from([(
    //                                 PARQUET_FIELD_ID_META_KEY.to_string(),
    //                                 "8".to_string(),
    //                             )]),
    //                         ),
    //                     ])),
    //                     false,
    //                 )),
    //                 false,
    //             ),
    //             false,
    //         )
    //         .with_metadata(HashMap::from([(
    //             PARQUET_FIELD_ID_META_KEY.to_string(),
    //             "6".to_string(),
    //         )])),
    //     ]);

    //     let struct_type: StructType = (&arrow_schema).try_into().unwrap();

    //     // Check field1
    //     assert_eq!(struct_type[0].id, 1);
    //     assert_eq!(struct_type[0].name, "field1");
    //     assert_eq!(struct_type[0].required, true);
    //     assert_eq!(
    //         struct_type[0].field_type,
    //         Type::Primitive(PrimitiveType::Int)
    //     );

    //     // Check field2 (list of structs)
    //     match &struct_type[1].field_type {
    //         Type::List(list_type) => {
    //             assert_eq!(list_type.element_id, 3);
    //             assert_eq!(list_type.element_required, true);
    //             match &*list_type.element {
    //                 Type::Struct(nested_struct) => {
    //                     assert_eq!(nested_struct[0].id, 4);
    //                     assert_eq!(nested_struct[0].name, "nested1");
    //                     assert_eq!(nested_struct[0].required, false);
    //                     assert_eq!(
    //                         nested_struct[0].field_type,
    //                         Type::Primitive(PrimitiveType::Long)
    //                     );
    //                     assert_eq!(nested_struct[1].id, 5);
    //                     assert_eq!(nested_struct[1].name, "nested2");
    //                     assert_eq!(nested_struct[1].required, true);
    //                     assert_eq!(
    //                         nested_struct[1].field_type,
    //                         Type::Primitive(PrimitiveType::Boolean)
    //                     );
    //                 }
    //                 _ => panic!("Expected nested struct in list"),
    //             }
    //         }
    //         _ => panic!("Expected list type"),
    //     }

    //     // Check field3 (map)
    //     match &struct_type[2].field_type {
    //         Type::Map(map_type) => {
    //             assert_eq!(map_type.key_id, 7);
    //             assert_eq!(map_type.value_id, 8);
    //             assert_eq!(map_type.value_required, false);
    //             assert_eq!(*map_type.key, Type::Primitive(PrimitiveType::String));
    //             assert_eq!(*map_type.value, Type::Primitive(PrimitiveType::Date));
    //         }
    //         _ => panic!("Expected map type"),
    //     }
    // }

    #[test]
    fn test_arrow_timestamp_to_iceberg_type() {
        // Timestamp without timezone -> Timestamp
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )]))]);
        let struct_type: StructType = (&arrow_schema).try_into().unwrap();
        assert_eq!(
            struct_type[0].field_type,
            Type::Primitive(PrimitiveType::Timestamp)
        );

        // Timestamp with UTC timezone -> Timestamptz
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "tstz",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )]))]);
        let struct_type: StructType = (&arrow_schema).try_into().unwrap();
        assert_eq!(
            struct_type[0].field_type,
            Type::Primitive(PrimitiveType::Timestamptz)
        );

        // Timestamp with non-UTC timezone -> NotSupported (Iceberg only supports UTC instants)
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "tstz",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+05:30"))),
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )]))]);
        let result: Result<StructType, Error> = (&arrow_schema).try_into();
        assert!(matches!(result.unwrap_err(), Error::NotSupported(_)));
    }

    #[test]
    fn test_arrow_schema_to_struct_type_missing_field_id() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("field1", DataType::Int32, false)]);

        let result: Result<StructType, Error> = (&arrow_schema).try_into();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
    }

    #[test]
    fn test_arrow_schema_to_struct_type_invalid_field_id() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("field1", DataType::Int32, false)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "invalid".to_string(),
            )]))]);

        let result: Result<StructType, Error> = (&arrow_schema).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_arrow_schema_to_struct_type_unsupported_datatype() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("field1", DataType::UInt8, false)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )]))]);

        let result: Result<StructType, Error> = (&arrow_schema).try_into();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotSupported(_)));
    }

    #[test]
    fn test_nested_field_name() {
        let schema = crate::schema::Schema::builder()
            .with_schema_id(1)
            .with_struct_field(StructField::new(
                1,
                "nested_object",
                true,
                Type::Struct(StructType::new(vec![
                    StructField::new(
                        2,
                        "key1",
                        true,
                        Type::Primitive(PrimitiveType::String),
                        None,
                    ),
                    StructField::new(3, "key2", true, Type::Primitive(PrimitiveType::Int), None),
                ])),
                None,
            ))
            .build()
            .unwrap();

        let field_name = schema.get_name("nested_object.key1");
        assert!(field_name.is_some());
    }

    // =====================================================================
    // Cycle L2: TestArrowSchemaUtil port — Java's ArrowSchemaUtil suite
    // tests the bidirectional Iceberg ↔ Arrow type converter. Rust
    // implements `TryFrom<&Type> for DataType` (forward) and
    // `TryFrom<&DataType> for Type` (backward); this port covers the
    // parametrized type-mapping table, divergences from Java/spec, and
    // explicit gaps where the backward direction isn't implemented.
    // =====================================================================

    /// Every Iceberg `PrimitiveType` maps to the documented Arrow
    /// `DataType` in the forward direction.
    #[test]
    fn test_arrow_iceberg_to_arrow_primitive_table_per_java() {
        let cases: Vec<(PrimitiveType, DataType)> = vec![
            (PrimitiveType::Boolean, DataType::Boolean),
            (PrimitiveType::Int, DataType::Int32),
            (PrimitiveType::Long, DataType::Int64),
            (PrimitiveType::Float, DataType::Float32),
            (PrimitiveType::Double, DataType::Float64),
            (PrimitiveType::Date, DataType::Date32),
            (PrimitiveType::Time, DataType::Time64(TimeUnit::Microsecond)),
            (
                PrimitiveType::Timestamp,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                PrimitiveType::Timestamptz,
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            ),
            (
                PrimitiveType::TimestampNs,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                PrimitiveType::TimestamptzNs,
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            ),
            (PrimitiveType::String, DataType::Utf8),
            (PrimitiveType::Uuid, DataType::Utf8),
            (PrimitiveType::Fixed(16), DataType::FixedSizeBinary(16)),
            (PrimitiveType::Binary, DataType::Binary),
            (PrimitiveType::Unknown, DataType::Null),
        ];
        for (iceberg, expected_arrow) in cases {
            let t = Type::Primitive(iceberg.clone());
            let dt: DataType = (&t).try_into().expect("forward conversion must succeed");
            assert_eq!(
                dt, expected_arrow,
                "Iceberg {iceberg:?} must map to Arrow {expected_arrow:?}",
            );
        }
    }

    /// `Decimal{p, s}` preserves precision and scale through the forward
    /// conversion (mapped to `Decimal128`).
    #[test]
    fn test_arrow_iceberg_decimal_preserves_precision_and_scale_per_java() {
        for (p, s) in [(5_u32, 2_u32), (38_u32, 18_u32), (10_u32, 0_u32)] {
            let dt: DataType = (&Type::Primitive(PrimitiveType::Decimal {
                precision: p,
                scale: s,
            }))
                .try_into()
                .unwrap();
            assert_eq!(dt, DataType::Decimal128(p as u8, s as i8));
        }
    }

    /// `Fixed(n)` carries its width through to `FixedSizeBinary(n)`.
    #[test]
    fn test_arrow_iceberg_fixed_round_trip_width_per_java() {
        for n in [1_u64, 16, 64, 1024] {
            let dt: DataType = (&Type::Primitive(PrimitiveType::Fixed(n)))
                .try_into()
                .unwrap();
            assert_eq!(dt, DataType::FixedSizeBinary(n as i32));
            let back: Type = (&dt).try_into().unwrap();
            assert_eq!(back, Type::Primitive(PrimitiveType::Fixed(n)));
        }
    }

    /// Forward + backward conversion preserves round-trippable primitive
    /// types. Excluded: types whose round-trip is intentionally lossy
    /// (UUID → Utf8 → String; TimestampNs → Timestamp(ns, None) →
    /// Timestamp [collapses unit]; Variant/Geometry/Geography → Binary).
    #[test]
    fn test_arrow_iceberg_primitive_round_trip_per_java() {
        let lossless: Vec<PrimitiveType> = vec![
            PrimitiveType::Boolean,
            PrimitiveType::Int,
            PrimitiveType::Long,
            PrimitiveType::Float,
            PrimitiveType::Double,
            PrimitiveType::Decimal {
                precision: 18,
                scale: 6,
            },
            PrimitiveType::Date,
            PrimitiveType::Time,
            PrimitiveType::Timestamp,
            PrimitiveType::Timestamptz,
            PrimitiveType::String,
            PrimitiveType::Fixed(32),
            PrimitiveType::Binary,
        ];
        for p in lossless {
            let iceberg = Type::Primitive(p.clone());
            let arrow: DataType = (&iceberg).try_into().unwrap();
            let back: Type = (&arrow).try_into().unwrap();
            assert_eq!(back, iceberg, "round-trip must preserve {p:?}");
        }
    }

    /// Arrow's `Int8` and `Int16` widen to Iceberg's `Int`. Iceberg has
    /// no sub-32-bit integer type, so the small-int Arrow types collapse
    /// to `Type::Primitive(PrimitiveType::Int)`.
    #[test]
    fn test_arrow_iceberg_int8_and_int16_widen_to_iceberg_int_per_java() {
        for narrow in [DataType::Int8, DataType::Int16] {
            let t: Type = (&narrow).try_into().expect("must convert");
            assert_eq!(
                t,
                Type::Primitive(PrimitiveType::Int),
                "narrow Arrow integer {narrow:?} must widen to Iceberg Int",
            );
        }
    }

    /// Arrow's `Utf8View` is treated as equivalent to `Utf8` and maps to
    /// Iceberg `String`. This makes view-typed Arrow columns
    /// pass-through compatible with the spec.
    #[test]
    fn test_arrow_iceberg_utf8_view_maps_to_string_per_java() {
        let t: Type = (&DataType::Utf8View).try_into().unwrap();
        assert_eq!(t, Type::Primitive(PrimitiveType::String));
    }

    /// Arrow `Timestamp(_, Some("UTC"))` maps to `Timestamptz` regardless
    /// of unit; `Timestamp(_, Some("+00:00"))` also maps to `Timestamptz`.
    #[test]
    fn test_arrow_iceberg_utc_timestamp_becomes_timestamptz_per_java() {
        for tz in ["UTC", "+00:00"] {
            let dt = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from(tz)));
            let t: Type = (&dt).try_into().unwrap();
            assert_eq!(
                t,
                Type::Primitive(PrimitiveType::Timestamptz),
                "Arrow timestamp tz '{tz}' must map to Timestamptz",
            );
        }
    }

    /// A non-UTC timezone (e.g., "America/Los_Angeles") is rejected at
    /// conversion time — Iceberg's spec only models UTC offsets for
    /// timestamptz.
    #[test]
    fn test_arrow_iceberg_non_utc_timezone_returns_error_per_java() {
        let dt = DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("America/Los_Angeles")),
        );
        let err: Result<Type, _> = (&dt).try_into();
        assert!(err.is_err(), "non-UTC timezone must be rejected");
    }

    /// A nested Arrow `Struct` round-trips field-by-field through both
    /// directions of the converter.
    #[test]
    fn test_arrow_iceberg_struct_round_trip_per_java() {
        let inner = StructType::new(vec![
            StructField::new(
                11,
                "lon",
                true,
                Type::Primitive(PrimitiveType::Double),
                None,
            ),
            StructField::new(
                12,
                "lat",
                true,
                Type::Primitive(PrimitiveType::Double),
                None,
            ),
        ]);
        let outer = StructType::new(vec![
            StructField::new(1, "id", true, Type::Primitive(PrimitiveType::Long), None),
            StructField::new(2, "geo", false, Type::Struct(inner.clone()), None),
        ]);

        let arrow_schema: ArrowSchema = (&outer).try_into().unwrap();
        let back: StructType = (&arrow_schema).try_into().unwrap();
        assert_eq!(back, outer);
    }

    /// Field IDs round-trip via the `PARQUET:field_id` metadata key on
    /// every level of nesting.
    #[test]
    fn test_arrow_iceberg_field_id_round_trip_per_java() {
        let inner = StructType::new(vec![StructField::new(
            42,
            "deep",
            true,
            Type::Primitive(PrimitiveType::String),
            None,
        )]);
        let outer = StructType::new(vec![
            StructField::new(7, "top", true, Type::Primitive(PrimitiveType::Int), None),
            StructField::new(9, "nested", false, Type::Struct(inner), None),
        ]);

        let arrow_schema: ArrowSchema = (&outer).try_into().unwrap();
        let f_top = arrow_schema.field(0);
        let f_nested = arrow_schema.field(1);
        assert_eq!(get_field_id(f_top).unwrap(), 7);
        assert_eq!(get_field_id(f_nested).unwrap(), 9);

        if let DataType::Struct(inner_fields) = f_nested.data_type() {
            assert_eq!(get_field_id(&inner_fields[0]).unwrap(), 42);
        } else {
            unreachable!("nested field must be Struct");
        }
    }

    /// Conversion of an Arrow field that lacks a `PARQUET:field_id`
    /// metadata entry must error — the converter relies on the field id
    /// to construct Iceberg `StructField`s.
    #[test]
    fn test_arrow_iceberg_missing_field_id_returns_error_per_java() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("no_id", DataType::Int32, true)]);
        let result: Result<StructType, Error> = (&arrow_schema).try_into();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotFound(_)));
    }

    /// Nullability flips on the round-trip: Iceberg `required = true` ↔
    /// Arrow `nullable = false`, and vice versa.
    #[test]
    fn test_arrow_iceberg_nullability_inverts_per_java() {
        let st = StructType::new(vec![
            StructField::new(1, "req", true, Type::Primitive(PrimitiveType::Int), None),
            StructField::new(
                2,
                "opt",
                false,
                Type::Primitive(PrimitiveType::String),
                None,
            ),
        ]);
        let arrow_schema: ArrowSchema = (&st).try_into().unwrap();
        assert!(!arrow_schema.field(0).is_nullable());
        assert!(arrow_schema.field(1).is_nullable());

        let back: StructType = (&arrow_schema).try_into().unwrap();
        assert!(back.iter().next().unwrap().required);
        assert!(!back.iter().nth(1).unwrap().required);
    }

    // ---------- Divergences pinned as #[ignore] ----------------------------

    /// Java's Arrow→Iceberg converter handles `Map` (decomposing the
    /// `entries: Struct{key, value}` payload back into `MapType`).
    /// Rust's `TryFrom<&DataType> for Type` has no Map arm — the
    /// catch-all returns `Error::NotSupported`. Forward `Map` works;
    /// the backward direction is the gap.
    #[test]
    #[ignore = "feature gap: TryFrom<&DataType> for Type has no Map arm; Arrow Map → Iceberg Map conversion not implemented"]
    fn test_arrow_iceberg_map_backward_conversion_per_java() {}

    /// Java preserves `UUIDType` across round-trip via a distinct Arrow
    /// FixedSizeBinary(16) representation. Rust collapses
    /// `PrimitiveType::Uuid` → `DataType::Utf8`, so the backward
    /// direction recovers `String`, not `Uuid`.
    #[test]
    #[ignore = "divergence: Iceberg UUID maps to Arrow Utf8 (lossy); round-trip recovers String, not UUID"]
    fn test_arrow_iceberg_uuid_round_trip_preserves_uuid_per_java() {}

    /// Java preserves nanosecond timestamp precision through round-trip.
    /// Rust's backward conversion matches `Timestamp(_, None)` →
    /// `Timestamp` (microseconds), so `TimestampNs` → `Timestamp(ns, None)`
    /// → `Timestamp` collapses the unit.
    #[test]
    #[ignore = "divergence: Arrow Timestamp(Nanosecond, _) → Iceberg loses the Ns suffix; backward arm matches `_` for any unit"]
    fn test_arrow_iceberg_timestamp_nanosecond_round_trip_preserves_unit_per_java() {}

    /// Java preserves `Variant`, `Geometry`, `Geography` types through a
    /// round-trip via dedicated Arrow extension types. Rust collapses all
    /// three to `DataType::Binary`, so the backward conversion recovers
    /// `Binary`, not the original.
    #[test]
    #[ignore = "divergence: Variant/Geometry/Geography → Arrow Binary (lossy forward); backward direction recovers Binary, not the original type"]
    fn test_arrow_iceberg_variant_round_trip_preserves_type_per_java() {}

    /// Java handles Arrow `Date64` as an equivalent of `Date32` for
    /// Iceberg `Date`. Rust's backward conversion only has a `Date32`
    /// arm — `Date64` falls into the catch-all and errors.
    #[test]
    #[ignore = "feature gap: Arrow Date64 not handled in backward conversion; only Date32 maps to Iceberg Date"]
    fn test_arrow_iceberg_date64_maps_to_iceberg_date_per_java() {}

    /// Java supports `LargeUtf8` (i64-offset UTF-8 strings) as equivalent
    /// to `Utf8`. Rust only has a `Utf8` and `Utf8View` arm in the
    /// backward direction, so `LargeUtf8` errors.
    #[test]
    #[ignore = "feature gap: Arrow LargeUtf8 not handled in backward conversion; only Utf8 + Utf8View map to Iceberg String"]
    fn test_arrow_iceberg_large_utf8_maps_to_string_per_java() {}
}
