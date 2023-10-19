/*!
 * Value in iceberg
 */

use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    fmt,
    io::Cursor,
    ops::Deref,
};

use anyhow::{anyhow, Result};

use chrono::{NaiveDate, NaiveDateTime};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize,
};
use serde_bytes::ByteBuf;

use super::{
    partition::{PartitionField, Transform},
    types::{PrimitiveType, StructType, Type},
};

/// Values present in iceberg type
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum Value {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    LongInt(i64),
    /// Stored as 4-byte little-endian
    Float(OrderedFloat<f32>),
    /// Stored as 8-byte little-endian
    Double(OrderedFloat<f64>),
    /// Stores days from the 1970-01-01 in an 4-byte little-endian int
    Date(i32),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(i64),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    Timestamp(i64),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    TimestampTZ(i64),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID([u8; 16]),
    /// Binary value
    Fixed(usize, Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as two’s-complement big-endian binary,
    /// using the minimum number of bytes for the value
    Decimal(Decimal),
    /// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
    /// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
    /// Fields may have an optional comment or doc string. Fields can have default values.
    Struct(Struct),
    /// A list is a collection of values with some element type.
    /// The element field has an integer id that is unique in the table schema.
    /// Elements can be either optional or required. Element types may be any type.
    List(Vec<Option<Value>>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    /// Both the key field and value field each have an integer id that is unique in the table schema.
    /// Map keys are required and map values can be either optional or required. Both map keys and map values may be any type, including nested types.
    Map(BTreeMap<String, Option<Value>>),
}

impl From<Value> for ByteBuf {
    fn from(value: Value) -> Self {
        match value {
            Value::Boolean(val) => {
                if val {
                    ByteBuf::from([1u8])
                } else {
                    ByteBuf::from([0u8])
                }
            }
            Value::Int(val) => ByteBuf::from(val.to_le_bytes()),
            Value::LongInt(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Float(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Double(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Date(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Time(val) => ByteBuf::from(val.to_le_bytes()),
            Value::Timestamp(val) => ByteBuf::from(val.to_le_bytes()),
            Value::TimestampTZ(val) => ByteBuf::from(val.to_le_bytes()),
            Value::String(val) => ByteBuf::from(val.as_bytes()),
            Value::UUID(val) => ByteBuf::from(val),
            Value::Fixed(_, val) => ByteBuf::from(val),
            Value::Binary(val) => ByteBuf::from(val),
            _ => todo!(),
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Struct {
    /// Vector to store the field values
    pub fields: Vec<Option<Value>>,
    /// A lookup that matches the field name to the entry in the vector
    pub lookup: BTreeMap<String, usize>,
}

impl Deref for Struct {
    type Target = [Option<Value>];

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl Struct {
    /// Get reference to partition value
    pub fn get(&self, name: &str) -> Option<&Option<Value>> {
        self.fields.get(*self.lookup.get(name)?)
    }
    /// Get mutable reference to partition value
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Option<Value>> {
        self.fields.get_mut(*self.lookup.get(name)?)
    }

    pub(crate) fn cast(
        self,
        schema: &StructType,
        partition_spec: &[PartitionField],
    ) -> Result<Self, anyhow::Error> {
        let map = partition_spec
            .iter()
            .map(|partition_field| {
                let field = schema
                    .get(partition_field.source_id as usize)
                    .ok_or(anyhow!("Partition field doesn't exist in schema"))?;
                Ok((
                    field.name.clone(),
                    field.field_type.tranform(&partition_field.transform)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Struct::from_iter(
            self.fields
                .into_iter()
                .enumerate()
                .map(|(idx, field)| {
                    let name = self
                        .lookup
                        .iter()
                        .find(|(_, v)| **v == idx)
                        .ok_or(anyhow!("Index not in lookup."))?
                        .0;
                    let datatype = map
                        .get(name)
                        .ok_or(anyhow!("Can't find partition field by name"))?;
                    let value = field.map(|value| value.cast(datatype)).transpose()?;
                    Ok((name.clone(), value))
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?,
        ))
    }
}

impl FromIterator<(String, Option<Value>)> for Struct {
    fn from_iter<I: IntoIterator<Item = (String, Option<Value>)>>(iter: I) -> Self {
        let mut fields = Vec::new();
        let mut lookup = BTreeMap::new();

        for (i, (key, value)) in iter.into_iter().enumerate() {
            fields.push(value);
            lookup.insert(key, i);
        }

        Struct { fields, lookup }
    }
}

impl Serialize for Struct {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut record = serializer.serialize_struct("r102", self.fields.len())?;
        for (i, value) in self.fields.iter().enumerate() {
            let (key, _) = self.lookup.iter().find(|(_, value)| **value == i).unwrap();
            record.serialize_field(Box::leak(key.clone().into_boxed_str()), value)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for Struct {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PartitionStructVisitor;

        impl<'de> Visitor<'de> for PartitionStructVisitor {
            type Value = Struct;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("map")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Struct, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut fields: Vec<Option<Value>> = Vec::new();
                let mut lookup: BTreeMap<String, usize> = BTreeMap::new();
                let mut index = 0;
                while let Some(key) = map.next_key()? {
                    fields.push(map.next_value()?);
                    lookup.insert(key, index);
                    index += 1;
                }
                Ok(Struct { fields, lookup })
            }
        }
        deserializer.deserialize_struct(
            "r102",
            Box::leak(vec![].into_boxed_slice()),
            PartitionStructVisitor,
        )
    }
}

impl Value {
    /// Perform a partition transformation for the given value
    pub fn tranform(&self, transform: &Transform) -> Result<Value> {
        match transform {
            Transform::Identity => Ok(self.clone()),
            Transform::Bucket(n) => {
                let mut bytes = Cursor::new(<Value as Into<ByteBuf>>::into(self.clone()));
                let hash = murmur3::murmur3_32(&mut bytes, 0).unwrap();
                Ok(Value::Int((hash % n) as i32))
            }
            Transform::Truncate(w) => match self {
                Value::Int(i) => Ok(Value::Int(i - i.rem_euclid(*w as i32))),
                Value::LongInt(i) => Ok(Value::LongInt(i - i.rem_euclid(*w as i64))),
                Value::String(s) => {
                    let mut s = s.clone();
                    s.truncate(*w as usize);
                    Ok(Value::String(s))
                }
                _ => Err(anyhow!(
                    "Datatype is not supported for truncate partition transform."
                )),
            },
            Transform::Year => match self {
                Value::Date(date) => Ok(Value::Int(*date / 365)),
                Value::Timestamp(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_days()
                        / 364) as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_days()
                        / 364) as i32,
                )),
                _ => Err(anyhow!(
                    "Datatype is not supported for year partition transform."
                )),
            },
            Transform::Month => match self {
                Value::Date(date) => Ok(Value::Int(*date / 30)),
                Value::Timestamp(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_weeks()) as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_weeks()) as i32,
                )),
                _ => Err(anyhow!(
                    "Datatype is not supported for month partition transform."
                )),
            },
            Transform::Day => match self {
                Value::Date(date) => Ok(Value::Int(*date)),
                Value::Timestamp(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_days()) as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_days()) as i32,
                )),
                _ => Err(anyhow!(
                    "Datatype is not supported for day partition transform."
                )),
            },
            Transform::Hour => match self {
                Value::Timestamp(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_hours()) as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    (NaiveDateTime::from_timestamp_millis(time / 1000)
                        .unwrap()
                        .signed_duration_since(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .num_hours()) as i32,
                )),
                _ => Err(anyhow!(
                    "Datatype is not supported for hour partition transform."
                )),
            },
            _ => Err(anyhow!(
                "Partition transform operation currently not supported."
            )),
        }
    }

    #[inline]
    /// Create iceberg value from bytes
    pub fn try_from_bytes(bytes: &[u8], data_type: &Type) -> Result<Self, anyhow::Error> {
        match data_type {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Boolean => {
                    if bytes.len() == 1 && bytes[0] == 0u8 {
                        Ok(Value::Boolean(false))
                    } else {
                        Ok(Value::Boolean(true))
                    }
                }
                PrimitiveType::Int => Ok(Value::Int(i32::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Long => Ok(Value::LongInt(i64::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Float => Ok(Value::Float(OrderedFloat(f32::from_le_bytes(
                    bytes.try_into()?,
                )))),
                PrimitiveType::Double => Ok(Value::Double(OrderedFloat(f64::from_le_bytes(
                    bytes.try_into()?,
                )))),
                PrimitiveType::Date => Ok(Value::Date(i32::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Time => Ok(Value::Time(i64::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Timestamp => {
                    Ok(Value::Timestamp(i64::from_le_bytes(bytes.try_into()?)))
                }
                PrimitiveType::Timestampz => {
                    Ok(Value::TimestampTZ(i64::from_le_bytes(bytes.try_into()?)))
                }
                PrimitiveType::String => Ok(Value::String(std::str::from_utf8(bytes)?.to_string())),
                PrimitiveType::Uuid => Ok(Value::UUID(bytes.try_into()?)),
                PrimitiveType::Fixed(len) => Ok(Value::Fixed(*len as usize, Vec::from(bytes))),
                PrimitiveType::Binary => Ok(Value::Binary(Vec::from(bytes))),
                _ => Err(anyhow!("Decimal cannot be stored as bytes.")),
            },
            _ => Err(anyhow!("Only primitive types can be stored as bytes.")),
        }
    }

    /// Get datatype of value
    pub fn datatype(&self) -> Type {
        match self {
            Value::Boolean(_) => Type::Primitive(PrimitiveType::Boolean),
            Value::Int(_) => Type::Primitive(PrimitiveType::Int),
            Value::LongInt(_) => Type::Primitive(PrimitiveType::Long),
            Value::Float(_) => Type::Primitive(PrimitiveType::Float),
            Value::Double(_) => Type::Primitive(PrimitiveType::Double),
            Value::Date(_) => Type::Primitive(PrimitiveType::Date),
            Value::Time(_) => Type::Primitive(PrimitiveType::Time),
            Value::Timestamp(_) => Type::Primitive(PrimitiveType::Timestamp),
            Value::TimestampTZ(_) => Type::Primitive(PrimitiveType::Timestampz),
            Value::Fixed(len, _) => Type::Primitive(PrimitiveType::Fixed(*len as u64)),
            Value::Binary(_) => Type::Primitive(PrimitiveType::Binary),
            Value::String(_) => Type::Primitive(PrimitiveType::String),
            Value::UUID(_) => Type::Primitive(PrimitiveType::Uuid),
            Value::Decimal(dec) => Type::Primitive(PrimitiveType::Decimal {
                precision: 38,
                scale: dec.scale(),
            }),
            _ => unimplemented!(),
        }
    }

    /// Convert Value to the any type
    pub fn into_any(self) -> Box<dyn Any> {
        match self {
            Value::Boolean(any) => Box::new(any),
            Value::Int(any) => Box::new(any),
            Value::LongInt(any) => Box::new(any),
            Value::Float(any) => Box::new(any),
            Value::Double(any) => Box::new(any),
            Value::Date(any) => Box::new(any),
            Value::Time(any) => Box::new(any),
            Value::Timestamp(any) => Box::new(any),
            Value::TimestampTZ(any) => Box::new(any),
            Value::Fixed(_, any) => Box::new(any),
            Value::Binary(any) => Box::new(any),
            Value::String(any) => Box::new(any),
            Value::UUID(any) => Box::new(any),
            Value::Decimal(any) => Box::new(any),
            _ => unimplemented!(),
        }
    }
    /// Cast value to different type
    pub fn cast(self, data_type: &Type) -> Result<Self, anyhow::Error> {
        dbg!(&self, data_type);
        if self.datatype() == *data_type {
            Ok(self)
        } else {
            match (self, data_type) {
                (Value::Int(input), Type::Primitive(PrimitiveType::Long)) => {
                    Ok(Value::LongInt(input as i64))
                }
                (Value::Int(input), Type::Primitive(PrimitiveType::Date)) => Ok(Value::Date(input)),
                (Value::LongInt(input), Type::Primitive(PrimitiveType::Time)) => {
                    Ok(Value::Time(input))
                }
                (Value::LongInt(input), Type::Primitive(PrimitiveType::Timestamp)) => {
                    Ok(Value::Timestamp(input))
                }
                (Value::LongInt(input), Type::Primitive(PrimitiveType::Timestampz)) => {
                    Ok(Value::TimestampTZ(input))
                }
                _ => Err(anyhow!("Cast is not supported")),
            }
        }
    }
}
