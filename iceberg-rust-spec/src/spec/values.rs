/*!
 * Value types and operations for Iceberg data
 *
 * This module implements the runtime value system for Iceberg, including:
 * - Primitive values (boolean, numeric, string, binary, etc.)
 * - Complex values (structs, lists, maps)
 * - Value transformations for partitioning
 * - Serialization/deserialization to/from various formats
 * - Value comparison and manipulation operations
 *
 * The value system provides:
 * - Type-safe data representation
 * - Efficient value storage and access
 * - Support for partition transforms
 * - JSON/binary format conversions
 */

use core::panic;
use std::{
    any::Any,
    collections::{btree_map::Keys, BTreeMap, HashMap},
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    io::Cursor,
    ops::Sub,
    slice::Iter,
};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use datetime::{
    date_to_months, date_to_years, datetime_to_days, datetime_to_hours, datetime_to_months,
    days_to_date, micros_to_datetime,
};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize,
};
use serde_bytes::ByteBuf;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use uuid::Uuid;

use crate::error::Error;

use super::{
    partition::{PartitionField, Transform},
    types::{PrimitiveType, StructType, Type},
};

pub static YEARS_BEFORE_UNIX_EPOCH: i32 = 1970;

/// Values present in iceberg type
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    UUID(Uuid),
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
    Map(BTreeMap<Value, Option<Value>>),
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
            Value::UUID(val) => ByteBuf::from(val.as_u128().to_be_bytes()),
            Value::Fixed(_, val) => ByteBuf::from(val),
            Value::Binary(val) => ByteBuf::from(val),
            Value::Decimal(val) => {
                // rust_decimal mantissa is 96 bits
                // so we can remove the first 32 bits of the i128 representation
                let bytes = val.mantissa().to_be_bytes()[4..].to_vec();
                ByteBuf::from(bytes)
            }
            _ => todo!(),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::LongInt(l) => write!(f, "{l}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::Double(d) => write!(f, "{d}"),
            Value::Date(d) => write!(f, "{d}"),
            Value::Time(t) => write!(f, "{t}"),
            Value::Timestamp(ts) => write!(f, "{ts}"),
            Value::TimestampTZ(ts) => write!(f, "{ts}"),
            Value::String(s) => write!(f, "{s}"),
            Value::UUID(u) => write!(f, "{u}"),
            Value::Fixed(size, data) => write!(f, "{data:?} ({size} bytes)"),
            Value::Binary(data) => write!(f, "{:?} ({} bytes)", data, data.len()),
            Value::Decimal(d) => write!(f, "{d}"),
            _ => panic!("Printing of compound types is not supported"),
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, Eq, PartialOrd, Ord)]
pub struct Struct {
    /// Vector to store the field values
    pub fields: Vec<Option<Value>>,
    /// A lookup that matches the field name to the entry in the vector
    pub lookup: BTreeMap<String, usize>,
}

impl Struct {
    /// Gets a reference to the value associated with the given field name
    ///
    /// # Arguments
    /// * `name` - The name of the field to retrieve
    ///
    /// # Returns
    /// * `Some(&Option<Value>)` if the field exists
    /// * `None` if the field doesn't exist
    pub fn get(&self, name: &str) -> Option<&Option<Value>> {
        self.fields.get(*self.lookup.get(name)?)
    }
    /// Gets a mutable reference to the value associated with the given field name
    ///
    /// # Arguments
    /// * `name` - The name of the field to retrieve
    ///
    /// # Returns
    /// * `Some(&mut Option<Value>)` if the field exists
    /// * `None` if the field doesn't exist
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Option<Value>> {
        self.fields.get_mut(*self.lookup.get(name)?)
    }

    /// Returns an iterator over all field values in this struct
    ///
    /// # Returns
    /// * An iterator yielding references to each optional Value in order
    pub fn iter(&self) -> Iter<'_, Option<Value>> {
        self.fields.iter()
    }

    /// Returns an iterator over all field names in this struct
    ///
    /// # Returns
    /// * An iterator yielding references to each field name in sorted order
    pub fn keys(&self) -> Keys<'_, String, usize> {
        self.lookup.keys()
    }

    /// Casts the struct's values according to a schema and partition specification
    ///
    /// # Arguments
    /// * `schema` - The StructType defining the expected types
    /// * `partition_spec` - The partition fields specification
    ///
    /// # Returns
    /// * `Ok(Struct)` - A new Struct with values cast to match the schema and partition spec
    /// * `Err(Error)` - If casting fails or schema references are invalid
    ///
    /// This method transforms the struct's values based on the partition specification,
    /// applying any necessary type conversions to match the target schema.
    pub(crate) fn cast(
        self,
        schema: &StructType,
        partition_spec: &[PartitionField],
    ) -> Result<Self, Error> {
        if self.fields.is_empty() {
            return Ok(self);
        }
        // Returns a HashMap mapping partition field names to transformed types.
        let map = partition_spec
            .iter()
            .map(|partition_field| {
                let field = schema.get(*partition_field.source_id() as usize).ok_or(
                    Error::InvalidFormat(format!(
                        "partition spec references unknown column id {}",
                        partition_field.source_id()
                    )),
                )?;

                Ok((
                    partition_field.name().clone(),
                    field.field_type.tranform(partition_field.transform())?,
                ))
            })
            .collect::<Result<HashMap<_, _>, Error>>()?;
        Ok(Struct::from_iter(
            self.fields
                .into_iter()
                .enumerate()
                .map(|(idx, field)| {
                    // Get name of the column
                    let name = self
                        .lookup
                        .iter()
                        .find(|(_, v)| **v == idx)
                        .ok_or(Error::InvalidFormat("partition struct".to_string()))?
                        .0;

                    // Get datatype after tranform
                    let datatype = map
                        .get(name)
                        .ok_or(Error::InvalidFormat("partition_struct".to_string()))?;
                    // Cast the value to the datatype
                    let value = field.map(|value| value.cast(datatype)).transpose()?;
                    Ok((name.clone(), value))
                })
                .collect::<Result<Vec<_>, Error>>()?,
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

impl PartialEq for Struct {
    fn eq(&self, other: &Self) -> bool {
        self.keys().all(|key| self.get(key).eq(&other.get(key)))
    }
}

impl Hash for Struct {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for key in self.keys().sorted() {
            key.hash(state);
            self.get(key).hash(state);
        }
    }
}

impl Value {
    /// Applies a partition transform to the value
    ///
    /// # Arguments
    /// * `transform` - The partition transform to apply
    ///
    /// # Returns
    /// * `Ok(Value)` - The transformed value
    /// * `Err(Error)` - If the transform cannot be applied to this value type
    ///
    /// Supported transforms include:
    /// * Identity - Returns the value unchanged
    /// * Bucket - Applies a hash function and returns bucket number
    /// * Truncate - Truncates numbers or strings
    /// * Year/Month/Day/Hour - Extracts time components from dates and timestamps
    pub fn transform(&self, transform: &Transform) -> Result<Value, Error> {
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
                _ => Err(Error::NotSupported(
                    "Datatype for truncate partition transform.".to_string(),
                )),
            },
            Transform::Year => match self {
                Value::Date(date) => Ok(Value::Int(date_to_years(&days_to_date(*date)))),
                Value::Timestamp(time) => Ok(Value::Int(
                    micros_to_datetime(*time).year() - YEARS_BEFORE_UNIX_EPOCH,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(
                    micros_to_datetime(*time).year() - YEARS_BEFORE_UNIX_EPOCH,
                )),
                _ => Err(Error::NotSupported(
                    "Datatype for year partition transform.".to_string(),
                )),
            },
            Transform::Month => match self {
                Value::Date(date) => Ok(Value::Int(date_to_months(&days_to_date(*date)))),
                Value::Timestamp(time) => {
                    Ok(Value::Int(datetime_to_months(&micros_to_datetime(*time))))
                }
                Value::TimestampTZ(time) => {
                    Ok(Value::Int(datetime_to_months(&micros_to_datetime(*time))))
                }
                _ => Err(Error::NotSupported(
                    "Datatype for month partition transform.".to_string(),
                )),
            },
            Transform::Day => match self {
                Value::Date(date) => Ok(Value::Int(*date)),
                Value::Timestamp(time) => Ok(Value::Int(
                    datetime_to_days(&micros_to_datetime(*time)) as i32,
                )),
                Value::TimestampTZ(time) => Ok(Value::Int(datetime_to_days(&micros_to_datetime(
                    *time,
                )) as i32)),
                _ => Err(Error::NotSupported(
                    "Datatype for day partition transform.".to_string(),
                )),
            },
            Transform::Hour => match self {
                Value::Timestamp(time) => Ok(Value::Int(datetime_to_hours(&micros_to_datetime(
                    *time,
                )) as i32)),
                Value::TimestampTZ(time) => Ok(Value::Int(datetime_to_hours(&micros_to_datetime(
                    *time,
                )) as i32)),
                _ => Err(Error::NotSupported(
                    "Datatype for hour partition transform.".to_string(),
                )),
            },
            _ => Err(Error::NotSupported(
                "Partition transform operation".to_string(),
            )),
        }
    }

    /// Attempts to create a Value from raw bytes according to a specified type
    ///
    /// # Arguments
    /// * `bytes` - The raw byte slice to parse
    /// * `data_type` - The expected type of the value
    ///
    /// # Returns
    /// * `Ok(Value)` - Successfully parsed value of the specified type
    /// * `Err(Error)` - If the bytes cannot be parsed as the specified type
    ///
    /// # Note
    /// Currently only supports primitive types. Complex types like structs, lists,
    /// and maps are not supported and will return an error.
    #[inline]
    pub fn try_from_bytes(bytes: &[u8], data_type: &Type) -> Result<Self, Error> {
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
                PrimitiveType::Timestamptz => {
                    Ok(Value::TimestampTZ(i64::from_le_bytes(bytes.try_into()?)))
                }
                PrimitiveType::String => Ok(Value::String(std::str::from_utf8(bytes)?.to_string())),
                PrimitiveType::Uuid => Ok(Value::UUID(Uuid::from_u128(u128::from_be_bytes(
                    bytes.try_into()?,
                )))),
                PrimitiveType::Fixed(len) => Ok(Value::Fixed(*len as usize, Vec::from(bytes))),
                PrimitiveType::Binary => Ok(Value::Binary(Vec::from(bytes))),
                PrimitiveType::Decimal { scale, .. } => {
                    let val = if bytes.len() <= 16 {
                        i128::from_be_bytes(sign_extend_be(bytes))
                    } else {
                        return Err(Error::Type("decimal".to_string(), "bytes".to_string()));
                    };
                    Ok(Value::Decimal(Decimal::from_i128_with_scale(val, *scale)))
                }
                PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs
                | PrimitiveType::Unknown
                | PrimitiveType::Variant
                | PrimitiveType::Geometry(_)
                | PrimitiveType::Geography(_, _) => Err(Error::NotSupported(format!(
                    "Value::try_from_bytes for {primitive}"
                ))),
            },
            _ => Err(Error::NotSupported("Complex types as bytes".to_string())),
        }
    }

    /// Attempts to create a Value from a JSON value according to a specified type
    ///
    /// # Arguments
    /// * `value` - The JSON value to parse
    /// * `data_type` - The expected Iceberg type
    ///
    /// # Returns
    /// * `Ok(Some(Value))` - Successfully parsed value of the specified type
    /// * `Ok(None)` - If the JSON value is null
    /// * `Err(Error)` - If the JSON value cannot be parsed as the specified type
    ///
    /// # Note
    /// Handles all primitive types as well as complex types like structs, lists and maps.
    /// For complex types, recursively parses their contents according to their type specifications.
    pub fn try_from_json(value: JsonValue, data_type: &Type) -> Result<Option<Self>, Error> {
        match data_type {
            Type::Primitive(primitive) => match (primitive, value) {
                (PrimitiveType::Boolean, JsonValue::Bool(bool)) => Ok(Some(Value::Boolean(bool))),
                (PrimitiveType::Int, JsonValue::Number(number)) => Ok(Some(Value::Int(
                    number
                        .as_i64()
                        .ok_or(Error::Conversion(
                            "json number".to_string(),
                            "int".to_string(),
                        ))?
                        .try_into()?,
                ))),
                (PrimitiveType::Long, JsonValue::Number(number)) => {
                    Ok(Some(Value::LongInt(number.as_i64().ok_or(
                        Error::Conversion("json number".to_string(), "long".to_string()),
                    )?)))
                }
                (PrimitiveType::Float, JsonValue::Number(number)) => Ok(Some(Value::Float(
                    OrderedFloat(number.as_f64().ok_or(Error::Conversion(
                        "json number".to_string(),
                        "float".to_string(),
                    ))? as f32),
                ))),
                (PrimitiveType::Double, JsonValue::Number(number)) => {
                    Ok(Some(Value::Double(OrderedFloat(number.as_f64().ok_or(
                        Error::Conversion("json number".to_string(), "double".to_string()),
                    )?))))
                }
                (PrimitiveType::Date, JsonValue::String(s)) => Ok(Some(Value::Date(
                    datetime::date_to_days(&NaiveDate::parse_from_str(&s, "%Y-%m-%d")?),
                ))),
                (PrimitiveType::Time, JsonValue::String(s)) => Ok(Some(Value::Time(
                    datetime::time_to_microseconds(&NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")?),
                ))),
                (PrimitiveType::Timestamp, JsonValue::String(s)) => {
                    Ok(Some(Value::Timestamp(datetime::datetime_to_micros(
                        &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")?,
                    ))))
                }
                (PrimitiveType::Timestamptz, JsonValue::String(s)) => Ok(Some(Value::TimestampTZ(
                    datetime::datetimetz_to_micros(&Utc.from_utc_datetime(
                        &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f+00:00")?,
                    )),
                ))),
                (PrimitiveType::String, JsonValue::String(s)) => Ok(Some(Value::String(s))),
                (PrimitiveType::Uuid, JsonValue::String(s)) => {
                    Ok(Some(Value::UUID(Uuid::parse_str(&s)?)))
                }
                (PrimitiveType::Fixed(_), JsonValue::String(_)) => todo!(),
                (PrimitiveType::Binary, JsonValue::String(_)) => todo!(),
                (
                    PrimitiveType::Decimal {
                        precision: _,
                        scale: _,
                    },
                    JsonValue::String(_),
                ) => todo!(),
                (_, JsonValue::Null) => Ok(None),
                (i, j) => Err(Error::Type(i.to_string(), j.to_string())),
            },
            Type::Struct(schema) => {
                if let JsonValue::Object(mut object) = value {
                    Ok(Some(Value::Struct(Struct::from_iter(schema.iter().map(
                        |field| {
                            (
                                field.name.clone(),
                                object.remove(&field.name).and_then(|value| {
                                    Value::try_from_json(value, &field.field_type)
                                        .and_then(|value| {
                                            value.ok_or(Error::InvalidFormat(
                                                "key of map".to_string(),
                                            ))
                                        })
                                        .ok()
                                }),
                            )
                        },
                    )))))
                } else {
                    Err(Error::Type(
                        "json for a struct".to_string(),
                        "object".to_string(),
                    ))
                }
            }
            Type::List(list) => {
                if let JsonValue::Array(array) = value {
                    Ok(Some(Value::List(
                        array
                            .into_iter()
                            .map(|value| Value::try_from_json(value, &list.element))
                            .collect::<Result<Vec<_>, Error>>()?,
                    )))
                } else {
                    Err(Error::Type(
                        "json for a list".to_string(),
                        "array".to_string(),
                    ))
                }
            }
            Type::Map(map) => {
                if let JsonValue::Object(mut object) = value {
                    if let (Some(JsonValue::Array(keys)), Some(JsonValue::Array(values))) =
                        (object.remove("keys"), object.remove("values"))
                    {
                        Ok(Some(Value::Map(BTreeMap::from_iter(
                            keys.into_iter()
                                .zip(values)
                                .map(|(key, value)| {
                                    Ok((
                                        Value::try_from_json(key, &map.key).and_then(|value| {
                                            value.ok_or(Error::InvalidFormat(
                                                "key of map".to_string(),
                                            ))
                                        })?,
                                        Value::try_from_json(value, &map.value)?,
                                    ))
                                })
                                .collect::<Result<Vec<_>, Error>>()?,
                        ))))
                    } else {
                        Err(Error::Type(
                            "json for a list".to_string(),
                            "array".to_string(),
                        ))
                    }
                } else {
                    Err(Error::Type(
                        "json for a list".to_string(),
                        "array".to_string(),
                    ))
                }
            }
        }
    }

    /// Returns the Iceberg Type that corresponds to this Value
    ///
    /// # Returns
    /// * The Type (primitive or complex) that matches this Value's variant
    ///
    /// # Note
    /// Currently only implemented for primitive types. Complex types like
    /// structs, lists, and maps will cause a panic.
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
            Value::TimestampTZ(_) => Type::Primitive(PrimitiveType::Timestamptz),
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

    /// Converts this Value into a boxed Any trait object
    ///
    /// # Returns
    /// * `Box<dyn Any>` containing the underlying value
    ///
    /// # Note
    /// Currently only implemented for primitive types. Complex types like
    /// structs, lists, and maps will panic with unimplemented!()
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

    /// Attempts to cast this Value to a different Type
    ///
    /// # Arguments
    /// * `data_type` - The target Type to cast to
    ///
    /// # Returns
    /// * `Ok(Value)` - Successfully cast Value of the target type
    /// * `Err(Error)` - If the value cannot be cast to the target type
    ///
    /// # Note
    /// Currently supports casting between numeric types (Int -> Long, Int -> Date, etc)
    /// and temporal types (Long -> Time/Timestamp/TimestampTZ).
    /// Returns the original value if the target type matches the current type.
    pub fn cast(self, data_type: &Type) -> Result<Self, Error> {
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
                (Value::LongInt(input), Type::Primitive(PrimitiveType::Timestamptz)) => {
                    Ok(Value::TimestampTZ(input))
                }
                _ => Err(Error::NotSupported("cast".to_string())),
            }
        }
    }
}

/// Performs big endian sign extension
/// Copied from arrow-rs repo/parquet crate:
/// https://github.com/apache/arrow-rs/blob/b25c441745602c9967b1e3cc4a28bc469cfb1311/parquet/src/arrow/buffer/bit_util.rs#L54
pub fn sign_extend_be<const N: usize>(b: &[u8]) -> [u8; N] {
    assert!(b.len() <= N, "Array too large, expected less than {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}

impl From<&Value> for JsonValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Boolean(val) => JsonValue::Bool(*val),
            Value::Int(val) => JsonValue::Number((*val).into()),
            Value::LongInt(val) => JsonValue::Number((*val).into()),
            Value::Float(val) => match Number::from_f64(val.0 as f64) {
                Some(number) => JsonValue::Number(number),
                None => JsonValue::Null,
            },
            Value::Double(val) => match Number::from_f64(val.0) {
                Some(number) => JsonValue::Number(number),
                None => JsonValue::Null,
            },
            Value::Date(val) => JsonValue::String(datetime::days_to_date(*val).to_string()),
            Value::Time(val) => JsonValue::String(datetime::micros_to_time(*val).to_string()),
            Value::Timestamp(val) => JsonValue::String(
                datetime::micros_to_datetime(*val)
                    .format("%Y-%m-%dT%H:%M:%S%.f")
                    .to_string(),
            ),
            Value::TimestampTZ(val) => JsonValue::String(
                datetime::micros_to_datetimetz(*val)
                    .format("%Y-%m-%dT%H:%M:%S%.f+00:00")
                    .to_string(),
            ),
            Value::String(val) => JsonValue::String(val.clone()),
            Value::UUID(val) => JsonValue::String(val.to_string()),
            Value::Fixed(_, val) => {
                JsonValue::String(val.iter().fold(String::new(), |mut acc, x| {
                    acc.push_str(&format!("{x:x}"));
                    acc
                }))
            }
            Value::Binary(val) => {
                JsonValue::String(val.iter().fold(String::new(), |mut acc, x| {
                    acc.push_str(&format!("{x:x}"));
                    acc
                }))
            }
            Value::Decimal(_) => todo!(),

            Value::Struct(s) => JsonValue::Object(JsonMap::from_iter(
                s.lookup
                    .iter()
                    .map(|(k, v)| (k, &s.fields[*v]))
                    .map(|(id, value)| {
                        let json: JsonValue = match value {
                            Some(val) => val.into(),
                            None => JsonValue::Null,
                        };
                        (id.to_string(), json)
                    }),
            )),
            Value::List(list) => JsonValue::Array(
                list.iter()
                    .map(|opt| match opt {
                        Some(literal) => literal.into(),
                        None => JsonValue::Null,
                    })
                    .collect(),
            ),
            Value::Map(map) => {
                let mut object = JsonMap::with_capacity(2);
                object.insert(
                    "keys".to_string(),
                    JsonValue::Array(map.keys().map(|literal| literal.into()).collect()),
                );
                object.insert(
                    "values".to_string(),
                    JsonValue::Array(
                        map.values()
                            .map(|literal| match literal {
                                Some(literal) => literal.into(),
                                None => JsonValue::Null,
                            })
                            .collect(),
                    ),
                );
                JsonValue::Object(object)
            }
        }
    }
}

mod datetime {
    #[inline]
    pub(crate) fn date_to_years(date: &NaiveDate) -> i32 {
        date.years_since(
            // This is always the same and shouldn't fail
            NaiveDate::from_ymd_opt(YEARS_BEFORE_UNIX_EPOCH, 1, 1).unwrap(),
        )
        .unwrap() as i32
    }

    #[inline]
    pub(crate) fn date_to_months(date: &NaiveDate) -> i32 {
        let years = date
            .years_since(
                // This is always the same and shouldn't fail
                NaiveDate::from_ymd_opt(YEARS_BEFORE_UNIX_EPOCH, 1, 1).unwrap(),
            )
            .unwrap() as i32;
        let months = date.month();
        years * 12 + months as i32
    }

    #[inline]
    pub(crate) fn datetime_to_months(date: &NaiveDateTime) -> i32 {
        let years = date.year() - YEARS_BEFORE_UNIX_EPOCH;
        let months = date.month();
        years * 12 + months as i32
    }

    #[inline]
    pub(crate) fn date_to_days(date: &NaiveDate) -> i32 {
        date.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        )
        .num_days() as i32
    }

    #[inline]
    pub(crate) fn days_to_date(days: i32) -> NaiveDate {
        // This shouldn't fail until the year 262000
        DateTime::from_timestamp(days as i64 * 86_400, 0)
            .unwrap()
            .naive_utc()
            .date()
    }

    #[inline]
    pub(crate) fn time_to_microseconds(time: &NaiveTime) -> i64 {
        time.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
        )
        .num_microseconds()
        .unwrap()
    }

    #[inline]
    pub(crate) fn micros_to_time(micros: i64) -> NaiveTime {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, rem as u32 * 1_000).unwrap()
    }

    #[inline]
    pub(crate) fn datetime_to_micros(time: &NaiveDateTime) -> i64 {
        time.and_utc().timestamp_micros()
    }

    #[inline]
    pub(crate) fn micros_to_datetime(time: i64) -> NaiveDateTime {
        DateTime::from_timestamp_micros(time).unwrap().naive_utc()
    }

    #[inline]
    pub(crate) fn datetime_to_days(time: &NaiveDateTime) -> i64 {
        time.signed_duration_since(
            // This is always the same and shouldn't fail
            DateTime::from_timestamp_micros(0).unwrap().naive_utc(),
        )
        .num_days()
    }

    #[inline]
    pub(crate) fn datetime_to_hours(time: &NaiveDateTime) -> i64 {
        time.signed_duration_since(
            // This is always the same and shouldn't fail
            DateTime::from_timestamp_micros(0).unwrap().naive_utc(),
        )
        .num_hours()
    }

    use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

    use super::YEARS_BEFORE_UNIX_EPOCH;

    #[inline]
    pub(crate) fn datetimetz_to_micros(time: &DateTime<Utc>) -> i64 {
        time.timestamp_micros()
    }

    #[inline]
    pub(crate) fn micros_to_datetimetz(micros: i64) -> DateTime<Utc> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        Utc.from_utc_datetime(
            // This shouldn't fail until the year 262000
            &DateTime::from_timestamp(secs, rem as u32 * 1_000)
                .unwrap()
                .naive_utc(),
        )
    }
}

/// A trait for types that support fallible subtraction
///
/// This trait is similar to the standard `Sub` trait, but returns a `Result`
/// to handle cases where subtraction might fail.
///
/// # Type Parameters
/// * `Sized` - Required to ensure the type can be used by value
pub trait TrySub: Sized {
    fn try_sub(&self, other: &Self) -> Result<Self, Error>;
}

impl<T: Sub<Output = T> + Copy> TrySub for T {
    fn try_sub(&self, other: &Self) -> Result<Self, Error> {
        Ok(*self - *other)
    }
}

impl TrySub for Value {
    fn try_sub(&self, other: &Self) -> Result<Self, Error> {
        match (self, other) {
            (Value::Int(own), Value::Int(other)) => Ok(Value::Int(own - other)),
            (Value::LongInt(own), Value::LongInt(other)) => Ok(Value::LongInt(own - other)),
            (Value::Float(own), Value::Float(other)) => Ok(Value::Float(*own - *other)),
            (Value::Double(own), Value::Double(other)) => Ok(Value::Double(*own - *other)),
            (Value::Date(own), Value::Date(other)) => Ok(Value::Date(own - other)),
            (Value::Time(own), Value::Time(other)) => Ok(Value::Time(own - other)),
            (Value::Timestamp(own), Value::Timestamp(other)) => Ok(Value::Timestamp(own - other)),
            (Value::TimestampTZ(own), Value::TimestampTZ(other)) => {
                Ok(Value::TimestampTZ(own - other))
            }
            (Value::String(own), Value::String(other)) => {
                Ok(Value::LongInt(sub_string(own, other) as i64))
            }
            (Value::UUID(own), Value::UUID(other)) => {
                let (own1, own2, own3, own4) = own.to_fields_le();
                let (other1, other2, other3, other4) = other.to_fields_le();
                let mut sub4 = [0; 8];
                for i in 0..own4.len() {
                    sub4[i] = own4[i] - other4[i];
                }
                Ok(Value::UUID(Uuid::from_fields_le(
                    own1 - other1,
                    own2 - other2,
                    own3 - other3,
                    &sub4,
                )))
            }
            (Value::Fixed(own_size, own), Value::Fixed(other_size, other)) => Ok(Value::Fixed(
                if own_size <= other_size {
                    *own_size
                } else if own_size > other_size {
                    *other_size
                } else {
                    panic!("Size must be either smaller, equal or larger");
                },
                own.iter()
                    .zip(other.iter())
                    .map(|(own, other)| own - other)
                    .collect(),
            )),
            (x, y) => Err(Error::Type(
                x.datatype().to_string(),
                y.datatype().to_string(),
            )),
        }
    }
}

/// Calculates a numeric distance between two strings
///
/// # Arguments
/// * `left` - First string to compare
/// * `right` - Second string to compare
///
/// # Returns
/// * For strings that can be converted to base-36 numbers, returns sum of squared differences
/// * For other strings, returns difference of their hash values
///
/// First attempts to compare up to 256 characters as base-36 numbers.
/// Falls back to hash-based comparison if conversion fails.
fn sub_string(left: &str, right: &str) -> u64 {
    if let Some(distance) = left
        .chars()
        .zip(right.chars())
        .take(256)
        .skip_while(|(l, r)| l == r)
        .try_fold(0, |acc, (l, r)| {
            if let (Some(l), Some(r)) = (l.to_digit(36), r.to_digit(36)) {
                Some(acc + l.abs_diff(r).pow(2))
            } else {
                None
            }
        })
    {
        distance as u64
    } else {
        let mut hasher = DefaultHasher::new();
        hasher.write(left.as_bytes());
        let left = hasher.finish();
        hasher.write(right.as_bytes());
        let right = hasher.finish();
        left.abs_diff(right)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        spec::types::{ListType, MapType, StructType},
        types::StructField,
    };

    fn check_json_serde(json: &str, expected_literal: Value, expected_type: &Type) {
        let raw_json_value = serde_json::from_str::<JsonValue>(json).unwrap();
        let desered_literal = Value::try_from_json(raw_json_value.clone(), expected_type).unwrap();
        assert_eq!(desered_literal, Some(expected_literal.clone()));

        let expected_json_value: JsonValue = (&expected_literal).into();
        let sered_json = serde_json::to_string(&expected_json_value).unwrap();
        let parsed_json_value = serde_json::from_str::<JsonValue>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, raw_json_value);
    }

    fn check_avro_bytes_serde(input: Vec<u8>, expected_literal: Value, expected_type: &Type) {
        let raw_schema = r#""bytes""#;
        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let bytes = ByteBuf::from(input);
        let literal = Value::try_from_bytes(&bytes, expected_type).unwrap();
        assert_eq!(literal, expected_literal);

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer.append_ser(bytes).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<ByteBuf>(&record.unwrap()).unwrap();
            let desered_literal = Value::try_from_bytes(&result, expected_type).unwrap();
            assert_eq!(desered_literal, expected_literal);
        }
    }

    #[test]
    fn json_boolean() {
        let record = r#"true"#;

        check_json_serde(
            record,
            Value::Boolean(true),
            &Type::Primitive(PrimitiveType::Boolean),
        );
    }

    #[test]
    fn json_int() {
        let record = r#"32"#;

        check_json_serde(record, Value::Int(32), &Type::Primitive(PrimitiveType::Int));
    }

    #[test]
    fn json_long() {
        let record = r#"32"#;

        check_json_serde(
            record,
            Value::LongInt(32),
            &Type::Primitive(PrimitiveType::Long),
        );
    }

    #[test]
    fn json_float() {
        let record = r#"1.0"#;

        check_json_serde(
            record,
            Value::Float(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Float),
        );
    }

    #[test]
    fn json_double() {
        let record = r#"1.0"#;

        check_json_serde(
            record,
            Value::Double(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Double),
        );
    }

    #[test]
    fn json_date() {
        let record = r#""2017-11-16""#;

        check_json_serde(
            record,
            Value::Date(17486),
            &Type::Primitive(PrimitiveType::Date),
        );
    }

    #[test]
    fn json_time() {
        let record = r#""22:31:08.123456""#;

        check_json_serde(
            record,
            Value::Time(81068123456),
            &Type::Primitive(PrimitiveType::Time),
        );
    }

    #[test]
    fn json_timestamp() {
        let record = r#""2017-11-16T22:31:08.123456""#;

        check_json_serde(
            record,
            Value::Timestamp(1510871468123456),
            &Type::Primitive(PrimitiveType::Timestamp),
        );
    }

    #[test]
    fn json_timestamptz() {
        let record = r#""2017-11-16T22:31:08.123456+00:00""#;

        check_json_serde(
            record,
            Value::TimestampTZ(1510871468123456),
            &Type::Primitive(PrimitiveType::Timestamptz),
        );
    }

    #[test]
    fn json_string() {
        let record = r#""iceberg""#;

        check_json_serde(
            record,
            Value::String("iceberg".to_string()),
            &Type::Primitive(PrimitiveType::String),
        );
    }

    #[test]
    fn json_uuid() {
        let record = r#""f79c3e09-677c-4bbd-a479-3f349cb785e7""#;

        check_json_serde(
            record,
            Value::UUID(Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap()),
            &Type::Primitive(PrimitiveType::Uuid),
        );
    }

    #[test]
    fn json_struct() {
        let record = r#"{"id": 1, "name": "bar", "address": null}"#;

        check_json_serde(
            record,
            Value::Struct(Struct::from_iter(vec![
                ("id".to_string(), Some(Value::Int(1))),
                ("name".to_string(), Some(Value::String("bar".to_string()))),
                ("address".to_string(), None),
            ])),
            &Type::Struct(StructType::new(vec![
                StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 2,
                    name: "name".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 3,
                    name: "address".to_string(),
                    required: false,
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ])),
        );
    }

    #[test]
    fn json_list() {
        let record = r#"[1, 2, 3, null]"#;

        check_json_serde(
            record,
            Value::List(vec![
                Some(Value::Int(1)),
                Some(Value::Int(2)),
                Some(Value::Int(3)),
                None,
            ]),
            &Type::List(ListType {
                element_id: 0,
                element_required: true,
                element: Box::new(Type::Primitive(PrimitiveType::Int)),
            }),
        );
    }

    #[test]
    fn json_map() {
        let record = r#"{ "keys": ["a", "b", "c"], "values": [1, 2, null] }"#;

        check_json_serde(
            record,
            Value::Map(BTreeMap::from([
                (Value::String("a".to_string()), Some(Value::Int(1))),
                (Value::String("b".to_string()), Some(Value::Int(2))),
                (Value::String("c".to_string()), None),
            ])),
            &Type::Map(MapType {
                key_id: 0,
                key: Box::new(Type::Primitive(PrimitiveType::String)),
                value_id: 1,
                value: Box::new(Type::Primitive(PrimitiveType::Int)),
                value_required: true,
            }),
        );
    }

    #[test]
    fn avro_bytes_boolean() {
        let bytes = vec![1u8];

        check_avro_bytes_serde(
            bytes,
            Value::Boolean(true),
            &Type::Primitive(PrimitiveType::Boolean),
        );
    }

    #[test]
    fn avro_bytes_int() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(bytes, Value::Int(32), &Type::Primitive(PrimitiveType::Int));
    }

    #[test]
    fn avro_bytes_long() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(
            bytes,
            Value::LongInt(32),
            &Type::Primitive(PrimitiveType::Long),
        );
    }

    #[test]
    fn avro_bytes_float() {
        let bytes = vec![0u8, 0u8, 128u8, 63u8];

        check_avro_bytes_serde(
            bytes,
            Value::Float(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Float),
        );
    }

    #[test]
    fn avro_bytes_double() {
        let bytes = vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 240u8, 63u8];

        check_avro_bytes_serde(
            bytes,
            Value::Double(OrderedFloat(1.0)),
            &Type::Primitive(PrimitiveType::Double),
        );
    }

    #[test]
    fn avro_bytes_string() {
        let bytes = vec![105u8, 99u8, 101u8, 98u8, 101u8, 114u8, 103u8];

        check_avro_bytes_serde(
            bytes,
            Value::String("iceberg".to_string()),
            &Type::Primitive(PrimitiveType::String),
        );
    }

    #[test]
    fn avro_bytes_decimal() {
        let value = Value::Decimal(Decimal::from_str_exact("104899.50").unwrap());

        // Test serialization
        let byte_buf: ByteBuf = value.clone().into();
        let bytes: Vec<u8> = byte_buf.into_vec();
        assert_eq!(
            bytes,
            vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 160u8, 16u8, 94u8]
        );

        // Test deserialization
        check_avro_bytes_serde(
            bytes,
            value,
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 15,
                scale: 2,
            }),
        );
    }

    #[test]
    fn test_transform_identity() {
        let value = Value::Int(42);
        let result = value.transform(&Transform::Identity).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_transform_bucket() {
        let value = Value::String("test".to_string());
        let result = value.transform(&Transform::Bucket(10)).unwrap();
        assert!(matches!(result, Value::Int(_)));
    }

    #[test]
    fn test_transform_truncate_int() {
        let value = Value::Int(42);
        let result = value.transform(&Transform::Truncate(10)).unwrap();
        assert_eq!(result, Value::Int(40));
    }

    #[test]
    fn test_transform_truncate_long_int() {
        let value = Value::LongInt(1234567890);
        let result = value.transform(&Transform::Truncate(1000000)).unwrap();
        assert_eq!(result, Value::LongInt(1234000000));
    }

    #[test]
    fn test_transform_truncate_string() {
        let value = Value::String("Hello, World!".to_string());
        let result = value.transform(&Transform::Truncate(5)).unwrap();
        assert_eq!(result, Value::String("Hello".to_string()));
    }

    #[test]
    fn test_transform_truncate_unsupported() {
        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Truncate(5));
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_transform_year_date() {
        let value = Value::Date(19478);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Date(19523);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Date(19723);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(54));
    }

    #[test]
    fn test_transform_year_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(53));

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Year).unwrap();
        assert_eq!(result, Value::Int(54));
    }

    #[test]
    fn test_transform_month_date() {
        let value = Value::Date(19478);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(641)); // 0-based month index

        let value = Value::Date(19523);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(642)); // 0-based month index

        let value = Value::Date(19723);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(649)); // 0-based month index
    }

    #[test]
    fn test_transform_month_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(641)); // 0-based month index

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(642)); // 0-based month index

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Month).unwrap();
        assert_eq!(result, Value::Int(649)); // 0-based month index
    }

    #[test]
    fn test_transform_month_unsupported() {
        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Month);
        assert!(matches!(result, Err(Error::NotSupported(_))));

        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Month);
        assert!(matches!(result, Err(Error::NotSupported(_))));

        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Month);
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_transform_day_date() {
        let value = Value::Date(19478);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(19478)); // 0-based day index

        let value = Value::Date(19523);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(19523)); // 0-based day index

        let value = Value::Date(19723);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(19723)); // 0-based day index
    }

    #[test]
    fn test_transform_day_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(19478)); // 0-based day index

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(19523)); // 0-based day index

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Day).unwrap();
        assert_eq!(result, Value::Int(19723)); // 0-based day index
    }

    #[test]
    fn test_transform_day_unsupported() {
        let value = Value::Boolean(true);
        let result = value.transform(&Transform::Day);
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_transform_hour_timestamp() {
        let value = Value::Timestamp(1682937000000000);
        let result = value.transform(&Transform::Hour).unwrap();
        assert_eq!(result, Value::Int(467482)); // Assuming the timestamp is at 12:00 UTC

        let value = Value::Timestamp(1686840330000000);
        let result = value.transform(&Transform::Hour).unwrap();
        assert_eq!(result, Value::Int(468566)); // Assuming the timestamp is at 12:00 UTC

        let value = Value::Timestamp(1704067200000000);
        let result = value.transform(&Transform::Hour).unwrap();
        assert_eq!(result, Value::Int(473352)); // Assuming the timestamp is at 12:00 UTC
    }

    #[test]
    fn test_transform_hour_unsupported() {
        let value = Value::Date(0);
        let result = value.transform(&Transform::Hour);
        assert!(matches!(result, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_sub_string() {
        assert_eq!(
            sub_string("zyxwvutsrqponmlkjihgfedcba", "abcdefghijklmnopqrstuvxyz"),
            5354
        );
        assert_eq!(
            sub_string("abcdefghijklmnopqrstuvxyz", "zyxwvutsrqponmlkjihgfedcba"),
            5354
        );
    }

    // -----------------------------------------------------------------------
    // Placeholders for V3 Variant value wrapper + serialization.
    //
    // Rust has `PrimitiveType::Variant` in types.rs but no value-level Variant
    // implementation: no `Variant` / `VariantPrimitive` / `VariantMetadata`
    // value types, no LE little-endian byte writer, no Conversions::to_byte_buffer
    // for variants. The 30-element case list mirrors the spec's primitive variants
    // covered by `Variants::ofXxx` factory methods.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Placeholders for timestamp/date literal conversion gaps in Value::cast +
    // Value::try_from_json. Rust has no Value::TimestampNano variant, the
    // Timestamptz JSON parser hardcodes +00:00, neither timestamp variant
    // rejects offset-vs-no-offset mismatches when casting between types.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no Value::TimestampNano variant; Value::cast Timestamp->TimestampNano unimplemented"]
    fn test_timestamp_micros_promotes_to_timestamp_nanos_by_multiplying_by_one_thousand() {
        // Value::Timestamp(micros).cast(TimestampNs) yields Value::TimestampNano(micros * 1000).
        unimplemented!("Timestamp->TimestampNano cast");
    }

    #[test]
    #[ignore = "Value::cast Timestamp->Date unimplemented"]
    fn test_timestamp_micros_casts_to_date_with_floor_for_negative_sub_day_values() {
        // Value::Timestamp(micros).cast(Date) returns days-since-epoch using floor division
        // (negative sub-day micros round down to the previous day).
        unimplemented!("Timestamp->Date cast");
    }

    #[test]
    #[ignore = "Value::cast Timestamp->Date unimplemented (micros precision branch)"]
    fn test_timestamp_micros_to_date_repeats_the_floor_rule_at_micros_precision() {
        // Same as above but explicit micros-precision contract.
        unimplemented!("Timestamp->Date cast micros");
    }

    #[test]
    #[ignore = "no Value::TimestampNano variant; Value::cast TimestampNano->Timestamp unimplemented"]
    fn test_timestamp_nanos_narrows_to_timestamp_micros_by_floor_division_by_one_thousand() {
        // Value::TimestampNano(nanos).cast(Timestamp) yields Value::Timestamp(nanos.div_euclid(1000)).
        unimplemented!("TimestampNano->Timestamp cast");
    }

    #[test]
    #[ignore = "no Value::TimestampNano variant; Value::cast TimestampNano->Date unimplemented"]
    fn test_timestamp_nanos_casts_to_date_using_floor_division() {
        // Value::TimestampNano(nanos).cast(Date) returns days-since-epoch with floor semantics.
        unimplemented!("TimestampNano->Date cast");
    }

    #[test]
    #[ignore = "no Value::TimestampNano variant; offset-bearing string rejected for without-zone"]
    fn test_timestamp_nanos_with_zone_strings_rejected_for_without_zone_targets() {
        // `try_from_json` of `"2024-11-07T12:33:54.123456789+00:00"` is accepted for
        // TimestampTzNs but rejected for TimestampNs (and similarly for TimestampTz/Timestamp).
        unimplemented!("TimestampNano zone parsing");
    }

    #[test]
    #[ignore = "Timestamptz parser hardcodes +00:00; offset-bearing string rejected for without-zone"]
    fn test_timestamp_micros_with_zone_strings_rejected_for_without_zone_targets() {
        // Same shape at micros precision: offset-bearing input is accepted for Timestamptz
        // but rejected for Timestamp (without zone).
        unimplemented!("Timestamptz zone parsing");
    }

    #[test]
    #[ignore = "Timestamptz parser hardcodes +00:00; offset-less string rejected for with-zone"]
    fn test_timestamp_nanos_without_zone_strings_rejected_for_with_zone_targets() {
        // `try_from_json` of `"2024-11-07T12:33:54.123456789"` accepted for TimestampNs but
        // rejected for TimestampTzNs.
        unimplemented!("TimestampNano no-zone parsing");
    }

    #[test]
    #[ignore = "Timestamptz parser hardcodes +00:00; offset-less string rejected for with-zone"]
    fn test_timestamp_micros_without_zone_strings_rejected_for_with_zone_targets() {
        // Same shape at micros: offset-less input accepted for Timestamp but rejected for Timestamptz.
        unimplemented!("Timestamp no-zone parsing");
    }

    // -----------------------------------------------------------------------
    // Cast misc + invalid-cast coverage for Value::cast.
    //
    // Rust's Value::cast is intentionally strict: only same-type identity and
    // {Int->Long, Int->Date, Long->Time/Timestamp/Timestamptz} succeed.
    // Everything else returns Err(NotSupported). The invalid-cast tests can
    // therefore pass today by iterating over the disallowed-target list.
    // -----------------------------------------------------------------------

    fn assert_invalid_casts(value: &Value, targets: &[Type]) {
        for target in targets {
            assert!(
                value.clone().cast(target).is_err(),
                "expected cast {:?} -> {:?} to error",
                value,
                target
            );
        }
    }

    fn all_other_primitive_types(excluded: &[PrimitiveType]) -> Vec<Type> {
        let candidates = [
            PrimitiveType::Boolean,
            PrimitiveType::Int,
            PrimitiveType::Long,
            PrimitiveType::Float,
            PrimitiveType::Double,
            PrimitiveType::Date,
            PrimitiveType::Time,
            PrimitiveType::Timestamp,
            PrimitiveType::Timestamptz,
            PrimitiveType::String,
            PrimitiveType::Uuid,
            PrimitiveType::Fixed(1),
            PrimitiveType::Binary,
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
        ];
        candidates
            .into_iter()
            .filter(|c| !excluded.contains(c))
            .map(Type::Primitive)
            .collect()
    }

    #[test]
    fn test_identity_cast_returns_same_value_for_every_supported_primitive_variant() {
        // Same-type Value::cast is a no-op. Decimal datatype() hardcodes precision=38, so
        // the identity cast must target precision=38 too.
        let dec_38_2 = Decimal::from_i128_with_scale(1234, 2);
        let cases = vec![
            Value::Boolean(true),
            Value::Int(123),
            Value::LongInt(12345),
            Value::Float(OrderedFloat(1.5_f32)),
            Value::Double(OrderedFloat(1.5_f64)),
            Value::Date(19700),
            Value::Time(60_000_000),
            Value::Timestamp(1_700_000_000_000_000),
            Value::TimestampTZ(1_700_000_000_000_000),
            Value::String(String::from("hello")),
            Value::UUID(uuid::Uuid::nil()),
            Value::Fixed(3, vec![1, 2, 3]),
            Value::Binary(vec![1, 2, 3]),
            Value::Decimal(dec_38_2),
        ];
        for v in cases {
            let target = v.datatype();
            assert_eq!(v.clone().cast(&target).unwrap(), v);
        }
    }

    #[test]
    #[ignore = "Value::cast Timestamp->Date unimplemented"]
    fn test_timestamp_with_microseconds_casts_to_date_via_floor_division() {
        // Timestamp(micros).cast(Date) returns days-since-epoch with floor semantics.
        unimplemented!("Timestamp->Date cast");
    }

    #[test]
    #[ignore = "no Value::TimestampNano variant"]
    fn test_timestamp_with_nanoseconds_casts_to_date_via_floor_division() {
        // TimestampNano(nanos).cast(Date) returns days-since-epoch with floor semantics.
        unimplemented!("TimestampNano->Date cast");
    }

    #[test]
    #[ignore = "Value::cast Binary->Fixed size-check unimplemented"]
    fn test_binary_with_matching_length_casts_to_fixed() {
        // Binary(bytes).cast(Fixed(n)) succeeds when bytes.len() == n; rejects when length mismatches.
        unimplemented!("Binary->Fixed cast");
    }

    #[test]
    #[ignore = "Value::cast Fixed->Binary unimplemented"]
    fn test_fixed_casts_to_binary_unconditionally() {
        // Fixed(_, bytes).cast(Binary) yields Binary(bytes) without size check.
        unimplemented!("Fixed->Binary cast");
    }

    #[test]
    #[ignore = "Value::cast String->Fixed (hex decode + length check) unimplemented"]
    fn test_string_hex_casts_to_fixed_with_length_check() {
        // String("0A0B0C").cast(Fixed(3)) decodes the hex string and yields Fixed(3, [10,11,12]).
        unimplemented!("String->Fixed cast");
    }

    #[test]
    #[ignore = "Value::cast String->Binary (hex decode) unimplemented"]
    fn test_string_hex_casts_to_binary() {
        // String("0A0B0C").cast(Binary) decodes the hex string and yields Binary([10,11,12]).
        unimplemented!("String->Binary cast");
    }

    #[test]
    fn test_boolean_value_rejects_every_non_boolean_target_type() {
        let value = Value::Boolean(true);
        let targets = all_other_primitive_types(&[PrimitiveType::Boolean]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_int_value_rejects_targets_outside_long_and_date() {
        let value = Value::Int(34);
        // Rust's cast allows Int->Long, Int->Date; reject everything else.
        let targets = all_other_primitive_types(&[
            PrimitiveType::Int,
            PrimitiveType::Long,
            PrimitiveType::Date,
        ]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_long_value_rejects_targets_outside_time_timestamp_and_timestamptz() {
        let value = Value::LongInt(34);
        // Rust's cast allows Long->Time/Timestamp/Timestamptz; reject everything else.
        let targets = all_other_primitive_types(&[
            PrimitiveType::Long,
            PrimitiveType::Time,
            PrimitiveType::Timestamp,
            PrimitiveType::Timestamptz,
        ]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_float_value_rejects_every_non_float_target_type() {
        let value = Value::Float(OrderedFloat(34.11_f32));
        let targets = all_other_primitive_types(&[PrimitiveType::Float]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_double_value_rejects_every_non_double_target_type() {
        let value = Value::Double(OrderedFloat(34.11_f64));
        let targets = all_other_primitive_types(&[PrimitiveType::Double]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_date_value_rejects_every_non_date_target_type() {
        let value = Value::Date(17396); // 2017-08-18
        let targets = all_other_primitive_types(&[PrimitiveType::Date]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_time_value_rejects_every_non_time_target_type() {
        let value = Value::Time(51_661_919_000);
        let targets = all_other_primitive_types(&[PrimitiveType::Time]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_timestamp_micros_value_rejects_every_non_timestamp_target_type() {
        let value = Value::Timestamp(1_503_065_561_919_123);
        let targets = all_other_primitive_types(&[PrimitiveType::Timestamp]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    #[ignore = "no Value::TimestampNano variant"]
    fn test_timestamp_nanos_value_rejects_every_non_timestamp_target_type() {
        // Once Value::TimestampNano lands, iterating its invalid-target list mirrors the
        // Timestamp invalid-cast contract.
        unimplemented!("TimestampNano variant");
    }

    #[test]
    fn test_decimal_value_rejects_every_non_decimal_target_type() {
        let value = Value::Decimal(Decimal::from_i128_with_scale(3411, 2));
        // Decimal datatype() hardcodes precision=38 so identity uses precision=38; any other
        // decimal precision/scale variant is therefore "not the same type" but still allowed.
        let targets = all_other_primitive_types(&[PrimitiveType::Decimal {
            precision: 9,
            scale: 2,
        }]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_string_value_rejects_every_non_string_target_type() {
        let value = Value::String(String::from("abc"));
        let targets = all_other_primitive_types(&[PrimitiveType::String]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_uuid_value_rejects_every_non_uuid_target_type() {
        let value = Value::UUID(uuid::Uuid::nil());
        let targets = all_other_primitive_types(&[PrimitiveType::Uuid]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_fixed_value_rejects_every_non_fixed_target_type() {
        let value = Value::Fixed(1, vec![10]);
        let targets = all_other_primitive_types(&[PrimitiveType::Fixed(1)]);
        assert_invalid_casts(&value, &targets);
    }

    #[test]
    fn test_binary_value_rejects_every_non_binary_target_type() {
        let value = Value::Binary(vec![10, 11, 12]);
        let targets = all_other_primitive_types(&[PrimitiveType::Binary]);
        assert_invalid_casts(&value, &targets);
    }

    // -----------------------------------------------------------------------
    // Literal serde JSON round-trip across every supported Value variant.
    // -----------------------------------------------------------------------

    #[test]
    fn test_literal_round_trips_via_try_from_json_for_every_decoder_supported_variant() {
        // Iterates the 12 Value variants whose `Value::try_from_json` decoder is implemented.
        // Fixed and Binary are todo!() on the decode side (covered separately below). The
        // Timestamptz example uses +00:00 because Rust's parser hardcodes that offset.
        let cases: Vec<(Value, Type, JsonValue)> = vec![
            (
                Value::Boolean(false),
                Type::Primitive(PrimitiveType::Boolean),
                JsonValue::Bool(false),
            ),
            (
                Value::Int(34),
                Type::Primitive(PrimitiveType::Int),
                JsonValue::Number(34.into()),
            ),
            (
                Value::LongInt(35),
                Type::Primitive(PrimitiveType::Long),
                JsonValue::Number(35_i64.into()),
            ),
            (
                Value::Float(OrderedFloat(36.75_f32)),
                Type::Primitive(PrimitiveType::Float),
                JsonValue::Number(Number::from_f64(36.75).unwrap()),
            ),
            (
                Value::Double(OrderedFloat(8.75_f64)),
                Type::Primitive(PrimitiveType::Double),
                JsonValue::Number(Number::from_f64(8.75).unwrap()),
            ),
            (
                Value::Date(17499), // 2017-11-29
                Type::Primitive(PrimitiveType::Date),
                JsonValue::String(String::from("2017-11-29")),
            ),
            (
                Value::Time(41_407_000_000),
                Type::Primitive(PrimitiveType::Time),
                JsonValue::String(String::from("11:30:07")),
            ),
            (
                Value::Timestamp(1_511_955_007_123_456),
                Type::Primitive(PrimitiveType::Timestamp),
                JsonValue::String(String::from("2017-11-29T11:30:07.123456")),
            ),
            (
                Value::TimestampTZ(1_511_955_007_123_456),
                Type::Primitive(PrimitiveType::Timestamptz),
                JsonValue::String(String::from("2017-11-29T11:30:07.123456+00:00")),
            ),
            (
                Value::String(String::from("abc")),
                Type::Primitive(PrimitiveType::String),
                JsonValue::String(String::from("abc")),
            ),
            (
                Value::UUID(uuid::Uuid::nil()),
                Type::Primitive(PrimitiveType::Uuid),
                JsonValue::String(uuid::Uuid::nil().to_string()),
            ),
        ];

        for (value, ty, json) in cases {
            let parsed = Value::try_from_json(json.clone(), &ty)
                .expect("try_from_json should not error for supported variants");
            assert_eq!(parsed.as_ref(), Some(&value), "decoded {json}");

            let emitted: JsonValue = (&value).into();
            assert_eq!(emitted, json, "encoded {value:?}");
        }
    }

    #[test]
    #[ignore = "Value::try_from_json hits todo!() for Fixed and Binary on the JSON-string decode path"]
    fn test_literal_round_trips_via_json_for_fixed_and_binary_variants() {
        // Fixed(3, [1,2,3]) <-> "010203" and Binary([3,4,5,6]) <-> "03040506" should round-trip
        // via hex encoding. Encode path works; decode path is todo!() in values.rs.
        unimplemented!("Fixed/Binary JSON decode");
    }

    #[test]
    #[ignore = "Value::try_from_json and From<&Value> for JsonValue both todo!() for Decimal"]
    fn test_literal_round_trips_via_json_for_decimal_variant() {
        // Decimal("122.50") <-> "122.50" should round-trip via canonical string; both encode
        // and decode are todo!() today.
        unimplemented!("Decimal JSON round-trip");
    }

    #[test]
    #[ignore = "no Value::TimestampNano variant; try_from_json for timestamp_ns / timestamptz_ns absent"]
    fn test_literal_round_trips_via_json_for_timestamp_nano_variants() {
        // Once Value::TimestampNano lands, two more cases extend the round-trip set:
        //   ("2017-11-29T11:30:07.123456789", TimestampNs, no-offset string)
        //   ("2017-11-29T11:30:07.123456789+00:00", TimestamptzNs, +00:00 offset)
        unimplemented!("TimestampNano literal serde");
    }

    // -----------------------------------------------------------------------
    // Placeholders for a future V3 Variant array (`ValueArray`) value model.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Edge-case width behaviour for Truncate(int).
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Datetime helper math for micros-precision timestamps.
    //
    // Rust models timestamps at micros precision only; nanos counterparts +
    // millis/ISO helpers are gaps. The micros-precision bucket helpers
    // (year/month/day/hour) reuse the same chrono routines as the production
    // transform path, so a positive and a pre-epoch instant exercise the
    // floor / zero-indexed contracts.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "datetime_to_months is off-by-one (1-indexed month vs spec's 0-indexed); year/day/hour pass but the whole convertNanos counterpart aborts on the month assertion"]
    fn test_datetime_micros_year_month_day_hour_helpers_for_positive_instant() {
        // Instant 2017-11-16T22:31:08.000001 (micros = 1_510_871_468_000_001).
        // Bucket helpers should match the spec values for the same wall-clock instant:
        // years=47, months=574, days=17486, hours=419686.
        let micros: i64 = 1_510_871_468_000_001;
        let dt = datetime::micros_to_datetime(micros);
        assert_eq!(datetime::date_to_years(&dt.date()), 47);
        assert_eq!(datetime::datetime_to_months(&dt), 574);
        assert_eq!(datetime::datetime_to_days(&dt), 17486);
        assert_eq!(datetime::datetime_to_hours(&dt), 419686);
    }

    #[test]
    #[ignore = "datetime_to_months is off-by-one for the negative branch too"]
    fn test_datetime_micros_year_month_day_hour_helpers_floor_for_pre_epoch_instant() {
        // Pre-epoch instant 1922-02-15T01:28:51.999999 (micros = -1_510_871_468_000_001).
        // Floor (Euclidean) division produces the lower bucket for non-aligned negatives:
        // years=-48, months=-575, days=-17487, hours=-419687.
        let micros: i64 = -1_510_871_468_000_001;
        let dt = datetime::micros_to_datetime(micros);
        assert_eq!(datetime::date_to_years(&dt.date()), -48);
        assert_eq!(datetime::datetime_to_months(&dt), -575);
        assert_eq!(datetime::datetime_to_days(&dt), -17487);
        assert_eq!(datetime::datetime_to_hours(&dt), -419687);
    }

    #[test]
    fn test_datetime_micros_hours_div_24_equals_days_for_positive_instant() {
        // For any positive instant, the hour-bucket divided by 24 yields the day-bucket.
        let dt = datetime::micros_to_datetime(1_750_000_500_000_001);
        assert_eq!(
            datetime::datetime_to_hours(&dt).div_euclid(24),
            datetime::datetime_to_days(&dt)
        );
    }

    #[test]
    #[ignore = "no nanos_to_micros helper in iceberg-rust-spec datetime module"]
    fn test_datetime_nanos_to_micros_floors_toward_negative_infinity() {
        // nanos_to_micros(1234567890) = 1234567; nanos_to_micros(-1234567890) = -1234568.
        unimplemented!("datetime::nanos_to_micros");
    }

    #[test]
    #[ignore = "no micros_to_nanos helper"]
    fn test_datetime_micros_to_nanos_multiplies_by_one_thousand() {
        // micros_to_nanos(123456) = 123_456_000.
        unimplemented!("datetime::micros_to_nanos");
    }

    #[test]
    #[ignore = "no iso_timestamp_to_nanos helper"]
    fn test_datetime_iso_timestamp_string_decodes_to_nanos_since_epoch() {
        // iso_timestamp_to_nanos("2017-11-16T22:31:08.000001001") = 1_510_871_468_000_001_001.
        unimplemented!("datetime::iso_timestamp_to_nanos");
    }

    #[test]
    #[ignore = "no iso_timestamptz_to_nanos helper"]
    fn test_datetime_iso_timestamptz_string_decodes_to_nanos_since_epoch() {
        // iso_timestamptz_to_nanos("2017-11-16T22:31:08.000001001+00:00") = 1_510_871_468_000_001_001.
        unimplemented!("datetime::iso_timestamptz_to_nanos");
    }

    #[test]
    #[ignore = "no timestamp_from_millis helper"]
    fn test_datetime_timestamp_from_millis_round_trips_known_wall_clock_instants() {
        // timestamp_from_millis(1510871468000) = 2017-11-16T22:31:08; pre-epoch + zero too.
        unimplemented!("datetime::timestamp_from_millis");
    }

    #[test]
    #[ignore = "no millis_from_timestamp helper"]
    fn test_datetime_millis_from_timestamp_emits_known_millis_values() {
        // millis_from_timestamp(2017-11-16T22:31:08) = 1510871468000.
        unimplemented!("datetime::millis_from_timestamp");
    }

    // -----------------------------------------------------------------------
    // Placeholders for UUID v7 + Variant-helper LE byte reading.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "uuid crate v7 feature not enabled in iceberg-rust-spec/Cargo.toml; no generate_uuid_v7 helper"]
    fn test_generate_uuid_v7_has_version_7_and_rfc4122_variant() {
        // generate_uuid_v7() returns a UUID whose `.get_version_num()` == 7 and
        // `.get_variant() == Variant::RFC4122` (== 2 in numeric form).
        unimplemented!("uuid v7 helper");
    }

    #[test]
    #[ignore = "no V3 VariantUtil 1-byte unsigned LE reader"]
    fn test_variant_util_reads_unsigned_1_byte_little_endian_integer() {
        // VariantUtil::read_byte_unsigned(&[0xff]) returns 255 (vs i8 interpretation -1).
        unimplemented!("VariantUtil read_byte_unsigned");
    }

    #[test]
    #[ignore = "no V3 VariantUtil 2-byte unsigned LE reader"]
    fn test_variant_util_reads_unsigned_2_byte_little_endian_integer() {
        // read_2_byte_unsigned([0xff,0xff]) returns 65535.
        unimplemented!("VariantUtil read_2_byte_unsigned");
    }

    #[test]
    #[ignore = "no V3 VariantUtil 3-byte unsigned LE reader"]
    fn test_variant_util_reads_unsigned_3_byte_little_endian_integer() {
        // read_3_byte_unsigned([0xff,0xff,0xff]) returns 16777215.
        unimplemented!("VariantUtil read_3_byte_unsigned");
    }

    #[test]
    fn test_truncate_int_zero_width_panics_and_negative_width_unreachable_at_type_level() {
        // Rust's Transform::Truncate(u32) cannot carry a negative width by construction
        // (the upstream contract requires a runtime check; in Rust the type system enforces it).
        // Width=0 makes the implementation call i32::rem_euclid(0), which panics — semantic
        // equivalent of Java's IllegalArgumentException.
        let size_of_u32 = std::mem::size_of::<u32>();
        assert_eq!(
            size_of_u32, 4,
            "Transform::Truncate(u32) prevents negative widths"
        );

        let value = Value::Int(100);
        let result = std::panic::catch_unwind(|| value.transform(&Transform::Truncate(0)));
        assert!(result.is_err(), "truncate with width=0 should panic");
    }

    #[test]
    #[ignore = "no Variant ValueArray value model"]
    fn test_variant_value_array_indexed_element_access() {
        // ValueArray with 3 elements: get(0)/get(1)/get(2) return the original elements
        // by type + payload equality.
        unimplemented!("Variant ValueArray indexed access");
    }

    #[test]
    #[ignore = "no Variant ValueArray value model"]
    fn test_variant_value_array_serializes_into_exactly_size_in_bytes_buffer() {
        // Write into a buffer sized exactly arr.size_in_bytes(), then read back via
        // Variants::value with empty metadata; result equals input.
        unimplemented!("Variant ValueArray minimal buffer serialization");
    }

    #[test]
    #[ignore = "no Variant ValueArray value model"]
    fn test_variant_value_array_serializes_at_nonzero_offset_in_large_buffer() {
        // Write into a buffer of size arr.size_in_bytes() + 1000 at offset 300; read back via
        // Variants::value over the offset slice; result equals input.
        unimplemented!("Variant ValueArray large buffer serialization");
    }

    #[test]
    #[ignore = "no Variant ValueArray + Conversions for Variant"]
    fn test_variant_value_array_round_trips_through_conversions() {
        // Conversions::to_byte_buffer(VariantType, Variant::of(metadata, array)) +
        // from_byte_buffer round-trips a full Variant containing a ValueArray.
        unimplemented!("Variant ValueArray Conversions");
    }

    #[rstest::rstest]
    #[case(300)]
    #[case(70_000)]
    #[case(16_777_300)]
    #[ignore = "no Variant ValueArray value model"]
    fn test_variant_value_array_writer_auto_selects_one_to_four_byte_offset_sizing(
        #[case] _array_len: usize,
    ) {
        // ValueArray writer must auto-select 1/2/3/4-byte offset sizing based on the largest
        // element offset. Three array sizes pin the 1-byte, 2-byte, and 3+-byte branches.
        unimplemented!("Variant ValueArray multi-byte offsets");
    }

    #[test]
    #[ignore = "no Variant ValueArray value model"]
    fn test_variant_value_array_round_trips_over_ten_thousand_elements() {
        // 10_000-element ValueArray fan-out: every index round-trips via the serialize-read pair.
        unimplemented!("Variant ValueArray large fan-out");
    }

    #[rstest::rstest]
    #[case("null")]
    #[case("bool_true")]
    #[case("bool_false")]
    #[case("i8_positive")]
    #[case("i8_negative")]
    #[case("i16_positive")]
    #[case("i16_negative")]
    #[case("i32_positive")]
    #[case("i32_negative")]
    #[case("i64_positive")]
    #[case("i64_negative")]
    #[case("f32_positive")]
    #[case("f32_negative")]
    #[case("f64_positive")]
    #[case("f64_negative")]
    #[case("date_post_epoch")]
    #[case("date_pre_epoch")]
    #[case("timestamptz_post_epoch")]
    #[case("timestamptz_pre_epoch")]
    #[case("timestamp_post_epoch")]
    #[case("timestamp_pre_epoch")]
    #[case("decimal4_positive")]
    #[case("decimal4_negative")]
    #[case("decimal8_positive")]
    #[case("decimal8_negative")]
    #[case("decimal16_positive")]
    #[case("decimal16_negative")]
    #[case("binary_short")]
    #[case("string_short_63_chars")]
    #[case("string_long_64_chars")]
    #[ignore = "no Variant value model: no VariantPrimitive::write_to LE serialization"]
    fn test_variant_primitive_round_trips_through_le_byte_writer(#[case] _case_label: &str) {
        // Allocate a buffer larger than primitive.size_in_bytes(), write at offset 300 in
        // little-endian order, then read back via `Variants::value(EMPTY_METADATA, slice)`
        // and assert type + payload equality with the original.
        unimplemented!("Variant primitive write_to / Variants::value");
    }

    #[rstest::rstest]
    #[case("null")]
    #[case("bool_true")]
    #[case("bool_false")]
    #[case("i8_positive")]
    #[case("i8_negative")]
    #[case("i16_positive")]
    #[case("i16_negative")]
    #[case("i32_positive")]
    #[case("i32_negative")]
    #[case("i64_positive")]
    #[case("i64_negative")]
    #[case("f32_positive")]
    #[case("f32_negative")]
    #[case("f64_positive")]
    #[case("f64_negative")]
    #[case("date_post_epoch")]
    #[case("date_pre_epoch")]
    #[case("timestamptz_post_epoch")]
    #[case("timestamptz_pre_epoch")]
    #[case("timestamp_post_epoch")]
    #[case("timestamp_pre_epoch")]
    #[case("decimal4_positive")]
    #[case("decimal4_negative")]
    #[case("decimal8_positive")]
    #[case("decimal8_negative")]
    #[case("decimal16_positive")]
    #[case("decimal16_negative")]
    #[case("binary_short")]
    #[case("string_short_63_chars")]
    #[case("string_long_64_chars")]
    #[ignore = "no Conversions::to_byte_buffer / from_byte_buffer for Variant"]
    fn test_variant_full_value_round_trips_through_conversions(#[case] _case_label: &str) {
        // `Conversions::to_byte_buffer(VariantType, Variant::of(metadata, primitive))` then
        // `from_byte_buffer` returns a Variant whose metadata + value bytes equal the original.
        unimplemented!("Conversions for Variant");
    }
}
