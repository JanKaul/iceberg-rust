use std::ops::Index;

use anyhow::anyhow;
use serde::{
    de::{Error, IntoDeserializer},
    Deserialize, Deserializer, Serialize, Serializer,
};

/**
 * Data Types
*/

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
/// All data types are either primitives or nested types, which are maps, lists, or structs.
pub enum Type {
    /// Primitive types
    Primitive(PrimitiveType),
    /// Struct type
    Struct(Struct),
    /// List type.
    List(List),
    /// Map type
    Map(Map),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", remote = "Self")]
pub enum PrimitiveType {
    /// True or False
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 753 floating bit.
    Float,
    /// 64-bit IEEE 753 floating bit.
    Double,
    /// Fixed point decimal
    #[serde(serialize_with = "serialize_decimal")]
    Decimal {
        /// Precision
        precision: u32,
        /// Scale
        scale: u32,
    },
    /// Calendar date without timezone or time.
    Date,
    /// Time of day without date or timezone.
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    Timestampz,
    /// Arbitrary-length character sequences
    String,
    /// Universally Unique Identifiers
    Uuid,
    /// Fixed length byte array
    #[serde(serialize_with = "serialize_fixed")]
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("decimal") {
            deserialize_decimal(s.into_deserializer())
        } else if s.starts_with("fixed") {
            deserialize_fixed(s.into_deserializer())
        } else {
            PrimitiveType::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        PrimitiveType::serialize(self, serializer)
    }
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let (precision, scale) = s
        .trim_start_matches(r"decimal(")
        .trim_end_matches(")")
        .split_once(",")
        .ok_or_else(|| anyhow!("Decimal requires precision and scale: {s}"))
        .map_err(D::Error::custom)?;

    Ok(PrimitiveType::Decimal {
        precision: precision.parse().map_err(D::Error::custom)?,
        scale: scale.parse().map_err(D::Error::custom)?,
    })
}

fn serialize_decimal<S>(precision: &u32, scale: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("decimal({precision},{scale})"))
}

fn deserialize_fixed<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let fixed = String::deserialize(deserializer)?
        .trim_start_matches(r"fixed[")
        .trim_end_matches("]")
        .to_owned();

    fixed
        .parse()
        .map(|x| PrimitiveType::Fixed(x))
        .map_err(D::Error::custom)
}

fn serialize_fixed<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("fixed[{value}]"))
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename = "lowercase", tag = "type")]
pub struct Struct {
    fields: Vec<StructField>,
}

impl Index<usize> for Struct {
    type Output = StructField;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
/// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
/// Fields may have an optional comment or doc string. Fields can have default values.
pub struct StructField {
    /// Id unique in table schema
    pub id: i32,
    /// Field Name
    pub name: String,
    /// Optional or required
    pub required: bool,
    /// Datatype
    #[serde(rename = "type")]
    pub field_type: Type,
    /// Fields may have an optional comment or doc string.
    pub doc: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// A list is a collection of values with some element type. The element field has an integer id that is unique in the table schema.
/// Elements can be either optional or required. Element types may be any type.
pub struct List {
    /// Id unique in table schema
    pub element_id: i32,

    /// Elements can be either optional or required.
    pub element_required: bool,

    /// Datatype
    pub element: Box<Type>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique in the table schema.
/// Map keys are required and map values can be either optional or required.
/// Both map keys and map values may be any type, including nested types.
pub struct Map {
    /// Key Id that is unique in table schema
    pub key_id: i32,
    /// Datatype of key
    pub key: Box<Type>,
    /// Value Id that is unique in table schema
    pub value_id: i32,
    /// If value is optional or required
    pub value_required: bool,
    /// Datatype of value
    pub value: Box<Type>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal() {
        let record = r#"
        {
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "decimal(9,2)"
                }
            ]
        }
        "#;

        let result: Struct = serde_json::from_str(&record).unwrap();
        assert_eq!(
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2
            }),
            result.fields[0].field_type
        );
    }

    #[test]
    fn fixed() {
        let record = r#"
        {
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "fixed[8]"
                }
            ]
        }
        "#;

        let result: Struct = serde_json::from_str(&record).unwrap();
        assert_eq!(
            Type::Primitive(PrimitiveType::Fixed(8)),
            result.fields[0].field_type
        );
    }

    #[test]
    fn struct_type() {
        let record = r#"
        {
            "type": "struct",
            "fields": [ {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "uuid",
            "initial-default": "0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb",
            "write-default": "ec5911be-b0a7-458c-8438-c9a3e53cffae"
            }, {
            "id": 2,
            "name": "data",
            "required": false,
            "type": "int"
            } ]
            }
        "#;

        let result: Struct = serde_json::from_str(&record).unwrap();
        assert_eq!(
            Type::Primitive(PrimitiveType::Uuid),
            result.fields[0].field_type
        );
        assert_eq!(1, result.fields[0].id);
        assert_eq!(true, result.fields[0].required);

        assert_eq!(
            Type::Primitive(PrimitiveType::Int),
            result.fields[1].field_type
        );
        assert_eq!(2, result.fields[1].id);
        assert_eq!(false, result.fields[1].required);
    }

    #[test]
    fn list() {
        let record = r#"
        {
            "type": "list",
            "element-id": 3,
            "element-required": true,
            "element": "string"
            }
        "#;

        let result: List = serde_json::from_str(&record).unwrap();
        assert_eq!(Type::Primitive(PrimitiveType::String), *result.element);
    }

    #[test]
    fn map() {
        let record = r#"
        {
            "type": "map",
            "key-id": 4,
            "key": "string",
            "value-id": 5,
            "value-required": false,
            "value": "double"
            }
        "#;

        let result: Map = serde_json::from_str(&record).unwrap();
        assert_eq!(Type::Primitive(PrimitiveType::String), *result.key);
        assert_eq!(Type::Primitive(PrimitiveType::Double), *result.value);
    }
}
