/*!
 * Iceberg type system implementation
 *
 * This module implements Iceberg's type system, which includes:
 *
 * - Primitive types: boolean, numeric types, strings, binary, etc.
 * - Complex types: structs, lists, and maps
 * - Type conversion and validation logic
 * - Serialization/deserialization support
 *
 * The type system is used throughout the Iceberg format to:
 * - Define table schemas
 * - Validate data values
 * - Support schema evolution
 * - Enable efficient data access patterns
 */

use std::{collections::HashMap, fmt, ops::Index, slice::Iter};

use derive_builder::Builder;

use itertools::Itertools;
use serde::{
    de::{self, Error as SerdeError, IntoDeserializer, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::error::Error;

use super::partition::Transform;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
/// All data types are either primitives or nested types, which are maps, lists, or structs.
pub enum Type {
    /// Primitive types
    Primitive(PrimitiveType),
    /// Struct type
    Struct(StructType),
    /// List type.
    List(ListType),
    /// Map type
    Map(MapType),
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Primitive(primitive) => write!(f, "{primitive}"),
            Type::Struct(_) => write!(f, "struct"),
            Type::List(_) => write!(f, "list"),
            Type::Map(_) => write!(f, "map"),
        }
    }
}

/// Primitive data types
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
    Timestamptz,
    /// Arbitrary-length character sequences
    String,
    /// Universally Unique Identifiers
    Uuid,
    /// Fixed length byte array
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
        match self {
            PrimitiveType::Decimal { precision, scale } => {
                serialize_decimal(precision, scale, serializer)
            }
            PrimitiveType::Fixed(l) => serialize_fixed(l, serializer),
            _ => PrimitiveType::serialize(self, serializer),
        }
    }
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let binding = s.replace(" ", "");
    let (precision, scale) = binding
        .trim_start_matches(r"decimal(")
        .trim_end_matches(')')
        .split_once(',')
        .ok_or_else(|| D::Error::custom("Decimal requires precision and scale: {s}"))?;

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
        .trim_end_matches(']')
        .to_owned();

    fixed
        .parse()
        .map(PrimitiveType::Fixed)
        .map_err(D::Error::custom)
}

fn serialize_fixed<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("fixed[{value}]"))
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Int => write!(f, "int"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            } => write!(f, "decimal"),
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Time => write!(f, "time"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::Timestamptz => write!(f, "timestamptz"),
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Uuid => write!(f, "uuid"),
            PrimitiveType::Fixed(_) => write!(f, "fixed"),
            PrimitiveType::Binary => write!(f, "binary"),
        }
    }
}

/// DataType for a specific struct
#[derive(Debug, Serialize, PartialEq, Eq, Clone, Builder)]
#[serde(rename = "struct", tag = "type")]
#[builder(build_fn(error = "Error"))]
pub struct StructType {
    /// Struct fields
    #[builder(setter(each(name = "with_struct_field")))]
    fields: Vec<StructField>,
    /// Lookup for index by field id
    #[serde(skip_serializing)]
    #[builder(
        default = "self.fields.as_ref().unwrap().iter().enumerate().map(|(idx, field)| (field.id, idx)).collect()"
    )]
    lookup: HashMap<i32, usize>,
}

impl<'de> Deserialize<'de> for StructType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Type,
            Fields,
        }

        struct StructTypeVisitor;

        impl<'de> Visitor<'de> for StructTypeVisitor {
            type Value = StructType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StructType, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut fields = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Type => (),
                        Field::Fields => {
                            if fields.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields = Some(map.next_value()?);
                        }
                    }
                }
                let fields: Vec<StructField> =
                    fields.ok_or_else(|| de::Error::missing_field("fields"))?;

                Ok(StructType::new(fields))
            }
        }

        const FIELDS: &[&str] = &["type", "fields"];
        deserializer.deserialize_struct("struct", FIELDS, StructTypeVisitor)
    }
}

impl StructType {
    /// Creates a new StructType with the given fields
    ///
    /// # Arguments
    /// * `fields` - Vector of StructField that define the structure
    ///
    /// The method automatically builds a lookup table mapping field IDs to their position
    /// in the fields vector for efficient field access by ID.
    pub fn new(fields: Vec<StructField>) -> Self {
        let lookup = fields
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.id, idx))
            .collect();
        StructType { fields, lookup }
    }

    /// Creates a new StructTypeBuilder to construct a StructType using the builder pattern
    ///
    /// This is the recommended way to construct complex StructType instances
    /// when you need to add fields incrementally or conditionally.
    pub fn builder() -> StructTypeBuilder {
        StructTypeBuilder::default()
    }

    /// Gets a reference to the StructField at the given index
    ///
    /// # Arguments
    /// * `index` - The index of the field to retrieve
    ///
    /// # Returns
    /// * `Some(&StructField)` if a field exists at that index
    /// * `None` if no field exists at that index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&StructField> {
        self.lookup
            .get(&(index as i32))
            .map(|idx| &self.fields[*idx])
    }

    /// Gets a reference to the StructField with the given name
    ///
    /// # Arguments
    /// * `name` - The name of the field to retrieve
    ///
    /// # Returns
    /// * `Some(&StructField)` if a field with the given name exists
    /// * `None` if no field with that name exists
    pub fn get_name(&self, name: &str) -> Option<&StructField> {
        let res = self.fields.iter().find(|field| field.name == name);
        if res.is_some() {
            return res;
        }
        let parts: Vec<&str> = name.split('.').collect();
        let mut current_struct = self;
        let mut current_field = None;

        for (i, part) in parts.iter().enumerate() {
            current_field = current_struct
                .fields
                .iter()
                .find(|field| field.name == *part);

            if i == parts.len() - 1 || current_field.is_some() {
                return current_field;
            }

            if let Some(field) = current_field {
                if let Type::Struct(struct_type) = &field.field_type {
                    current_struct = struct_type;
                } else {
                    return None;
                }
            }
        }

        current_field
    }

    /// Returns the number of fields in this struct
    ///
    /// # Returns
    /// * The total count of StructFields contained in this struct
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns true if the struct contains no fields
    ///
    /// # Returns
    /// * `true` if this struct has no fields
    /// * `false` if this struct has at least one field
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns an iterator over all fields in this struct
    ///
    /// # Returns
    /// * An iterator yielding references to each StructField in order
    pub fn iter(&self) -> Iter<'_, StructField> {
        self.fields.iter()
    }

    /// Returns an iterator over all field IDs in this struct, sorted in ascending order
    ///
    /// # Returns
    /// * An iterator yielding field IDs (i32) in sorted order
    pub fn field_ids(&self) -> impl Iterator<Item = i32> {
        self.lookup.keys().map(ToOwned::to_owned).sorted()
    }

    /// Returns an iterator over field IDs of primitive-type fields only, sorted in ascending order
    ///
    /// This method filters the struct's fields to return only those with primitive types
    /// (boolean, numeric, string, etc.), excluding complex types like structs, lists, and maps.
    ///
    /// # Returns
    /// * An iterator yielding field IDs (i32) of primitive fields in sorted order
    pub fn primitive_field_ids(&self) -> impl Iterator<Item = i32> {
        self.lookup
            .iter()
            .filter(|(_, x)| matches!(self.fields[**x].field_type, Type::Primitive(_)))
            .map(|x| x.0.to_owned())
            .sorted()
    }
}

impl Index<usize> for StructType {
    type Output = StructField;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

impl StructField {
    /// Creates a new StructField with the given parameters
    ///
    /// # Arguments
    /// * `id` - Unique identifier for this field within the table schema
    /// * `name` - Name of the field
    /// * `required` - Whether this field is required (true) or optional (false)
    /// * `field_type` - The data type of this field
    /// * `doc` - Optional documentation string for this field
    pub fn new(id: i32, name: &str, required: bool, field_type: Type, doc: Option<String>) -> Self {
        Self {
            id,
            name: name.to_owned(),
            required,
            field_type,
            doc,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename = "list", rename_all = "kebab-case", tag = "type")]
/// A list is a collection of values with some element type. The element field has an integer id that is unique in the table schema.
/// Elements can be either optional or required. Element types may be any type.
pub struct ListType {
    /// Id unique in table schema
    pub element_id: i32,

    /// Elements can be either optional or required.
    pub element_required: bool,

    /// Datatype
    pub element: Box<Type>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename = "map", rename_all = "kebab-case", tag = "type")]
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique in the table schema.
/// Map keys are required and map values can be either optional or required.
/// Both map keys and map values may be any type, including nested types.
pub struct MapType {
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

impl Type {
    /// Perform a partition transformation for the given type
    pub fn tranform(&self, transform: &Transform) -> Result<Type, Error> {
        match transform {
            Transform::Identity => Ok(self.clone()),
            Transform::Bucket(_) => Ok(Type::Primitive(PrimitiveType::Int)),
            Transform::Truncate(_) => Ok(self.clone()),
            Transform::Year => Ok(Type::Primitive(PrimitiveType::Int)),
            Transform::Month => Ok(Type::Primitive(PrimitiveType::Int)),
            Transform::Day => Ok(Type::Primitive(PrimitiveType::Int)),
            Transform::Hour => Ok(Type::Primitive(PrimitiveType::Int)),
            Transform::Void => Err(Error::NotSupported("void transform".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_type_serde(json: &str, expected_type: Type) {
        let desered_type: Type = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<serde_json::Value>(&sered_json).unwrap();
        let raw_json_value = serde_json::from_str::<serde_json::Value>(json).unwrap();

        assert_eq!(parsed_json_value, raw_json_value);
    }

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

        check_type_serde(
            record,
            Type::Struct(StructType::new(vec![StructField {
                id: 1,
                name: "id".to_string(),
                field_type: Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
                required: true,
                doc: None,
            }])),
        )
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

        check_type_serde(
            record,
            Type::Struct(StructType::new(vec![StructField {
                id: 1,
                name: "id".to_string(),
                field_type: Type::Primitive(PrimitiveType::Fixed(8)),
                required: true,
                doc: None,
            }])),
        )
    }

    #[test]
    fn struct_type() {
        let record = r#"
        {
            "type": "struct",
            "fields": [ 
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "uuid"
                }, {
                    "id": 2,
                    "name": "data",
                    "required": false,
                    "type": "int"
                } 
            ]
        }
        "#;

        check_type_serde(
            record,
            Type::Struct(StructType::new(vec![
                StructField {
                    id: 1,
                    name: "id".to_string(),
                    field_type: Type::Primitive(PrimitiveType::Uuid),
                    required: true,
                    doc: None,
                },
                StructField {
                    id: 2,
                    name: "data".to_string(),
                    field_type: Type::Primitive(PrimitiveType::Int),
                    required: false,
                    doc: None,
                },
            ])),
        )
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

        let result: ListType = serde_json::from_str(record).unwrap();
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

        let result: MapType = serde_json::from_str(record).unwrap();
        assert_eq!(Type::Primitive(PrimitiveType::String), *result.key);
        assert_eq!(Type::Primitive(PrimitiveType::Double), *result.value);
    }
}
