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

/// Default CRS used by Iceberg geospatial types when none is specified.
pub const DEFAULT_GEO_CRS: &str = "OGC:CRS84";

/// Edge interpolation algorithm for the V3 `geography` type. Default is `Spherical`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum EdgeAlgorithm {
    /// Spherical interpolation (default).
    Spherical,
    /// Vincenty formula.
    Vincenty,
    /// Thomas-Andoyer.
    Andoyer,
    /// Karney algorithm.
    Karney,
}

impl fmt::Display for EdgeAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EdgeAlgorithm::Spherical => write!(f, "spherical"),
            EdgeAlgorithm::Vincenty => write!(f, "vincenty"),
            EdgeAlgorithm::Andoyer => write!(f, "andoyer"),
            EdgeAlgorithm::Karney => write!(f, "karney"),
        }
    }
}

impl std::str::FromStr for EdgeAlgorithm {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "spherical" => Ok(EdgeAlgorithm::Spherical),
            "vincenty" => Ok(EdgeAlgorithm::Vincenty),
            "andoyer" => Ok(EdgeAlgorithm::Andoyer),
            "karney" => Ok(EdgeAlgorithm::Karney),
            _ => Err(Error::Conversion(
                "string".to_string(),
                "EdgeAlgorithm".to_string(),
            )),
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
    /// Timestamp without timezone, nanosecond precision. Added in v3.
    #[serde(rename = "timestamp_ns")]
    TimestampNs,
    /// Timestamp with timezone, nanosecond precision. Added in v3.
    #[serde(rename = "timestamptz_ns")]
    TimestamptzNs,
    /// Arbitrary-length character sequences
    String,
    /// Universally Unique Identifiers
    Uuid,
    /// Fixed length byte array
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
    /// Default/null column type used when a more specific type is not known. Added in v3.
    Unknown,
    /// Semi-structured value using the Parquet variant encoding. Added in v3.
    Variant,
    /// Geospatial geometry. CRS defaults to `OGC:CRS84` when `None`. Added in v3.
    Geometry(Option<String>),
    /// Geospatial geography parameterised by CRS and edge-interpolation algorithm.
    /// Defaults: CRS `OGC:CRS84`, algorithm `Spherical`. Added in v3.
    Geography(Option<String>, Option<EdgeAlgorithm>),
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
        } else if s == "geometry" || s.starts_with("geometry(") {
            deserialize_geometry::<D>(&s)
        } else if s == "geography" || s.starts_with("geography(") {
            deserialize_geography::<D>(&s)
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
            PrimitiveType::Geometry(crs) => serialize_geometry(crs.as_deref(), serializer),
            PrimitiveType::Geography(crs, algorithm) => {
                serialize_geography(crs.as_deref(), algorithm.as_ref(), serializer)
            }
            _ => PrimitiveType::serialize(self, serializer),
        }
    }
}

fn deserialize_geometry<'de, D>(s: &str) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    if s == "geometry" {
        return Ok(PrimitiveType::Geometry(None));
    }
    let inner = s
        .strip_prefix("geometry(")
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| D::Error::custom(format!("invalid geometry type: {s}")))?
        .trim();
    if inner.is_empty() {
        return Err(D::Error::custom("geometry type requires non-empty CRS"));
    }
    Ok(PrimitiveType::Geometry(
        if inner.eq_ignore_ascii_case(DEFAULT_GEO_CRS) {
            None
        } else {
            Some(inner.to_string())
        },
    ))
}

fn serialize_geometry<S>(crs: Option<&str>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match crs {
        None => serializer.serialize_str("geometry"),
        Some(crs) => serializer.serialize_str(&format!("geometry({crs})")),
    }
}

fn deserialize_geography<'de, D>(s: &str) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    if s == "geography" {
        return Ok(PrimitiveType::Geography(None, None));
    }
    let inner = s
        .strip_prefix("geography(")
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| D::Error::custom(format!("invalid geography type: {s}")))?
        .trim();
    if inner.is_empty() {
        return Err(D::Error::custom("geography type requires non-empty CRS"));
    }
    let mut parts = inner.splitn(2, ',');
    let crs_part = parts.next().unwrap_or("").trim();
    let algorithm_part = parts.next().map(|x| x.trim()).filter(|x| !x.is_empty());
    let crs = if crs_part.eq_ignore_ascii_case(DEFAULT_GEO_CRS) {
        None
    } else {
        Some(crs_part.to_string())
    };
    let algorithm = algorithm_part
        .map(|x| x.parse::<EdgeAlgorithm>())
        .transpose()
        .map_err(D::Error::custom)?;
    Ok(PrimitiveType::Geography(crs, algorithm))
}

fn serialize_geography<S>(
    crs: Option<&str>,
    algorithm: Option<&EdgeAlgorithm>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match (crs, algorithm) {
        (None, None) => serializer.serialize_str("geography"),
        (Some(crs), None) => serializer.serialize_str(&format!("geography({crs})")),
        (crs, Some(algorithm)) => {
            let crs = crs.unwrap_or(DEFAULT_GEO_CRS);
            serializer.serialize_str(&format!("geography({crs}, {algorithm})"))
        }
    }
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let (precision, scale) = s
        .trim_start_matches(r"decimal(")
        .trim_end_matches(')')
        .split_once(',')
        .ok_or_else(|| D::Error::custom("Decimal requires precision and scale: {s}"))?;

    Ok(PrimitiveType::Decimal {
        precision: precision.parse().map_err(D::Error::custom)?,
        scale: scale.trim().parse().map_err(D::Error::custom)?,
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
            PrimitiveType::TimestampNs => write!(f, "timestamp_ns"),
            PrimitiveType::TimestamptzNs => write!(f, "timestamptz_ns"),
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Uuid => write!(f, "uuid"),
            PrimitiveType::Fixed(_) => write!(f, "fixed"),
            PrimitiveType::Binary => write!(f, "binary"),
            PrimitiveType::Unknown => write!(f, "unknown"),
            PrimitiveType::Variant => write!(f, "variant"),
            PrimitiveType::Geometry(None) => write!(f, "geometry"),
            PrimitiveType::Geometry(Some(crs)) => write!(f, "geometry({crs})"),
            PrimitiveType::Geography(None, None) => write!(f, "geography"),
            PrimitiveType::Geography(Some(crs), None) => write!(f, "geography({crs})"),
            PrimitiveType::Geography(crs, Some(algorithm)) => {
                let crs = crs.as_deref().unwrap_or(DEFAULT_GEO_CRS);
                write!(f, "geography({crs}, {algorithm})")
            }
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
    /// Default value used to fill this field for existing rows when the field is added.
    /// Added in v3. Stored as raw JSON; the value is typed by `field_type`.
    #[serde(
        rename = "initial-default",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub initial_default: Option<serde_json::Value>,
    /// Default value used by writers when no value is provided for this field.
    /// Added in v3. Stored as raw JSON; the value is typed by `field_type`.
    #[serde(
        rename = "write-default",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub write_default: Option<serde_json::Value>,
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
            initial_default: None,
            write_default: None,
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
                initial_default: None,
                write_default: None,
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
                initial_default: None,
                write_default: None,
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
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 2,
                    name: "data".to_string(),
                    field_type: Type::Primitive(PrimitiveType::Int),
                    required: false,
                    doc: None,
                    initial_default: None,
                    write_default: None,
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

    fn check_primitive_round_trip(json_value: &str, expected: PrimitiveType) {
        let json = format!(r#""{json_value}""#);
        let parsed: PrimitiveType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, expected);
        let serialized = serde_json::to_string(&expected).unwrap();
        assert_eq!(serialized, json);
    }

    #[test]
    fn v3_simple_primitive_types() {
        check_primitive_round_trip("unknown", PrimitiveType::Unknown);
        check_primitive_round_trip("variant", PrimitiveType::Variant);
        check_primitive_round_trip("timestamp_ns", PrimitiveType::TimestampNs);
        check_primitive_round_trip("timestamptz_ns", PrimitiveType::TimestamptzNs);
    }

    #[test]
    fn v3_geometry_default_crs() {
        check_primitive_round_trip("geometry", PrimitiveType::Geometry(None));
    }

    #[test]
    fn v3_geometry_explicit_crs() {
        check_primitive_round_trip(
            "geometry(EPSG:4326)",
            PrimitiveType::Geometry(Some("EPSG:4326".to_string())),
        );
    }

    #[test]
    fn v3_geometry_default_crs_string_normalises() {
        // The spec says CRS84 is the default; reading it back must be None
        // (so equal to the no-arg form).
        let parsed: PrimitiveType = serde_json::from_str(r#""geometry(OGC:CRS84)""#).unwrap();
        assert_eq!(parsed, PrimitiveType::Geometry(None));
    }

    #[test]
    fn v3_geography_default() {
        check_primitive_round_trip("geography", PrimitiveType::Geography(None, None));
    }

    #[test]
    fn v3_geography_with_crs() {
        check_primitive_round_trip(
            "geography(EPSG:4326)",
            PrimitiveType::Geography(Some("EPSG:4326".to_string()), None),
        );
    }

    #[test]
    fn v3_geography_with_crs_and_algorithm() {
        let parsed: PrimitiveType =
            serde_json::from_str(r#""geography(EPSG:4326, vincenty)""#).unwrap();
        assert_eq!(
            parsed,
            PrimitiveType::Geography(Some("EPSG:4326".to_string()), Some(EdgeAlgorithm::Vincenty),)
        );
        let serialized = serde_json::to_string(&parsed).unwrap();
        assert_eq!(serialized, r#""geography(EPSG:4326, vincenty)""#);
    }

    #[test]
    fn v3_geography_default_crs_with_algorithm_uses_default_crs_in_output() {
        let value = PrimitiveType::Geography(None, Some(EdgeAlgorithm::Karney));
        let serialized = serde_json::to_string(&value).unwrap();
        // Java emits the default CRS explicitly when an algorithm is set.
        assert_eq!(serialized, r#""geography(OGC:CRS84, karney)""#);
        let parsed: PrimitiveType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed, value);
    }

    // -----------------------------------------------------------------------
    // Placeholders for a future `type_util` module.
    //
    // iceberg-rust-spec exposes only `Schema::project(&[i32])`, which filters
    // top-level fields without recursing into structs/maps/lists and without
    // rejecting explicit list/map ids. The remaining helpers (assign_ids,
    // reassign_ids, select, select_not, select_in_id_order, get_projected_ids,
    // index_by_id, index_name_by_id, index_by_name, reassign_or_refresh_ids,
    // reassign_doc, ancestor_fields) have no Rust analog. Each test below pins
    // the behaviour the eventual implementation must satisfy.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no type_util::reassign_ids"]
    fn test_reassign_ids_remaps_target_to_source_ids_when_columns_differ_only_by_case() {
        // Target has ids 0,1 for case-distinct columns "a","A". Source has 1,2.
        // reassign_ids(target, source) returns a schema whose struct equals source's struct
        // (target's column names ride source's ids).
        unimplemented!("type_util::reassign_ids");
    }

    #[test]
    #[ignore = "no type_util::reassign_ids"]
    fn test_reassign_ids_propagates_identifier_field_ids_from_source() {
        // Both schemas mark one identifier field; after reassignment the result
        // carries the source's identifier_field_ids set.
        unimplemented!("type_util::reassign_ids");
    }

    #[test]
    #[ignore = "no type_util::assign_increasing_fresh_ids"]
    fn test_assign_increasing_fresh_ids_remaps_identifier_field_id_to_new_ids() {
        // Source ids 10,11 with identifier {10} → fresh ids 1,2 with identifier {1}.
        unimplemented!("type_util::assign_increasing_fresh_ids");
    }

    #[test]
    #[ignore = "no type_util::reassign_ids"]
    fn test_reassign_ids_with_source_missing_identifier_promotes_first_match() {
        // Source has no identifier field; reassign_ids derives the identifier id
        // from the source field whose name matches the target's identifier field.
        unimplemented!("type_util::reassign_ids");
    }

    #[test]
    #[ignore = "Schema::project is flat: no recursion into nested structs"]
    fn test_project_recurses_into_nested_structs_and_auto_includes_parents() {
        // Build 3-level nested struct (top → someStruct → anotherStruct).
        // project(schema, [11]) → only top field A.
        // project(schema, [10,12,13]) → a (10) + someStruct keeping only b (13).
        // project(schema, [11,12,15,17]) ≡ project(schema, [11,17])
        //   → A + someStruct.anotherStruct.C (parents auto-included).
        unimplemented!("recursive type_util::project");
    }

    #[test]
    #[ignore = "Schema::project is flat: no recursion"]
    fn test_project_preserves_naturally_empty_struct_chain() {
        // Nested struct chain ending in an already-empty struct.
        // Selecting any ancestor id keeps the chain down to (and including) that ancestor;
        // descendants below the selected id collapse into empty struct shells.
        unimplemented!("recursive type_util::project");
    }

    #[test]
    #[ignore = "Schema::project is flat: no recursion"]
    fn test_project_with_parent_only_yields_empty_struct_subtree() {
        // Selecting only a parent id (without any child id) returns a struct whose body is empty.
        unimplemented!("recursive type_util::project");
    }

    #[test]
    #[ignore = "no type_util::select (auto-includes whole subtree when parent is selected)"]
    fn test_select_returns_whole_subtree_when_parent_id_present() {
        // select(schema, [10,12]) on a 3-level nested schema returns a (10) + the entire
        // someStruct subtree (including its children).
        // select(schema, [11,17]) auto-includes ancestors and returns A + someStruct →
        // anotherStruct → C only, similar to project's parent auto-inclusion.
        unimplemented!("type_util::select");
    }

    #[test]
    #[ignore = "no type_util::select_in_id_order"]
    fn test_select_in_id_order_emits_columns_in_ascending_id_regardless_of_input_order() {
        // Schema with field order id=1,3,2 → select_in_id_order(schema, {2,3}) returns
        // columns in [id=2, id=3] order. The result struct is the same regardless of
        // whether the input schema's field order is normal or reversed.
        unimplemented!("type_util::select_in_id_order");
    }

    #[test]
    #[ignore = "Schema::project does not reject explicit map ids"]
    fn test_project_rejects_explicit_map_id_but_accepts_keys_and_values() {
        // For a Map<Struct, Struct> field with id 12 (key id 13, value id 14):
        //   project([12]) and project([201]) (inner map id) raise IllegalArgument
        //     "Cannot explicitly project List or Map types".
        //   project([10,13,14,100,101]) projects only the map's key struct; value collapses to empty.
        //   project([10,13,14]) (no key ids) yields the same result.
        //   project([10,13,14,100,101,200,202,203]) keeps both key + nested map shells.
        unimplemented!("type_util::project map handling");
    }

    #[test]
    #[ignore = "no type_util::get_projected_ids"]
    fn test_get_projected_ids_walks_every_nested_field() {
        // For a schema with top fields a (10), A (11), emptyStruct (35), someStruct (12) →
        //   b (13), B (14), anotherStruct (15) → c (16), C (17),
        // get_projected_ids returns exactly {10,11,12,13,14,15,16,17,35}.
        unimplemented!("type_util::get_projected_ids");
    }

    #[test]
    #[ignore = "Schema::project does not reject explicit list/map ids at any depth"]
    fn test_project_rejects_explicit_list_or_map_id_at_any_nesting_depth() {
        // Schema = List<List<Map<Int, Struct>>>.
        // project on any of the container ids (12 outer list, 13 inner list, 14 map) throws.
        // project([16]) (struct id inside map value) succeeds and walks back up, leaving
        // an empty struct shell as the deepest body.
        unimplemented!("type_util::project list/map rejection");
    }

    #[test]
    #[ignore = "Schema::project does not reject map ids at any depth"]
    fn test_project_rejects_explicit_map_id_at_any_nesting_depth() {
        // Schema = Map<Int, Map<Int, List<Struct>>>.
        // project on container ids 12 (outer map), 14 (inner map value), 16 (innermost map value)
        // all throw "Cannot explicitly project List or Map types".
        // project([17]) (list element struct id) walks back up and yields the chain with an
        // empty struct shell at the leaf.
        unimplemented!("type_util::project map rejection");
    }

    #[test]
    #[ignore = "no type_util::reassign_ids"]
    fn test_reassign_ids_throws_when_target_field_missing_from_source() {
        // Target has field b not present in source → reassign_ids errors with
        // "Field b not found in source schema".
        unimplemented!("type_util::reassign_ids");
    }

    #[test]
    #[ignore = "no type_util::index_by_name"]
    fn test_index_by_name_rejects_ambiguous_dotted_path_collision() {
        // Struct with both nested path a.b.c (via two nested structs) and a flat name "a.b.c"
        // as a sibling field → index_by_name errors with
        // "Invalid schema: multiple fields for name a.b.c".
        unimplemented!("type_util::index_by_name");
    }

    #[test]
    #[ignore = "no type_util::select_not"]
    fn test_select_not_drops_specified_fields_and_removes_empty_structs() {
        // Schema = id (1, Long) + location (2, Struct{lat (3), long (4)}).
        // select_not({1}) drops id → keeps location with both children.
        // select_not({3,4}) drops both children of location → drops location too
        //   (struct left with no fields is removed; result keeps only id).
        // select_not({2}) on a struct id is a no-op (legacy behaviour) → result equals input.
        unimplemented!("type_util::select_not");
    }

    #[test]
    #[ignore = "no type_util::reassign_or_refresh_ids"]
    fn test_reassign_or_refresh_ids_keeps_defaults_and_uses_source_or_fresh_ids() {
        // Target has fields a (10), c (11 with initial/write defaults), B (12) and identifier {10}.
        // Source has a (1), B (15).
        // reassign_or_refresh_ids(target, source) yields a (1), c (16 fresh) preserving its
        // defaults, B (15). New ids for unmatched-target-only fields continue from last source id.
        unimplemented!("type_util::reassign_or_refresh_ids");
    }

    #[test]
    #[ignore = "no type_util::reassign_or_refresh_ids (case-insensitive variant)"]
    fn test_reassign_or_refresh_ids_resolves_field_match_case_insensitive() {
        // Target FIELD1,FIELD2; source field1,field2. With case_sensitive=false the
        // result keeps target's original casing while inheriting source's ids 1,2.
        unimplemented!("type_util::reassign_or_refresh_ids case-insensitive");
    }

    #[test]
    #[ignore = "no type_util::assign_ids"]
    fn test_assign_ids_applies_custom_remap_function() {
        // assign_ids(struct, |old| old + 10) shifts every field id by 10
        // while preserving initial_default / write_default on the column with defaults.
        unimplemented!("type_util::assign_ids");
    }

    #[test]
    #[ignore = "no type_util::assign_fresh_ids"]
    fn test_assign_fresh_ids_uses_supplier_to_allocate_each_field_id() {
        // assign_fresh_ids(schema, counter starting at 10 with pre-increment) yields fresh
        // ids 11,12,13 over the three top-level fields, preserving the column's defaults.
        unimplemented!("type_util::assign_fresh_ids");
    }

    #[test]
    #[ignore = "no type_util::reassign_doc"]
    fn test_reassign_doc_copies_docs_without_overwriting_defaults() {
        // Target schema with one field carrying initial/write defaults. Source carries docs
        // for every field. reassign_doc(target, source) leaves ids unchanged, copies docs
        // onto each target field, and preserves defaults on the column that has them.
        unimplemented!("type_util::reassign_doc");
    }

    #[test]
    #[ignore = "no type_util::ancestor_fields"]
    fn test_ancestor_fields_returns_empty_for_unknown_id_in_empty_schema() {
        // ancestor_fields(empty_schema, -1) and ancestor_fields(empty_schema, 1) are both empty.
        unimplemented!("type_util::ancestor_fields");
    }

    #[test]
    #[ignore = "no type_util::ancestor_fields"]
    fn test_ancestor_fields_returns_empty_for_top_level_fields() {
        // For a flat schema with two top-level fields, ancestor_fields(schema, id) is empty
        // for every top-level id.
        unimplemented!("type_util::ancestor_fields");
    }

    #[test]
    #[ignore = "no type_util::ancestor_fields"]
    fn test_ancestor_fields_returns_parents_outermost_to_innermost_for_deeply_nested_id() {
        // Schema with: id (1), data (2), preferences (3) → feature1 (6), feature2 (7),
        //   inner_preferences (8) → feature3 (12), feature4 (13);
        // locations (4) MapType → key=struct{address,city,state,zip}, value=struct{lat,long};
        // points (5) ListType<struct{x,y}>.
        //
        // ancestor_fields for top-level ids is empty.
        // For preferences subtree: ids 6,7 → [preferences]; 12,13 → [inner_preferences, preferences].
        // For locations subtree: key (9)/value (10) → [locations]; 20-23 → [key, locations];
        //   14,15 → [value, locations].
        // For points subtree: element (11) → [points]; 16,17 → [element, points].
        unimplemented!("type_util::ancestor_fields");
    }

    // 8 V3 type variants exercised by every @ParameterizedTest below:
    //   Unknown, Variant, TimestampNs, TimestamptzNs,
    //   Geometry(default-crs), Geometry(srid:3857),
    //   Geography(default-crs, default-algo), Geography(srid:4269, Karney).

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "no type_util::assign_ids"]
    fn test_assign_ids_preserves_v3_primitive_payload(#[case] _payload: PrimitiveType) {
        // Source struct = required(id=0,"id",Int) + optional(id=1,"data",payload).
        // assign_ids(source, |old| old + 10) yields ids 10,11 with the payload unchanged.
        unimplemented!("type_util::assign_ids");
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "no type_util::assign_fresh_ids"]
    fn test_assign_fresh_ids_preserves_v3_primitive_payload(#[case] _payload: PrimitiveType) {
        // assign_fresh_ids(schema, counter starting at 10 with pre-increment) yields ids 11,12
        // over (id, data) with the V3 payload unchanged.
        unimplemented!("type_util::assign_fresh_ids");
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "no type_util::reassign_ids"]
    fn test_reassign_ids_preserves_v3_primitive_payload(#[case] _payload: PrimitiveType) {
        // Target ids 0,1; source ids 1,2 over (id, data:payload). reassign_ids returns a struct
        // equal to source's struct, payload unchanged.
        unimplemented!("type_util::reassign_ids");
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "no type_util::index_by_id"]
    fn test_index_by_id_returns_v3_primitive_field_type(#[case] _payload: PrimitiveType) {
        // index_by_id(struct{id (0), data (1, payload)})[1].type == payload.
        unimplemented!("type_util::index_by_id");
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "no type_util::index_name_by_id"]
    fn test_index_name_by_id_returns_field_name_for_v3_primitive(#[case] _payload: PrimitiveType) {
        // index_name_by_id(struct{id (0), data (1, payload)})[1] == "data".
        unimplemented!("type_util::index_name_by_id");
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "Schema::project is flat: handling V3 payloads to be pinned by recursive project"]
    fn test_project_returns_v3_primitive_field(#[case] _payload: PrimitiveType) {
        // project(schema(id 0, data 1:payload), {1}) returns just the data field with payload intact.
        unimplemented!("type_util::project");
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "no type_util::get_projected_ids"]
    fn test_get_projected_ids_includes_v3_primitive(#[case] _payload: PrimitiveType) {
        // get_projected_ids(schema(id 0, data 1:payload)) returns {0, 1}.
        unimplemented!("type_util::get_projected_ids");
    }

    #[rstest::rstest]
    #[case(PrimitiveType::Unknown)]
    #[case(PrimitiveType::Variant)]
    #[case(PrimitiveType::TimestampNs)]
    #[case(PrimitiveType::TimestamptzNs)]
    #[case(PrimitiveType::Geometry(None))]
    #[case(PrimitiveType::Geometry(Some(String::from("srid:3857"))))]
    #[case(PrimitiveType::Geography(None, None))]
    #[case(PrimitiveType::Geography(Some(String::from("srid:4269")), Some(EdgeAlgorithm::Karney)))]
    #[ignore = "no type_util::reassign_doc"]
    fn test_reassign_doc_works_through_v3_primitive_field(#[case] _payload: PrimitiveType) {
        // Target schema id (0), data (1, payload). Doc source has same ids/types with docs "id","data".
        // reassign_doc(target, doc_source) returns a struct equal to doc_source's struct.
        unimplemented!("type_util::reassign_doc");
    }

    // -----------------------------------------------------------------------
    // Placeholders for a future `compatibility::check` module.
    //
    // Eventual surface:
    //   compatibility::write_compatibility_errors(&Schema, &Schema) -> Vec<String>
    //   compatibility::read_compatibility_errors(&Schema, &Schema)  -> Vec<String>
    //   compatibility::type_compatibility_errors(&Schema, &Schema)  -> Vec<String>
    // Plus supporting helpers `type_util::is_promotion_allowed(&Type, &Type) -> bool`
    // and `Schema::case_insensitive_select(&str) -> Schema`.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors over the primitive promotion matrix"]
    fn test_write_compatibility_pins_primitive_promotion_matrix() {
        // For every (from, to) pair in the 24-primitive list, write_compatibility_errors
        // returns an empty list iff the promotion is allowed by the spec; otherwise it
        // returns a single error describing the disallowed promotion.
        unimplemented!("compatibility primitive matrix");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for Variant"]
    fn test_write_compatibility_variant_to_variant_succeeds() {
        // Variant on both sides → no errors.
        unimplemented!("compatibility variant-to-variant");
    }

    #[rstest::rstest]
    #[case(Type::Struct(StructType::new(vec![StructField::new(
        1, "from", true, Type::Primitive(PrimitiveType::Int), None,
    )])))]
    #[case(Type::Map(MapType {
        key_id: 1,
        key: Box::new(Type::Primitive(PrimitiveType::String)),
        value_id: 2,
        value_required: true,
        value: Box::new(Type::Primitive(PrimitiveType::Int)),
    }))]
    #[case(Type::List(ListType {
        element_id: 1,
        element_required: true,
        element: Box::new(Type::Primitive(PrimitiveType::String)),
    }))]
    #[case(Type::Primitive(PrimitiveType::Boolean))]
    #[case(Type::Primitive(PrimitiveType::Int))]
    #[case(Type::Primitive(PrimitiveType::Long))]
    #[case(Type::Primitive(PrimitiveType::Float))]
    #[case(Type::Primitive(PrimitiveType::Double))]
    #[case(Type::Primitive(PrimitiveType::Date))]
    #[case(Type::Primitive(PrimitiveType::Time))]
    #[case(Type::Primitive(PrimitiveType::Timestamp))]
    #[case(Type::Primitive(PrimitiveType::Timestamptz))]
    #[case(Type::Primitive(PrimitiveType::TimestampNs))]
    #[case(Type::Primitive(PrimitiveType::TimestamptzNs))]
    #[case(Type::Primitive(PrimitiveType::String))]
    #[case(Type::Primitive(PrimitiveType::Uuid))]
    #[case(Type::Primitive(PrimitiveType::Fixed(3)))]
    #[case(Type::Primitive(PrimitiveType::Fixed(4)))]
    #[case(Type::Primitive(PrimitiveType::Binary))]
    #[case(Type::Primitive(PrimitiveType::Decimal { precision: 9, scale: 2 }))]
    #[case(Type::Primitive(PrimitiveType::Decimal { precision: 11, scale: 2 }))]
    #[case(Type::Primitive(PrimitiveType::Decimal { precision: 9, scale: 3 }))]
    #[case(Type::Primitive(PrimitiveType::Geometry(None)))]
    #[case(Type::Primitive(PrimitiveType::Geometry(Some(String::from("srid:3857")))))]
    #[case(Type::Primitive(PrimitiveType::Geography(None, None)))]
    #[case(Type::Primitive(PrimitiveType::Geography(Some(String::from("srid:4269")), None)))]
    #[case(Type::Primitive(PrimitiveType::Geography(
        Some(String::from("srid:4269")),
        Some(EdgeAlgorithm::Karney)
    )))]
    #[ignore = "no compatibility::write_compatibility_errors for to-Variant"]
    fn test_write_compatibility_non_variant_to_variant_is_rejected(#[case] _from: Type) {
        // Any non-Variant `from` type written to a Variant `to` type produces exactly one
        // error containing the phrase "cannot be read as a variant".
        unimplemented!("compatibility to-variant rejection");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for required-vs-optional"]
    fn test_write_compatibility_required_to_field_rejects_optional_from_field() {
        // Writing optional field into a required schema field → one error.
        unimplemented!("compatibility required");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for missing top-level field"]
    fn test_write_compatibility_missing_required_field_produces_one_error() {
        // Read schema has a required field with no counterpart in write schema → one error.
        unimplemented!("compatibility missing");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors inside struct"]
    fn test_write_compatibility_required_nested_field_rejects_optional_nested_field() {
        // Inside a struct: required field on read, optional on write → one error.
        unimplemented!("compatibility required nested");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors inside struct"]
    fn test_write_compatibility_missing_required_nested_field_produces_one_error() {
        // Required nested field missing in write schema → one error.
        unimplemented!("compatibility missing required nested");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for optional nested-field"]
    fn test_write_compatibility_missing_optional_nested_field_is_ok() {
        // Optional nested field missing on write side → no errors.
        unimplemented!("compatibility missing optional nested");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for nested-type incompatibility"]
    fn test_write_compatibility_incompatible_nested_struct_field_produces_one_error() {
        // Nested field types differ in an incompatible way (e.g. long vs string) → one error.
        unimplemented!("compatibility incompatible nested struct field");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for struct-vs-primitive shape"]
    fn test_write_compatibility_incompatible_struct_and_primitive_produces_one_error() {
        // Struct on read, primitive on write at the same field id → one error.
        unimplemented!("compatibility struct-vs-primitive");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors with multi-error accumulation"]
    fn test_write_compatibility_accumulates_multiple_errors_in_one_call() {
        // A single check reports BOTH a nullability violation and a promotion violation.
        unimplemented!("compatibility multi-error");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for map value required"]
    fn test_write_compatibility_required_map_value_rejects_optional_write() {
        // Map<K, required V> on read, optional V on write → one error.
        unimplemented!("compatibility required map value");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for map key promotion"]
    fn test_write_compatibility_incompatible_map_key_produces_one_error() {
        // Map<int, V> on read, Map<string, V> on write → one error on the key type.
        unimplemented!("compatibility incompatible map key");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for map value promotion"]
    fn test_write_compatibility_incompatible_map_value_produces_one_error() {
        // Map<K, int> read, Map<K, string> write → one error on the value type.
        unimplemented!("compatibility incompatible map value");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for map-vs-primitive"]
    fn test_write_compatibility_incompatible_map_and_primitive_produces_one_error() {
        // Map on read, primitive on write at the same field id → one error.
        unimplemented!("compatibility map-vs-primitive");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for required list element"]
    fn test_write_compatibility_required_list_element_rejects_optional_write_element() {
        // List<required E> on read, List<optional E> on write → one error.
        unimplemented!("compatibility required list element");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for incompatible list element"]
    fn test_write_compatibility_incompatible_list_element_produces_one_error() {
        // List<int> read, List<string> write → one error on the element type.
        unimplemented!("compatibility incompatible list element");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for list-vs-primitive"]
    fn test_write_compatibility_incompatible_list_and_primitive_produces_one_error() {
        // List on read, primitive on write at the same field id → one error.
        unimplemented!("compatibility list-vs-primitive");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors for differing field order"]
    fn test_write_compatibility_different_field_ordering_produces_one_error() {
        // Same fields, different orders between read and write schemas → one error
        // (write-side reordering rejected by default).
        unimplemented!("compatibility field ordering");
    }

    #[test]
    #[ignore = "no compatibility::write_compatibility_errors check_ordering=false"]
    fn test_write_compatibility_struct_write_reordering_with_check_ordering_false_succeeds() {
        // With check_ordering=false, write-side struct reordering is accepted.
        unimplemented!("compatibility check_ordering=false");
    }

    #[test]
    #[ignore = "no compatibility::read_compatibility_errors"]
    fn test_read_compatibility_accepts_struct_field_reordering_unconditionally() {
        // read_compatibility_errors accepts reordering even when write_compatibility_errors rejects it.
        unimplemented!("compatibility read-side reordering");
    }

    #[test]
    #[ignore = "no Schema::case_insensitive_select"]
    fn test_case_insensitive_select_resolves_dotted_path_ignoring_case() {
        // case_insensitive_select("Location.Lat") resolves to the lower-case nested field.
        unimplemented!("Schema::case_insensitive_select");
    }

    #[test]
    #[ignore = "no compatibility::type_compatibility_errors"]
    fn test_type_compatibility_ignores_required_optional_at_top_level() {
        // type_compatibility_errors does not flag a required-vs-optional mismatch at the top level.
        unimplemented!("compatibility type-only top-level");
    }

    #[test]
    #[ignore = "no compatibility::type_compatibility_errors"]
    fn test_type_compatibility_ignores_required_optional_inside_struct() {
        // type_compatibility_errors does not flag a required-vs-optional mismatch inside a struct.
        unimplemented!("compatibility type-only nested");
    }
}
