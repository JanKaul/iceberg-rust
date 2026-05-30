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

    // --- StructType accessors -----------------------------------------------
    //
    // Catalogue: TestSchemaCaseSensitivity, TestTypeUtil. These tests pin
    // the StructType lookup contract — by-id (`get`), by-name (`get_name`),
    // sorted field-id iteration — and document two gaps relative to the
    // Iceberg Java contract:
    //
    //   * Java's `Schema.findField(String)` resolves dotted paths into
    //     nested fields. The Rust impl short-circuits on the first match
    //     and returns the parent. Pinned by an `#[ignore]`'d test below.
    //   * Java's `Schema.caseInsensitiveFindField` looks up by lowered
    //     name. The Rust impl has no case-insensitive variant; the test
    //     below pins the case-sensitive miss.

    fn struct_with_nested() -> StructType {
        let inner = StructType::new(vec![
            StructField::new(
                101,
                "street",
                true,
                Type::Primitive(PrimitiveType::String),
                None,
            ),
            StructField::new(
                102,
                "city",
                true,
                Type::Primitive(PrimitiveType::String),
                None,
            ),
        ]);
        StructType::new(vec![
            StructField::new(1, "id", true, Type::Primitive(PrimitiveType::Long), None),
            StructField::new(
                2,
                "name",
                true,
                Type::Primitive(PrimitiveType::String),
                None,
            ),
            StructField::new(3, "address", false, Type::Struct(inner), None),
        ])
    }

    #[test]
    fn test_struct_type_get_by_id_returns_field_with_matching_id() {
        let s = struct_with_nested();
        let field = s.get(2).expect("field with id=2 must exist");
        assert_eq!(field.name, "name");
        assert_eq!(field.id, 2);
    }

    #[test]
    fn test_struct_type_get_by_unknown_id_returns_none() {
        let s = struct_with_nested();
        assert!(s.get(999).is_none());
    }

    #[test]
    fn test_struct_type_get_name_finds_top_level_field_by_exact_name() {
        let s = struct_with_nested();
        let field = s.get_name("id").expect("field 'id' must exist");
        assert_eq!(field.id, 1);
    }

    #[test]
    fn test_struct_type_get_name_returns_none_for_missing_field() {
        let s = struct_with_nested();
        assert!(s.get_name("missing").is_none());
    }

    #[test]
    fn test_struct_type_get_name_is_case_sensitive() {
        // Pins current Rust behaviour: lookup uses string equality, so
        // "ID" / "Name" do not match "id" / "name". Java exposes a
        // separate `caseInsensitiveFindField` for the lowered-name path,
        // which has no Rust counterpart today.
        let s = struct_with_nested();
        assert!(s.get_name("ID").is_none());
        assert!(s.get_name("Name").is_none());
    }

    #[test]
    fn test_struct_type_field_ids_are_sorted_ascending() {
        let s = StructType::new(vec![
            StructField::new(7, "a", true, Type::Primitive(PrimitiveType::Long), None),
            StructField::new(3, "b", true, Type::Primitive(PrimitiveType::Long), None),
            StructField::new(5, "c", true, Type::Primitive(PrimitiveType::Long), None),
        ]);
        let ids: Vec<i32> = s.field_ids().collect();
        assert_eq!(ids, vec![3, 5, 7]);
    }

    #[test]
    fn test_struct_type_dotted_path_returns_parent_today() {
        // Current Rust behaviour: `get_name("address.city")` first tries
        // the literal name, doesn't find it, then walks the dotted parts.
        // On the first match (the parent `address`) it short-circuits and
        // returns that parent rather than descending into the nested
        // struct. Pinning this so any future fix that resolves into the
        // child field becomes a deliberate change.
        let s = struct_with_nested();
        let found = s.get_name("address.city").expect("expected some hit");
        assert_eq!(found.id, 3, "today returns the parent `address` field");
        assert_eq!(found.name, "address");
    }

    #[test]
    #[ignore = "spec gap: StructType::get_name short-circuits on the first dotted-path part instead of descending into nested structs; Java's Schema.findField resolves nested names"]
    fn test_struct_type_dotted_path_resolves_into_nested_struct_per_spec() {
        let s = struct_with_nested();
        let found = s.get_name("address.city").unwrap();
        assert_eq!(found.id, 102);
        assert_eq!(found.name, "city");
    }

    // --- TestTypeUtil port -------------------------------------------------
    //
    // Java's `org.apache.iceberg.types.TypeUtil` is the central toolbox for
    // schema/struct manipulation: project / select / select_not / assign_ids
    // family / reassign_ids family / index_by_* / ancestor_fields /
    // reassign_doc / get_projected_ids.
    //
    // Rust today exposes only `Schema::project(&[i32]) -> Schema`, which is
    // flat (no nested recursion, no list/map rejection). Everything else is
    // a Rust feature gap. Each #[ignore] below names the expected Rust API
    // (signature inline) so a future `type_util` module has a ready spec.
    //
    // Fixture: column-id ordering on both sides is by spec convention; the
    // test bodies stay descriptive (no executable Rust calls) because the
    // helpers do not exist yet.

    #[test]
    #[ignore = "feature gap: type_util::reassign_ids(&Schema, &Schema) -> Schema not implemented; should map field ids by name lookup"]
    fn test_type_util_reassign_ids_remaps_field_ids_by_name() {
        // Source: required(1, "a"), required(2, "A").
        // Target: required(0, "a"), required(1, "A").
        // After reassign_ids(target, source): target.struct == source.struct
        // (target's ids replaced with source's ids matched by name).
    }

    #[test]
    #[ignore = "feature gap: reassign_ids must preserve identifier-field-ids, remapping them through the name map"]
    fn test_type_util_reassign_ids_preserves_identifier_field_ids() {
        // Target identifier {0} maps to source identifier {1} via the
        // remap derived from "a" -> id 1.
    }

    #[test]
    #[ignore = "feature gap: assign_increasing_fresh_ids(&Schema) -> Schema not implemented; identifier-field-ids must follow the new ids"]
    fn test_type_util_assign_increasing_fresh_ids_preserves_identifier() {
        // Target ids 10, 11 -> 1, 2 (fresh increasing). Identifier {10} -> {1}.
    }

    #[test]
    #[ignore = "feature gap: reassign_ids when source omits identifier-field-ids must keep the target's identifier remapped through the name map"]
    fn test_type_util_reassign_ids_with_source_missing_identifier_remaps_existing() {
        // Source has no identifier set. Target identifier {10 -> "a"}.
        // Result identifier = { source.field("a").id }.
    }

    #[test]
    #[ignore = "feature gap: Schema::project is flat; Java's TypeUtil.project recurses into nested structs"]
    fn test_type_util_project_recurses_into_nested_structs() {
        // schema with someStruct(b, B, anotherStruct(c, C)).
        // project({11}) -> {A}; project({10, 12, 13}) -> {a, someStruct(b)};
        // project({11, 12, 15, 17}) -> {A, someStruct(anotherStruct(C))};
        // project({11, 17}) auto-includes parents -> same as above.
    }

    #[test]
    #[ignore = "feature gap: TypeUtil.project preserves naturally-empty nested structs when selecting their parent ids"]
    fn test_type_util_project_preserves_naturally_empty_nested_structs() {
        // schema: someStruct(anotherStruct(empty struct)).
        // project({12}) -> someStruct(empty); project({12, 15}) -> someStruct(anotherStruct(empty));
        // project({20}) auto-includes parents; project({12, 15, 20}) -> full chain.
    }

    #[test]
    #[ignore = "feature gap: TypeUtil.project with only-parent ids drops all primitive fields of that parent struct"]
    fn test_type_util_project_with_only_parent_id_drops_primitive_children() {
        // schema with id, A, someStruct(b, B, anotherStruct(c, C)).
        // project({12}) -> someStruct(empty);
        // project({12, 15}) -> someStruct(anotherStruct(empty)).
    }

    #[test]
    #[ignore = "feature gap: type_util::select(&Schema, &HashSet<i32>) -> Schema not implemented; auto-includes children when a parent is selected"]
    fn test_type_util_select_auto_includes_children_of_selected_parent() {
        // schema with id, A, someStruct(b, B, anotherStruct(c, C)).
        // select({11}) -> {A};
        // select({10, 12}) -> {a, someStruct(b, B, anotherStruct(c, C))};
        // select({11, 17}) -> {A, someStruct(anotherStruct(C))}.
    }

    #[test]
    #[ignore = "feature gap: type_util::select_in_id_order(&Schema, &HashSet<i32>) -> Schema sorts top-level columns by field id"]
    fn test_type_util_select_in_id_order_returns_columns_sorted_by_id() {
        // schema: id=1 "id", id=3 "b", id=2 "a"; select_in_id_order({2,3})
        // -> columns ordered by id: "a" (id=2), then "b" (id=3). Same
        // result regardless of input schema's column order.
    }

    #[test]
    #[ignore = "feature gap: TypeUtil.project must reject explicit projection of List or Map types"]
    fn test_type_util_project_rejects_explicit_list_or_map_id() {
        // schema with map(13->14: struct(...) -> struct(..., innerMap)).
        // project({12}) and project({201}) must throw with
        // "Cannot explicitly project List or Map types".
        // Selecting only descendant ids (e.g. {10}) is fine.
    }

    #[test]
    #[ignore = "feature gap: type_util::get_projected_ids(&Schema) -> HashSet<i32> not implemented; should return every field id including empty struct ids"]
    fn test_type_util_get_projected_ids_returns_every_field_id() {
        // schema with id, A, emptyStruct, someStruct(b, B, anotherStruct(c, C)).
        // get_projected_ids -> {10, 11, 35, 12, 13, 14, 15, 16, 17}.
    }

    #[test]
    #[ignore = "feature gap: project must recurse through nested lists rejecting explicit list ids"]
    fn test_type_util_project_nested_list_descends_to_struct() {
        // list(13 -> list(14 -> map(15,16 -> int, struct(x, y)))).
        // project({12}), project({13}), project({14}) must throw.
        // project({16}) -> list of list of map of struct(empty).
    }

    #[test]
    #[ignore = "feature gap: project must recurse through nested maps rejecting explicit map ids"]
    fn test_type_util_project_nested_map_descends_to_struct() {
        // map(13,14 -> int, map(15,16 -> int, list(17 -> struct(x, y)))).
        // project({12}), project({14}), project({16}) must throw.
        // project({17}) -> map of map of list of struct(empty).
    }

    #[test]
    #[ignore = "feature gap: reassign_ids must throw when target field is missing from source"]
    fn test_type_util_reassign_ids_throws_when_field_missing_from_source() {
        // Target: required(1, "a"), required(2, "b").
        // Source: required(1, "a") (no "b").
        // reassign_ids -> error: 'Field b not found in source schema'.
    }

    #[test]
    #[ignore = "feature gap: index_by_name must reject schemas where two distinct dotted paths produce the same name"]
    fn test_type_util_index_by_name_rejects_ambiguous_dotted_paths() {
        // Struct with nested a.b.c AND a top-level "b.c" sibling under "a"
        // produces an ambiguous "a.b.c" lookup. Should error with
        // 'multiple fields for name a.b.c'.
    }

    #[test]
    #[ignore = "feature gap: type_util::select_not(&Schema, &HashSet<i32>) -> Schema removes only the ids requested; collapses empty structs; ignores struct ids"]
    fn test_type_util_select_not_removes_primitive_collapses_empty_struct_ignores_struct_id() {
        // schema: id (id=1), location struct(lat=3, long=4) at id=2.
        // select_not({1}) -> still has location struct intact.
        // select_not({3, 4}) -> drops location struct (collapsed to empty
        //   and removed); schema is just {id}.
        // select_not({2}) -> ignored, returns original schema.
    }

    #[test]
    #[ignore = "feature gap: reassign_or_refresh_ids must reuse source ids for matched fields and assign next-fresh ids for unmatched ones"]
    fn test_type_util_reassign_or_refresh_ids_combines_reuse_and_fresh() {
        // Target: a@10, c@11 (with defaults), B@12, identifier {10}.
        // Source: a@1, B@15.
        // Result: a@1 (from source), c@16 (fresh, next id after source max),
        //   B@15 (from source). Defaults preserved on c.
    }

    #[test]
    #[ignore = "feature gap: reassign_or_refresh_ids must support case-insensitive name lookup when flag is false"]
    fn test_type_util_reassign_or_refresh_ids_case_insensitive_lookup() {
        // Target: FIELD1@1, FIELD2@2.
        // Source: field1@1, field2@2.
        // reassign_or_refresh_ids(target, source, case_sensitive=false)
        //   -> target field names retained (FIELD1, FIELD2), ids retained.
    }

    #[test]
    #[ignore = "feature gap: assign_ids(&Type, impl Fn(i32) -> i32) -> Type applies a remap function to every field id, preserving defaults"]
    fn test_type_util_assign_ids_applies_remap_function() {
        // Apply oldId -> oldId + 10 to schema {a@0, c@1 with defaults, B@2}.
        // Result: {a@10, c@11 with defaults preserved, B@12}.
    }

    #[test]
    #[ignore = "feature gap: assign_fresh_ids must walk fields in pre-order assigning next id; preserves defaults; identifier-field-ids follow"]
    fn test_type_util_assign_fresh_ids_walks_preorder_and_preserves_defaults() {
        // Schema {a@0, c@1 with defaults, B@2} with id supplier starting
        // from 11. Result: {a@11, c@12 (defaults preserved), B@13}.
    }

    #[test]
    #[ignore = "feature gap: type_util::reassign_doc(&Schema, &Schema) -> Schema copies docs from source onto matched target fields by id"]
    fn test_type_util_reassign_doc_overlays_doc_from_source() {
        // Schema fields lack docs; doc source has docs on every field.
        // After reassign_doc: every field has the doc from source.
        // Defaults on field c are preserved.
    }

    #[test]
    #[ignore = "feature gap: assign_ids must round-trip V3 type variants (Unknown, Variant, TimestampNano, Geometry, Geography) unchanged"]
    fn test_type_util_assign_ids_with_v3_type_round_trips_type() {
        // For each V3 type T, struct(id: int, data: T) -> after
        // assign_ids(remap +10) -> struct(id+10: int, data+10: T).
        // T must come back identical (parameters preserved).
    }

    #[test]
    #[ignore = "feature gap: assign_fresh_ids must round-trip V3 type variants in nested positions"]
    fn test_type_util_assign_fresh_ids_with_v3_type_round_trips_type() {
        // Same fixture as above but exercising the fresh-id supplier path.
    }

    #[test]
    #[ignore = "feature gap: reassign_ids must round-trip V3 type variants when matched by name"]
    fn test_type_util_reassign_ids_with_v3_type_round_trips_type() {
        // Source (data: T@2) and target (data: T@1) should produce a
        // result whose struct equals the source's struct. T preserved.
    }

    #[test]
    #[ignore = "feature gap: type_util::index_by_id(&StructType) -> HashMap<i32, &NestedField> must round-trip V3 type variants in nested positions"]
    fn test_type_util_index_by_id_with_v3_type_returns_field_with_type() {
        // Schema with data: T@1. index_by_id(struct).get(1).type == T.
    }

    #[test]
    #[ignore = "feature gap: type_util::index_name_by_id(&StructType) -> HashMap<i32, String> must round-trip V3 typed fields"]
    fn test_type_util_index_name_by_id_with_v3_type_returns_field_name() {
        // Schema with data: T@1. index_name_by_id(struct).get(1) == "data".
    }

    #[test]
    #[ignore = "feature gap: project must produce a schema whose data field of V3 type is preserved unchanged"]
    fn test_type_util_project_with_v3_type_preserves_field_type() {
        // Schema {id: int, data: T}. project({1}) -> {data: T}.
    }

    #[test]
    #[ignore = "feature gap: get_projected_ids must enumerate all field ids even when nested types are V3"]
    fn test_type_util_get_projected_ids_with_v3_type_includes_all() {
        // Schema {id@0, data@1: T}. get_projected_ids -> {0, 1}.
    }

    #[test]
    #[ignore = "feature gap: reassign_doc with V3 types must overlay docs on the V3-typed field"]
    fn test_type_util_reassign_doc_with_v3_type_overlays_doc() {
        // Both schemas have {id, data: T}; only the source has docs.
        // After reassign_doc: both fields have docs.
    }

    #[test]
    #[ignore = "feature gap: type_util::ancestor_fields(&Schema, i32) -> Vec<NestedField> returns the parent chain, nearest first; empty for unknown / top-level / empty schema"]
    fn test_type_util_ancestor_fields_in_empty_schema_is_empty() {
        // ancestor_fields(empty schema, -1) and (empty schema, 1) -> [].
    }

    #[test]
    #[ignore = "feature gap: ancestor_fields on a flat schema returns empty for every top-level field id"]
    fn test_type_util_ancestor_fields_in_non_nested_schema_is_empty() {
        // schema {a@0, A@1}; ancestor_fields(schema, 0) == [];
        // ancestor_fields(schema, 1) == [].
    }

    #[test]
    #[ignore = "feature gap: ancestor_fields walks parent chain from nearest-first through structs, lists, and maps"]
    fn test_type_util_ancestor_fields_in_nested_schema_returns_nearest_first_chain() {
        // Mixed schema with preferences (struct of struct), locations (map
        // of struct), points (list of struct). ancestor_fields returns:
        //   - [] for top-level ids
        //   - [preferences] for direct children of preferences
        //   - [innerPreferences, preferences] for inner-inner ids
        //   - [locationsKey, locations] / [locationsValue, locations] for
        //     map key / value descendants
        //   - [pointsElement, points] for list element descendants.
    }

    // --- TestReadabilityChecks port ----------------------------------------
    //
    // Java's `CheckCompatibility.writeCompatibilityErrors(readSchema,
    // writeSchema)` validates that data written under one schema is
    // readable with another. It returns a list of error messages
    // (`String`s) describing each compatibility violation. The companion
    // `typeCompatibilityErrors(read, write)` checks types only (ignoring
    // nullability), and `readCompatibilityErrors(read, write)` allows
    // read-side field reordering.
    //
    // Pinned errors include phrases like:
    //   "cannot be promoted to ..."
    //   "should be required, but is optional"
    //   "is required, but is missing"
    //   "<kind> cannot be read as a <other-kind>"
    //   "values should be required, but are optional"
    //   "elements should be required, but are optional"
    //   "field_X is out of order, before field_Y"
    //   "cannot be read as a variant"
    //
    // Rust has NO `CheckCompatibility` analog (grep across the workspace
    // finds zero matches), no `TypeUtil::isPromotionAllowed` helper, and
    // no `Schema::caseInsensitiveSelect`. All 24 Java @Test scenarios are
    // pinned `#[ignore]` here for a future `compatibility` module.
    //
    // Expected eventual Rust API:
    //   compatibility::write_compatibility_errors(&Schema, &Schema) -> Vec<String>;
    //   compatibility::read_compatibility_errors(&Schema, &Schema)  -> Vec<String>;
    //   compatibility::type_compatibility_errors(&Schema, &Schema)  -> Vec<String>;
    //   type_util::is_promotion_allowed(&Type, &Type) -> bool;
    //   Schema::case_insensitive_select(&str) -> Schema;

    #[test]
    #[ignore = "feature gap: no compatibility::write_compatibility_errors / type_util::is_promotion_allowed; promotion table covers Int->Long, Float->Double, Decimal precision growth, Date->Timestamp/Timestamptz, FixedType length equality, etc."]
    fn test_readability_checks_primitive_types_per_java() {
        // Java: testPrimitiveTypes. Iterates the 24-element PRIMITIVES
        // list (Boolean, Int/Long/Float/Double, Date, Time,
        // Timestamp+TimestampNano with/without zone, String, UUID,
        // Fixed(3), Fixed(4), Binary, Decimal(9,2), Decimal(11,2),
        // Decimal(9,3), Geometry crs84, Geometry srid:3857, Geography
        // crs84, Geography srid:4269, Geography srid:4269 + KARNEY).
        // For each (from, to) pair (24*24=576):
        //   - if is_promotion_allowed(from, to): zero errors;
        //   - else: 1 error mentioning "cannot be promoted to".
        // Also per `from`: testDisallowPrimitiveToStruct/List/Map.
    }

    #[test]
    #[ignore = "feature gap: no compatibility check for Variant -> Variant; should accept identity (zero errors)"]
    fn test_readability_checks_variant_to_variant_per_java() {
        // Java: testVariantToVariant.
        // Schema(Variant) -> Schema(Variant) yields zero errors.
    }

    #[test]
    #[ignore = "feature gap: no compatibility check rejecting non-Variant types into Variant; struct/map/list + every primitive must each report 'cannot be read as a variant'"]
    fn test_readability_checks_incompatible_types_to_variant_per_java() {
        // Java: testIncompatibleTypesToVariant (parametrized).
        // From: {Struct(int), Map(string, int), List(string)} plus the
        // 24 primitives. Each must yield 1 error containing
        // "cannot be read as a variant".
    }

    #[test]
    #[ignore = "feature gap: no compatibility check for optional->required column promotion; should flag 'should be required, but is optional'"]
    fn test_readability_checks_required_schema_field_per_java() {
        // Java: testRequiredSchemaField.
        // Write: optional(1, "from", Int); Read: required(1, "to", Int).
        // Errors: 1, containing "should be required, but is optional".
    }

    #[test]
    #[ignore = "feature gap: no compatibility check for missing required column; should flag 'is required, but is missing'"]
    fn test_readability_checks_missing_schema_field_per_java() {
        // Java: testMissingSchemaField.
        // Write: required(0, "other"); Read: required(1, "to").
        // Errors: 1, containing "is required, but is missing".
    }

    #[test]
    #[ignore = "feature gap: no compatibility check for optional->required struct field"]
    fn test_readability_checks_required_struct_field_per_java() {
        // Java: testRequiredStructField.
        // Write struct(optional(1, "from")); Read struct(required(1, "to")).
        // Errors: 1, containing "should be required, but is optional".
    }

    #[test]
    #[ignore = "feature gap: missing required struct child must flag 'is required, but is missing'"]
    fn test_readability_checks_missing_required_struct_field_per_java() {
        // Java: testMissingRequiredStructField.
        // Write struct(optional(2, "from")); Read struct(required(1, "to")).
        // Errors: 1, containing "is required, but is missing".
    }

    #[test]
    #[ignore = "feature gap: a write-side required child that the read schema declares optional must be accepted (no errors)"]
    fn test_readability_checks_missing_optional_struct_field_per_java() {
        // Java: testMissingOptionalStructField.
        // Write struct(required(2, "from")); Read struct(optional(1, "to")).
        // Errors: empty.
    }

    #[test]
    #[ignore = "feature gap: incompatible primitive types inside a struct must flag 'cannot be promoted to <type>'"]
    fn test_readability_checks_incompatible_struct_field_per_java() {
        // Java: testIncompatibleStructField.
        // Write struct(required(1, "from", Int)); Read struct(required(1, "to", Float)).
        // Errors: 1, containing "cannot be promoted to float".
    }

    #[test]
    #[ignore = "feature gap: struct-vs-primitive type mismatch must flag 'struct cannot be read as a string'"]
    fn test_readability_checks_incompatible_struct_and_primitive_per_java() {
        // Java: testIncompatibleStructAndPrimitive.
        // Write struct; Read string.
        // Errors: 1, containing "struct cannot be read as a string".
    }

    #[test]
    #[ignore = "feature gap: must accumulate MULTIPLE errors per check — both nullability and promotion violations in one struct field"]
    fn test_readability_checks_multiple_errors_per_java() {
        // Java: testMultipleErrors.
        // Write struct(optional(1, "from", Int)); Read struct(required(1, "to", Float)).
        // Errors: 2 — "should be required, but is optional" + "cannot be promoted to float".
    }

    #[test]
    #[ignore = "feature gap: map with optional values can't satisfy a read schema requiring required values; flag 'values should be required, but are optional'"]
    fn test_readability_checks_required_map_value_per_java() {
        // Java: testRequiredMapValue.
        // Write Map(string -> optional int); Read Map(string -> required int).
        // Errors: 1, containing "values should be required, but are optional".
    }

    #[test]
    #[ignore = "feature gap: map key type promotion failure flags 'cannot be promoted to <type>'"]
    fn test_readability_checks_incompatible_map_key_per_java() {
        // Java: testIncompatibleMapKey.
        // Write Map(int -> string); Read Map(double -> string).
        // Errors: 1, containing "cannot be promoted to double".
    }

    #[test]
    #[ignore = "feature gap: map value type promotion failure flags 'cannot be promoted to <type>'"]
    fn test_readability_checks_incompatible_map_value_per_java() {
        // Java: testIncompatibleMapValue.
        // Write Map(string -> int); Read Map(string -> double).
        // Errors: 1, containing "cannot be promoted to double".
    }

    #[test]
    #[ignore = "feature gap: map-vs-primitive type mismatch must flag 'map cannot be read as a string'"]
    fn test_readability_checks_incompatible_map_and_primitive_per_java() {
        // Java: testIncompatibleMapAndPrimitive.
        // Write Map; Read string.
        // Errors: 1, containing "map cannot be read as a string".
    }

    #[test]
    #[ignore = "feature gap: list with optional elements can't satisfy a read schema requiring required elements; flag 'elements should be required, but are optional'"]
    fn test_readability_checks_required_list_element_per_java() {
        // Java: testRequiredListElement.
        // Write List(optional int); Read List(required int).
        // Errors: 1, containing "elements should be required, but are optional".
    }

    #[test]
    #[ignore = "feature gap: list element promotion failure flags 'cannot be promoted to <type>'"]
    fn test_readability_checks_incompatible_list_element_per_java() {
        // Java: testIncompatibleListElement.
        // Write List(int); Read List(string).
        // Errors: 1, containing "cannot be promoted to string".
    }

    #[test]
    #[ignore = "feature gap: list-vs-primitive type mismatch must flag 'list cannot be read as a string'"]
    fn test_readability_checks_incompatible_list_and_primitive_per_java() {
        // Java: testIncompatibleListAndPrimitive.
        // Write List; Read string.
        // Errors: 1, containing "list cannot be read as a string".
    }

    #[test]
    #[ignore = "feature gap: write-side field reordering must be accepted when caller opts out of strict ordering (write_compatibility_errors with check_ordering=false)"]
    fn test_readability_checks_different_field_ordering_per_java() {
        // Java: testDifferentFieldOrdering.
        // Read struct(field_a@1, field_b@2); Write struct(field_b@2, field_a@1).
        // writeCompatibilityErrors(read, write, false) -> zero errors.
    }

    #[test]
    #[ignore = "feature gap: write-side field reordering must be REJECTED under strict ordering (the default); error 'field_b is out of order, before field_a'"]
    fn test_readability_checks_struct_write_reordering_per_java() {
        // Java: testStructWriteReordering.
        // Same fixture; writeCompatibilityErrors(read, write) -> 1 error
        // containing "field_b is out of order, before field_a".
    }

    #[test]
    #[ignore = "feature gap: read_compatibility_errors must allow read-side field reordering (zero errors)"]
    fn test_readability_checks_struct_read_reordering_per_java() {
        // Java: testStructReadReordering.
        // Read struct(a, b); Write struct(b, a).
        // readCompatibilityErrors(read, write) -> zero errors.
    }

    #[test]
    #[ignore = "feature gap: Schema::case_insensitive_select(&str) -> Schema; must resolve case-insensitive dotted paths"]
    fn test_readability_checks_case_insensitive_schema_projection_per_java() {
        // Java: testCaseInsensitiveSchemaProjection.
        // Schema with id@0 long, locations@5 map<string, struct(lat@1 float, long@2 float)>.
        //   caseInsensitiveSelect("ID").findField(0) != null;
        //   caseInsensitiveSelect("loCATIONs").findField(5) != null;
        //   caseInsensitiveSelect("LoCaTiOnS.LaT").findField(1) != null;
        //   caseInsensitiveSelect("locations.LONG").findField(2) != null.
    }

    #[test]
    #[ignore = "feature gap: type_compatibility_errors ignores nullability — optional->required must yield ZERO errors when only the type is checked"]
    fn test_readability_checks_type_only_required_schema_field_per_java() {
        // Java: testCheckNullabilityRequiredSchemaField.
        // Write optional(1, "from", Int); Read required(1, "to", Int).
        // typeCompatibilityErrors(read, write) -> empty.
    }

    #[test]
    #[ignore = "feature gap: type_compatibility_errors ignores nullability inside struct fields too"]
    fn test_readability_checks_type_only_required_struct_field_per_java() {
        // Java: testCheckNullabilityRequiredStructField.
        // Write struct(optional(1, "from")); Read struct(required(1, "to")).
        // typeCompatibilityErrors(read, write) -> empty.
    }
}
