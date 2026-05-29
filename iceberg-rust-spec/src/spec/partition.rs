//! Partition specification and transformation functionality for Iceberg tables.
//!
//! This module provides the core types and implementations for defining how data is partitioned
//! in Iceberg tables. It includes:
//!
//! - [`Transform`] - Transformations that can be applied to partition columns
//! - [`PartitionField`] - Definition of individual partition fields
//! - [`PartitionSpec`] - Complete specification of table partitioning
//! - [`BoundPartitionField`] - Runtime binding of partition fields to schema fields
//!
//! Partitioning is a key concept in Iceberg that determines how data files are organized
//! and enables efficient querying through partition pruning.

use std::{
    fmt::{self, Display},
    str,
};

use derive_getters::Getters;
use serde::{
    de::{Error as SerdeError, IntoDeserializer},
    Deserialize, Deserializer, Serialize, Serializer,
};

use derive_builder::Builder;

use crate::{error::Error, types::StructField};

use super::types::{StructType, Type};

pub static DEFAULT_PARTITION_SPEC_ID: i32 = 0;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", remote = "Self")]
/// A Transform that is applied to each source column to produce a partition value.
pub enum Transform {
    /// Source value, unmodified
    Identity,
    /// Hash of value, mod N
    Bucket(u32),
    /// Value truncated to width
    Truncate(u32),
    /// Extract a date or timestamp year as years from 1970
    Year,
    /// Extract a date or timestamp month as months from 1970-01-01
    Month,
    /// Extract a date or timestamp day as days from 1970-01-01
    Day,
    /// Extract a date or timestamp hour as hours from 1970-01-01 00:00:00
    Hour,
    /// Always produces `null`
    Void,
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("bucket") {
            deserialize_bucket(s.into_deserializer())
        } else if s.starts_with("truncate") {
            deserialize_truncate(s.into_deserializer())
        } else {
            Transform::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Transform::Bucket(bucket) => serialize_bucket(bucket, serializer),
            Transform::Truncate(truncate) => serialize_truncate(truncate, serializer),
            x => Transform::serialize(x, serializer),
        }
    }
}

fn deserialize_bucket<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: Deserializer<'de>,
{
    let bucket = String::deserialize(deserializer)?
        .trim_start_matches(r"bucket[")
        .trim_end_matches(']')
        .to_owned();

    bucket
        .parse()
        .map(Transform::Bucket)
        .map_err(D::Error::custom)
}

fn serialize_bucket<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("bucket[{value}]"))
}

fn deserialize_truncate<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: Deserializer<'de>,
{
    let truncate = String::deserialize(deserializer)?
        .trim_start_matches(r"truncate[")
        .trim_end_matches(']')
        .to_owned();

    truncate
        .parse()
        .map(Transform::Truncate)
        .map_err(D::Error::custom)
}

fn serialize_truncate<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("truncate[{value}]"))
}

impl Display for Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Bucket(i) => write!(f, "bucket[{i}]"),
            Transform::Truncate(i) => write!(f, "truncate[{i}]"),
            Transform::Void => write!(f, "void"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Getters)]
#[serde(rename_all = "kebab-case")]
/// Partition fields capture the transform from table data to partition values.
pub struct PartitionField {
    /// A source column id from the table’s schema
    source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    field_id: i32,
    /// A partition name.
    name: String,
    /// A transform that is applied to the source column to produce a partition value.
    transform: Transform,
}

impl PartitionField {
    /// Create a new PartitionField
    pub fn new(source_id: i32, field_id: i32, name: &str, transform: Transform) -> Self {
        Self {
            source_id,
            field_id,
            name: name.to_string(),
            transform,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder, Getters)]
#[serde(rename_all = "kebab-case")]
#[builder(build_fn(error = "Error"), setter(prefix = "with"))]
///  Partition spec that defines how to produce a tuple of partition values from a record.
pub struct PartitionSpec {
    /// Identifier for PartitionSpec
    #[builder(default = "DEFAULT_PARTITION_SPEC_ID")]
    spec_id: i32,
    /// Details of the partition spec
    #[builder(setter(each(name = "with_partition_field")))]
    fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create partition spec builder
    pub fn builder() -> PartitionSpecBuilder {
        PartitionSpecBuilder::default()
    }
    /// Get datatypes of partition fields
    pub fn data_types(&self, schema: &StructType) -> Result<Vec<Type>, Error> {
        self.fields
            .iter()
            .map(|field| {
                schema
                    .get(field.source_id as usize)
                    .ok_or(Error::NotFound(format!("Schema field {}", field.name)))
                    .and_then(|x| x.field_type.clone().tranform(&field.transform))
            })
            .collect::<Result<Vec<_>, Error>>()
    }
}

impl fmt::Display for PartitionSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for PartitionSpec {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

/// A partition field bound to its source schema field, providing access to both partition and source field information.
/// This allows accessing the partition field definition along with the schema field it references.
#[derive(Debug)]
pub struct BoundPartitionField<'a> {
    partition_field: &'a PartitionField,
    struct_field: &'a StructField,
}

impl<'a> BoundPartitionField<'a> {
    /// Creates a new BoundPartitionField by binding together a partition field with its corresponding schema field.
    ///
    /// # Arguments
    /// * `partition_field` - The partition field definition
    /// * `struct_field` - The source schema field that this partition is derived from
    pub fn new(partition_field: &'a PartitionField, struct_field: &'a StructField) -> Self {
        Self {
            partition_field,
            struct_field,
        }
    }

    /// Name of partition field
    pub fn name(&self) -> &str {
        &self.partition_field.name
    }

    /// Name of source field
    pub fn source_name(&self) -> &str {
        &self.struct_field.name
    }

    /// Datatype of source field
    pub fn field_type(&self) -> &Type {
        &self.struct_field.field_type
    }

    /// Datatype of source field
    pub fn transform(&self) -> &Transform {
        &self.partition_field.transform
    }

    /// Field id if partition field
    pub fn field_id(&self) -> i32 {
        self.partition_field.field_id
    }

    /// Field id if partition field
    pub fn source_id(&self) -> i32 {
        self.partition_field.source_id
    }

    /// Field id if partition field
    pub fn required(&self) -> bool {
        self.struct_field.required
    }

    /// Partition field
    pub fn partition_field(&self) -> &PartitionField {
        self.partition_field
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_spec() {
        let sort_order = r#"
        {
        "spec-id": 1,
        "fields": [ {
            "source-id": 4,
            "field-id": 1000,
            "name": "ts_day",
            "transform": "day"
            }, {
            "source-id": 1,
            "field-id": 1001,
            "name": "id_bucket",
            "transform": "bucket[16]"
            }, {
            "source-id": 2,
            "field-id": 1002,
            "name": "id_truncate",
            "transform": "truncate[4]"
            } ]
        }
        "#;

        let partition_spec: PartitionSpec = serde_json::from_str(sort_order).unwrap();
        assert_eq!(4, partition_spec.fields[0].source_id);
        assert_eq!(1000, partition_spec.fields[0].field_id);
        assert_eq!("ts_day", partition_spec.fields[0].name);
        assert_eq!(Transform::Day, partition_spec.fields[0].transform);

        assert_eq!(1, partition_spec.fields[1].source_id);
        assert_eq!(1001, partition_spec.fields[1].field_id);
        assert_eq!("id_bucket", partition_spec.fields[1].name);
        assert_eq!(Transform::Bucket(16), partition_spec.fields[1].transform);

        assert_eq!(2, partition_spec.fields[2].source_id);
        assert_eq!(1002, partition_spec.fields[2].field_id);
        assert_eq!("id_truncate", partition_spec.fields[2].name);
        assert_eq!(Transform::Truncate(4), partition_spec.fields[2].transform);
    }

    // --- PartitionSpec JSON serde and behaviour --------------------------

    #[test]
    fn test_partition_spec_json_round_trip_covers_every_transform() {
        let json = r#"{
            "spec-id": 7,
            "fields": [
                { "source-id": 1, "field-id": 1000, "name": "id_ident",    "transform": "identity"   },
                { "source-id": 2, "field-id": 1001, "name": "data_bucket", "transform": "bucket[16]" },
                { "source-id": 3, "field-id": 1002, "name": "data_trunc",  "transform": "truncate[8]"},
                { "source-id": 4, "field-id": 1003, "name": "ts_year",     "transform": "year"       },
                { "source-id": 4, "field-id": 1004, "name": "ts_month",    "transform": "month"      },
                { "source-id": 4, "field-id": 1005, "name": "ts_day",      "transform": "day"        },
                { "source-id": 4, "field-id": 1006, "name": "ts_hour",     "transform": "hour"       },
                { "source-id": 5, "field-id": 1007, "name": "void_field",  "transform": "void"       }
            ]
        }"#;

        let spec: PartitionSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.spec_id(), &7);
        assert_eq!(spec.fields().len(), 8);
        assert_eq!(
            spec.fields()
                .iter()
                .map(|f| f.transform.clone())
                .collect::<Vec<_>>(),
            vec![
                Transform::Identity,
                Transform::Bucket(16),
                Transform::Truncate(8),
                Transform::Year,
                Transform::Month,
                Transform::Day,
                Transform::Hour,
                Transform::Void,
            ],
        );

        let again: PartitionSpec =
            serde_json::from_str(&serde_json::to_string(&spec).unwrap()).unwrap();
        assert_eq!(again, spec);
    }

    #[test]
    fn test_partition_spec_rejects_partition_field_without_field_id() {
        // Java's `PartitionSpecParser` auto-assigns missing field-ids starting at
        // 1000; the Rust struct treats `field-id` as required and rejects input
        // that omits it. This test pins the current Rust behaviour.
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "source-id": 1, "name": "id_bucket", "transform": "bucket[8]" }
            ]
        }"#;
        assert!(serde_json::from_str::<PartitionSpec>(json).is_err());
    }

    #[test]
    fn test_partition_spec_preserves_field_order_through_round_trip() {
        // Field-ids are intentionally out-of-numeric-order in the input to
        // confirm the Vec preserves the JSON order rather than sorting.
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "source-id": 2, "field-id": 1001, "name": "second", "transform": "identity" },
                { "source-id": 1, "field-id": 1000, "name": "first",  "transform": "identity" }
            ]
        }"#;

        let spec: PartitionSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.fields()[0].name, "second");
        assert_eq!(spec.fields()[1].name, "first");

        let again: PartitionSpec =
            serde_json::from_str(&serde_json::to_string(&spec).unwrap()).unwrap();
        assert_eq!(again, spec);
    }

    #[test]
    fn test_partition_spec_builder_and_display_fromstr_round_trip() {
        let spec = PartitionSpec::builder()
            .with_spec_id(42)
            .with_partition_field(PartitionField::new(
                1,
                1000,
                "id_bucket",
                Transform::Bucket(8),
            ))
            .with_partition_field(PartitionField::new(
                2,
                1001,
                "data_trunc",
                Transform::Truncate(10),
            ))
            .build()
            .unwrap();

        assert_eq!(spec.spec_id(), &42);
        assert_eq!(spec.fields().len(), 2);
        assert_eq!(spec.fields()[0].name, "id_bucket");
        assert_eq!(spec.fields()[0].transform, Transform::Bucket(8));

        // The Display impl emits JSON, and FromStr parses it back to an
        // equal value.
        let parsed: PartitionSpec = spec.to_string().parse().unwrap();
        assert_eq!(parsed, spec);
    }

    #[test]
    fn test_partition_spec_data_types_apply_each_transform_to_source_field() {
        use crate::spec::types::{PrimitiveType, StructField, StructType, Type};

        let schema = StructType::new(vec![
            StructField {
                id: 0,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            },
            StructField {
                id: 1,
                name: "ts".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Timestamp),
                doc: None,
                initial_default: None,
                write_default: None,
            },
            StructField {
                id: 2,
                name: "label".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            },
        ]);

        let spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_partition_field(PartitionField::new(
                0,
                1000,
                "id_ident",
                Transform::Identity,
            ))
            .with_partition_field(PartitionField::new(1, 1001, "ts_year", Transform::Year))
            .with_partition_field(PartitionField::new(
                2,
                1002,
                "label_bucket",
                Transform::Bucket(16),
            ))
            .build()
            .unwrap();

        let types = spec.data_types(&schema).unwrap();
        assert_eq!(
            types,
            vec![
                Type::Primitive(PrimitiveType::Long), // Identity over Long stays Long
                Type::Primitive(PrimitiveType::Int),  // Year transform always yields Int
                Type::Primitive(PrimitiveType::Int),  // Bucket always yields Int
            ],
        );
    }

    #[test]
    fn test_transform_json_round_trips_every_variant() {
        for t in [
            Transform::Identity,
            Transform::Bucket(1024),
            Transform::Truncate(16),
            Transform::Year,
            Transform::Month,
            Transform::Day,
            Transform::Hour,
            Transform::Void,
        ] {
            let json = serde_json::to_string(&t).unwrap();
            let parsed: Transform = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, t, "round-trip {t:?} via {json}");
        }
    }
}
