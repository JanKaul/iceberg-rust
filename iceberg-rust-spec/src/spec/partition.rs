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

    // --- PartitionSpec validation ------------------------------------------
    //
    // Catalogue entry: TestPartitionSpecValidation. The spec requires the
    // builder to reject duplicate partition-field names and duplicate
    // field ids, and to refuse `Void` over a Void-typed source. The Rust
    // builder is derive_builder-generated and does not yet enforce these
    // checks; the `#[ignore]`'d tests below pin the gaps so removing the
    // marker is the natural last step of fixing the underlying validation.

    use crate::spec::types::{PrimitiveType, StructField, StructType, Type};

    fn primitive_schema(fields: &[(i32, &str, PrimitiveType)]) -> StructType {
        StructType::new(
            fields
                .iter()
                .map(|(id, name, prim)| {
                    StructField::new(*id, name, true, Type::Primitive(prim.clone()), None)
                })
                .collect(),
        )
    }

    #[test]
    fn test_partition_spec_default_spec_id_is_zero() {
        // Per spec, an unset spec id defaults to 0 (`DEFAULT_PARTITION_SPEC_ID`).
        // The `fields` setter has no default, so the empty vec must be
        // supplied explicitly.
        let spec = PartitionSpec::builder()
            .with_fields(Vec::new())
            .build()
            .unwrap();
        assert_eq!(*spec.spec_id(), DEFAULT_PARTITION_SPEC_ID);
        assert_eq!(*spec.spec_id(), 0);
    }

    #[test]
    fn test_partition_spec_builder_supports_empty_fields_for_unpartitioned_table() {
        // An empty `fields` list is the canonical representation of an
        // unpartitioned table; the builder must accept it once supplied
        // explicitly.
        let spec = PartitionSpec::builder()
            .with_fields(Vec::new())
            .build()
            .unwrap();
        assert!(spec.fields().is_empty());
    }

    #[test]
    fn test_partition_spec_data_types_errors_when_source_id_missing_from_schema() {
        // `data_types` resolves each partition field's source-id against
        // the schema; an undefined id must surface as `NotFound`.
        let schema = primitive_schema(&[(0, "id", PrimitiveType::Long)]);
        let spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(
                99, // undefined in the schema
                1000,
                "ghost",
                Transform::Identity,
            ))
            .build()
            .unwrap();
        let err = spec.data_types(&schema).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    #[test]
    fn test_partition_spec_data_types_errors_on_void_transform() {
        // `Type::tranform(&Transform::Void)` returns `NotSupported` — this
        // surfaces through `PartitionSpec::data_types` for any field whose
        // declared transform is `Void`.
        let schema = primitive_schema(&[(0, "id", PrimitiveType::Long)]);
        let spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(0, 1000, "id_void", Transform::Void))
            .build()
            .unwrap();
        let err = spec.data_types(&schema).unwrap_err();
        assert!(matches!(err, Error::NotSupported(_)), "got {err:?}");
    }

    #[test]
    #[ignore = "spec gap: PartitionSpecBuilder does not reject duplicate partition-field names; spec requires rejection"]
    fn test_partition_spec_builder_rejects_duplicate_partition_field_names() {
        let result = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(0, 1000, "same", Transform::Identity))
            .with_partition_field(PartitionField::new(1, 1001, "same", Transform::Identity))
            .build();
        assert!(
            result.is_err(),
            "duplicate partition-field names should be rejected",
        );
    }

    #[test]
    #[ignore = "spec gap: PartitionSpecBuilder does not reject duplicate partition-field ids; spec requires rejection"]
    fn test_partition_spec_builder_rejects_duplicate_partition_field_ids() {
        let result = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(0, 1000, "a", Transform::Identity))
            .with_partition_field(PartitionField::new(1, 1000, "b", Transform::Identity))
            .build();
        assert!(
            result.is_err(),
            "duplicate partition-field ids should be rejected",
        );
    }

    // --- PartitionSpec parser rejection (TestPartitionSpecParser) ----------
    //
    // The parser should accept well-formed input and refuse malformed
    // partition specs. These tests pin the rejection paths beyond the
    // missing-field-id case already covered above.

    #[test]
    fn test_partition_spec_parser_rejects_unknown_transform_name() {
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "source-id": 1, "field-id": 1000, "name": "x", "transform": "frobnicate" }
            ]
        }"#;
        assert!(serde_json::from_str::<PartitionSpec>(json).is_err());
    }

    #[test]
    fn test_partition_spec_parser_rejects_bucket_with_non_numeric_width() {
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "source-id": 1, "field-id": 1000, "name": "x", "transform": "bucket[abc]" }
            ]
        }"#;
        assert!(serde_json::from_str::<PartitionSpec>(json).is_err());
    }

    #[test]
    fn test_partition_spec_parser_rejects_truncate_with_negative_width() {
        // The width is parsed into u32, so a negative integer cannot fit.
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "source-id": 1, "field-id": 1000, "name": "x", "transform": "truncate[-5]" }
            ]
        }"#;
        assert!(serde_json::from_str::<PartitionSpec>(json).is_err());
    }

    #[test]
    fn test_partition_spec_parser_rejects_missing_source_id() {
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "field-id": 1000, "name": "x", "transform": "identity" }
            ]
        }"#;
        assert!(serde_json::from_str::<PartitionSpec>(json).is_err());
    }

    #[test]
    fn test_partition_spec_parser_rejects_missing_transform() {
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "source-id": 1, "field-id": 1000, "name": "x" }
            ]
        }"#;
        assert!(serde_json::from_str::<PartitionSpec>(json).is_err());
    }

    #[test]
    fn test_partition_spec_parser_rejects_missing_name() {
        let json = r#"{
            "spec-id": 1,
            "fields": [
                { "source-id": 1, "field-id": 1000, "transform": "identity" }
            ]
        }"#;
        assert!(serde_json::from_str::<PartitionSpec>(json).is_err());
    }

    #[test]
    fn test_partition_spec_parser_accepts_empty_fields_as_unpartitioned() {
        // An empty `fields` array is a valid representation of an
        // unpartitioned table; the parser must accept it.
        let json = r#"{
            "spec-id": 0,
            "fields": []
        }"#;
        let spec: PartitionSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.spec_id(), &0);
        assert!(spec.fields().is_empty());
    }

    // --- Port: TestPartitioning (Apache Iceberg Java, 26 @Test) ----------
    //
    // Java's `Partitioning` is a static helper that synthesises a single
    // `StructType` from a table's multi-spec history (`Partitioning
    // ::partitionType(table)`) and projects a subset based on a query schema
    // (`Partitioning::groupingKeyType(table, projectedSchema, deletedFields)`).
    // Both functions reconcile field names, IDs, transforms across the
    // partition_specs map plus the schemas map to honour rename / drop /
    // re-add / void-transform rules that differ between v1 and v2.
    //
    // Rust has no analogue. The data the helpers reduce over IS present —
    // `metadata.partition_specs` holds every spec the table has ever had —
    // but there is no reducer. `PartitionSpec::data_types(schema)` exposes
    // the SINGLE-SPEC view of partition field types (already tested in
    // cycle 20 via `metadata.partition_fields(snapshot_id)`).
    //
    // Each Java scenario below is pinned as a single `#[ignore]` Rust test
    // documenting the input setup and the unified `StructType` Java expects.
    // When Rust grows `Partitioning::partition_type(&TableMetadata) ->
    // Result<StructType, Error>` and `Partitioning::grouping_key_type(...)`,
    // these tests can be wired up by removing the `#[ignore]` and replacing
    // the stub call with the real helper. The leading `partitioning_*` doc
    // comment names each helper.

    // ---- partitionType scenarios (8 Java @Test) -----------------------------

    #[test]
    #[ignore = "feature gap: Rust has no Partitioning::partition_type(&TableMetadata) reducer over partition_specs; Java pins V1 spec evolution (add bucket then remove fields) and asserts the unified type is StructType.of(optional 1000 data:string, optional 1001 category_bucket_8:int)"]
    fn test_partition_type_v1_spec_evolution_per_java() {
        // Java: testPartitionTypeWithSpecEvolutionInV1Tables.
        // Initial spec: identity("data"); evolved: + bucket("category", 8).
        // Java's Partitioning.partitionType yields 1000 data:string + 1001
        // category_bucket_8:int (both optional).
    }

    #[test]
    #[ignore = "feature gap: same gap as above; Java's testPartitionTypeWithSpecEvolutionInV2Tables expects {1000 data:string, 1001 category:string} when v2 removes `data` and adds `category` — v2 drops removed fields rather than retaining as void transforms"]
    fn test_partition_type_v2_spec_evolution_per_java() {
        // Java: testPartitionTypeWithSpecEvolutionInV2Tables.
    }

    #[test]
    #[ignore = "feature gap: Java's testPartitionTypeWithRenamesInV1Table asserts that after renaming p1 -> p2 the unified type reports the new name {1000 p2:string, 1001 category:string}; Rust has no rename evolution helper on PartitionSpec"]
    fn test_partition_type_v1_renames_per_java() {
        // Java: testPartitionTypeWithRenamesInV1Table.
    }

    #[test]
    #[ignore = "feature gap: Java's testPartitionTypeWithRenamesInV1TableCaseInsensitive asserts case-insensitive identity(\"DATA\") in the initial spec round-trips and the rename {p1 -> p2} still applies; Rust PartitionSpec name matching is strictly case-sensitive"]
    fn test_partition_type_v1_renames_case_insensitive_per_java() {
        // Java: testPartitionTypeWithRenamesInV1TableCaseInsensitive.
    }

    #[test]
    #[ignore = "feature gap: Java's testPartitionTypeWithAddingBackSamePartitionFieldInV1Table asserts v1 retains the removed `data` slot as void(data_1000:string) and the re-added `data` gets a new column 1001; Rust has no void-transform retention logic across specs"]
    fn test_partition_type_v1_adding_back_same_field_per_java() {
        // Java: testPartitionTypeWithAddingBackSamePartitionFieldInV1Table.
    }

    #[test]
    #[ignore = "feature gap: Java's testPartitionTypeWithAddingBackSamePartitionFieldInV2Table asserts v2 reuses the original spec when the same field is re-added — the unified type is a single {1000 data:string}; Rust has no spec-reuse logic"]
    fn test_partition_type_v2_adding_back_same_field_per_java() {
        // Java: testPartitionTypeWithAddingBackSamePartitionFieldInV2Table.
    }

    #[test]
    #[ignore = "feature gap: Java's testPartitionTypeWithIncompatibleSpecEvolution swaps the spec without going through updateSpec; Partitioning.partitionType throws ValidationException(\"Conflicting partition fields\"); Rust has no validator that detects conflicting fields across specs"]
    fn test_partition_type_incompatible_spec_evolution_rejected_per_java() {
        // Java: testPartitionTypeWithIncompatibleSpecEvolution.
    }

    #[test]
    #[ignore = "feature gap: Java's testPartitionTypeIgnoreInactiveFields drops a partition field + the underlying schema column, then asserts the unified type omits the inactive field; Rust has no inactive-field filtering across specs"]
    fn test_partition_type_ignores_inactive_fields_per_java() {
        // Java: testPartitionTypeIgnoreInactiveFields.
    }

    // ---- groupingKeyType scenarios (15 Java @Test) --------------------------

    #[test]
    #[ignore = "feature gap: Rust has no Partitioning::grouping_key_type(&TableMetadata, &Schema, &Set<i32>) that projects the unified partition type onto the query schema; Java's testGroupingKeyTypeWithSpecEvolutionInV1Tables pins the v1 evolution case"]
    fn test_grouping_key_type_v1_spec_evolution_per_java() {
        // Java: testGroupingKeyTypeWithSpecEvolutionInV1Tables.
    }

    #[test]
    #[ignore = "feature gap: same as above; testGroupingKeyTypeWithSpecEvolutionInV2Tables pins v2 spec evolution (drop-then-add)"]
    fn test_grouping_key_type_v2_spec_evolution_per_java() {
        // Java: testGroupingKeyTypeWithSpecEvolutionInV2Tables.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithDroppedPartitionFieldInV1Tables pins v1 where a dropped field still appears (as void) in the grouping key"]
    fn test_grouping_key_type_v1_dropped_field_per_java() {
        // Java: testGroupingKeyTypeWithDroppedPartitionFieldInV1Tables.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithDroppedPartitionFieldInV2Tables pins v2 where a dropped field is absent from the grouping key"]
    fn test_grouping_key_type_v2_dropped_field_per_java() {
        // Java: testGroupingKeyTypeWithDroppedPartitionFieldInV2Tables.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithRenamesInV1Table pins that the grouping key uses the latest field names after a rename in v1"]
    fn test_grouping_key_type_v1_renames_per_java() {
        // Java: testGroupingKeyTypeWithRenamesInV1Table.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithRenamesInV1TableCaseInsensitive pins case-insensitive identity field references work in v1"]
    fn test_grouping_key_type_v1_renames_case_insensitive_per_java() {
        // Java: testGroupingKeyTypeWithRenamesInV1TableCaseInsensitive.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithRenamesInV2Table pins v2 rename behaviour for the grouping key"]
    fn test_grouping_key_type_v2_renames_per_java() {
        // Java: testGroupingKeyTypeWithRenamesInV2Table.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithEvolvedIntoUnpartitionedSpecV1Table pins v1 case where the latest spec is unpartitioned but historical partitioned data still groups by void(prev_field)"]
    fn test_grouping_key_type_v1_evolved_to_unpartitioned_per_java() {
        // Java: testGroupingKeyTypeWithEvolvedIntoUnpartitionedSpecV1Table.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithEvolvedIntoUnpartitionedSpecV2Table pins v2 case where evolving into unpartitioned yields an empty grouping key (no void retention)"]
    fn test_grouping_key_type_v2_evolved_to_unpartitioned_per_java() {
        // Java: testGroupingKeyTypeWithEvolvedIntoUnpartitionedSpecV2Table.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithAddingBackSamePartitionFieldInV1Table pins v1 where the re-added field gets a new id and the original slot is void-retained"]
    fn test_grouping_key_type_v1_adding_back_same_field_per_java() {
        // Java: testGroupingKeyTypeWithAddingBackSamePartitionFieldInV1Table.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithAddingBackSamePartitionFieldInV2Table pins v2 where re-adding the same field reuses the original spec / id"]
    fn test_grouping_key_type_v2_adding_back_same_field_per_java() {
        // Java: testGroupingKeyTypeWithAddingBackSamePartitionFieldInV2Table.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithOnlyUnpartitionedSpec pins that an unpartitioned-only table has an empty grouping key"]
    fn test_grouping_key_type_only_unpartitioned_spec_per_java() {
        // Java: testGroupingKeyTypeWithOnlyUnpartitionedSpec.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithEvolvedUnpartitionedSpec pins that historically-unpartitioned-then-still-unpartitioned tables yield an empty grouping key"]
    fn test_grouping_key_type_evolved_unpartitioned_spec_per_java() {
        // Java: testGroupingKeyTypeWithEvolvedUnpartitionedSpec.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithProjectedSchema pins that the grouping key is filtered to only fields whose source column is in the projected schema"]
    fn test_grouping_key_type_projected_schema_per_java() {
        // Java: testGroupingKeyTypeWithProjectedSchema.
    }

    #[test]
    #[ignore = "feature gap: testGroupingKeyTypeWithIncompatibleSpecEvolution mirrors the partitionType conflict path and throws ValidationException"]
    fn test_grouping_key_type_incompatible_spec_evolution_rejected_per_java() {
        // Java: testGroupingKeyTypeWithIncompatibleSpecEvolution.
    }

    // ---- spec evolution side effects (3 Java @Test) -------------------------

    #[test]
    #[ignore = "feature gap: testDeletingPartitionField pins that after `updateSpec().removeField(\"data\")` + `updateSchema().deleteColumn(\"data\")` + `updateSpec().addField(\"id\")` the resulting spec has alwaysNull(\"data\",\"data\") + identity(\"id\"); Rust has no schema-column-delete operation"]
    fn test_deleting_partition_field_yields_always_null_per_java() {
        // Java: testDeletingPartitionField.
    }

    #[test]
    #[ignore = "feature gap: deleteFileAfterDeletingAllPartitionFields exercises append-then-evolve-then-delete-file flow and asserts the delete-summary key `deleted-data-files=1`; Rust transaction logic for schema-delete-column doesn't exist"]
    fn test_delete_file_after_deleting_all_partition_fields_per_java() {
        // Java: deleteFileAfterDeletingAllPartitionFields.
    }

    #[test]
    #[ignore = "feature gap: deleteFileAfterDeletingOnePartitionField same pattern but with multi-field spec; deletes the partitioned data file after one field is dropped"]
    fn test_delete_file_after_deleting_one_partition_field_per_java() {
        // Java: deleteFileAfterDeletingOnePartitionField.
    }

    // --- TestPartitionSpecInfo port ---------------------------------------
    //
    // Java's `TestPartitionSpecInfo` has 7 @TestTemplate methods, each
    // parametrised over format versions {V1, V2, V3}. The tests exercise:
    //
    //   - `PartitionSpec.unpartitioned()` factory + `isUnpartitioned()`
    //   - `PartitionSpec.builderFor(schema).alwaysNull(name)` (void
    //     transform shorthand) that still reports `isUnpartitioned`
    //   - `builderFor(schema).caseSensitive(bool).identity(...)` —
    //     case-insensitive name resolution + case-sensitive failure mode
    //   - `PartitionSpec.lastAssignedFieldId()` accessor
    //   - `table.spec()` / `table.specs()` map of historical specs
    //   - `table.ops().commit(base, base.updatePartitionSpec(newSpec))`
    //     spec-evolution path
    //   - `updateSchema().deleteColumn("id")` schema-evolution path
    //
    // Rust today has:
    //   - PartitionSpec data struct + builder, but NO `unpartitioned()`
    //     factory and NO `is_unpartitioned()` predicate.
    //   - NO `always_null` shorthand (void transform must be wired in
    //     manually as `Transform::Void` on a PartitionField).
    //   - NO `case_sensitive(bool)` builder setting. The builder doesn't
    //     resolve source names against a schema at all — caller supplies
    //     source-id directly.
    //   - NO `last_assigned_field_id()` accessor on PartitionSpec.
    //   - TableMetadata has `partition_specs` (the map) but NO
    //     `update_partition_spec()` op and NO `update_schema().delete_column()`.
    //
    // Each Java @TestTemplate is pinned as 1 Rust `#[ignore]` test
    // (format-version parametrisation isn't observable in the assertions —
    // the same body runs unchanged across V1/V2/V3).

    #[test]
    #[ignore = "feature gap: PartitionSpec::is_unpartitioned() not implemented; all-void-transform spec should still report unpartitioned=true"]
    fn test_partition_spec_info_void_only_spec_is_unpartitioned_per_java() {
        // Java: testSpecIsUnpartitionedForVoidTranforms.
        // builderFor(schema).alwaysNull("id").alwaysNull("data").build()
        // -> spec.is_unpartitioned() == true.
    }

    #[test]
    #[ignore = "feature gap: PartitionSpec::unpartitioned() factory not implemented; no Table::spec()/specs() map accessor on Rust TableMetadata"]
    fn test_partition_spec_info_unpartitioned_table_pins_spec_map_per_java() {
        // Java: testSpecInfoUnpartitionedTable.
        // spec = PartitionSpec.unpartitioned();
        // spec.is_unpartitioned() == true;
        // table.spec() == spec;
        // table.spec().last_assigned_field_id() == spec.last_assigned_field_id();
        // table.specs() == { spec.spec_id() -> spec }.
    }

    #[test]
    #[ignore = "feature gap: same accessors on a partitioned table — table.spec()/specs() not exposed in Rust"]
    fn test_partition_spec_info_partitioned_table_pins_spec_map_per_java() {
        // Java: testSpecInfoPartitionedTable.
        // spec = builderFor(schema).identity("data").build();
        // table.spec() == spec;
        // table.spec().last_assigned_field_id() == spec.last_assigned_field_id();
        // table.specs() == { spec.spec_id() -> spec }.
    }

    #[test]
    #[ignore = "feature gap: PartitionSpec builder has no case_sensitive(bool) option; case-insensitive name resolution unimplemented"]
    fn test_partition_spec_info_partitioned_case_insensitive_per_java() {
        // Java: testSpecInfoPartitionedTableCaseInsensitive.
        // builderFor(schema).caseSensitive(false).identity("DATA").build()
        // resolves "DATA" against "data" successfully.
        // Resulting spec, when applied to a table, matches table.spec()
        // and last_assigned_field_id.
    }

    #[test]
    #[ignore = "feature gap: PartitionSpec builder has no case_sensitive(bool); the case-sensitive failure path 'Cannot find source column: DATA' is not pinned in Rust"]
    fn test_partition_spec_info_partitioned_case_sensitive_fails_per_java() {
        // Java: testSpecInfoPartitionedTableCaseSensitiveFails.
        // builderFor(schema).caseSensitive(true).identity("DATA").build()
        // -> IllegalArgumentException "Cannot find source column: DATA".
    }

    #[test]
    #[ignore = "feature gap: no table-level updateSchema().deleteColumn() op and no PartitionSpec update_partition_spec(newSpec) reducer"]
    fn test_partition_spec_info_column_drop_with_spec_evolution_per_java() {
        // Java: testColumnDropWithPartitionSpecEvolution.
        // Start: spec.identity("id"); table created.
        // Commit a new spec (identity("data"), spec_id=1) via
        // base.updatePartitionSpec.
        // Then updateSchema().deleteColumn("id").commit().
        // Expected: table.spec() == newSpec;
        // table.specs() == {old_spec_id->old_spec, 1->newSpec};
        // table.schema() == Schema { data:string (id=2) }.
    }

    #[test]
    #[ignore = "feature gap: spec-evolution path (bucket(data,4) -> bucket(data,10)) requires update_partition_spec, not implemented in Rust"]
    fn test_partition_spec_info_v1_spec_evolution_per_java() {
        // Java: testSpecInfoPartitionSpecEvolutionForV1Table.
        // Start: spec.bucket("data", 4); table created.
        // Commit a new spec (bucket("data", 10), spec_id=1).
        // Expected: table.spec() == newSpec;
        // table.specs() == {0->old_spec, 1->newSpec}.
    }

    // --- TestPartitionSpecBuilderCaseSensitivity port ----------------------
    //
    // Java's `PartitionSpec.builderFor(schema).caseSensitive(bool).<transform>(
    // sourceName, [args,] [targetName])` exhaustively pins how the builder:
    //   - Resolves source column names against the schema, with a
    //     case-sensitive (default) and case-insensitive mode.
    //   - Validates that the same TARGET partition name isn't used twice
    //     (with the same case-sensitivity rules).
    //   - Auto-generates a default target name when one isn't supplied
    //     (e.g. "data_bucket", "data_trunc", "data_year", etc.).
    //
    // Rust today has:
    //   - `PartitionSpecBuilder` (via `derive_builder`), but no
    //     `case_sensitive(bool)` option and no source-name resolution at
    //     all — the caller supplies the source-id directly.
    //   - No `always_null` shorthand.
    //   - No default target-name generation (caller supplies the name).
    //   - No name-uniqueness validation across partition fields.
    //
    // All 58 Java @Test scenarios are pinned `#[ignore]` here for the
    // eventual `partition_spec_builder::case_sensitive(bool)` /
    // `.<transform>(source_name, ..., [target_name])` builder surface.
    // The pattern across transforms is consistent — pinning all 58 keeps
    // the spec contract behind every variant on the grid:
    //
    //   transforms = { identity, bucket, truncate, year, month, day,
    //                  hour, always_null (void) }
    //   axes       = { default target name, default + case-insensitive,
    //                  source exact duplicate (cs / ci),
    //                  source inexact duplicate (cs / ci),
    //                  target exact duplicate (cs / ci),
    //                  target inexact duplicate (ci),
    //                  target exact duplicate (cs disallowed) }
    //
    // The fixture schemas used by Java are:
    //   - SCHEMA_WITHOUT_NAME_CONFLICTS: id, data, category, order_date,
    //     order_time, ship_date, ship_time.
    //   - SCHEMA_WITH_NAME_CONFLICTS: id, data, DATA, order_date,
    //     ORDER_DATE, order_time, ORDER_TIME.

    // --- Top-level: column names differing in case ---

    #[test]
    #[ignore = "feature gap: partition_type() on a spec with identity('data') + identity('DATA') against a schema that has both columns must produce 2 partition fields with ids 1000+1001 named 'data' and 'DATA'"]
    fn test_partition_spec_builder_column_names_differ_only_in_case_per_java() {
        // Java: testPartitionTypeWithColumnNamesThatDifferOnlyInLetterCase.
        // Schema: id, data, DATA, order_date.
        // Spec: identity("data").identity("DATA").
        // Expected partition fields: (1000, "data", String), (1001, "DATA", String).
    }

    #[test]
    #[ignore = "feature gap: identity('data', 'partition1') should produce a partition field named 'partition1' bound to source id of 'data'"]
    fn test_partition_spec_builder_identity_target_name_per_java() {
        // Java: testPartitionTypeWithIdentityTargetName.
        // Spec: identity("data", "partition1").
        // Expected: (1000, "partition1", String).
    }

    // --- bucket() transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: bucket source-name exact duplicates in different target names — case sensitive (default) accepts them when source columns differ only in case ('data' vs 'DATA')"]
    fn test_partition_spec_builder_bucket_source_allows_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // Spec on SCHEMA_WITH_NAME_CONFLICTS:
        //   bucket("data", 10, "partition1").bucket("data", 10, "PARTITION1").
        // Expected: (1000, "partition1", Int), (1001, "PARTITION1", Int).
    }

    #[test]
    #[ignore = "feature gap: bucket('data', 10) with no explicit target name must auto-generate 'data_bucket'"]
    fn test_partition_spec_builder_bucket_target_name_default_per_java() {
        // Java: testBucketTargetNameDefaultValue.
        // bucket("data", 10) -> (1000, "data_bucket", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).bucket('DATA', 10) resolves DATA to data and auto-generates 'data_bucket'"]
    fn test_partition_spec_builder_bucket_target_name_default_case_insensitive_per_java() {
        // Java: testBucketTargetNameDefaultValueCaseInsensitive.
        // case_sensitive(false).bucket("DATA", 10) -> (1000, "data_bucket", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows bucket source name 'data' and 'DATA' with different explicit target names"]
    fn test_partition_spec_builder_bucket_source_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // Spec: case_sensitive(false).bucket("data", 10, "partition1").bucket("DATA", 10, "PARTITION1").
        // Expected: (1000, "partition1", Int), (1001, "PARTITION1", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows bucket target names that differ only in case"]
    fn test_partition_spec_builder_bucket_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // Spec: case_sensitive(false).bucket("data", 10, "partition1").bucket("category", 10, "PARTITION1").
        // Expected: (1000, "partition1", Int), (1001, "PARTITION1", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) must reject bucket target name reuse (exact duplicate) with 'Cannot use partition name more than once: partition1'"]
    fn test_partition_spec_builder_bucket_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).bucket("data", 10, "partition1").bucket("category", 10, "partition1")
        // throws IllegalArgumentException.
    }

    #[test]
    #[ignore = "feature gap: even case-sensitive mode must reject exact-duplicate target names"]
    fn test_partition_spec_builder_bucket_target_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // bucket("data", 10, "partition1").bucket("DATA", 10, "partition1")
        // throws IllegalArgumentException.
    }

    // --- truncate() transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: truncate('data', 10) must auto-generate 'data_trunc'"]
    fn test_partition_spec_builder_truncate_target_name_default_per_java() {
        // truncate("data", 10) -> (1000, "data_trunc", String).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).truncate('DATA', 10) -> 'data_trunc'"]
    fn test_partition_spec_builder_truncate_target_name_default_case_insensitive_per_java() {
        // case_sensitive(false).truncate("DATA", 10) -> (1000, "data_trunc", String).
    }

    #[test]
    #[ignore = "feature gap: truncate source name exact duplicate accepted when case sensitive (default)"]
    fn test_partition_spec_builder_truncate_source_allows_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // Spec on SCHEMA_WITH_NAME_CONFLICTS:
        //   truncate("data", 10, "partition1").truncate("data", 10, "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows truncate('data') and truncate('DATA') as inexact source duplicates"]
    fn test_partition_spec_builder_truncate_source_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).truncate("data", 10, "partition1").truncate("DATA", 10, "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows truncate target names that differ only in case"]
    fn test_partition_spec_builder_truncate_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).truncate("data", 10, "partition1").truncate("category", 10, "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) must reject exact-duplicate truncate target names"]
    fn test_partition_spec_builder_truncate_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).truncate("data", 10, "partition1").truncate("category", 10, "partition1")
        // throws.
    }

    #[test]
    #[ignore = "feature gap: case-sensitive mode must reject exact-duplicate truncate target names"]
    fn test_partition_spec_builder_truncate_target_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // truncate("data", 10, "partition1").truncate("DATA", 10, "partition1") throws.
    }

    // --- identity() transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: identity('data') with no explicit target name uses the source name as the target name"]
    fn test_partition_spec_builder_identity_target_name_default_per_java() {
        // identity("data") -> (1000, "data", String).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).identity('DATA') resolves to source 'data' and uses 'data' as the target name"]
    fn test_partition_spec_builder_identity_target_name_default_case_insensitive_per_java() {
        // case_sensitive(false).identity("DATA") -> (1000, "data", String).
    }

    #[test]
    #[ignore = "feature gap: case-sensitive mode rejects identity source-name exact duplicates (identity copies the name unless overridden)"]
    fn test_partition_spec_builder_identity_source_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // identity("data").identity("data") throws — same source means same target.
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) rejects inexact-duplicate identity source names ('data' and 'DATA') because they collapse to the same target"]
    fn test_partition_spec_builder_identity_source_rejects_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).identity("data").identity("DATA") throws.
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows explicit identity target names differing only in case"]
    fn test_partition_spec_builder_identity_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).identity("data", "partition1").identity("category", "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) rejects exact-duplicate identity target names"]
    fn test_partition_spec_builder_identity_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).identity("data", "partition1").identity("category", "partition1") throws.
    }

    #[test]
    #[ignore = "feature gap: case-sensitive mode rejects exact-duplicate identity target names"]
    fn test_partition_spec_builder_identity_target_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // identity("data", "partition1").identity("DATA", "partition1") throws.
    }

    // --- always_null() (void) transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: always_null('data') with no explicit target name uses source name 'data' as target"]
    fn test_partition_spec_builder_always_null_target_name_default_per_java() {
        // always_null("data") -> (1000, "data", String) with Transform::Void.
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).always_null('DATA') resolves to source 'data', target 'data'"]
    fn test_partition_spec_builder_always_null_target_name_default_case_insensitive_per_java() {
        // case_sensitive(false).always_null("DATA") -> (1000, "data", String).
    }

    #[test]
    #[ignore = "feature gap: case-sensitive always_null accepts exact-duplicate source names with different target names"]
    fn test_partition_spec_builder_always_null_source_allows_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // always_null("data", "partition1").always_null("data", "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows always_null inexact-duplicate source names"]
    fn test_partition_spec_builder_always_null_source_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).always_null("data", "partition1").always_null("DATA", "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows always_null target names differing only in case"]
    fn test_partition_spec_builder_always_null_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).always_null("data", "partition1").always_null("category", "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) rejects exact-duplicate always_null target names"]
    fn test_partition_spec_builder_always_null_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).always_null("data", "partition1").always_null("category", "partition1") throws.
    }

    #[test]
    #[ignore = "feature gap: case-sensitive mode rejects exact-duplicate always_null target names"]
    fn test_partition_spec_builder_always_null_target_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // always_null("data", "partition1").always_null("DATA", "partition1") throws.
    }

    // --- year() transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: year('order_date') must auto-generate 'order_date_year'"]
    fn test_partition_spec_builder_year_target_name_default_per_java() {
        // year("order_date") -> (1000, "order_date_year", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).year('ORDER_DATE') -> 'order_date_year'"]
    fn test_partition_spec_builder_year_target_name_default_case_insensitive_per_java() {
        // case_sensitive(false).year("ORDER_DATE") -> (1000, "order_date_year", Int).
    }

    #[test]
    #[ignore = "feature gap: case-sensitive year exact source duplicate rejected (same source -> same default target)"]
    fn test_partition_spec_builder_year_source_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // year("order_date").year("order_date") throws.
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) rejects year inexact-duplicate source names"]
    fn test_partition_spec_builder_year_source_rejects_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).year("order_date").year("ORDER_DATE") throws.
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) allows explicit year target names differing only in case"]
    fn test_partition_spec_builder_year_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).year("order_date", "partition1").year("ship_date", "PARTITION1").
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) rejects exact-duplicate year target names"]
    fn test_partition_spec_builder_year_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
        // case_sensitive(false).year("order_date", "partition1").year("ship_date", "partition1") throws.
    }

    #[test]
    #[ignore = "feature gap: case-sensitive mode rejects exact-duplicate year target names"]
    fn test_partition_spec_builder_year_target_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
        // year("order_date", "partition1").year("ORDER_DATE", "partition1") throws.
    }

    // --- month() transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: month('order_date') -> 'order_date_month'"]
    fn test_partition_spec_builder_month_target_name_default_per_java() {
        // month("order_date") -> (1000, "order_date_month", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).month('ORDER_DATE') -> 'order_date_month'"]
    fn test_partition_spec_builder_month_target_name_default_case_insensitive_per_java() {}

    #[test]
    #[ignore = "feature gap: case-sensitive month source exact duplicate rejected"]
    fn test_partition_spec_builder_month_source_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) month source inexact duplicate rejected"]
    fn test_partition_spec_builder_month_source_rejects_inexact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) month target allows inexact duplicate"]
    fn test_partition_spec_builder_month_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) month target rejects exact duplicate"]
    fn test_partition_spec_builder_month_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case-sensitive month target rejects exact duplicate"]
    fn test_partition_spec_builder_month_target_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
    }

    // --- day() transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: day('order_date') -> 'order_date_day'"]
    fn test_partition_spec_builder_day_target_name_default_per_java() {
        // day("order_date") -> (1000, "order_date_day", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).day('ORDER_DATE') -> 'order_date_day'"]
    fn test_partition_spec_builder_day_target_name_default_case_insensitive_per_java() {}

    #[test]
    #[ignore = "feature gap: case-sensitive day source exact duplicate rejected"]
    fn test_partition_spec_builder_day_source_rejects_exact_duplicate_when_case_sensitive_per_java()
    {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) day source inexact duplicate rejected"]
    fn test_partition_spec_builder_day_source_rejects_inexact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) day target allows inexact duplicate"]
    fn test_partition_spec_builder_day_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) day target rejects exact duplicate"]
    fn test_partition_spec_builder_day_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case-sensitive day target rejects exact duplicate"]
    fn test_partition_spec_builder_day_target_rejects_exact_duplicate_when_case_sensitive_per_java()
    {
    }

    // --- hour() transform: 7 scenarios ---

    #[test]
    #[ignore = "feature gap: hour('order_time') -> 'order_time_hour'"]
    fn test_partition_spec_builder_hour_target_name_default_per_java() {
        // hour("order_time") -> (1000, "order_time_hour", Int).
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false).hour('ORDER_TIME') -> 'order_time_hour'"]
    fn test_partition_spec_builder_hour_target_name_default_case_insensitive_per_java() {}

    #[test]
    #[ignore = "feature gap: case-sensitive hour source exact duplicate rejected"]
    fn test_partition_spec_builder_hour_source_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) hour source inexact duplicate rejected"]
    fn test_partition_spec_builder_hour_source_rejects_inexact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) hour target allows inexact duplicate"]
    fn test_partition_spec_builder_hour_target_allows_inexact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case_sensitive(false) hour target rejects exact duplicate"]
    fn test_partition_spec_builder_hour_target_rejects_exact_duplicate_when_case_insensitive_per_java(
    ) {
    }

    #[test]
    #[ignore = "feature gap: case-sensitive hour target rejects exact duplicate"]
    fn test_partition_spec_builder_hour_target_rejects_exact_duplicate_when_case_sensitive_per_java(
    ) {
    }

    // --- TestPartitionSpecValidation port ---------------------------------
    //
    // Java's `PartitionSpec.builderFor(schema)` performs extensive
    // validation when adding partition fields: rejects duplicate field
    // names, rejects redundant time-bucket transforms on the same source
    // column, rejects target names colliding with schema column names,
    // rejects non-primitive source types, and walks dotted source paths
    // into nested structs (but not lists or maps).
    //
    // Rust's PartitionSpecBuilder (via derive_builder) does NONE of this
    // validation. Caller supplies source-id directly and there's no
    // schema-aware resolution. All 20 Java @Test scenarios are pinned
    // `#[ignore]` here for an eventual `PartitionSpec::builder_for(&Schema)`
    // surface with these validations.

    #[test]
    #[ignore = "feature gap: builder must reject 10 redundant time-transform combinations on the same timestamp column ('Cannot use partition name more than once' for exact dupes; 'Cannot add redundant partition' for year/month/day/hour cross-pairs)"]
    fn test_partition_spec_validation_multiple_timestamp_partitions_per_java() {
        // Java: testMultipleTimestampPartitions.
        // Same ts column: year+year, year+month, year+day, year+hour,
        // month+month, month+day, month+hour, day+day, day+hour,
        // hour+hour all throw IllegalArgumentException.
    }

    #[test]
    #[ignore = "feature gap: builder must reject 6 redundant time-transform combinations on the same date column"]
    fn test_partition_spec_validation_multiple_date_partitions_per_java() {
        // Java: testMultipleDatePartitions.
        // Same d column: year+year, year+month, year+day, month+month,
        // month+day, day+day all throw IllegalArgumentException.
    }

    #[test]
    #[ignore = "feature gap: builder must ACCEPT 10 time-transform combinations on DIFFERENT source columns"]
    fn test_partition_spec_validation_multiple_timestamp_partitions_with_different_source_columns_per_java(
    ) {
        // Java: testMultipleTimestampPartitionsWithDifferentSourceColumns.
        // ts + another_ts: year+year, year+month, ..., hour+hour all build successfully.
    }

    #[test]
    #[ignore = "feature gap: builder must ACCEPT 10 time-transform combinations on different date source columns"]
    fn test_partition_spec_validation_multiple_date_partitions_with_different_source_columns_per_java(
    ) {
        // Java: testMultipleDatePartitionsWithDifferentSourceColumns.
        // d + another_d: same matrix as timestamp version.
    }

    #[test]
    #[ignore = "feature gap: builder must allow identity-of-multiple-distinct-sources but reject duplicate identity targets (4 sub-assertions)"]
    fn test_partition_spec_validation_multiple_identity_partitions_per_java() {
        // Java: testMultipleIdentityPartitions.
        // year(d).identity(id).identity(d).identity(s) — OK.
        // identity(id).identity(id) throws ("more than once").
        // identity(id).identity(id, "test-id") throws ("redundant partition").
        // identity(id, "test-id").identity(d, "test-id") throws ("more than once").
    }

    #[test]
    #[ignore = "feature gap: builder must accept custom partition target names for year/month/day/hour/bucket/truncate (6 happy paths verifying spec.fields()[0].name)"]
    fn test_partition_spec_validation_custom_target_names_per_java() {
        // Java: testSettingPartitionTransformsWithCustomTargetNames.
        // year("ts", "custom_year") -> spec.fields[0].name == "custom_year".
        // Same for month/day/hour/bucket(4)/truncate(1).
    }

    #[test]
    #[ignore = "feature gap: builder must reject custom target names that collide with an existing schema column name (7 sub-assertions for year/month/day/hour/truncate/bucket/identity)"]
    fn test_partition_spec_validation_custom_target_names_that_already_exist_per_java() {
        // Java: testSettingPartitionTransformsWithCustomTargetNamesThatAlreadyExist.
        // year("ts", "another_ts") -> 'Cannot create partition from name
        // that exists in schema: another_ts'. Same for month/day/hour/
        // truncate/bucket. identity has a different message:
        // 'Cannot create identity partition sourced from different field
        // in schema: another_ts'.
    }

    #[test]
    #[ignore = "feature gap: builder must support manual add(sourceId, fieldId, name, transform) with a source id NOT present in the current schema (legacy dropped-column case)"]
    fn test_partition_spec_validation_stale_source_id_with_reused_column_name_per_java() {
        // Java: testStalePartitionSourceIdWithReusedColumnName.
        // Schema has only newFieldId=2 "category"; builder adds
        // alwaysNull on droppedFieldId=1 (same name "category").
        // spec.fields has 1 entry with sourceId=1, name="category".
    }

    #[test]
    #[ignore = "feature gap: builder must reject every transform on a missing source column with 'Cannot find source column: missing' (7 sub-assertions)"]
    fn test_partition_spec_validation_missing_source_column_per_java() {
        // Java: testMissingSourceColumn.
        // year/month/day/hour/bucket/truncate/identity on "missing" all
        // throw IllegalArgumentException with that exact message.
    }

    #[test]
    #[ignore = "feature gap: auto field-id assignment starts at 1000 and increments per added field; lastAssignedFieldId() reflects the highest"]
    fn test_partition_spec_validation_auto_setting_field_ids_per_java() {
        // Java: testAutoSettingPartitionFieldIds.
        // 4-field spec via year/bucket/add/truncate -> field ids
        // 1000, 1001, 1002, 1003. lastAssignedFieldId == 1003.
    }

    #[test]
    #[ignore = "feature gap: builder must accept explicit field-ids on add() and use them verbatim"]
    fn test_partition_spec_validation_explicit_field_ids_per_java() {
        // Java: testAddPartitionFieldsWithFieldIds.
        // add(1, 1005, ...).add(1, 1006, ...).add(1, 1002, ...) ->
        // ids = [1005, 1006, 1002]; lastAssignedFieldId == 1006.
    }

    #[test]
    #[ignore = "feature gap: builder must mix explicit and auto-assigned field-ids; auto starts after the highest explicit id"]
    fn test_partition_spec_validation_mixed_explicit_and_auto_field_ids_per_java() {
        // Java: testAddPartitionFieldsWithAndWithoutFieldIds.
        // add(1, "id_partition2", bucket(5))         -> field id 1000 (auto).
        // add(1, 1005, "id_partition1", bucket(4))   -> field id 1005 (explicit).
        // truncate("s", 1, "custom_truncate")        -> field id 1006 (auto continues from 1005).
        // lastAssignedFieldId == 1006.
    }

    #[test]
    #[ignore = "feature gap: builder must reject non-primitive source field types — 4 parametrized cases: Variant, Geometry (CRS84), Geography (CRS84), Unknown — each yields a ValidationException with a type-specific message"]
    fn test_partition_spec_validation_unsupported_source_types_per_java() {
        // Java: testUnsupported (@ParameterizedTest).
        // sourceId=7 (variant)  -> 'Cannot partition by non-primitive source field: variant'.
        // sourceId=8 (geom)     -> 'Invalid source type geometry for transform: bucket[5]'.
        // sourceId=9 (geog)     -> 'Invalid source type geography for transform: bucket[5]'.
        // sourceId=10 (unknown) -> 'Invalid source type unknown for transform: bucket[5]'.
    }

    #[test]
    #[ignore = "feature gap: add(sourceId, fieldId, name, transform) must reject when sourceId isn't in the schema with 'Cannot find source column for partition field: 1000: Test: identity(99)'"]
    fn test_partition_spec_validation_source_id_not_found_per_java() {
        // Java: testSourceIdNotFound.
        // builderFor(SCHEMA).add(99, 1000, "Test", identity()) throws
        // ValidationException with the above message.
    }

    #[test]
    #[ignore = "feature gap: builder must walk a dotted source path 'MyStruct.id' into a nested struct"]
    fn test_partition_spec_validation_partition_field_in_struct_per_java() {
        // Java: testPartitionFieldInStruct.
        // Schema with MyStruct: Struct(...); identity("MyStruct.id") builds OK.
    }

    #[test]
    #[ignore = "feature gap: builder must walk 'Outer.Inner.id' through 2 levels of nested struct"]
    fn test_partition_spec_validation_partition_field_in_struct_in_struct_per_java() {
        // Java: testPartitionFieldInStructInStruct.
        // Schema with Outer.Inner.id; identity("Outer.Inner.id") builds OK.
    }

    #[test]
    #[ignore = "feature gap: builder must reject 'MyList.element' with 'Invalid partition field parent: list<int>'"]
    fn test_partition_spec_validation_partition_field_in_list_per_java() {
        // Java: testPartitionFieldInList.
        // Schema with MyList: List<Int>; identity("MyList.element") throws
        // ValidationException with the above message.
    }

    #[test]
    #[ignore = "feature gap: builder must reject 'MyList.element.Foo' even when the list's element is a struct"]
    fn test_partition_spec_validation_partition_field_in_struct_in_list_per_java() {
        // Java: testPartitionFieldInStructInList.
        // Schema: MyList: List<Struct(Foo: int)>; identity("MyList.element.Foo") throws.
        // Message: 'Invalid partition field parent: list<struct<1: Foo: optional int>>'.
    }

    #[test]
    #[ignore = "feature gap: builder must reject both 'MyMap.key' and 'MyMap.value' for any map type"]
    fn test_partition_spec_validation_partition_field_in_map_per_java() {
        // Java: testPartitionFieldInMap.
        // Schema with MyMap: Map<int, int>.
        // identity("MyMap.key")   throws with 'Invalid partition field parent: map<int, int>'.
        // identity("MyMap.value") throws with same.
    }

    #[test]
    #[ignore = "feature gap: builder must reject map.key.field and map.value.field even when the key/value are structs"]
    fn test_partition_spec_validation_partition_field_in_struct_in_map_per_java() {
        // Java: testPartitionFieldInStructInMap.
        // Schema with MyMap: Map<Struct(Foo), Struct(Bar)>.
        // identity("MyMap.key.Foo")   throws.
        // identity("MyMap.value.Bar") throws.
        // Message includes both inner struct types.
    }
}
