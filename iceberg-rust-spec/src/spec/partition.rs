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

    // -----------------------------------------------------------------------
    // Placeholders for a schema-aware PartitionSpec builder with case-sensitivity.
    //
    // Rust's `PartitionSpecBuilder` (via derive_builder) requires the caller to
    // pass source_id directly: it has no schema-aware name resolution, no
    // `case_sensitive(bool)` opt-in, no `always_null` shorthand, no default
    // target-name generator, and no name-uniqueness validation. Each test below
    // pins one cell of the upstream {transform x case-sensitivity x duplicate-class}
    // grid.
    // -----------------------------------------------------------------------

    const SPEC_BUILDER_GAP: &str = "no PartitionSpec::builder_for(schema) + case_sensitive setter";

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) + case_sensitive setter"]
    fn test_partition_spec_with_two_columns_differing_only_in_case_succeeds() {
        // Schema with both "data" and "DATA" supports `identity("data")` + `identity("DATA")`
        // under default case_sensitive=true.
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) + identity target-name override"]
    fn test_identity_target_name_override_succeeds() {
        // `identity("data", "data_partition")` emits a partition field with target_name="data_partition".
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- bucket transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) bucket exact-duplicate source"]
    fn test_bucket_source_name_exact_duplicate_allowed_when_case_sensitive() {
        // case_sensitive(true): two bucket(...) calls naming the same source column (with
        // different target names) succeed.
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) bucket default target name"]
    fn test_bucket_target_name_defaults_to_source_bucket() {
        // `bucket("data", 16)` defaults the target name to "data_bucket".
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) bucket default target name (case-insensitive)"]
    fn test_bucket_target_name_default_under_case_insensitive() {
        // case_sensitive(false).bucket("DATA", 16) defaults target name to "data_bucket".
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) bucket inexact-duplicate source (case-insensitive)"]
    fn test_bucket_source_inexact_duplicate_allowed_under_case_insensitive() {
        // case_sensitive(false).bucket("DATA", 16).bucket("data", 32) succeeds.
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) bucket inexact target duplicate (case-insensitive)"]
    fn test_bucket_target_inexact_duplicate_allowed_under_case_insensitive() {
        // case_sensitive(false): two bucket targets that differ only in case both accepted.
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) bucket exact target duplicate (case-insensitive)"]
    fn test_bucket_target_exact_duplicate_rejected_under_case_insensitive() {
        // case_sensitive(false): two bucket targets sharing an identical lower-cased name → reject.
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) bucket exact target duplicate (case-sensitive)"]
    fn test_bucket_target_exact_duplicate_rejected_under_case_sensitive() {
        // case_sensitive(true): two bucket targets sharing the exact same target name → reject.
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- truncate transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) truncate default target name"]
    fn test_truncate_target_name_defaults_to_source_trunc() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) truncate default target name (case-insensitive)"]
    fn test_truncate_target_name_default_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) truncate exact-duplicate source"]
    fn test_truncate_source_name_exact_duplicate_allowed_when_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) truncate inexact-duplicate source (case-insensitive)"]
    fn test_truncate_source_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) truncate inexact target duplicate (case-insensitive)"]
    fn test_truncate_target_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) truncate exact target duplicate (case-insensitive)"]
    fn test_truncate_target_exact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) truncate exact target duplicate (case-sensitive)"]
    fn test_truncate_target_exact_duplicate_rejected_under_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- identity transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) identity default target name"]
    fn test_identity_target_name_defaults_to_source_name() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) identity default target name (case-insensitive)"]
    fn test_identity_target_name_default_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) identity exact-duplicate source rejected"]
    fn test_identity_source_exact_duplicate_rejected_when_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) identity inexact-duplicate source rejected (case-insensitive)"]
    fn test_identity_source_inexact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) identity inexact target duplicate (case-insensitive)"]
    fn test_identity_target_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) identity exact target duplicate (case-insensitive)"]
    fn test_identity_target_exact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) identity exact target duplicate (case-sensitive)"]
    fn test_identity_target_exact_duplicate_rejected_under_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- always_null (void) transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) always_null default target name"]
    fn test_always_null_target_name_defaults_to_source_null() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) always_null default target name (case-insensitive)"]
    fn test_always_null_target_name_default_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) always_null exact-duplicate source"]
    fn test_always_null_source_name_exact_duplicate_allowed_when_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) always_null inexact-duplicate source (case-insensitive)"]
    fn test_always_null_source_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) always_null inexact target duplicate (case-insensitive)"]
    fn test_always_null_target_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) always_null exact target duplicate (case-insensitive)"]
    fn test_always_null_target_exact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) always_null exact target duplicate (case-sensitive)"]
    fn test_always_null_target_exact_duplicate_rejected_under_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- year transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) year default target name"]
    fn test_year_target_name_defaults_to_source_year() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) year default target name (case-insensitive)"]
    fn test_year_target_name_default_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) year exact-duplicate source rejected"]
    fn test_year_source_exact_duplicate_rejected_when_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) year inexact-duplicate source rejected (case-insensitive)"]
    fn test_year_source_inexact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) year inexact target duplicate (case-insensitive)"]
    fn test_year_target_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) year exact target duplicate (case-insensitive)"]
    fn test_year_target_exact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) year exact target duplicate (case-sensitive)"]
    fn test_year_target_exact_duplicate_rejected_under_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- month transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) month default target name"]
    fn test_month_target_name_defaults_to_source_month() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) month default target name (case-insensitive)"]
    fn test_month_target_name_default_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) month exact-duplicate source rejected"]
    fn test_month_source_exact_duplicate_rejected_when_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) month inexact-duplicate source rejected (case-insensitive)"]
    fn test_month_source_inexact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) month inexact target duplicate (case-insensitive)"]
    fn test_month_target_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) month exact target duplicate (case-insensitive)"]
    fn test_month_target_exact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) month exact target duplicate (case-sensitive)"]
    fn test_month_target_exact_duplicate_rejected_under_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- day transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) day default target name"]
    fn test_day_target_name_defaults_to_source_day() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) day default target name (case-insensitive)"]
    fn test_day_target_name_default_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) day exact-duplicate source rejected"]
    fn test_day_source_exact_duplicate_rejected_when_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) day inexact-duplicate source rejected (case-insensitive)"]
    fn test_day_source_inexact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) day inexact target duplicate (case-insensitive)"]
    fn test_day_target_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) day exact target duplicate (case-insensitive)"]
    fn test_day_target_exact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) day exact target duplicate (case-sensitive)"]
    fn test_day_target_exact_duplicate_rejected_under_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // --- hour transform ---

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) hour default target name"]
    fn test_hour_target_name_defaults_to_source_hour() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) hour default target name (case-insensitive)"]
    fn test_hour_target_name_default_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) hour exact-duplicate source rejected"]
    fn test_hour_source_exact_duplicate_rejected_when_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) hour inexact-duplicate source rejected (case-insensitive)"]
    fn test_hour_source_inexact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) hour inexact target duplicate (case-insensitive)"]
    fn test_hour_target_inexact_duplicate_allowed_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) hour exact target duplicate (case-insensitive)"]
    fn test_hour_target_exact_duplicate_rejected_under_case_insensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema) hour exact target duplicate (case-sensitive)"]
    fn test_hour_target_exact_duplicate_rejected_under_case_sensitive() {
        unimplemented!("{SPEC_BUILDER_GAP}");
    }

    // -----------------------------------------------------------------------
    // Placeholders for PartitionSpec accessors + evolution.
    //
    // Rust gaps: no `PartitionSpec::unpartitioned()` factory, no
    // `is_unpartitioned()` predicate, no `always_null` shorthand, no
    // `last_assigned_field_id()` accessor, no schema-aware case-sensitivity
    // builder option, no `update_partition_spec()` table-level op, and no
    // `update_schema().delete_column()` builder. Each test below pins one
    // observable scenario for the eventual surface.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "no PartitionSpec::is_unpartitioned predicate"]
    fn test_partition_spec_with_only_void_transforms_reports_unpartitioned() {
        // A PartitionSpec whose every field uses Transform::Void / always_null reports
        // is_unpartitioned() == true.
        unimplemented!("PartitionSpec::is_unpartitioned");
    }

    #[test]
    #[ignore = "no PartitionSpec::unpartitioned factory + table.spec() accessors"]
    fn test_unpartitioned_table_reports_empty_default_partition_spec() {
        // PartitionSpec::unpartitioned() returns spec with 0 fields and the default spec_id.
        // table.spec() / table.specs() reflect this; last_assigned_field_id is 999 (one before
        // the first partition field id).
        unimplemented!("PartitionSpec::unpartitioned + table accessors");
    }

    #[test]
    #[ignore = "no PartitionSpec::is_unpartitioned, no table-level accessors"]
    fn test_partitioned_table_exposes_spec_accessors_and_last_assigned_field_id() {
        // table.spec().spec_id() / table.specs() map / last_assigned_field_id() report
        // the values set by the builder.
        unimplemented!("PartitionSpec table accessors");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema).case_sensitive(false)"]
    fn test_partitioned_table_builder_resolves_source_column_case_insensitively() {
        // `case_sensitive(false).identity("DATA")` resolves to the lower-case "data" column.
        unimplemented!("PartitionSpec case-insensitive name resolution");
    }

    #[test]
    #[ignore = "no PartitionSpec::builder_for(schema).case_sensitive(true) name lookup error"]
    fn test_partitioned_table_builder_rejects_unknown_column_under_case_sensitive() {
        // `case_sensitive(true).identity("DATA")` against a schema that only has "data" rejects
        // with `Cannot find source column: DATA`.
        unimplemented!("PartitionSpec case-sensitive name lookup");
    }

    #[test]
    #[ignore = "no table-level update_partition_spec + update_schema().delete_column() builders"]
    fn test_dropping_partition_source_column_preserves_historical_specs() {
        // Start with spec A. `update_partition_spec(newSpec)` introduces spec B. Then
        // `update_schema().delete_column("id")` drops the column that A's fields reference.
        // table.specs() keeps both A and B entries; the dropped column doesn't appear in B.
        unimplemented!("partition spec + schema evolution");
    }

    #[test]
    #[ignore = "no table-level update_partition_spec evolution"]
    fn test_partition_spec_evolution_introduces_new_spec_for_v1_table() {
        // `bucket("data", 4)` then `update_partition_spec(bucket("data", 10))` produces
        // a V1 table whose specs map carries both entries with distinct spec ids.
        unimplemented!("partition spec evolution v1");
    }

    // -----------------------------------------------------------------------
    // Placeholders for strict + inclusive Transform projection and residual
    // rewriting. Rust today only exposes the forward `Value::transform` path
    // for bucket / truncate / year / month / day / hour; projection of an
    // arbitrary BoundPredicate through the transform (strict for upper/lower
    // bounds; inclusive for membership) and the resulting residual expression
    // after partition pruning are not modelled.
    // -----------------------------------------------------------------------

    const PROJECTION_GAP: &str = "no Transform::project_strict / project_inclusive / Residuals";

    // --- TestBucketingProjection (12) ---

    #[test]
    #[ignore = "no Transform::project_strict for bucket(int)"]
    fn test_bucket_projection_int_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for bucket(int)"]
    fn test_bucket_projection_int_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for bucket(long)"]
    fn test_bucket_projection_long_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for bucket(long)"]
    fn test_bucket_projection_long_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for bucket(decimal)"]
    fn test_bucket_projection_decimal_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for bucket(decimal)"]
    fn test_bucket_projection_decimal_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for bucket(string)"]
    fn test_bucket_projection_string_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for bucket(string)"]
    fn test_bucket_projection_string_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for bucket(binary)"]
    fn test_bucket_projection_byte_buffer_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for bucket(binary)"]
    fn test_bucket_projection_byte_buffer_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for bucket(uuid)"]
    fn test_bucket_projection_uuid_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for bucket(uuid)"]
    fn test_bucket_projection_uuid_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }

    // --- TestTruncatesProjection (16) ---

    #[test]
    #[ignore = "no Transform::project_strict for truncate(int) lower bound"]
    fn test_truncate_projection_int_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for truncate(int) upper bound"]
    fn test_truncate_projection_int_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(int) lower bound"]
    fn test_truncate_projection_int_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(int) upper bound"]
    fn test_truncate_projection_int_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for truncate(long) lower bound"]
    fn test_truncate_projection_long_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for truncate(long) upper bound"]
    fn test_truncate_projection_long_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(long) lower bound"]
    fn test_truncate_projection_long_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(long) upper bound"]
    fn test_truncate_projection_long_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for truncate(decimal) lower bound"]
    fn test_truncate_projection_decimal_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for truncate(decimal) upper bound"]
    fn test_truncate_projection_decimal_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(decimal) lower bound"]
    fn test_truncate_projection_decimal_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(decimal) upper bound"]
    fn test_truncate_projection_decimal_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for truncate(string)"]
    fn test_truncate_projection_string_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(string)"]
    fn test_truncate_projection_string_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for truncate(binary)"]
    fn test_truncate_projection_binary_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for truncate(binary)"]
    fn test_truncate_projection_binary_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }

    // --- TestTruncatesResiduals (2) ---

    #[test]
    #[ignore = "no Transform::residual for truncate(int)"]
    fn test_truncate_residual_int_pushes_through_known_partition_value() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::residual for truncate(string)"]
    fn test_truncate_residual_string_pushes_through_known_partition_value() {
        unimplemented!("{PROJECTION_GAP}");
    }

    // --- TestDatesProjection (22) ---

    #[test]
    #[ignore = "no Transform::project_strict for month(date) at epoch"]
    fn test_dates_projection_month_strict_epoch() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(date) at epoch"]
    fn test_dates_projection_month_inclusive_epoch() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(date) post-epoch lower bound"]
    fn test_dates_projection_month_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(date) pre-epoch lower bound"]
    fn test_dates_projection_negative_month_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(date) post-epoch upper bound"]
    fn test_dates_projection_month_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(date) pre-epoch upper bound"]
    fn test_dates_projection_negative_month_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(date) post-epoch lower bound"]
    fn test_dates_projection_month_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(date) pre-epoch lower bound"]
    fn test_dates_projection_negative_month_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(date) post-epoch upper bound"]
    fn test_dates_projection_month_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(date) pre-epoch upper bound"]
    fn test_dates_projection_negative_month_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for day(date) post-epoch"]
    fn test_dates_projection_day_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for day(date) pre-epoch"]
    fn test_dates_projection_negative_day_strict() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for day(date) post-epoch"]
    fn test_dates_projection_day_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for day(date) pre-epoch"]
    fn test_dates_projection_negative_day_inclusive() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for year(date) post-epoch lower bound"]
    fn test_dates_projection_year_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for year(date) pre-epoch lower bound"]
    fn test_dates_projection_negative_year_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for year(date) post-epoch upper bound"]
    fn test_dates_projection_year_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for year(date) pre-epoch upper bound"]
    fn test_dates_projection_negative_year_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for year(date) post-epoch lower bound"]
    fn test_dates_projection_year_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for year(date) pre-epoch lower bound"]
    fn test_dates_projection_negative_year_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for year(date) post-epoch upper bound"]
    fn test_dates_projection_year_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for year(date) pre-epoch upper bound"]
    fn test_dates_projection_negative_year_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }

    // --- TestTimestampsProjection (26) ---

    #[test]
    #[ignore = "no Transform::project_strict for day(timestamp) at epoch"]
    fn test_timestamps_projection_day_strict_epoch() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for day(timestamp) at epoch"]
    fn test_timestamps_projection_day_inclusive_epoch() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(timestamp) post-epoch lower bound"]
    fn test_timestamps_projection_month_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(timestamp) pre-epoch lower bound"]
    fn test_timestamps_projection_negative_month_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(timestamp) post-epoch upper bound"]
    fn test_timestamps_projection_month_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for month(timestamp) pre-epoch upper bound"]
    fn test_timestamps_projection_negative_month_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(timestamp) post-epoch lower bound"]
    fn test_timestamps_projection_month_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(timestamp) pre-epoch lower bound"]
    fn test_timestamps_projection_negative_month_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(timestamp) post-epoch upper bound"]
    fn test_timestamps_projection_month_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for month(timestamp) pre-epoch upper bound"]
    fn test_timestamps_projection_negative_month_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for day(timestamp) post-epoch lower bound"]
    fn test_timestamps_projection_day_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for day(timestamp) pre-epoch lower bound"]
    fn test_timestamps_projection_negative_day_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for day(timestamp) post-epoch upper bound"]
    fn test_timestamps_projection_day_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for day(timestamp) pre-epoch upper bound"]
    fn test_timestamps_projection_negative_day_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for day(timestamp) post-epoch lower bound"]
    fn test_timestamps_projection_day_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for day(timestamp) pre-epoch lower bound"]
    fn test_timestamps_projection_negative_day_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for day(timestamp) post-epoch upper bound"]
    fn test_timestamps_projection_day_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for day(timestamp) pre-epoch upper bound"]
    fn test_timestamps_projection_negative_day_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for year(timestamp) lower bound"]
    fn test_timestamps_projection_year_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for year(timestamp) upper bound"]
    fn test_timestamps_projection_year_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for year(timestamp) lower bound"]
    fn test_timestamps_projection_year_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for year(timestamp) upper bound"]
    fn test_timestamps_projection_year_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for hour(timestamp) lower bound"]
    fn test_timestamps_projection_hour_strict_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_strict for hour(timestamp) upper bound"]
    fn test_timestamps_projection_hour_strict_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for hour(timestamp) lower bound"]
    fn test_timestamps_projection_hour_inclusive_lower_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
    #[test]
    #[ignore = "no Transform::project_inclusive for hour(timestamp) upper bound"]
    fn test_timestamps_projection_hour_inclusive_upper_bound() {
        unimplemented!("{PROJECTION_GAP}");
    }
}
