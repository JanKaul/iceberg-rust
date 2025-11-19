//! Manifest list handling and management for Iceberg tables.
//!
//! This module provides the core types and implementations for working with manifest lists,
//! which track all manifest files in a table snapshot. Key components include:
//!
//! - [`ManifestListEntry`] - Entries describing manifest files and their contents
//! - [`Content`] - Types of content tracked by manifests (data vs deletes)
//! - [`FieldSummary`] - Statistics and metadata about partition fields
//!
//! Manifest lists are a critical part of Iceberg's metadata hierarchy, providing an
//! index of all manifest files and enabling efficient manifest pruning during scans.
//! They include summary statistics that can be used to skip reading manifests that
//! don't contain relevant data for a query.

use std::sync::OnceLock;

use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::error::Error;

use self::_serde::{FieldSummarySerde, ManifestListEntryV1, ManifestListEntryV2};

use super::{
    table_metadata::{FormatVersion, TableMetadata},
    types::Type,
    values::Value,
};

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
#[serde(into = "ManifestListEntryEnum")]
/// A manifest list includes summary metadata that can be used to avoid scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a summary of values for each field of the partition spec used to write the manifest.
pub struct ManifestListEntry {
    /// Table format version
    pub format_version: FormatVersion,
    /// Location of the manifest file
    pub manifest_path: String,
    /// Length of the manifest file in bytes
    pub manifest_length: i64,
    /// ID of a partition spec used to write the manifest; must be listed in table metadata partition-specs
    pub partition_spec_id: i32,
    /// The type of files tracked by the manifest, either data or delete files; 0 for all v1 manifests
    pub content: Content,
    /// The sequence number when the manifest was added to the table; use 0 when reading v1 manifest lists
    pub sequence_number: i64,
    /// The minimum sequence number of all data or delete files in the manifest; use 0 when reading v1 manifest lists
    pub min_sequence_number: i64,
    /// ID of the snapshot where the manifest file was added
    pub added_snapshot_id: i64,
    /// Number of entries in the manifest that have status ADDED (1), when null this is assumed to be non-zero
    pub added_files_count: Option<i32>,
    /// Number of entries in the manifest that have status EXISTING (0), when null this is assumed to be non-zero
    pub existing_files_count: Option<i32>,
    /// Number of entries in the manifest that have status DELETED (2), when null this is assumed to be non-zero
    pub deleted_files_count: Option<i32>,
    /// Number of rows in all of files in the manifest that have status ADDED, when null this is assumed to be non-zero
    pub added_rows_count: Option<i64>,
    /// Number of rows in all of files in the manifest that have status EXISTING, when null this is assumed to be non-zero
    pub existing_rows_count: Option<i64>,
    /// Number of rows in all of files in the manifest that have status DELETED, when null this is assumed to be non-zero
    pub deleted_rows_count: Option<i64>,
    /// A list of field summaries for each partition field in the spec. Each field in the list corresponds to a field in the manifest file’s partition spec.
    pub partitions: Option<Vec<FieldSummary>>,
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
}

/// Entry in manifest file.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum ManifestListEntryEnum {
    /// Version 2 of the manifest file
    V2(ManifestListEntryV2),
    /// Version 1 of the manifest file
    V1(ManifestListEntryV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(into = "FieldSummarySerde")]
/// DataFile found in Manifest.
pub struct FieldSummary {
    /// Whether the manifest contains at least one partition with a null value for the field
    pub contains_null: bool,
    /// Whether the manifest contains at least one partition with a NaN value for the field
    pub contains_nan: Option<bool>,
    /// Lower bound for the non-null, non-NaN values in the partition field, or null if all values are null or NaN.
    /// If -0.0 is a value of the partition field, the lower_bound must not be +0.0
    pub lower_bound: Option<Value>,
    /// Upper bound for the non-null, non-NaN values in the partition field, or null if all values are null or NaN .
    /// If +0.0 is a value of the partition field, the upper_bound must not be -0.0.
    pub upper_bound: Option<Value>,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
/// Type of content stored by the data file.
pub enum Content {
    /// Data.
    Data = 0,
    /// Deletes
    Deletes = 1,
}

mod _serde {
    use crate::spec::table_metadata::FormatVersion;

    use super::{Content, FieldSummary, ManifestListEntry, ManifestListEntryEnum};
    use serde::{Deserialize, Serialize};
    use serde_bytes::ByteBuf;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// A manifest list includes summary metadata that can be used to avoid scanning all of the manifests in a snapshot when planning a table scan.
    /// This includes the number of added, existing, and deleted files, and a summary of values for each field of the partition spec used to write the manifest.
    pub struct ManifestListEntryV2 {
        /// Location of the manifest file
        pub manifest_path: String,
        /// Length of the manifest file in bytes
        pub manifest_length: i64,
        /// ID of a partition spec used to write the manifest; must be listed in table metadata partition-specs
        pub partition_spec_id: i32,
        /// The type of files tracked by the manifest, either data or delete files; 0 for all v1 manifests
        pub content: Content,
        /// The sequence number when the manifest was added to the table; use 0 when reading v1 manifest lists
        pub sequence_number: i64,
        /// The minimum sequence number of all data or delete files in the manifest; use 0 when reading v1 manifest lists
        pub min_sequence_number: i64,
        /// ID of the snapshot where the manifest file was added
        pub added_snapshot_id: i64,
        /// Number of entries in the manifest that have status ADDED (1), when null this is assumed to be non-zero
        pub added_files_count: i32,
        /// Number of entries in the manifest that have status EXISTING (0), when null this is assumed to be non-zero
        pub existing_files_count: i32,
        /// Number of entries in the manifest that have status DELETED (2), when null this is assumed to be non-zero
        pub deleted_files_count: i32,
        /// Number of rows in all of files in the manifest that have status ADDED, when null this is assumed to be non-zero
        pub added_rows_count: i64,
        /// Number of rows in all of files in the manifest that have status EXISTING, when null this is assumed to be non-zero
        pub existing_rows_count: i64,
        /// Number of rows in all of files in the manifest that have status DELETED, when null this is assumed to be non-zero
        pub deleted_rows_count: i64,
        /// A list of field summaries for each partition field in the spec. Each field in the list corresponds to a field in the manifest file’s partition spec.
        pub partitions: Option<Vec<FieldSummarySerde>>,
        /// Implementation-specific key metadata for encryption
        pub key_metadata: Option<ByteBuf>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// A manifest list includes summary metadata that can be used to avoid scanning all of the manifests in a snapshot when planning a table scan.
    /// This includes the number of added, existing, and deleted files, and a summary of values for each field of the partition spec used to write the manifest.
    pub struct ManifestListEntryV1 {
        /// Location of the manifest file
        pub manifest_path: String,
        /// Length of the manifest file in bytes
        pub manifest_length: i64,
        /// ID of a partition spec used to write the manifest; must be listed in table metadata partition-specs
        pub partition_spec_id: i32,
        /// ID of the snapshot where the manifest file was added
        pub added_snapshot_id: i64,
        /// Number of entries in the manifest that have status ADDED (1), when null this is assumed to be non-zero
        pub added_files_count: Option<i32>,
        /// Number of entries in the manifest that have status EXISTING (0), when null this is assumed to be non-zero
        pub existing_files_count: Option<i32>,
        /// Number of entries in the manifest that have status DELETED (2), when null this is assumed to be non-zero
        pub deleted_files_count: Option<i32>,
        /// Number of rows in all of files in the manifest that have status ADDED, when null this is assumed to be non-zero
        pub added_rows_count: Option<i64>,
        /// Number of rows in all of files in the manifest that have status EXISTING, when null this is assumed to be non-zero
        pub existing_rows_count: Option<i64>,
        /// Number of rows in all of files in the manifest that have status DELETED, when null this is assumed to be non-zero
        pub deleted_rows_count: Option<i64>,
        /// A list of field summaries for each partition field in the spec. Each field in the list corresponds to a field in the manifest file’s partition spec.
        pub partitions: Option<Vec<FieldSummarySerde>>,
        /// Implementation-specific key metadata for encryption
        pub key_metadata: Option<ByteBuf>,
    }

    impl From<ManifestListEntry> for ManifestListEntryEnum {
        fn from(value: ManifestListEntry) -> Self {
            match &value.format_version {
                FormatVersion::V2 => ManifestListEntryEnum::V2(value.into()),
                FormatVersion::V1 => ManifestListEntryEnum::V1(value.into()),
            }
        }
    }

    impl From<ManifestListEntry> for ManifestListEntryV1 {
        fn from(value: ManifestListEntry) -> Self {
            ManifestListEntryV1 {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                added_snapshot_id: value.added_snapshot_id,
                added_files_count: value.added_files_count,
                existing_files_count: value.existing_files_count,
                deleted_files_count: value.deleted_files_count,
                added_rows_count: value.added_rows_count,
                existing_rows_count: value.existing_rows_count,
                deleted_rows_count: value.deleted_rows_count,
                partitions: value
                    .partitions
                    .map(|v| v.into_iter().map(Into::into).collect()),
                key_metadata: value.key_metadata,
            }
        }
    }

    impl From<ManifestListEntry> for ManifestListEntryV2 {
        fn from(value: ManifestListEntry) -> Self {
            ManifestListEntryV2 {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                content: value.content,
                sequence_number: value.sequence_number,
                min_sequence_number: value.min_sequence_number,
                added_snapshot_id: value.added_snapshot_id,
                added_files_count: value.added_files_count.unwrap(),
                existing_files_count: value.existing_files_count.unwrap(),
                deleted_files_count: value.deleted_files_count.unwrap(),
                added_rows_count: value.added_rows_count.unwrap(),
                existing_rows_count: value.existing_rows_count.unwrap(),
                deleted_rows_count: value.deleted_rows_count.unwrap(),
                partitions: value
                    .partitions
                    .map(|v| v.into_iter().map(Into::into).collect()),
                key_metadata: value.key_metadata,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// DataFile found in Manifest.
    pub struct FieldSummarySerde {
        /// Whether the manifest contains at least one partition with a null value for the field
        pub contains_null: bool,
        /// Whether the manifest contains at least one partition with a NaN value for the field
        pub contains_nan: Option<bool>,
        /// Lower bound for the non-null, non-NaN values in the partition field, or null if all values are null or NaN.
        /// If -0.0 is a value of the partition field, the lower_bound must not be +0.0
        pub lower_bound: Option<ByteBuf>,
        /// Upper bound for the non-null, non-NaN values in the partition field, or null if all values are null or NaN .
        /// If +0.0 is a value of the partition field, the upper_bound must not be -0.0.
        pub upper_bound: Option<ByteBuf>,
    }

    impl From<FieldSummary> for FieldSummarySerde {
        fn from(value: FieldSummary) -> Self {
            FieldSummarySerde {
                contains_null: value.contains_null,
                contains_nan: value.contains_nan,
                lower_bound: value.lower_bound.map(Into::into),
                upper_bound: value.upper_bound.map(Into::into),
            }
        }
    }
}

impl ManifestListEntry {
    pub fn try_from_enum(
        entry: ManifestListEntryEnum,
        table_metadata: &TableMetadata,
    ) -> Result<ManifestListEntry, Error> {
        match entry {
            ManifestListEntryEnum::V2(entry) => {
                ManifestListEntry::try_from_v2(entry, table_metadata)
            }
            ManifestListEntryEnum::V1(entry) => {
                ManifestListEntry::try_from_v1(entry, table_metadata)
            }
        }
    }

    pub fn try_from_v2(
        entry: _serde::ManifestListEntryV2,
        table_metadata: &TableMetadata,
    ) -> Result<ManifestListEntry, Error> {
        let partition_types = table_metadata.default_partition_spec()?.data_types(
            table_metadata
                .current_schema(None)
                .or(table_metadata
                    .refs
                    .values()
                    .next()
                    .ok_or(Error::NotFound("Current schema".to_string()))
                    .and_then(|x| table_metadata.schema(x.snapshot_id)))
                .unwrap()
                .fields(),
        )?;
        Ok(ManifestListEntry {
            format_version: FormatVersion::V2,
            manifest_path: entry.manifest_path,
            manifest_length: entry.manifest_length,
            partition_spec_id: entry.partition_spec_id,
            content: entry.content,
            sequence_number: entry.sequence_number,
            min_sequence_number: entry.min_sequence_number,
            added_snapshot_id: entry.added_snapshot_id,
            added_files_count: Some(entry.added_files_count),
            existing_files_count: Some(entry.existing_files_count),
            deleted_files_count: Some(entry.deleted_files_count),
            added_rows_count: Some(entry.added_rows_count),
            existing_rows_count: Some(entry.existing_rows_count),
            deleted_rows_count: Some(entry.deleted_rows_count),
            partitions: entry
                .partitions
                .map(|v| {
                    v.into_iter()
                        .zip(partition_types.iter())
                        .map(|(x, d)| FieldSummary::try_from(x, d))
                        .collect::<Result<Vec<_>, Error>>()
                })
                .transpose()?,
            key_metadata: entry.key_metadata,
        })
    }

    pub fn try_from_v1(
        entry: _serde::ManifestListEntryV1,
        table_metadata: &TableMetadata,
    ) -> Result<ManifestListEntry, Error> {
        let partition_types = table_metadata.default_partition_spec()?.data_types(
            table_metadata
                .current_schema(None)
                .or(table_metadata
                    .refs
                    .values()
                    .next()
                    .ok_or(Error::NotFound("Current schema".to_string()))
                    .and_then(|x| table_metadata.schema(x.snapshot_id)))
                .unwrap()
                .fields(),
        )?;
        Ok(ManifestListEntry {
            format_version: FormatVersion::V1,
            manifest_path: entry.manifest_path,
            manifest_length: entry.manifest_length,
            partition_spec_id: entry.partition_spec_id,
            content: Content::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: entry.added_snapshot_id,
            added_files_count: entry.added_files_count,
            existing_files_count: entry.existing_files_count,
            deleted_files_count: entry.deleted_files_count,
            added_rows_count: entry.added_rows_count,
            existing_rows_count: entry.existing_rows_count,
            deleted_rows_count: entry.deleted_rows_count,
            partitions: entry
                .partitions
                .map(|v| {
                    v.into_iter()
                        .zip(partition_types.iter())
                        .map(|(x, d)| FieldSummary::try_from(x, d))
                        .collect::<Result<Vec<_>, Error>>()
                })
                .transpose()?,
            key_metadata: entry.key_metadata,
        })
    }
}

impl FieldSummary {
    fn try_from(value: _serde::FieldSummarySerde, data_type: &Type) -> Result<Self, Error> {
        Ok(FieldSummary {
            contains_null: value.contains_null,
            contains_nan: value.contains_nan,
            lower_bound: value
                .lower_bound
                .map(|x| Value::try_from_bytes(&x, data_type))
                .transpose()?,
            upper_bound: value
                .upper_bound
                .map(|x| Value::try_from_bytes(&x, data_type))
                .transpose()?,
        })
    }
}

pub fn manifest_list_schema_v1() -> &'static AvroSchema {
    static MANIFEST_LIST_SCHEMA_V1: OnceLock<AvroSchema> = OnceLock::new();
    MANIFEST_LIST_SCHEMA_V1.get_or_init(|| {
        AvroSchema::parse_str(
            r#"
        {
            "type": "record",
            "name": "manifest_file",
            "fields": [
                {
                    "name": "manifest_path",
                    "type": "string",
                    "field-id": 500
                },
                {
                    "name": "manifest_length",
                    "type": "long",
                    "field-id": 501
                },
                {
                    "name": "partition_spec_id",
                    "type": "int",
                    "field-id": 502
                },
                {
                    "name": "added_snapshot_id",
                    "type": "long",
                    "field-id": 503
                },
                {
                    "name": "added_files_count",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field-id": 504
                },
                {
                    "name": "existing_files_count",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field-id": 505
                },
                {
                    "name": "deleted_files_count",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field-id": 506
                },
                {
                    "name": "added_rows_count",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field-id": 512
                },
                {
                    "name": "existing_rows_count",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field-id": 513
                },
                {
                    "name": "deleted_rows_count",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field-id": 514
                },
                {
                    "name": "partitions",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "r508",
                                "fields": [
                                    {
                                        "name": "contains_null",
                                        "type": "boolean",
                                        "field-id": 509
                                    },
                                    {
                                        "name": "contains_nan",
                                        "type": [
                                            "null",
                                            "boolean"
                                        ],
                                        "field-id": 518
                                    },
                                    {
                                        "name": "lower_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field-id": 510
                                    },
                                    {
                                        "name": "upper_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field-id": 511
                                    }
                                ]
                            },
                            "element-id": 508
                        }
                    ],
                    "default": null,
                    "field-id": 507
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "default": null,
                    "field-id": 519
                }
            ]
        }
        "#,
        )
        .unwrap()
    })
}
pub fn manifest_list_schema_v2() -> &'static AvroSchema {
    static MANIFEST_LIST_SCHEMA_V2: OnceLock<AvroSchema> = OnceLock::new();
    MANIFEST_LIST_SCHEMA_V2.get_or_init(|| {
        AvroSchema::parse_str(
            r#"
        {
            "type": "record",
            "name": "manifest_file",
            "fields": [
                {
                    "name": "manifest_path",
                    "type": "string",
                    "field-id": 500
                },
                {
                    "name": "manifest_length",
                    "type": "long",
                    "field-id": 501
                },
                {
                    "name": "partition_spec_id",
                    "type": "int",
                    "field-id": 502
                },
                {
                    "name": "content",
                    "type": "int",
                    "field-id": 517
                },
                {
                    "name": "sequence_number",
                    "type": "long",
                    "field-id": 515
                },
                {
                    "name": "min_sequence_number",
                    "type": "long",
                    "field-id": 516
                },
                {
                    "name": "added_snapshot_id",
                    "type": "long",
                    "field-id": 503
                },
                {
                    "name": "added_files_count",
                    "type": "int",
                    "field-id": 504
                },
                {
                    "name": "existing_files_count",
                    "type": "int",
                    "field-id": 505
                },
                {
                    "name": "deleted_files_count",
                    "type": "int",
                    "field-id": 506
                },
                {
                    "name": "added_rows_count",
                    "type": "long",
                    "field-id": 512
                },
                {
                    "name": "existing_rows_count",
                    "type": "long",
                    "field-id": 513
                },
                {
                    "name": "deleted_rows_count",
                    "type": "long",
                    "field-id": 514
                },
                {
                    "name": "partitions",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "r508",
                                "fields": [
                                    {
                                        "name": "contains_null",
                                        "type": "boolean",
                                        "field-id": 509
                                    },
                                    {
                                        "name": "contains_nan",
                                        "type": [
                                            "null",
                                            "boolean"
                                        ],
                                        "field-id": 518
                                    },
                                    {
                                        "name": "lower_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field-id": 510
                                    },
                                    {
                                        "name": "upper_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field-id": 511
                                    }
                                ]
                            },
                            "element-id": 508
                        }
                    ],
                    "default": null,
                    "field-id": 507
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "default": null,
                    "field-id": 519
                }
            ]
        }
        "#,
        )
        .unwrap()
    })
}

/// Convert an avro value result to a manifest list version according to the provided format version
pub fn avro_value_to_manifest_list_entry(
    value: Result<AvroValue, apache_avro::Error>,
    table_metadata: &TableMetadata,
) -> Result<ManifestListEntry, Error> {
    let entry = value?;
    match table_metadata.format_version {
        FormatVersion::V1 => ManifestListEntry::try_from_v1(
            apache_avro::from_value::<_serde::ManifestListEntryV1>(&entry)?,
            table_metadata,
        ),
        FormatVersion::V2 => ManifestListEntry::try_from_v2(
            apache_avro::from_value::<_serde::ManifestListEntryV2>(&entry)?,
            table_metadata,
        ),
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;

    use crate::spec::{
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        table_metadata::TableMetadataBuilder,
        types::{PrimitiveType, StructField},
    };

    #[test]
    pub fn test_manifest_list_v2() {
        let table_metadata = TableMetadataBuilder::default()
            .location("/")
            .current_schema_id(1)
            .schemas(HashMap::from_iter(vec![(
                1,
                Schema::builder()
                    .with_schema_id(1)
                    .with_struct_field(StructField {
                        id: 0,
                        name: "date".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Date),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )]))
            .default_spec_id(0)
            .partition_specs(HashMap::from_iter(vec![(
                0,
                PartitionSpec::builder()
                    .with_partition_field(PartitionField::new(0, 1000, "day", Transform::Day))
                    .build()
                    .unwrap(),
            )]))
            .build()
            .unwrap();

        let manifest_file = ManifestListEntry {
            format_version: FormatVersion::V2,
            manifest_path: "".to_string(),
            manifest_length: 1200,
            partition_spec_id: 0,
            content: Content::Data,
            sequence_number: 566,
            min_sequence_number: 0,
            added_snapshot_id: 39487483032,
            added_files_count: Some(1),
            existing_files_count: Some(2),
            deleted_files_count: Some(0),
            added_rows_count: Some(1000),
            existing_rows_count: Some(8000),
            deleted_rows_count: Some(0),
            partitions: Some(vec![FieldSummary {
                contains_null: true,
                contains_nan: Some(false),
                lower_bound: Some(Value::Int(1234)),
                upper_bound: Some(Value::Int(76890)),
            }]),
            key_metadata: None,
        };

        let schema = manifest_list_schema_v2();

        let mut writer = apache_avro::Writer::new(schema, Vec::new());

        writer.append_ser(manifest_file.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result =
                apache_avro::from_value::<_serde::ManifestListEntryV2>(&record.unwrap()).unwrap();
            assert_eq!(
                manifest_file,
                ManifestListEntry::try_from_v2(result, &table_metadata).unwrap()
            );
        }
    }

    #[test]
    pub fn test_manifest_list_v1() {
        let table_metadata = TableMetadataBuilder::default()
            .format_version(FormatVersion::V1)
            .location("/")
            .current_schema_id(1)
            .schemas(HashMap::from_iter(vec![(
                1,
                Schema::builder()
                    .with_schema_id(1)
                    .with_struct_field(StructField {
                        id: 0,
                        name: "date".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Date),
                        doc: None,
                    })
                    .build()
                    .unwrap(),
            )]))
            .default_spec_id(0)
            .partition_specs(HashMap::from_iter(vec![(
                0,
                PartitionSpec::builder()
                    .with_partition_field(PartitionField::new(0, 1000, "day", Transform::Day))
                    .build()
                    .unwrap(),
            )]))
            .build()
            .unwrap();

        let manifest_file = ManifestListEntry {
            format_version: FormatVersion::V1,
            manifest_path: "".to_string(),
            manifest_length: 1200,
            partition_spec_id: 0,
            content: Content::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 39487483032,
            added_files_count: Some(1),
            existing_files_count: Some(2),
            deleted_files_count: Some(0),
            added_rows_count: Some(1000),
            existing_rows_count: Some(8000),
            deleted_rows_count: Some(0),
            partitions: Some(vec![FieldSummary {
                contains_null: true,
                contains_nan: Some(false),
                lower_bound: Some(Value::Int(1234)),
                upper_bound: Some(Value::Int(76890)),
            }]),
            key_metadata: None,
        };

        let schema = manifest_list_schema_v1();

        let mut writer = apache_avro::Writer::new(schema, Vec::new());

        writer.append_ser(manifest_file.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result =
                apache_avro::from_value::<_serde::ManifestListEntryV1>(&record.unwrap()).unwrap();
            assert_eq!(
                manifest_file,
                ManifestListEntry::try_from_v1(result, &table_metadata).unwrap()
            );
        }
    }
}
