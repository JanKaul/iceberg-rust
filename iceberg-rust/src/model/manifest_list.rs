/*!
 * Manifest lists
*/

use std::{
    io::Read,
    iter::{repeat, Map, Repeat, Zip},
};

use apache_avro::{types::Value as AvroValue, Reader as AvroReader};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use super::{
    manifest::Content,
    table_metadata::{FormatVersion, TableMetadata},
};

/// Iterator of ManifestFileEntries
pub struct ManifestFileReader<'a, 'metadata, R: Read> {
    reader: Map<
        Zip<AvroReader<'a, R>, Repeat<&'metadata TableMetadata>>,
        fn(
            (Result<AvroValue, apache_avro::Error>, &TableMetadata),
        ) -> Result<ManifestFileEntry, apache_avro::Error>,
    >,
}

impl<'a, 'metadata, R: Read> Iterator for ManifestFileReader<'a, 'metadata, R> {
    type Item = Result<ManifestFileEntry, apache_avro::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl<'a, 'metadata, R: Read> ManifestFileReader<'a, 'metadata, R> {
    /// Create a new ManifestFile reader
    pub fn new(
        reader: R,
        table_metadata: &'metadata TableMetadata,
    ) -> Result<Self, apache_avro::Error> {
        Ok(Self {
            reader: AvroReader::new(reader)?
                .zip(repeat(table_metadata))
                .map(avro_value_to_manifest_file),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// DataFile found in Manifest.
pub struct FieldSummary {
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

/// Entry in manifest file.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum ManifestFileEntry {
    /// Version 2 of the manifest file
    V2(ManifestFileEntryV2),
    /// Version 1 of the manifest file
    V1(ManifestFileEntryV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// A manifest list includes summary metadata that can be used to avoid scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a summary of values for each field of the partition spec used to write the manifest.
pub struct ManifestFileEntryV2 {
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
    pub partitions: Option<Vec<FieldSummary>>,
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// A manifest list includes summary metadata that can be used to avoid scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a summary of values for each field of the partition spec used to write the manifest.
pub struct ManifestFileEntryV1 {
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
    pub partitions: Option<Vec<FieldSummary>>,
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
}

impl From<ManifestFileEntryV1> for ManifestFileEntryV2 {
    fn from(v1: ManifestFileEntryV1) -> Self {
        ManifestFileEntryV2 {
            manifest_path: v1.manifest_path,
            manifest_length: v1.manifest_length,
            partition_spec_id: v1.partition_spec_id,
            content: Content::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: v1.added_snapshot_id,
            added_files_count: v1.added_files_count.unwrap_or(0),
            existing_files_count: v1.existing_files_count.unwrap_or(0),
            deleted_files_count: v1.deleted_files_count.unwrap_or(0),
            added_rows_count: v1.added_rows_count.unwrap_or(0),
            existing_rows_count: v1.existing_rows_count.unwrap_or(0),
            deleted_rows_count: v1.deleted_rows_count.unwrap_or(0),
            partitions: v1.partitions,
            key_metadata: v1.key_metadata,
        }
    }
}

impl ManifestFileEntry {
    /// Get schema of the manifest list
    pub fn schema(format_version: &FormatVersion) -> String {
        match format_version {
            FormatVersion::V1 => r#"
        {
            "type": "record",
            "name": "manifest_list",
            "fields": [
                {
                    "name": "manifest_path",
                    "type": "string",
                    "field_id": 500
                },
                {
                    "name": "manifest_length",
                    "type": "long",
                    "field_id": 501
                },
                {
                    "name": "partition_spec_id",
                    "type": "int",
                    "field_id": 502
                },
                {
                    "name": "added_snapshot_id",
                    "type": "long",
                    "default": null,
                    "field_id": 503
                },
                {
                    "name": "added_files_count",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 504
                },
                {
                    "name": "existing_files_count",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 505
                },
                {
                    "name": "deleted_files_count",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 506
                },
                {
                    "name": "added_rows_count",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 512
                },
                {
                    "name": "existing_rows_count",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 513
                },
                {
                    "name": "deleted_rows_count",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 514
                },
                {
                    "name": "partitions",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "field_summary",
                                "fields": [
                                    {
                                        "name": "contains_null",
                                        "type": "boolean",
                                        "field_id": 509
                                    },
                                    {
                                        "name": "contains_nan",
                                        "type": [
                                            "null",
                                            "boolean"
                                        ],
                                        "field_id": 518
                                    },
                                    {
                                        "name": "lower_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field_id": 510
                                    },
                                    {
                                        "name": "upper_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field_id": 511
                                    }
                                ]
                            },
                            "element-id": 112
                        }
                    ],
                    "default": null,
                    "field_id": 507
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "field_id": 519
                }
            ]
        }
        "#
            .to_owned(),
            &FormatVersion::V2 => r#"
        {
            "type": "record",
            "name": "manifest_list",
            "fields": [
                {
                    "name": "manifest_path",
                    "type": "string",
                    "field_id": 500
                },
                {
                    "name": "manifest_length",
                    "type": "long",
                    "field_id": 501
                },
                {
                    "name": "partition_spec_id",
                    "type": "int",
                    "field_id": 502
                },
                {
                    "name": "content",
                    "type": "int",
                    "field_id": 517
                },
                {
                    "name": "sequence_number",
                    "type": "long",
                    "field_id": 515
                },
                {
                    "name": "min_sequence_number",
                    "type": "long",
                    "field_id": 516
                },
                {
                    "name": "added_snapshot_id",
                    "type": "long",
                    "default": null,
                    "field_id": 503
                },
                {
                    "name": "added_files_count",
                    "type": "int",
                    "field_id": 504
                },
                {
                    "name": "existing_files_count",
                    "type": "int",
                    "field_id": 505
                },
                {
                    "name": "deleted_files_count",
                    "type": "int",
                    "field_id": 506
                },
                {
                    "name": "added_rows_count",
                    "type": "long",
                    "field_id": 512
                },
                {
                    "name": "existing_rows_count",
                    "type": "long",
                    "field_id": 513
                },
                {
                    "name": "deleted_rows_count",
                    "type": "long",
                    "field_id": 514
                },
                {
                    "name": "partitions",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "field_summary",
                                "fields": [
                                    {
                                        "name": "contains_null",
                                        "type": "boolean",
                                        "field_id": 509
                                    },
                                    {
                                        "name": "contains_nan",
                                        "type": [
                                            "null",
                                            "boolean"
                                        ],
                                        "field_id": 518
                                    },
                                    {
                                        "name": "lower_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field_id": 510
                                    },
                                    {
                                        "name": "upper_bound",
                                        "type": [
                                            "null",
                                            "bytes"
                                        ],
                                        "field_id": 511
                                    }
                                ]
                            },
                            "element-id": 112
                        }
                    ],
                    "default": null,
                    "field_id": 507
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "field_id": 519
                }
            ]
        }
        "#
            .to_owned(),
        }
    }
    /// Location of the manifest file
    pub fn manifest_path(&self) -> &str {
        match self {
            ManifestFileEntry::V1(file) => &file.manifest_path,
            ManifestFileEntry::V2(file) => &file.manifest_path,
        }
    }
    /// ID of a partition spec used to write the manifest; must be listed in table metadata partition-specs
    pub fn partition_spec_id(&self) -> i32 {
        match self {
            ManifestFileEntry::V1(file) => file.partition_spec_id,
            ManifestFileEntry::V2(file) => file.partition_spec_id,
        }
    }
    /// A list of field summaries for each partition field in the spec. Each field in the list corresponds to a field in the manifest file’s partition spec.
    pub fn partitions(&self) -> &std::option::Option<Vec<FieldSummary>> {
        match self {
            ManifestFileEntry::V1(file) => &file.partitions,
            ManifestFileEntry::V2(file) => &file.partitions,
        }
    }
    /// Number of entries in the manifest that have status ADDED (1), when null this is assumed to be non-zero
    pub fn added_files_count(&self) -> std::option::Option<i32> {
        match self {
            ManifestFileEntry::V1(file) => file.added_files_count,
            ManifestFileEntry::V2(file) => Some(file.added_files_count),
        }
    }
}

/// Convert an avro value to a [ManifestFile] according to the provided format version
fn avro_value_to_manifest_file(
    value: (Result<AvroValue, apache_avro::Error>, &TableMetadata),
) -> Result<ManifestFileEntry, apache_avro::Error> {
    let entry = value.0;
    let table_metadata = value.1;
    entry.and_then(|value| match table_metadata.format_version {
        FormatVersion::V1 => {
            apache_avro::from_value::<ManifestFileEntryV1>(&value).map(ManifestFileEntry::V1)
        }
        FormatVersion::V2 => {
            apache_avro::from_value::<ManifestFileEntryV2>(&value).map(ManifestFileEntry::V2)
        }
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    pub fn test_manifest_list_v2() {
        let manifest_file = ManifestFileEntry::V2(ManifestFileEntryV2 {
            manifest_path: "".to_string(),
            manifest_length: 1200,
            partition_spec_id: 0,
            content: Content::Data,
            sequence_number: 566,
            min_sequence_number: 0,
            added_snapshot_id: 39487483032,
            added_files_count: 1,
            existing_files_count: 2,
            deleted_files_count: 0,
            added_rows_count: 1000,
            existing_rows_count: 8000,
            deleted_rows_count: 0,
            partitions: Some(vec![FieldSummary {
                contains_null: true,
                contains_nan: Some(false),
                lower_bound: Some(ByteBuf::from(vec![0, 0, 0, 0])),
                upper_bound: None,
            }]),
            key_metadata: None,
        });

        let raw_schema = ManifestFileEntry::schema(&FormatVersion::V2);

        let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(manifest_file.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<ManifestFileEntryV2>(&record.unwrap()).unwrap();
            assert_eq!(manifest_file, ManifestFileEntry::V2(result));
        }
    }

    #[test]
    pub fn test_manifest_list_v1() {
        let manifest_file = ManifestFileEntry::V1(ManifestFileEntryV1 {
            manifest_path: "".to_string(),
            manifest_length: 1200,
            partition_spec_id: 0,
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
                lower_bound: Some(ByteBuf::from(vec![0, 0, 0, 0])),
                upper_bound: None,
            }]),
            key_metadata: None,
        });

        let raw_schema = ManifestFileEntry::schema(&FormatVersion::V1);

        let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(manifest_file.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<ManifestFileEntryV1>(&record.unwrap()).unwrap();
            assert_eq!(manifest_file, ManifestFileEntry::V1(result));
        }
    }
}
