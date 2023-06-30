/*!
Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
The main struct here is [TableMetadataV2] which defines the data for a table.
*/

use anyhow::anyhow;

use std::{cmp, collections::HashMap};

use crate::model::{
    partition::PartitionSpec,
    schema,
    snapshot::{Reference, SnapshotV1, SnapshotV2},
    sort,
};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use super::{types::StructType, partition::PartitionField};

/// Metadata of an iceberg table
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum TableMetadata {
    /// Version 2 of the table metadata
    V2(TableMetadataV2),
    /// Version 1 of the table metadata
    V1(TableMetadataV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadataV2 {
    /// Integer Version for the format.
    pub format_version: VersionNumber<2>,
    /// A UUID that identifies the table
    pub table_uuid: Uuid,
    /// Location tables base location
    pub location: String,
    /// The tables highest sequence number
    pub last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    pub last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: Vec<schema::SchemaV2>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: Vec<PartitionSpec>,
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    pub last_partition_id: i32,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    ///A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<SnapshotV2>>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLog>>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<MetadataLog>>,

    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: Vec<sort::SortOrder>,
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub default_sort_order_id: i64,
    ///A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects.
    /// There is always a main branch reference pointing to the current-snapshot-id
    /// even if the refs map is null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<HashMap<String, Reference>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Fields for the version 1 of the table metadata.
pub struct TableMetadataV1 {
    /// Integer Version for the format.
    pub format_version: VersionNumber<1>,
    /// A UUID that identifies the table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<Uuid>,
    /// Location tables base location
    pub location: String,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    pub last_column_id: i32,
    /// The table’s current schema.
    pub schema: schema::SchemaV1,
    /// A list of schemas, stored as objects with schema-id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<schema::SchemaV1>>,
    /// ID of the table’s current schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema_id: Option<i32>,
    /// The table’s current partition spec, stored as only fields. Note that this is used by writers to partition data,
    /// but is not used when reading because reads use the specs stored in manifest files.
    pub partition_spec: Vec<PartitionField>,
    /// A list of partition specs, stored as full partition spec objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_specs: Option<Vec<PartitionSpec>>,
    /// ID of the “current” spec that writers should use by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_spec_id: Option<i32>,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_partition_id: Option<i32>,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    ///A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<SnapshotV1>>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLog>>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<MetadataLog>>,

    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: Vec<sort::SortOrder>,
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub default_sort_order_id: i64,
}

/// Helper to serialize and deserialize the format version.
#[derive(Debug, PartialEq, Eq)]
pub struct VersionNumber<const V: u8>;

impl<const V: u8> Serialize for VersionNumber<V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(V)
    }
}

impl<'de, const V: u8> Deserialize<'de> for VersionNumber<V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        if value == V {
            Ok(VersionNumber::<V>)
        } else {
            Err(serde::de::Error::custom("Invalid Version"))
        }
    }
}

impl From<TableMetadataV1> for TableMetadataV2 {
    fn from(v1: TableMetadataV1) -> Self {
        let last_partition_id = v1.last_partition_id.unwrap_or_else(|| {
            v1.partition_spec
                .iter()
                .map(|field| field.field_id)
                .fold(0, cmp::max)
        });
        let current_schema_id = v1
            .current_schema_id
            .unwrap_or_else(|| v1.schema.schema_id.unwrap_or(0));
        let default_spec_id = v1.default_spec_id.unwrap_or(0);
        TableMetadataV2 {
            format_version: VersionNumber,
            table_uuid: v1.table_uuid.unwrap_or_else(Uuid::new_v4),
            location: v1.location,
            last_sequence_number: 0,
            last_updated_ms: v1.last_updated_ms,
            last_column_id: v1.last_column_id,
            schemas: v1
                .schemas
                .unwrap_or_else(|| vec![v1.schema])
                .into_iter()
                .map(|schema| schema.into())
                .collect(),
            current_schema_id,
            partition_specs: v1.partition_specs.unwrap_or_else(|| {
                vec![PartitionSpec {
                    spec_id: 0,
                    fields: v1.partition_spec,
                }]
            }),
            default_spec_id,
            last_partition_id,
            properties: v1.properties,
            current_snapshot_id: v1.current_snapshot_id,
            snapshots: v1.snapshots.map(|snapshots| {
                snapshots
                    .into_iter()
                    .map(|snapshot| snapshot.into())
                    .collect()
            }),
            snapshot_log: v1.snapshot_log,
            metadata_log: v1.metadata_log,
            sort_orders: v1.sort_orders,
            default_sort_order_id: v1.default_sort_order_id,
            refs: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Encodes changes to the previous metadata files for the table
pub struct MetadataLog {
    /// The file for the log.
    pub metadata_file: String,
    /// Time new metadata was created
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct SnapshotLog {
    /// Id of the snapshot.
    pub snapshot_id: i64,
    /// Last updated timestamp
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Iceberg format version
pub enum FormatVersion {
    /// Iceberg spec version 1
    V1 = b'1',
    /// Iceberg spec version 2
    V2 = b'2',
}

impl TryFrom<u8> for FormatVersion {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match char::from_u32(value as u32)
            .ok_or_else(|| anyhow!("Failed to convert u8 to char."))?
        {
            '1' => Ok(FormatVersion::V1),
            '2' => Ok(FormatVersion::V2),
            _ => Err(anyhow!("Failed to convert u8 to FormatVersion.")),
        }
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => b'1',
            FormatVersion::V2 => b'2',
        }
    }
}

impl TableMetadata {
    /// Get current schema of the table
    pub fn current_schema(&self) -> &StructType {
        match self {
            TableMetadata::V1(metadata) => &metadata.schema.fields,
            TableMetadata::V2(metadata) => {
                &metadata
                    .schemas
                    .iter()
                    .find(|schema| schema.schema_id == metadata.current_schema_id)
                    .unwrap()
                    .fields
            }
        }
    }
    /// Get the default partition spec for the table
    pub fn default_spec(&self) -> &[PartitionField] {
        match self {
            TableMetadata::V1(metadata) => &metadata.partition_spec,
            TableMetadata::V2(metadata) => {
                &metadata
                    .partition_specs
                    .iter()
                    .find(|spec| spec.spec_id == metadata.default_spec_id)
                    .unwrap()
                    .fields
            }
        }
    }
    /// Get the partition spec with thte given spec_id for the table
    pub fn get_spec(&self, id: i32) -> Option<&[PartitionField]> {
        match self {
            TableMetadata::V1(metadata) => metadata
                .partition_specs
                .as_ref()
                .and_then(|spec| spec.iter().find(|spec| spec.spec_id == id))
                .map(|spec| spec.fields.as_slice()),
            TableMetadata::V2(metadata) => metadata
                .partition_specs
                .iter()
                .find(|spec| spec.spec_id == id)
                .map(|spec| spec.fields.as_slice()),
        }
    }
    /// Get the manifest_list for the current snapshot of the table
    pub fn manifest_list(&self) -> Option<&str> {
        match self {
            TableMetadata::V1(metadata) => metadata
                .snapshots
                .as_ref()
                .zip(metadata.current_snapshot_id.as_ref())
                .and_then(|(snapshots, id)| {
                    snapshots
                        .iter()
                        .find(|snapshot| snapshot.snapshot_id == *id)
                        .and_then(|snapshot| snapshot.manifest_list.as_deref())
                }),
            TableMetadata::V2(metadata) => metadata
                .snapshots
                .as_ref()
                .zip(metadata.current_snapshot_id.as_ref())
                .and_then(|(snapshots, id)| {
                    snapshots
                        .iter()
                        .find(|snapshot| snapshot.snapshot_id == *id)
                        .map(|snapshot| snapshot.manifest_list.as_str())
                }),
        }
    }
    /// Get the base location of the table
    pub fn location(&self) -> &str {
        match self {
            TableMetadata::V1(metadata) => &metadata.location,
            TableMetadata::V2(metadata) => &metadata.location,
        }
    }
    /// Get the last_sequence_number of the table
    pub fn last_sequence_number(&self) -> i64 {
        match self {
            TableMetadata::V1(metadata) => metadata
                .snapshots
                .as_ref()
                .map(|snapshots| snapshots.len() as i64)
                .unwrap_or(1),
            TableMetadata::V2(metadata) => metadata.last_sequence_number,
        }
    }
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub fn last_updated_ms(&self) -> i64 {
        match self {
            TableMetadata::V1(metadata) => metadata.last_updated_ms,
            TableMetadata::V2(metadata) => metadata.last_updated_ms,
        }
    }
    /// Get the format version of the table
    pub fn format_version(&self) -> FormatVersion {
        match self {
            TableMetadata::V1(_) => FormatVersion::V1,
            TableMetadata::V2(_) => FormatVersion::V2,
        }
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;

    use crate::model::table_metadata::TableMetadata;

    use super::TableMetadataV2;

    #[test]
    fn test_deserialize_table_data_v2() -> Result<()> {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {
                        "schema-id" : 1,
                        "type" : "struct",
                        "fields" :[
                            {
                                "id": 1,
                                "name": "struct_name",
                                "required": true,
                                "type": "fixed[1]"
                            }
                        ]
                    }
                ],
                "current-schema-id" : 1,
                "partition-specs": [
                    {
                        "spec-id": 1,
                        "fields": [
                            {  
                                "source-id": 4,  
                                "field-id": 1000,  
                                "name": "ts_day",  
                                "transform": "day"
                            } 
                        ]
                    }
                ],
                "default-spec-id": 1,
                "last-partition-id": 1,
                "properties": {
                    "commit.retry.num-retries": "1"
                },
                "metadata-log": [
                    {  
                        "metadata-file": "s3://bucket/.../v1.json",  
                        "timestamp-ms": 1515100
                    }
                ],
                "sort-orders": [],
                "default-sort-order-id": 0
            }
        "#;
        let metadata =
            serde_json::from_str::<TableMetadata>(data).expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: TableMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    #[test]
    fn test_deserialize_table_data_v1() -> Result<()> {
        let data = r#"
        {
            "format-version" : 1,
            "table-uuid" : "df838b92-0b32-465d-a44e-d39936e538b7",
            "location" : "/home/iceberg/warehouse/nyc/taxis",
            "last-updated-ms" : 1662532818843,
            "last-column-id" : 5,
            "schema" : {
              "type" : "struct",
              "schema-id" : 0,
              "fields" : [ {
                "id" : 1,
                "name" : "vendor_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 2,
                "name" : "trip_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 3,
                "name" : "trip_distance",
                "required" : false,
                "type" : "float"
              }, {
                "id" : 4,
                "name" : "fare_amount",
                "required" : false,
                "type" : "double"
              }, {
                "id" : 5,
                "name" : "store_and_fwd_flag",
                "required" : false,
                "type" : "string"
              } ]
            },
            "current-schema-id" : 0,
            "schemas" : [ {
              "type" : "struct",
              "schema-id" : 0,
              "fields" : [ {
                "id" : 1,
                "name" : "vendor_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 2,
                "name" : "trip_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 3,
                "name" : "trip_distance",
                "required" : false,
                "type" : "float"
              }, {
                "id" : 4,
                "name" : "fare_amount",
                "required" : false,
                "type" : "double"
              }, {
                "id" : 5,
                "name" : "store_and_fwd_flag",
                "required" : false,
                "type" : "string"
              } ]
            } ],
            "partition-spec" : [ {
              "name" : "vendor_id",
              "transform" : "identity",
              "source-id" : 1,
              "field-id" : 1000
            } ],
            "default-spec-id" : 0,
            "partition-specs" : [ {
              "spec-id" : 0,
              "fields" : [ {
                "name" : "vendor_id",
                "transform" : "identity",
                "source-id" : 1,
                "field-id" : 1000
              } ]
            } ],
            "last-partition-id" : 1000,
            "default-sort-order-id" : 0,
            "sort-orders" : [ {
              "order-id" : 0,
              "fields" : [ ]
            } ],
            "properties" : {
              "owner" : "root"
            },
            "current-snapshot-id" : 638933773299822130,
            "refs" : {
              "main" : {
                "snapshot-id" : 638933773299822130,
                "type" : "branch"
              }
            },
            "snapshots" : [ {
              "snapshot-id" : 638933773299822130,
              "timestamp-ms" : 1662532818843,
              "summary" : {
                "operation" : "append",
                "spark.app.id" : "local-1662532784305",
                "added-data-files" : "4",
                "added-records" : "4",
                "added-files-size" : "6001",
                "changed-partition-count" : "2",
                "total-records" : "4",
                "total-files-size" : "6001",
                "total-data-files" : "4",
                "total-delete-files" : "0",
                "total-position-deletes" : "0",
                "total-equality-deletes" : "0"
              },
              "manifest-list" : "/home/iceberg/warehouse/nyc/taxis/metadata/snap-638933773299822130-1-7e6760f0-4f6c-4b23-b907-0a5a174e3863.avro",
              "schema-id" : 0
            } ],
            "snapshot-log" : [ {
              "timestamp-ms" : 1662532818843,
              "snapshot-id" : 638933773299822130
            } ],
            "metadata-log" : [ {
              "timestamp-ms" : 1662532805245,
              "metadata-file" : "/home/iceberg/warehouse/nyc/taxis/metadata/00000-8a62c37d-4573-4021-952a-c0baef7d21d0.metadata.json"
            } ]
          }
        "#;
        let metadata =
            serde_json::from_str::<TableMetadata>(data).expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: TableMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        dbg!(&metadata, &metadata_two);
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    #[test]
    fn test_invalid_table_uuid() -> Result<()> {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "xxxx"
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(data).is_err());
        Ok(())
    }
    #[test]
    fn test_deserialize_table_data_v2_invalid_format_version() -> Result<()> {
        let data = r#"
            {
                "format-version" : 1
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(data).is_err());
        Ok(())
    }
}
