/*!
Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
The main struct here is [TableMetadataV2] which defines the data for a table.
*/

use anyhow::anyhow;

use std::collections::HashMap;

use crate::spec::{partition::PartitionSpec, snapshot::Reference, sort};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use derive_builder::Builder;

use super::{schema::Schema, snapshot::Snapshot};

static MAIN_BRANCH: &str = "main";
static DEFAULT_SORT_ORDER_ID: i64 = 0;
static DEFAULT_SPEC_ID: i32 = 0;

use _serde::TableMetadataEnum;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(try_from = "TableMetadataEnum", into = "TableMetadataEnum")]
#[builder(default)]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadata {
    /// Integer Version for the format.
    pub format_version: FormatVersion,
    #[builder(default = "Uuid::new_v4()")]
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
    pub schemas: HashMap<i32, Schema>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: HashMap<i32, PartitionSpec>,
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
    pub snapshots: Option<HashMap<i64, Snapshot>>,
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
    pub sort_orders: HashMap<i64, sort::SortOrder>,
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

impl TableMetadata {
    /// Get current schema
    #[inline]
    pub fn current_schema(&self) -> Result<&Schema, anyhow::Error> {
        self.schemas
            .get(&self.current_schema_id)
            .ok_or_else(|| anyhow!("Schema {} not found", self.current_schema_id))
    }
    /// Get default partition spec
    #[inline]
    pub fn default_partition_spec(&self) -> Result<&PartitionSpec, anyhow::Error> {
        self.partition_specs
            .get(&self.default_spec_id)
            .ok_or_else(|| anyhow!("Partition spec {} not found", self.default_spec_id))
    }

    /// Get current snapshot
    #[inline]
    pub fn current_snapshot(&self) -> Result<Option<&Snapshot>, anyhow::Error> {
        match (&self.current_snapshot_id, &self.snapshots) {
            (Some(snapshot_id), Some(snapshots)) => Ok(snapshots.get(snapshot_id)),
            (Some(-1), None) => Ok(None),
            (None, None) => Ok(None),
            (Some(_), None) => Err(anyhow!("Snapshot id provided but there are no snapshots")),
            (None, Some(_)) => Err(anyhow!(
                "There are snapshots but no snapshot id is provided"
            )),
        }
    }
    /// Get particular snapshot
    #[inline]
    pub fn snapshot(&self, snapshot_id: i64) -> Option<&Snapshot> {
        match &self.snapshots {
            Some(snapshots) => snapshots.get(&snapshot_id),
            None => None,
        }
    }
}

mod _serde {
    use std::collections::HashMap;

    use anyhow::anyhow;
    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::spec::{
        partition::{PartitionField, PartitionSpec},
        schema,
        snapshot::{
            Reference, Retention,
            _serde::{SnapshotV1, SnapshotV2},
        },
        sort,
    };

    use super::{
        FormatVersion, MetadataLog, SnapshotLog, TableMetadata, VersionNumber,
        DEFAULT_SORT_ORDER_ID, DEFAULT_SPEC_ID, MAIN_BRANCH,
    };

    /// Metadata of an iceberg table
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum TableMetadataEnum {
        /// Version 2 of the table metadata
        V2(TableMetadataV2),
        /// Version 1 of the table metadata
        V1(TableMetadataV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Fields for the version 2 of the table metadata.
    pub(crate) struct TableMetadataV2 {
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
    pub(super) struct TableMetadataV1 {
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
        pub sort_orders: Option<Vec<sort::SortOrder>>,
        /// Default sort order id of the table. Note that this could be used by
        /// writers, but is not used when reading because reads use the specs
        /// stored in manifest files.
        pub default_sort_order_id: Option<i64>,
    }

    impl TryFrom<TableMetadataEnum> for TableMetadata {
        type Error = anyhow::Error;
        fn try_from(value: TableMetadataEnum) -> Result<Self, anyhow::Error> {
            match value {
                TableMetadataEnum::V2(value) => value.try_into(),
                TableMetadataEnum::V1(value) => value.try_into(),
            }
        }
    }

    impl From<TableMetadata> for TableMetadataEnum {
        fn from(value: TableMetadata) -> Self {
            match value.format_version {
                FormatVersion::V2 => TableMetadataEnum::V2(value.into()),
                FormatVersion::V1 => TableMetadataEnum::V1(value.into()),
            }
        }
    }

    impl TryFrom<TableMetadataV2> for TableMetadata {
        type Error = anyhow::Error;
        fn try_from(value: TableMetadataV2) -> Result<Self, anyhow::Error> {
            let current_snapshot_id = if let &Some(-1) = &value.current_snapshot_id {
                None
            } else {
                value.current_snapshot_id
            };
            let schemas = HashMap::from_iter(
                value
                    .schemas
                    .into_iter()
                    .map(|schema| Ok((schema.schema_id, schema.try_into()?)))
                    .collect::<Result<Vec<_>, anyhow::Error>>()?,
            );
            Ok(TableMetadata {
                format_version: FormatVersion::V2,
                table_uuid: value.table_uuid,
                location: value.location,
                last_sequence_number: value.last_sequence_number,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                current_schema_id: if schemas.keys().contains(&value.current_schema_id) {
                    Ok(value.current_schema_id)
                } else {
                    Err(anyhow!(
                        "No schema exists with the schema id {}",
                        &value.current_schema_id
                    ))
                }?,
                schemas,
                partition_specs: HashMap::from_iter(
                    value.partition_specs.into_iter().map(|x| (x.spec_id, x)),
                ),
                default_spec_id: value.default_spec_id,
                last_partition_id: value.last_partition_id,
                properties: value.properties,
                current_snapshot_id,
                snapshots: value.snapshots.map(|snapshots| {
                    HashMap::from_iter(snapshots.into_iter().map(|x| (x.snapshot_id, x.into())))
                }),
                snapshot_log: value.snapshot_log,
                metadata_log: value.metadata_log,
                sort_orders: HashMap::from_iter(
                    value
                        .sort_orders
                        .into_iter()
                        .map(|x| (x.order_id as i64, x)),
                ),
                default_sort_order_id: value.default_sort_order_id,
                refs: value.refs,
            })
        }
    }

    impl TryFrom<TableMetadataV1> for TableMetadata {
        type Error = anyhow::Error;
        fn try_from(value: TableMetadataV1) -> Result<Self, anyhow::Error> {
            let schemas = value
                .schemas
                .map(|schemas| {
                    Ok::<_, anyhow::Error>(HashMap::from_iter(
                        schemas
                            .into_iter()
                            .enumerate()
                            .map(|(i, schema)| {
                                Ok((schema.schema_id.unwrap_or(i as i32), schema.try_into()?))
                            })
                            .collect::<Result<Vec<_>, anyhow::Error>>()?
                            .into_iter(),
                    ))
                })
                .or_else(|| {
                    Some(Ok(HashMap::from_iter(vec![(
                        value.schema.schema_id.unwrap_or(0),
                        value.schema.try_into().ok()?,
                    )])))
                })
                .transpose()?
                .unwrap_or_default();
            let partition_specs = HashMap::from_iter(
                value
                    .partition_specs
                    .unwrap_or_else(|| {
                        vec![PartitionSpec {
                            spec_id: DEFAULT_SPEC_ID,
                            fields: value.partition_spec,
                        }]
                    })
                    .into_iter()
                    .map(|x| (x.spec_id, x)),
            );
            Ok(TableMetadata {
                format_version: FormatVersion::V1,
                table_uuid: value.table_uuid.unwrap_or_default(),
                location: value.location,
                last_sequence_number: 0,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                current_schema_id: value
                    .current_schema_id
                    .unwrap_or_else(|| schemas.keys().copied().max().unwrap_or_default()),
                default_spec_id: value
                    .default_spec_id
                    .unwrap_or_else(|| partition_specs.keys().copied().max().unwrap_or_default()),
                last_partition_id: value
                    .last_partition_id
                    .unwrap_or_else(|| partition_specs.keys().copied().max().unwrap_or_default()),
                partition_specs,
                schemas,

                properties: value.properties,
                current_snapshot_id: value.current_snapshot_id,
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        Ok::<_, anyhow::Error>(HashMap::from_iter(
                            snapshots
                                .into_iter()
                                .map(|x| Ok((x.snapshot_id, x.into())))
                                .collect::<Result<Vec<_>, anyhow::Error>>()?,
                        ))
                    })
                    .transpose()?,
                snapshot_log: value.snapshot_log,
                metadata_log: value.metadata_log,
                sort_orders: match value.sort_orders {
                    Some(sort_orders) => {
                        HashMap::from_iter(sort_orders.into_iter().map(|x| (x.order_id as i64, x)))
                    }
                    None => HashMap::new(),
                },
                default_sort_order_id: value.default_sort_order_id.unwrap_or(DEFAULT_SORT_ORDER_ID),
                refs: Some(HashMap::from_iter(vec![(
                    MAIN_BRANCH.to_string(),
                    Reference {
                        snapshot_id: value.current_snapshot_id.unwrap_or_default(),
                        retention: Retention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                )])),
            })
        }
    }

    impl From<TableMetadata> for TableMetadataV2 {
        fn from(v: TableMetadata) -> Self {
            TableMetadataV2 {
                format_version: VersionNumber::<2>,
                table_uuid: v.table_uuid,
                location: v.location,
                last_sequence_number: v.last_sequence_number,
                last_updated_ms: v.last_updated_ms,
                last_column_id: v.last_column_id,
                schemas: v.schemas.into_values().map(|x| x.into()).collect(),
                current_schema_id: v.current_schema_id,
                partition_specs: v.partition_specs.into_values().collect(),
                default_spec_id: v.default_spec_id,
                last_partition_id: v.last_partition_id,
                properties: v.properties,
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: v
                    .snapshots
                    .map(|snapshots| snapshots.into_values().map(|x| x.into()).collect()),
                snapshot_log: v.snapshot_log,
                metadata_log: v.metadata_log,
                sort_orders: v.sort_orders.into_values().collect(),
                default_sort_order_id: v.default_sort_order_id,
                refs: v.refs,
            }
        }
    }

    impl From<TableMetadata> for TableMetadataV1 {
        fn from(v: TableMetadata) -> Self {
            TableMetadataV1 {
                format_version: VersionNumber::<1>,
                table_uuid: Some(v.table_uuid),
                location: v.location,
                last_updated_ms: v.last_updated_ms,
                last_column_id: v.last_column_id,
                schema: v.schemas.get(&v.current_schema_id).unwrap().clone().into(),
                schemas: Some(v.schemas.into_values().map(|x| x.into()).collect()),
                current_schema_id: Some(v.current_schema_id),
                partition_spec: v
                    .partition_specs
                    .get(&v.default_spec_id)
                    .map(|x| x.fields.clone())
                    .unwrap_or_default(),
                partition_specs: Some(v.partition_specs.into_values().collect()),
                default_spec_id: Some(v.default_spec_id),
                last_partition_id: Some(v.last_partition_id),
                properties: v.properties,
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: v
                    .snapshots
                    .map(|snapshots| snapshots.into_values().map(|x| x.into()).collect()),
                snapshot_log: v.snapshot_log,
                metadata_log: v.metadata_log,
                sort_orders: Some(v.sort_orders.into_values().collect()),
                default_sort_order_id: Some(v.default_sort_order_id),
            }
        }
    }
}

/// Helper to serialize and deserialize the format version.
#[derive(Debug, PartialEq, Eq, Clone)]
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Encodes changes to the previous metadata files for the table
pub struct MetadataLog {
    /// The file for the log.
    pub metadata_file: String,
    /// Time new metadata was created
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
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
#[derive(Default)]
pub enum FormatVersion {
    /// Iceberg spec version 1
    V1 = b'1',
    /// Iceberg spec version 2
    #[default]
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

#[cfg(test)]
mod tests {

    use anyhow::Result;

    use crate::spec::table_metadata::TableMetadata;

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
}
