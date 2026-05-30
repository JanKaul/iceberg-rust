//! Table metadata implementation for Iceberg tables
//!
//! This module contains the implementation of table metadata for Iceberg tables, including:
//! - Table metadata structure and versioning (V1 and V2)
//! - Schema management
//! - Partition specifications
//! - Sort orders
//! - Snapshot management and history
//! - Metadata properties and logging
//!
//! The table metadata format is defined in the [Iceberg Table Spec](https://iceberg.apache.org/spec/#table-metadata)

use std::{
    collections::HashMap,
    fmt, str,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    error::Error,
    partition::BoundPartitionField,
    spec::{
        partition::PartitionSpec,
        sort::{self, SortOrder},
    },
};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use derive_builder::Builder;

use super::{
    schema::Schema,
    snapshot::{Snapshot, SnapshotReference},
    tabular::TabularMetadataRef,
};

pub static MAIN_BRANCH: &str = "main";
static DEFAULT_SORT_ORDER_ID: i32 = 0;
static DEFAULT_SPEC_ID: i32 = 0;

// Properties

pub const WRITE_PARQUET_COMPRESSION_CODEC: &str = "write.parquet.compression-codec";
pub const WRITE_PARQUET_COMPRESSION_LEVEL: &str = "write.parquet.compression-level";
pub const WRITE_OBJECT_STORAGE_ENABLED: &str = "write.object-storage.enabled";
pub const WRITE_DATA_PATH: &str = "write.data.path";
pub const WRITE_METADATA_METRICS_DISTINCT_COUNTS_ENABLED: &str =
    "write.metadata.metrics.distinct-counts.enabled";

pub use _serde::{TableMetadataV1, TableMetadataV2, TableMetadataV3};

use _serde::TableMetadataEnum;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Builder)]
#[serde(try_from = "TableMetadataEnum", into = "TableMetadataEnum")]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadata {
    #[builder(default)]
    /// Integer Version for the format.
    pub format_version: FormatVersion,
    #[builder(default = "Uuid::new_v4()")]
    /// A UUID that identifies the table
    pub table_uuid: Uuid,
    #[builder(setter(into))]
    /// Location tables base location
    pub location: String,
    #[builder(default)]
    /// The tables highest sequence number
    pub last_sequence_number: i64,
    #[builder(
        default = "SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64"
    )]
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    #[builder(default)]
    /// An integer; the highest assigned column ID for the table.
    pub last_column_id: i32,
    #[builder(setter(each(name = "with_schema")))]
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: HashMap<i32, Schema>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    #[builder(
        setter(each(name = "with_partition_spec")),
        default = "HashMap::from_iter(vec![(0,PartitionSpec::default())])"
    )]
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: HashMap<i32, PartitionSpec>,
    #[builder(default)]
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    #[builder(default)]
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    pub last_partition_id: i32,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    #[builder(default)]
    pub properties: HashMap<String, String>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    #[builder(default)]
    pub current_snapshot_id: Option<i64>,
    ///A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    #[builder(default)]
    pub snapshots: HashMap<i64, Snapshot>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    #[builder(default)]
    pub snapshot_log: Vec<SnapshotLog>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    #[builder(default)]
    pub metadata_log: Vec<MetadataLog>,
    #[builder(
        setter(each(name = "with_sort_order")),
        default = "HashMap::from_iter(vec![(0, SortOrder::default())])"
    )]
    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: HashMap<i32, sort::SortOrder>,
    #[builder(default)]
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub default_sort_order_id: i32,
    ///A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects.
    /// There is always a main branch reference pointing to the current-snapshot-id
    /// even if the refs map is null.
    #[builder(default)]
    pub refs: HashMap<String, SnapshotReference>,
    /// Highest assigned row id across the table. Required at v3 and above. For v1/v2 this
    /// is held at the default of 0.
    #[builder(default)]
    pub next_row_id: i64,
}

impl TableMetadata {
    /// Gets the current schema for a given branch, or the table's current schema if no branch is specified.
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to get the schema for
    ///
    /// # Returns
    /// * `Result<&Schema, Error>` - The current schema, or an error if the schema cannot be found
    #[inline]
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        let schema_id = self
            .current_snapshot(branch)?
            .and_then(|x| *x.schema_id())
            .unwrap_or(self.current_schema_id);
        self.schemas
            .get(&schema_id)
            .ok_or_else(|| Error::InvalidFormat("schema".to_string()))
    }

    /// Gets the schema for a specific snapshot ID.
    ///
    /// # Arguments
    /// * `snapshot_id` - The ID of the snapshot to get the schema for
    ///
    /// # Returns
    /// * `Result<&Schema, Error>` - The schema for the snapshot, or an error if the schema cannot be found
    #[inline]
    pub fn schema(&self, snapshot_id: i64) -> Result<&Schema, Error> {
        let schema_id = self
            .snapshots
            .get(&snapshot_id)
            .and_then(|x| *x.schema_id())
            .unwrap_or(self.current_schema_id);
        self.schemas
            .get(&schema_id)
            .ok_or_else(|| Error::InvalidFormat("schema".to_string()))
    }

    /// Gets the default partition specification for the table
    ///
    /// # Returns
    /// * `Result<&PartitionSpec, Error>` - The default partition spec, or an error if it cannot be found
    #[inline]
    pub fn default_partition_spec(&self) -> Result<&PartitionSpec, Error> {
        self.partition_specs
            .get(&self.default_spec_id)
            .ok_or_else(|| Error::InvalidFormat("partition spec".to_string()))
    }

    /// Gets the current partition fields for a given branch, binding them to their source schema fields
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to get the partition fields for
    ///
    /// # Returns
    /// * `Result<Vec<BoundPartitionField>, Error>` - Vector of partition fields bound to their source schema fields,
    ///   or an error if the schema or partition spec cannot be found
    pub fn current_partition_fields(
        &self,
        branch: Option<&str>,
    ) -> Result<Vec<BoundPartitionField<'_>>, Error> {
        let schema = self.current_schema(branch)?;
        let partition_spec = self.default_partition_spec()?;
        partition_fields(partition_spec, schema)
    }

    /// Gets the partition fields for a specific snapshot, binding them to their source schema fields
    ///
    /// # Arguments
    /// * `snapshot_id` - The ID of the snapshot to get the partition fields for
    ///
    /// # Returns
    /// * `Result<Vec<BoundPartitionField>, Error>` - Vector of partition fields bound to their source schema fields,
    ///   or an error if the schema or partition spec cannot be found
    pub fn partition_fields(
        &self,
        snapshot_id: i64,
    ) -> Result<Vec<BoundPartitionField<'_>>, Error> {
        let schema = self.schema(snapshot_id)?;
        self.default_partition_spec()?
            .fields()
            .iter()
            .map(|partition_field| {
                let field =
                    schema
                        .get(*partition_field.source_id() as usize)
                        .ok_or(Error::NotFound(format!(
                            "Schema field with id {}",
                            partition_field.source_id()
                        )))?;
                Ok(BoundPartitionField::new(partition_field, field))
            })
            .collect()
    }

    /// Gets the current snapshot for a given reference, or the table's current snapshot if no reference is specified
    ///
    /// # Arguments
    /// * `snapshot_ref` - Optional snapshot reference name to get the snapshot for
    ///
    /// # Returns
    /// * `Result<Option<&Snapshot>, Error>` - The current snapshot if it exists, None if there are no snapshots,
    ///   or an error if the snapshots are in an invalid state
    #[inline]
    pub fn current_snapshot(&self, snapshot_ref: Option<&str>) -> Result<Option<&Snapshot>, Error> {
        let snapshot_id = match snapshot_ref {
            None => self
                .refs
                .get("main")
                .map(|x| x.snapshot_id)
                .or(self.current_snapshot_id),
            Some(reference) => self.refs.get(reference).map(|x| x.snapshot_id),
        };
        match snapshot_id {
            Some(snapshot_id) => Ok(self.snapshots.get(&snapshot_id)),
            None => {
                if self.snapshots.is_empty()
                    || (snapshot_ref.is_some() && snapshot_ref != Some("main"))
                {
                    Ok(None)
                } else {
                    Err(Error::InvalidFormat("snapshots".to_string()))
                }
            }
        }
    }

    /// Gets a mutable reference to the current snapshot for a given reference, or the table's current snapshot if no reference is specified
    ///
    /// # Arguments
    /// * `snapshot_ref` - Optional snapshot reference name to get the snapshot for
    ///
    /// # Returns
    /// * `Result<Option<&mut Snapshot>, Error>` - Mutable reference to the current snapshot if it exists, None if there are no snapshots,
    ///   or an error if the snapshots are in an invalid state
    #[inline]
    pub fn current_snapshot_mut(
        &mut self,
        snapshot_ref: Option<String>,
    ) -> Result<Option<&mut Snapshot>, Error> {
        let snapshot_id = match &snapshot_ref {
            None => self
                .refs
                .get("main")
                .map(|x| x.snapshot_id)
                .or(self.current_snapshot_id),
            Some(reference) => self.refs.get(reference).map(|x| x.snapshot_id),
        };
        match snapshot_id {
            Some(-1) => {
                if self.snapshots.is_empty()
                    || (snapshot_ref.is_some() && snapshot_ref.as_deref() != Some("main"))
                {
                    Ok(None)
                } else {
                    Err(Error::InvalidFormat("snapshots".to_string()))
                }
            }
            Some(snapshot_id) => Ok(self.snapshots.get_mut(&snapshot_id)),
            None => {
                if self.snapshots.is_empty()
                    || (snapshot_ref.is_some() && snapshot_ref.as_deref() != Some("main"))
                {
                    Ok(None)
                } else {
                    Err(Error::InvalidFormat("snapshots".to_string()))
                }
            }
        }
    }

    /// Gets the sequence number for a specific snapshot
    ///
    /// # Arguments
    /// * `snapshot_id` - The ID of the snapshot to get the sequence number for
    ///
    /// # Returns
    /// * `Option<i64>` - The sequence number if the snapshot exists, None otherwise
    pub fn sequence_number(&self, snapshot_id: i64) -> Option<i64> {
        self.snapshots
            .get(&snapshot_id)
            .map(|x| *x.sequence_number())
    }

    pub fn as_ref(&self) -> TabularMetadataRef<'_> {
        TabularMetadataRef::Table(self)
    }
}

pub fn partition_fields<'a>(
    partition_spec: &'a PartitionSpec,
    schema: &'a Schema,
) -> Result<Vec<BoundPartitionField<'a>>, Error> {
    partition_spec
        .fields()
        .iter()
        .map(|partition_field| {
            let field =
                schema
                    .get(*partition_field.source_id() as usize)
                    .ok_or(Error::NotFound(format!(
                        "Schema field with id {}",
                        partition_field.source_id()
                    )))?;
            Ok(BoundPartitionField::new(partition_field, field))
        })
        .collect()
}

/// Creates a new metadata file location for a table
///
/// # Arguments
/// * `metadata` - The table metadata to create a location for
///
/// # Returns
/// * `String` - The path where the new metadata file should be stored
pub fn new_metadata_location<'a, T: Into<TabularMetadataRef<'a>>>(metadata: T) -> String {
    let metadata: TabularMetadataRef = metadata.into();
    let transaction_uuid = Uuid::new_v4();
    let version = metadata.sequence_number();

    format!(
        "{}/metadata/{:05}-{}.metadata.json",
        metadata.location(),
        version,
        transaction_uuid
    )
}

impl fmt::Display for TableMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for TableMetadata {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

pub mod _serde {
    use std::collections::HashMap;

    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::{
        error::Error,
        spec::{
            partition::{PartitionField, PartitionSpec},
            schema,
            snapshot::{
                _serde::{SnapshotV1, SnapshotV2},
                SnapshotReference, SnapshotRetention,
            },
            sort,
        },
    };

    use super::{
        FormatVersion, MetadataLog, SnapshotLog, TableMetadata, VersionNumber,
        DEFAULT_SORT_ORDER_ID, DEFAULT_SPEC_ID, MAIN_BRANCH,
    };

    /// Metadata of an iceberg table
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum TableMetadataEnum {
        /// Version 3 of the table metadata
        V3(TableMetadataV3),
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
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub properties: HashMap<String, String>,
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
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub snapshot_log: Vec<SnapshotLog>,

        /// A list (optional) of timestamp and metadata file location pairs
        /// that encodes changes to the previous metadata files for the table.
        /// Each time a new metadata file is created, a new entry of the
        /// previous metadata file location should be added to the list.
        /// Tables can be configured to remove oldest metadata log entries and
        /// keep a fixed-size log of the most recent entries after a commit.
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub metadata_log: Vec<MetadataLog>,

        /// A list of sort orders, stored as full sort order objects.
        pub sort_orders: Vec<sort::SortOrder>,
        /// Default sort order id of the table. Note that this could be used by
        /// writers, but is not used when reading because reads use the specs
        /// stored in manifest files.
        pub default_sort_order_id: i32,
        ///A map of snapshot references. The map keys are the unique snapshot reference
        /// names in the table, and the map values are snapshot reference objects.
        /// There is always a main branch reference pointing to the current-snapshot-id
        /// even if the refs map is null.
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub refs: HashMap<String, SnapshotReference>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Fields for the version 3 of the table metadata.
    pub struct TableMetadataV3 {
        /// Integer Version for the format.
        pub format_version: VersionNumber<3>,
        /// A UUID that identifies the table
        pub table_uuid: Uuid,
        /// Location tables base location
        pub location: String,
        /// Highest assigned row id across the table. Required from v3.
        pub next_row_id: i64,
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
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub properties: HashMap<String, String>,
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
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub snapshot_log: Vec<SnapshotLog>,

        /// A list (optional) of timestamp and metadata file location pairs
        /// that encodes changes to the previous metadata files for the table.
        /// Each time a new metadata file is created, a new entry of the
        /// previous metadata file location should be added to the list.
        /// Tables can be configured to remove oldest metadata log entries and
        /// keep a fixed-size log of the most recent entries after a commit.
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub metadata_log: Vec<MetadataLog>,

        /// A list of sort orders, stored as full sort order objects.
        pub sort_orders: Vec<sort::SortOrder>,
        /// Default sort order id of the table. Note that this could be used by
        /// writers, but is not used when reading because reads use the specs
        /// stored in manifest files.
        pub default_sort_order_id: i32,
        ///A map of snapshot references. The map keys are the unique snapshot reference
        /// names in the table, and the map values are snapshot reference objects.
        /// There is always a main branch reference pointing to the current-snapshot-id
        /// even if the refs map is null.
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub refs: HashMap<String, SnapshotReference>,
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
        #[serde(skip_serializing_if = "HashMap::is_empty", default)]
        pub properties: HashMap<String, String>,
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
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub snapshot_log: Vec<SnapshotLog>,

        /// A list (optional) of timestamp and metadata file location pairs
        /// that encodes changes to the previous metadata files for the table.
        /// Each time a new metadata file is created, a new entry of the
        /// previous metadata file location should be added to the list.
        /// Tables can be configured to remove oldest metadata log entries and
        /// keep a fixed-size log of the most recent entries after a commit.
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        pub metadata_log: Vec<MetadataLog>,

        /// A list of sort orders, stored as full sort order objects.
        pub sort_orders: Option<Vec<sort::SortOrder>>,
        /// Default sort order id of the table. Note that this could be used by
        /// writers, but is not used when reading because reads use the specs
        /// stored in manifest files.
        pub default_sort_order_id: Option<i32>,
    }

    impl TryFrom<TableMetadataEnum> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataEnum) -> Result<Self, Error> {
            match value {
                TableMetadataEnum::V3(value) => value.try_into(),
                TableMetadataEnum::V2(value) => value.try_into(),
                TableMetadataEnum::V1(value) => value.try_into(),
            }
        }
    }

    impl From<TableMetadata> for TableMetadataEnum {
        fn from(value: TableMetadata) -> Self {
            match value.format_version {
                FormatVersion::V3 => TableMetadataEnum::V3(value.into()),
                FormatVersion::V2 => TableMetadataEnum::V2(value.into()),
                FormatVersion::V1 => TableMetadataEnum::V1(value.into()),
            }
        }
    }

    impl TryFrom<TableMetadataV3> for TableMetadata {
        type Error = Error;
        #[allow(clippy::field_reassign_with_default)]
        fn try_from(value: TableMetadataV3) -> Result<Self, Error> {
            let next_row_id = value.next_row_id;
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
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            let mut refs = value.refs;
            if let Some(snapshot_id) = current_snapshot_id {
                refs.entry(MAIN_BRANCH.to_string())
                    .or_insert(SnapshotReference {
                        snapshot_id,
                        retention: SnapshotRetention::default(),
                    });
            }
            Ok(TableMetadata {
                format_version: FormatVersion::V3,
                table_uuid: value.table_uuid,
                location: value.location,
                last_sequence_number: value.last_sequence_number,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                current_schema_id: if schemas.keys().contains(&value.current_schema_id) {
                    Ok(value.current_schema_id)
                } else {
                    Err(Error::InvalidFormat("schema".to_string()))
                }?,
                schemas,
                partition_specs: HashMap::from_iter(
                    value.partition_specs.into_iter().map(|x| (*x.spec_id(), x)),
                ),
                default_spec_id: value.default_spec_id,
                last_partition_id: value.last_partition_id,
                properties: value.properties,
                current_snapshot_id,
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        HashMap::from_iter(snapshots.into_iter().map(|x| (x.snapshot_id, x.into())))
                    })
                    .unwrap_or_default(),
                snapshot_log: value.snapshot_log,
                metadata_log: value.metadata_log,
                sort_orders: HashMap::from_iter(
                    value.sort_orders.into_iter().map(|x| (x.order_id, x)),
                ),
                default_sort_order_id: value.default_sort_order_id,
                refs,
                next_row_id,
            })
        }
    }

    impl TryFrom<TableMetadataV2> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataV2) -> Result<Self, Error> {
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
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            let mut refs = value.refs;
            if let Some(snapshot_id) = current_snapshot_id {
                refs.entry(MAIN_BRANCH.to_string())
                    .or_insert(SnapshotReference {
                        snapshot_id,
                        retention: SnapshotRetention::default(),
                    });
            }
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
                    Err(Error::InvalidFormat("schema".to_string()))
                }?,
                schemas,
                partition_specs: HashMap::from_iter(
                    value.partition_specs.into_iter().map(|x| (*x.spec_id(), x)),
                ),
                default_spec_id: value.default_spec_id,
                last_partition_id: value.last_partition_id,
                properties: value.properties,
                current_snapshot_id,
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        HashMap::from_iter(snapshots.into_iter().map(|x| (x.snapshot_id, x.into())))
                    })
                    .unwrap_or_default(),
                snapshot_log: value.snapshot_log,
                metadata_log: value.metadata_log,
                sort_orders: HashMap::from_iter(
                    value.sort_orders.into_iter().map(|x| (x.order_id, x)),
                ),
                default_sort_order_id: value.default_sort_order_id,
                refs,
                next_row_id: 0,
            })
        }
    }

    impl TryFrom<TableMetadataV1> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataV1) -> Result<Self, Error> {
            let schemas = value
                .schemas
                .map(|schemas| {
                    Ok::<_, Error>(HashMap::from_iter(
                        schemas
                            .into_iter()
                            .enumerate()
                            .map(|(i, schema)| {
                                Ok((schema.schema_id.unwrap_or(i as i32), schema.try_into()?))
                            })
                            .collect::<Result<Vec<_>, Error>>()?,
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
                        vec![PartitionSpec::builder()
                            .with_spec_id(DEFAULT_SPEC_ID)
                            .with_fields(value.partition_spec)
                            .build()
                            .unwrap()]
                    })
                    .into_iter()
                    .map(|x| (*x.spec_id(), x)),
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
                current_snapshot_id: if let &Some(id) = &value.current_snapshot_id {
                    if id == -1 {
                        None
                    } else {
                        Some(id)
                    }
                } else {
                    value.current_snapshot_id
                },
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        Ok::<_, Error>(HashMap::from_iter(
                            snapshots
                                .into_iter()
                                .map(|x| Ok((x.snapshot_id, x.into())))
                                .collect::<Result<Vec<_>, Error>>()?,
                        ))
                    })
                    .transpose()?
                    .unwrap_or_default(),
                snapshot_log: value.snapshot_log,
                metadata_log: value.metadata_log,
                sort_orders: match value.sort_orders {
                    Some(sort_orders) => {
                        HashMap::from_iter(sort_orders.into_iter().map(|x| (x.order_id, x)))
                    }
                    None => HashMap::new(),
                },
                default_sort_order_id: value.default_sort_order_id.unwrap_or(DEFAULT_SORT_ORDER_ID),
                refs: HashMap::from_iter(vec![(
                    MAIN_BRANCH.to_string(),
                    SnapshotReference {
                        snapshot_id: value.current_snapshot_id.unwrap_or_default(),
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                )]),
                next_row_id: 0,
            })
        }
    }

    impl From<TableMetadata> for TableMetadataV3 {
        fn from(v: TableMetadata) -> Self {
            TableMetadataV3 {
                format_version: VersionNumber::<3>,
                table_uuid: v.table_uuid,
                location: v.location,
                next_row_id: v.next_row_id,
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
                snapshots: Some(v.snapshots.into_values().map(|x| x.into()).collect()),
                snapshot_log: v.snapshot_log,
                metadata_log: v.metadata_log,
                sort_orders: v.sort_orders.into_values().collect(),
                default_sort_order_id: v.default_sort_order_id,
                refs: v.refs,
            }
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
                snapshots: Some(v.snapshots.into_values().map(|x| x.into()).collect()),
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
                    .map(|x| x.fields().clone())
                    .unwrap_or_default(),
                partition_specs: Some(v.partition_specs.into_values().collect()),
                default_spec_id: Some(v.default_spec_id),
                last_partition_id: Some(v.last_partition_id),
                properties: v.properties,
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: Some(v.snapshots.into_values().map(|x| x.into()).collect()),
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

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
/// Iceberg format version
#[derive(Default)]
pub enum FormatVersion {
    /// Iceberg spec version 1
    V1 = b'1',
    /// Iceberg spec version 2
    #[default]
    V2 = b'2',
    /// Iceberg spec version 3
    V3 = b'3',
}

impl TryFrom<u8> for FormatVersion {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(FormatVersion::V1),
            2 => Ok(FormatVersion::V2),
            3 => Ok(FormatVersion::V3),
            _ => Err(Error::Conversion(
                "u8".to_string(),
                "format version".to_string(),
            )),
        }
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => b'1',
            FormatVersion::V2 => b'2',
            FormatVersion::V3 => b'3',
        }
    }
}

impl From<FormatVersion> for i32 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, fs};

    use uuid::Uuid;

    use crate::{
        error::Error,
        spec::{
            partition::{PartitionField, PartitionSpec, Transform},
            schema::SchemaBuilder,
            snapshot::{Operation, SnapshotBuilder, SnapshotReference, SnapshotRetention, Summary},
            sort::{NullOrder, SortDirection, SortField, SortOrderBuilder},
            table_metadata::{partition_fields, TableMetadata, TableMetadataBuilder},
            types::{PrimitiveType, StructField, Type},
        },
    };

    use super::{FormatVersion, SnapshotLog};
    use crate::spec::snapshot::Snapshot;

    fn check_table_metadata_serde(json: &str, expected_type: TableMetadata) {
        let desered_type: TableMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<TableMetadata>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    #[test]
    fn test_deserialize_table_data_v2() -> Result<(), Error> {
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
    fn test_deserialize_table_data_v1() -> Result<(), Error> {
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
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    #[test]
    fn test_deserialize_table_data_v3() -> Result<(), Error> {
        let data = r#"
            {
                "format-version" : 3,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "next-row-id": 7,
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
        assert_eq!(metadata.format_version, FormatVersion::V3);
        assert_eq!(metadata.next_row_id, 7);
        let metadata_two: TableMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    #[test]
    fn test_table_metadata_v2_file_valid() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2Valid.json").unwrap();

        let schema1 = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let schema2 = SchemaBuilder::default()
            .with_schema_id(1)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "y".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: Some("comment".to_owned()),
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "z".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_identifier_field_ids(vec![1, 2])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let sort_order = SortOrderBuilder::default()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build()
            .unwrap();

        let snapshot1 = SnapshotBuilder::default()
            .with_snapshot_id(3051729675574597004)
            .with_timestamp_ms(1515100955770)
            .with_sequence_number(0)
            .with_manifest_list("s3://a/b/1.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build()
            .expect("Failed to create snapshot");

        let snapshot2 = SnapshotBuilder::default()
            .with_snapshot_id(3055729675574597004)
            .with_parent_snapshot_id(3051729675574597004)
            .with_timestamp_ms(1555100955770)
            .with_sequence_number(1)
            .with_schema_id(1)
            .with_manifest_list("s3://a/b/2.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build()
            .expect("Failed to create snapshot");

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema1), (1, schema2)]),
            current_schema_id: 1,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order)]),
            snapshots: HashMap::from_iter(vec![
                (3051729675574597004, snapshot1),
                (3055729675574597004, snapshot2),
            ]),
            current_snapshot_id: Some(3055729675574597004),
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![
                SnapshotLog {
                    snapshot_id: 3051729675574597004,
                    timestamp_ms: 1515100955770,
                },
                SnapshotLog {
                    snapshot_id: 3055729675574597004,
                    timestamp_ms: 1555100955770,
                },
            ],
            metadata_log: Vec::new(),
            refs: HashMap::from_iter(vec![(
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 3055729675574597004,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )]),
            next_row_id: 0,
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v3_file_valid() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV3Valid.json").unwrap();

        let schema1 = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let schema2 = SchemaBuilder::default()
            .with_schema_id(1)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "y".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: Some("comment".to_owned()),
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "z".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_identifier_field_ids(vec![1, 2])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let sort_order = SortOrderBuilder::default()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build()
            .unwrap();

        let snapshot1 = SnapshotBuilder::default()
            .with_snapshot_id(3051729675574597004)
            .with_timestamp_ms(1515100955770)
            .with_sequence_number(0)
            .with_manifest_list("s3://a/b/1.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build()
            .expect("Failed to create snapshot");

        let snapshot2 = SnapshotBuilder::default()
            .with_snapshot_id(3055729675574597004)
            .with_parent_snapshot_id(3051729675574597004)
            .with_timestamp_ms(1555100955770)
            .with_sequence_number(1)
            .with_schema_id(1)
            .with_manifest_list("s3://a/b/2.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build()
            .expect("Failed to create snapshot");

        let expected = TableMetadata {
            format_version: FormatVersion::V3,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema1), (1, schema2)]),
            current_schema_id: 1,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order)]),
            snapshots: HashMap::from_iter(vec![
                (3051729675574597004, snapshot1),
                (3055729675574597004, snapshot2),
            ]),
            current_snapshot_id: Some(3055729675574597004),
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![
                SnapshotLog {
                    snapshot_id: 3051729675574597004,
                    timestamp_ms: 1515100955770,
                },
                SnapshotLog {
                    snapshot_id: 3055729675574597004,
                    timestamp_ms: 1555100955770,
                },
            ],
            metadata_log: Vec::new(),
            refs: HashMap::from_iter(vec![(
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 3055729675574597004,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )]),
            next_row_id: 42,
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v3_file_valid_minimal() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV3ValidMinimal.json").unwrap();

        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: Some(serde_json::json!(1)),
                write_default: Some(serde_json::json!(1)),
            })
            .with_struct_field(StructField {
                id: 2,
                name: "y".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: Some("comment".to_owned()),
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "z".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let sort_order = SortOrderBuilder::default()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V3,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema)]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order)]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![],
            metadata_log: Vec::new(),
            refs: HashMap::new(),
            next_row_id: 0,
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v2_file_valid_minimal() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2ValidMinimal.json").unwrap();

        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "y".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: Some("comment".to_owned()),
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "z".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let sort_order = SortOrderBuilder::default()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema)]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order)]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![],
            metadata_log: Vec::new(),
            refs: HashMap::new(),
            next_row_id: 0,
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v1_file_valid() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV1Valid.json").unwrap();

        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "x".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "y".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: Some("comment".to_owned()),
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "z".to_owned(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder()
            .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
            .build()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V1,
            table_uuid: Uuid::parse_str("d20125c8-7284-442c-9aea-15fee620737c").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573874,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, schema)]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 0,
            default_sort_order_id: 0,
            sort_orders: HashMap::new(),
            snapshots: HashMap::new(),
            current_snapshot_id: None,
            last_sequence_number: 0,
            properties: HashMap::new(),
            snapshot_log: vec![],
            metadata_log: Vec::new(),
            refs: HashMap::from_iter(vec![(
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: -1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )]),
            next_row_id: 0,
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v2_missing_sort_order() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2MissingSortOrder.json")
                .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum TableMetadataEnum"
        )
    }

    // --- Additional rejection fixtures -----------------------------------

    /// Returns `Err` if the fixture deserializes successfully; otherwise the
    /// loose assertion is enough — the specific serde error message has
    /// changed across serde versions in the past and is not stable enough
    /// to pin.
    fn assert_fixture_rejected(name: &str) {
        let payload = fs::read_to_string(format!("testdata/table_metadata/{name}")).unwrap();
        let result: Result<TableMetadata, _> = serde_json::from_str(&payload);
        assert!(
            result.is_err(),
            "fixture {name} unexpectedly deserialized to {:?}",
            result.ok(),
        );
    }

    #[test]
    fn test_table_metadata_unsupported_format_version_is_rejected() {
        assert_fixture_rejected("TableMetadataUnsupportedVersion.json");
    }

    #[test]
    fn test_table_metadata_v2_missing_schemas_is_rejected() {
        assert_fixture_rejected("TableMetadataV2MissingSchemas.json");
    }

    #[test]
    fn test_table_metadata_v2_missing_partition_specs_is_rejected() {
        assert_fixture_rejected("TableMetadataV2MissingPartitionSpecs.json");
    }

    #[test]
    fn test_table_metadata_v2_missing_last_partition_id_is_rejected() {
        assert_fixture_rejected("TableMetadataV2MissingLastPartitionId.json");
    }

    #[test]
    fn test_table_metadata_v2_current_schema_not_found_fails_schema_lookup() {
        // This fixture has a valid V2 shape but the `current-schema-id` does
        // not match any entry in `schemas`. Deserialization may succeed
        // (serde does not check the cross-reference), but resolving the
        // current schema must fail.
        let payload =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2CurrentSchemaNotFound.json")
                .unwrap();

        match serde_json::from_str::<TableMetadata>(&payload) {
            Ok(metadata) => {
                let err = metadata.current_schema(None).unwrap_err();
                assert!(
                    matches!(err, Error::InvalidFormat(_)),
                    "expected InvalidFormat, got {err:?}",
                );
            }
            Err(_) => {
                // If serde itself rejects it, that's also acceptable
                // protection for this gap.
            }
        }
    }

    // --- Behaviour on the minimal valid fixture --------------------------

    fn load_minimal_v2() -> TableMetadata {
        let payload =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2ValidMinimal.json").unwrap();
        serde_json::from_str(&payload).unwrap()
    }

    #[test]
    fn test_minimal_v2_current_schema_resolves_via_current_schema_id() {
        let metadata = load_minimal_v2();
        let schema = metadata.current_schema(None).unwrap();
        // Fixture pins schema-id 0 with fields x, y, z (all long).
        assert_eq!(schema.schema_id(), &0);
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "z"],
        );
    }

    #[test]
    fn test_minimal_v2_default_partition_spec_matches_default_spec_id() {
        let metadata = load_minimal_v2();
        let spec = metadata.default_partition_spec().unwrap();
        // Fixture has default-spec-id 0 with one identity field over `x`.
        assert_eq!(spec.spec_id(), &0);
        assert_eq!(spec.fields().len(), 1);
        assert_eq!(spec.fields()[0].name(), "x");
    }

    #[test]
    fn test_minimal_v2_current_snapshot_is_none_when_no_snapshots_exist() {
        let metadata = load_minimal_v2();
        assert!(metadata.current_snapshot(None).unwrap().is_none());
        // Also unknown refs return None instead of erroring.
        assert!(metadata.current_snapshot(Some("nope")).unwrap().is_none());
    }

    // --- TestSnapshotSelection: current_snapshot + sequence_number ---------
    //
    // The minimal V2 fixture has no snapshots. The tests below build
    // metadata with two snapshots + main/release branches and pin the
    // branch lookup tree used by reads, including the InvalidFormat
    // error when `current_snapshot_id` points at a missing snapshot but
    // refs/snapshots are otherwise populated.

    fn metadata_with_snapshots(
        snapshots: &[(i64, i64)],
        refs: &[(&str, i64)],
        current_snapshot_id: Option<i64>,
    ) -> TableMetadata {
        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                1,
                "id",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .build()
            .unwrap();

        let snapshots = snapshots
            .iter()
            .enumerate()
            .map(|(idx, (snapshot_id, timestamp))| {
                let snapshot = SnapshotBuilder::default()
                    .with_snapshot_id(*snapshot_id)
                    .with_sequence_number((idx + 1) as i64)
                    .with_timestamp_ms(*timestamp)
                    .with_manifest_list(format!("manifest-{snapshot_id}.avro"))
                    .with_summary(Summary {
                        operation: Operation::Append,
                        other: HashMap::new(),
                    })
                    .with_schema_id(0)
                    .build()
                    .unwrap();
                (*snapshot_id, snapshot)
            })
            .collect::<HashMap<_, _>>();

        let refs = refs
            .iter()
            .map(|(name, sid)| {
                (
                    (*name).to_string(),
                    SnapshotReference {
                        snapshot_id: *sid,
                        retention: SnapshotRetention::default(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        TableMetadataBuilder::default()
            .location("s3://tests/table".to_string())
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(0, schema)]))
            .snapshots(snapshots)
            .refs(refs)
            .current_snapshot_id(current_snapshot_id)
            .build()
            .unwrap()
    }

    #[test]
    fn test_current_snapshot_resolves_main_ref_when_branch_is_none() {
        let metadata = metadata_with_snapshots(&[(1, 1_000), (2, 2_000)], &[("main", 2)], Some(1));
        // `main` ref wins over `current_snapshot_id` per the impl.
        let snap = metadata.current_snapshot(None).unwrap().unwrap();
        assert_eq!(*snap.snapshot_id(), 2);
    }

    #[test]
    fn test_current_snapshot_falls_back_to_current_snapshot_id_when_main_ref_absent() {
        let metadata = metadata_with_snapshots(&[(7, 1_000)], &[], Some(7));
        let snap = metadata.current_snapshot(None).unwrap().unwrap();
        assert_eq!(*snap.snapshot_id(), 7);
    }

    #[test]
    fn test_current_snapshot_returns_explicit_branch() {
        let metadata = metadata_with_snapshots(
            &[(1, 1_000), (2, 2_000)],
            &[("main", 1), ("release", 2)],
            Some(1),
        );
        let snap = metadata.current_snapshot(Some("release")).unwrap().unwrap();
        assert_eq!(*snap.snapshot_id(), 2);
    }

    #[test]
    fn test_current_snapshot_unknown_ref_returns_none() {
        let metadata = metadata_with_snapshots(&[(1, 1_000)], &[("main", 1)], Some(1));
        let snap = metadata.current_snapshot(Some("ghost")).unwrap();
        assert!(snap.is_none());
    }

    #[test]
    fn test_current_snapshot_errors_when_main_missing_but_snapshots_present() {
        // No main ref, no current_snapshot_id, but the snapshots map is
        // populated — the impl treats this as a corrupted state.
        let metadata = metadata_with_snapshots(&[(1, 1_000)], &[], None);
        let err = metadata.current_snapshot(None).unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)), "got {err:?}");
    }

    #[test]
    fn test_sequence_number_lookup_by_snapshot_id() {
        let metadata = metadata_with_snapshots(&[(10, 1_000), (20, 2_000), (30, 3_000)], &[], None);
        // sequence_number is the insertion index + 1 in our fixture; the
        // helper just walks the snapshots map for the matching id.
        assert_eq!(metadata.sequence_number(10).unwrap(), 1);
        assert_eq!(metadata.sequence_number(20).unwrap(), 2);
        assert_eq!(metadata.sequence_number(30).unwrap(), 3);
        assert!(metadata.sequence_number(999).is_none());
    }

    // --- TestPartitioning: partition_fields + current_partition_fields ------

    fn metadata_for_partitioning() -> TableMetadata {
        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                1,
                "id",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .with_struct_field(StructField::new(
                2,
                "ts",
                true,
                Type::Primitive(PrimitiveType::Timestamp),
                None,
            ))
            .build()
            .unwrap();

        let spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_partition_field(PartitionField::new(1, 1000, "id_b", Transform::Bucket(4)))
            .with_partition_field(PartitionField::new(2, 1001, "ts_d", Transform::Day))
            .build()
            .unwrap();

        let snapshot = SnapshotBuilder::default()
            .with_snapshot_id(50)
            .with_sequence_number(1)
            .with_timestamp_ms(1_000)
            .with_manifest_list("manifest-50.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
            .unwrap();

        TableMetadataBuilder::default()
            .location("s3://tests/table".to_string())
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(0, schema)]))
            .partition_specs(HashMap::from_iter(vec![(0, spec)]))
            .default_spec_id(0)
            .snapshots(HashMap::from_iter(vec![(50, snapshot)]))
            .refs(HashMap::from_iter(vec![(
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 50,
                    retention: SnapshotRetention::default(),
                },
            )]))
            .current_snapshot_id(Some(50))
            .build()
            .unwrap()
    }

    #[test]
    fn test_partition_fields_helper_binds_each_partition_to_its_source() {
        let metadata = metadata_for_partitioning();
        let schema = metadata.current_schema(None).unwrap();
        let spec = metadata.default_partition_spec().unwrap();
        let bound = partition_fields(spec, schema).unwrap();

        assert_eq!(bound.len(), 2);
        assert_eq!(bound[0].name(), "id_b");
        assert_eq!(bound[0].source_name(), "id");
        assert_eq!(bound[0].transform(), &Transform::Bucket(4));
        assert_eq!(bound[1].name(), "ts_d");
        assert_eq!(bound[1].source_name(), "ts");
        assert_eq!(bound[1].transform(), &Transform::Day);
    }

    #[test]
    fn test_partition_fields_helper_errors_when_source_id_missing_from_schema() {
        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                42,
                "other",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .build()
            .unwrap();
        let spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_partition_field(PartitionField::new(99, 1000, "ghost", Transform::Identity))
            .build()
            .unwrap();

        let err = partition_fields(&spec, &schema).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    #[test]
    fn test_current_partition_fields_uses_default_spec_with_current_schema() {
        let metadata = metadata_for_partitioning();
        let bound = metadata.current_partition_fields(None).unwrap();
        assert_eq!(
            bound.iter().map(|b| b.name()).collect::<Vec<_>>(),
            vec!["id_b", "ts_d"],
        );
    }

    #[test]
    fn test_current_partition_fields_branch_routes_through_main_ref() {
        let metadata = metadata_for_partitioning();
        let by_none = metadata.current_partition_fields(None).unwrap();
        let by_main = metadata.current_partition_fields(Some("main")).unwrap();
        let names_none = by_none.iter().map(|b| b.name()).collect::<Vec<_>>();
        let names_main = by_main.iter().map(|b| b.name()).collect::<Vec<_>>();
        assert_eq!(names_none, names_main);
    }

    #[test]
    fn test_partition_fields_by_snapshot_id_uses_that_snapshots_schema() {
        let metadata = metadata_for_partitioning();
        let bound = metadata.partition_fields(50).unwrap();
        assert_eq!(
            bound.iter().map(|b| b.name()).collect::<Vec<_>>(),
            vec!["id_b", "ts_d"],
        );
    }

    // --- FormatVersion conversions ----------------------------------------
    //
    // FormatVersion has Default=V2, a TryFrom<u8> that accepts 1/2/3, and
    // two `From<FormatVersion>` impls (u8 returning ASCII digits, i32
    // returning the bare integer). The u8 impls disagree about
    // representation — `From` emits the ASCII byte `b'1'/b'2'/b'3'` while
    // `TryFrom` expects 1/2/3, so a round-trip through u8 cannot succeed
    // today. The #[ignore]'d test below pins that contract for the
    // eventual fix.

    #[test]
    fn test_format_version_default_is_v2() {
        assert_eq!(FormatVersion::default(), FormatVersion::V2);
    }

    #[test]
    fn test_format_version_to_u8_returns_ascii_digit() {
        let v1: u8 = FormatVersion::V1.into();
        let v2: u8 = FormatVersion::V2.into();
        let v3: u8 = FormatVersion::V3.into();
        assert_eq!(v1, b'1');
        assert_eq!(v2, b'2');
        assert_eq!(v3, b'3');
    }

    #[test]
    fn test_format_version_to_i32_returns_bare_integer() {
        let v1: i32 = FormatVersion::V1.into();
        let v2: i32 = FormatVersion::V2.into();
        let v3: i32 = FormatVersion::V3.into();
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);
    }

    #[test]
    fn test_format_version_try_from_u8_accepts_bare_integers_1_2_3() {
        assert_eq!(FormatVersion::try_from(1).unwrap(), FormatVersion::V1);
        assert_eq!(FormatVersion::try_from(2).unwrap(), FormatVersion::V2);
        assert_eq!(FormatVersion::try_from(3).unwrap(), FormatVersion::V3);
    }

    #[test]
    fn test_format_version_try_from_u8_rejects_zero_and_high_value() {
        assert!(FormatVersion::try_from(0u8).is_err());
        assert!(FormatVersion::try_from(4u8).is_err());
        assert!(FormatVersion::try_from(99u8).is_err());
    }

    #[test]
    fn test_format_version_into_u8_does_not_round_trip_via_try_from_today() {
        // `From<FormatVersion> for u8` returns the ASCII digit (b'1'/b'2'/
        // b'3') but `TryFrom<u8>` expects bare integers 1/2/3, so feeding
        // the Into result back into TryFrom errors.
        for v in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let as_u8: u8 = v.into();
            assert!(
                FormatVersion::try_from(as_u8).is_err(),
                "expected round-trip to fail for {as_u8}",
            );
        }
    }

    #[test]
    fn test_format_version_into_i32_then_to_u8_round_trips_via_try_from() {
        // The i32 conversion returns 1/2/3, which fits in u8 and matches
        // TryFrom — so this *indirect* path is consistent.
        for v in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let as_i32: i32 = v.into();
            let as_u8 = as_i32 as u8;
            assert_eq!(FormatVersion::try_from(as_u8).unwrap(), v);
        }
    }

    #[test]
    #[ignore = "spec gap: From<FormatVersion> for u8 emits ASCII digits (b'1'/b'2'/b'3') but TryFrom<u8> expects bare integers 1/2/3; the two should agree"]
    fn test_format_version_into_u8_round_trips_via_try_from_per_spec() {
        for v in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let as_u8: u8 = v.into();
            assert_eq!(FormatVersion::try_from(as_u8).unwrap(), v);
        }
    }

    // --- Port: TestSnapshotUtil (Apache Iceberg Java, 12 @Test) ------------
    //
    // Java's SnapshotUtil is a static helper class (ancestor walks + schema
    // lookups). Rust has no SnapshotUtil module; the data behind it lives
    // on `TableMetadata`:
    //   - `snapshots: HashMap<i64, Snapshot>` with `parent_snapshot_id` chain
    //   - `refs: HashMap<String, SnapshotReference>` for branches/tags
    //   - `current_snapshot(branch)`, `schema(snapshot_id)`,
    //     `current_schema(branch)` methods
    // The ancestor walks are open-coded inline per test below since Rust has
    // no equivalent helper functions. Where Rust's behaviour diverges (e.g.
    // `schema(invalid_id)` returns the current schema rather than erroring),
    // the divergence is pinned with `#[ignore]`.
    //
    // Fixture mirrors Java's TestSnapshotUtil layout:
    //   base ← main1 ← main2          (main branch)
    //   base ← branch                 ("b1" branch)
    //   base ← fork0(expired) ← fork1 ← fork2  ("fork" branch)

    fn snapshot_util_fixture() -> TableMetadata {
        let schema_v1 = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                1,
                "id",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .build()
            .unwrap();

        let build_snapshot = |id: i64, parent: Option<i64>, ts: i64, schema_id: i32| {
            let mut builder = SnapshotBuilder::default();
            builder
                .with_snapshot_id(id)
                .with_sequence_number(id)
                .with_timestamp_ms(ts)
                .with_manifest_list(format!("manifest-{id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    other: HashMap::new(),
                })
                .with_schema_id(schema_id);
            if let Some(p) = parent {
                builder.with_parent_snapshot_id(p);
            }
            builder.build().unwrap()
        };

        // Fork0 is "expired" — referenced as parent of fork1 but not present
        // in the snapshots map. This mirrors Java's expireSnapshotId(fork0).
        let snapshots: HashMap<i64, Snapshot> = HashMap::from_iter(vec![
            (10, build_snapshot(10, None, 1_000, 0)),     // base
            (11, build_snapshot(11, Some(10), 1_100, 0)), // main1
            (12, build_snapshot(12, Some(11), 1_200, 0)), // main2
            (20, build_snapshot(20, Some(10), 1_300, 0)), // branch
            // fork0 (id=30) expired — not in snapshots map
            (31, build_snapshot(31, Some(30), 1_500, 0)), // fork1 (parent expired)
            (32, build_snapshot(32, Some(31), 1_600, 0)), // fork2
        ]);

        let refs: HashMap<String, SnapshotReference> = HashMap::from_iter(vec![
            (
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 12,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            ),
            (
                "b1".to_string(),
                SnapshotReference {
                    snapshot_id: 20,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            ),
            (
                "fork".to_string(),
                SnapshotReference {
                    snapshot_id: 32,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            ),
        ]);

        TableMetadataBuilder::default()
            .location("s3://tests/table".to_string())
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(0, schema_v1)]))
            .snapshots(snapshots)
            .refs(refs)
            .current_snapshot_id(Some(12_i64))
            .build()
            .unwrap()
    }

    /// Walk the parent chain from `start` up to (but not including) the root,
    /// returning a vec of snapshot ids in newest-to-oldest order.
    fn snapshot_ancestor_ids(metadata: &TableMetadata, start: i64) -> Vec<i64> {
        let mut out = vec![];
        let mut cursor = Some(start);
        while let Some(id) = cursor {
            let Some(snap) = metadata.snapshots.get(&id) else {
                break;
            };
            out.push(id);
            cursor = *snap.parent_snapshot_id();
        }
        out
    }

    #[test]
    fn test_is_parent_ancestor_of_per_java() {
        // Java: isParentAncestorOf.
        let metadata = snapshot_util_fixture();
        // direct parent
        let parent = metadata
            .snapshots
            .get(&11)
            .and_then(|s| *s.parent_snapshot_id());
        assert_eq!(parent, Some(10));
        // not a direct parent (branch is off base, not main1)
        let parent = metadata
            .snapshots
            .get(&20)
            .and_then(|s| *s.parent_snapshot_id());
        assert_ne!(parent, Some(11));
        // direct parent walk even with expired intermediate
        let parent = metadata
            .snapshots
            .get(&32)
            .and_then(|s| *s.parent_snapshot_id());
        assert_eq!(parent, Some(31));
    }

    #[test]
    fn test_is_ancestor_of_walks_chain_per_java() {
        // Java: isAncestorOf — base is an ancestor of main1; branch is NOT an
        // ancestor of main1; fork0 is NOT an ancestor of fork2 because fork0
        // is expired (gap in the chain).
        let metadata = snapshot_util_fixture();
        // base (10) is ancestor of main1 (11): walk parent chain of 11.
        assert!(snapshot_ancestor_ids(&metadata, 11).contains(&10));
        // branch (20) is NOT ancestor of main1 (11).
        assert!(!snapshot_ancestor_ids(&metadata, 11).contains(&20));
        // fork0 (30) NOT reachable from fork2 (32) because fork0 is expired.
        assert!(!snapshot_ancestor_ids(&metadata, 32).contains(&30));
    }

    #[test]
    fn test_current_ancestors_walks_from_current_snapshot_per_java() {
        // Java: currentAncestors → [main2, main1, base].
        let metadata = snapshot_util_fixture();
        let current = metadata.current_snapshot(None).unwrap().unwrap();
        let ids = snapshot_ancestor_ids(&metadata, *current.snapshot_id());
        assert_eq!(ids, vec![12, 11, 10]);
    }

    #[test]
    fn test_oldest_ancestor_walks_to_root_per_java() {
        // Java: oldestAncestor on main → base.
        let metadata = snapshot_util_fixture();
        let oldest = snapshot_ancestor_ids(&metadata, 12).last().copied();
        assert_eq!(oldest, Some(10));
    }

    #[test]
    fn test_oldest_ancestor_of_specific_snapshot_per_java() {
        // Java: oldestAncestorOf(main2) → base.
        let metadata = snapshot_util_fixture();
        let oldest = snapshot_ancestor_ids(&metadata, 12).last().copied();
        assert_eq!(oldest, Some(10));
    }

    #[test]
    fn test_oldest_ancestor_after_timestamp_per_java() {
        // Java: oldestAncestorAfter(baseTimestamp + 1) → main1.
        let metadata = snapshot_util_fixture();
        let base_ts = *metadata.snapshots.get(&10).unwrap().timestamp_ms();
        // Filter ancestor chain to snapshots with timestamp > base_ts.
        let candidates: Vec<i64> = snapshot_ancestor_ids(&metadata, 12)
            .into_iter()
            .filter(|id| *metadata.snapshots.get(id).unwrap().timestamp_ms() > base_ts)
            .collect();
        // The newest-to-oldest walk; the oldest still-newer ancestor is the last.
        assert_eq!(candidates.last().copied(), Some(11));
    }

    #[test]
    fn test_snapshot_ids_between_excludes_endpoint_inclusive_start_per_java() {
        // Java: snapshotIdsBetween(table, base, main2) → [main2, main1].
        let metadata = snapshot_util_fixture();
        // Walk from main2 (12) backwards, collect until we hit base (10).
        let ids: Vec<i64> = snapshot_ancestor_ids(&metadata, 12)
            .into_iter()
            .take_while(|id| *id != 10)
            .collect();
        assert_eq!(ids, vec![12, 11]);
    }

    #[test]
    fn test_ancestors_between_per_java() {
        // Java: ancestorsBetween(main2, main1) → [main2].
        let metadata = snapshot_util_fixture();
        let ids: Vec<i64> = snapshot_ancestor_ids(&metadata, 12)
            .into_iter()
            .take_while(|id| *id != 11)
            .collect();
        assert_eq!(ids, vec![12]);

        // Java: ancestorsBetween(main2, branchId=20) → [main2, main1, base].
        // Branch (20) is not in main's ancestor chain, so walk goes to root.
        let chain = snapshot_ancestor_ids(&metadata, 12);
        let ids: Vec<i64> = chain.into_iter().take_while(|id| *id != 20).collect();
        assert_eq!(ids, vec![12, 11, 10]);
    }

    #[test]
    fn test_ancestors_of_walks_until_chain_breaks_per_java() {
        // Java: ancestorsOf(fork2, table::snapshot) → [fork2, fork1] (fork0
        // expired so the walk stops at fork1).
        let metadata = snapshot_util_fixture();
        let ids = snapshot_ancestor_ids(&metadata, 32);
        assert_eq!(ids, vec![32, 31]);
    }

    #[test]
    fn test_schema_for_ref_handles_null_unknown_and_main_per_java() {
        // Java: schemaFor(table, null) == schemaFor(table, "main") == initial.
        // Rust's current_schema(None) and current_schema(Some("main")) both
        // resolve to the current snapshot's schema; the snapshot's
        // schema_id is 0, which is the initial schema.
        let metadata = snapshot_util_fixture();
        let schema_null = *metadata.current_schema(None).unwrap().schema_id();
        let schema_main = *metadata.current_schema(Some("main")).unwrap().schema_id();
        // Rust returns the current_schema_id for non-existing refs (since
        // current_snapshot returns None and current_schema falls back).
        let schema_unknown = *metadata
            .current_schema(Some("non-existing-ref"))
            .unwrap()
            .schema_id();
        assert_eq!(schema_null, 0);
        assert_eq!(schema_main, 0);
        assert_eq!(schema_unknown, 0);
    }

    fn schema_evolution_fixture() -> TableMetadata {
        // Two schemas (id 0 = id-only, id 1 = id+zip). Snapshot 100 references
        // schema 0; snapshot 200 references schema 1. current_schema_id = 1
        // and "main" points to 200; "tag" points to 100.
        let schema_v0 = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                1,
                "id",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .build()
            .unwrap();
        let schema_v1 = SchemaBuilder::default()
            .with_schema_id(1)
            .with_struct_field(StructField::new(
                1,
                "id",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .with_struct_field(StructField::new(
                2,
                "zip",
                false,
                Type::Primitive(PrimitiveType::Int),
                None,
            ))
            .build()
            .unwrap();

        let snap_at = |id: i64, parent: Option<i64>, ts: i64, schema_id: i32| {
            let mut b = SnapshotBuilder::default();
            b.with_snapshot_id(id)
                .with_sequence_number(id)
                .with_timestamp_ms(ts)
                .with_manifest_list(format!("manifest-{id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    other: HashMap::new(),
                })
                .with_schema_id(schema_id);
            if let Some(p) = parent {
                b.with_parent_snapshot_id(p);
            }
            b.build().unwrap()
        };

        let snapshots = HashMap::from_iter(vec![
            (100_i64, snap_at(100, None, 1_000, 0)),
            (200_i64, snap_at(200, Some(100), 2_000, 1)),
        ]);

        let refs = HashMap::from_iter(vec![
            (
                "main".to_string(),
                SnapshotReference {
                    snapshot_id: 200,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            ),
            (
                "tag".to_string(),
                SnapshotReference {
                    snapshot_id: 100,
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                },
            ),
        ]);

        TableMetadataBuilder::default()
            .location("s3://tests/table".to_string())
            .current_schema_id(1)
            .schemas(HashMap::from_iter(vec![(0, schema_v0), (1, schema_v1)]))
            .snapshots(snapshots)
            .refs(refs)
            .current_snapshot_id(Some(200_i64))
            .build()
            .unwrap()
    }

    #[test]
    fn test_schema_for_branch_returns_branch_head_schema_per_java() {
        // Java: schemaForBranch — after evolving schema, the branch ref's
        // schema reflects the *evolution* because the branch advances with
        // the schema update.
        let metadata = schema_evolution_fixture();
        // main points at snap 200 which references schema id 1 (with zip).
        let schema = metadata.current_schema(Some("main")).unwrap();
        assert_eq!(*schema.schema_id(), 1);
        // The schema has two fields (id + zip).
        let mut field_names: Vec<&str> = schema.fields().iter().map(|f| f.name.as_str()).collect();
        field_names.sort();
        assert_eq!(field_names, vec!["id", "zip"]);
    }

    #[test]
    fn test_schema_for_tag_is_frozen_at_creation_time_per_java() {
        // Java: schemaForTag — tag was created when only the initial schema
        // existed, so schemaFor(table, tag) returns the OLD (initial) schema
        // even after evolution.
        let metadata = schema_evolution_fixture();
        // tag points at snap 100 which references schema id 0.
        let schema = metadata.current_schema(Some("tag")).unwrap();
        assert_eq!(*schema.schema_id(), 0);
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name.as_str()).collect();
        assert_eq!(field_names, vec!["id"]);
    }

    #[test]
    fn test_schema_for_snapshot_id_returns_that_snapshots_schema_per_java() {
        // Java: schemaForSnapshotId — schemaFor(table, snapshotId) returns the
        // schema active when that snapshot was created.
        let metadata = schema_evolution_fixture();
        let s100 = metadata.schema(100).unwrap();
        assert_eq!(*s100.schema_id(), 0);
        let s200 = metadata.schema(200).unwrap();
        assert_eq!(*s200.schema_id(), 1);
    }

    #[test]
    #[ignore = "behaviour gap: Rust's `schema(invalid_id)` silently falls back to current_schema_id and returns the current schema; Java's SnapshotUtil.schemaFor(table, invalidId) throws IllegalArgumentException(\"Cannot find snapshot with ID {id}\")"]
    fn test_schema_for_invalid_snapshot_id_errors_per_java() {
        let metadata = schema_evolution_fixture();
        let result = metadata.schema(999_999);
        assert!(
            result.is_err(),
            "expected error for invalid snapshot id, got {:?}",
            result.map(|s| *s.schema_id())
        );
    }

    #[test]
    #[ignore = "feature gap: Rust has no MetadataTable abstraction (MetadataTableUtils.createMetadataTableInstance / MetadataTableType.SNAPSHOTS); Java's schemaForSnapshotIdMetadataTable asserts the metadata table's own schema is returned regardless of snapshot id"]
    fn test_schema_for_snapshot_id_metadata_table_per_java() {
        let metadata = schema_evolution_fixture();
        // Rust has no metadata-table layer; this test cannot be expressed.
        // Pin the gap; once the layer is added, swap to asserting the fixed
        // metadata-table schema is returned for any snapshot id.
        let _ = metadata.schema(100);
    }

    // --- TestSchemaID port -------------------------------------------------
    //
    // Java's `TestSchemaID` has 2 @TestTemplate methods, each parametrised
    // over format versions {V1, V2, V3}. They pin the contract that:
    //
    //   1. `table.schema().schema_id()` and `table.snapshots[*].schema_id`
    //      stay in lockstep when no schema update occurs (append, delete,
    //      fast-append all retain the table's current schema_id).
    //   2. After `updateSchema().addColumn(name, type).commit()`:
    //        - `table.schema()` reflects the new fields,
    //        - `table.schema().schema_id()` is incremented,
    //        - the schema-update commit does NOT itself create a snapshot,
    //          so `table.current_snapshot().schema_id` still reads the
    //          old id until the NEXT data commit,
    //        - `table.schemas()` keeps BOTH the original and the updated
    //          schema entries.
    //
    // Rust has the data structures (`TableMetadata.schemas` map,
    // `Snapshot.schema_id` field, `Schema.schema_id` field) but no
    // operational API: there is no `updateSchema().addColumn(...).commit()`
    // builder reaching back into TableMetadata, and no `newAppend` /
    // `newDelete` / `newFastAppend` transaction-producer surface that
    // ties freshly committed snapshots to the current schema id.
    //
    // Both @TestTemplate scenarios are pinned as 1 Rust #[ignore] each.
    // Format-version parametrisation isn't observable in the assertions —
    // the same body runs unchanged across V1/V2/V3.

    #[test]
    #[ignore = "feature gap: no transaction-level newAppend/newDelete/newFastAppend; cannot assert that snapshots[*].schema_id stays equal to current schema_id across data-only ops"]
    fn test_schema_id_no_change_after_data_only_ops_per_java() {
        // Java: testNoChange. With no updateSchema():
        //   - initial schema_id S0.
        //   - newAppend(FILE_A, FILE_B).commit():
        //       table.current_snapshot().schema_id == S0;
        //       table.snapshots[*].schema_id == [S0].
        //   - newDelete(FILE_A).commit():
        //       table.current_snapshot().schema_id == S0;
        //       table.snapshots[*].schema_id == [S0, S0].
        //   - newFastAppend(FILE_A2).commit():
        //       table.current_snapshot().schema_id == S0;
        //       table.snapshots[*].schema_id == [S0, S0, S0].
        //   - throughout: table.schemas() == { S0 -> initial_schema }.
    }

    #[test]
    #[ignore = "feature gap: no updateSchema().addColumn(...).commit() flow; cannot assert that schema_id increments while current_snapshot's schema_id lags until the next data commit"]
    fn test_schema_id_change_after_schema_update_per_java() {
        // Java: testSchemaIdChangeInSchemaUpdate.
        //   - initial schema S0 with schema_id 0.
        //   - newAppend(FILE_A, FILE_B).commit():
        //       table.current_snapshot().schema_id == 0;
        //       table.schemas() == { 0 -> S0 }.
        //   - updateSchema().addColumn("data2", string).commit():
        //       table.schema() reflects new field;
        //       table.schema().schema_id() == 1;
        //       table.current_snapshot().schema_id == 0  // unchanged
        //                                                  (no snapshot
        //                                                  created by
        //                                                  schema update);
        //       table.schemas() == { 0 -> S0, 1 -> S1 }.
        //   - newDelete(FILE_A).commit():
        //       table.current_snapshot().schema_id == 1;
        //       table.snapshots[*].schema_id == [0, 1].
        //   - newAppend(FILE_A2).commit():
        //       table.current_snapshot().schema_id == 1;
        //       table.snapshots[*].schema_id == [0, 1, 1].
    }

    // --- TestTableMetadataParserCodec port ---------------------------------
    //
    // Java's `TableMetadataParser.Codec` enum tags the on-disk compression
    // applied to a `*.metadata.json` file. Static methods:
    //   - `Codec.fromName(name)` — case-insensitive lookup ("gzip" | "none");
    //     rejects unknown names with 'Invalid codec name: <name>'.
    //   - `Codec.fromFileName(path)` — detects the codec from the file
    //     suffix; rejects non-metadata filenames with
    //     '<path> is not a valid metadata file'.
    //
    // Supported filename patterns:
    //   v3.metadata.json                                  -> NONE
    //   v3-<uuid>.metadata.json                           -> NONE
    //   v3.gz.metadata.json                               -> GZIP
    //   v3-<uuid>.gz.metadata.json                        -> GZIP
    //   v3-<uuid>.metadata.json.gz                        -> GZIP
    //
    // Rust has NO `MetadataCodec` enum and no gzip-detection helper.
    // `TableMetadata` reads/writes JSON directly without an encoding
    // pre-pass. All 3 Java @Test scenarios are pinned `#[ignore]` for an
    // eventual `metadata::Codec::{from_name, from_file_name}` API.

    #[test]
    #[ignore = "feature gap: no metadata::Codec enum; Codec::from_name should be case-insensitive ('gzip'/'gZiP' -> Gzip, 'none'/'nOnE' -> None); from_file_name should detect codec from path suffix"]
    fn test_table_metadata_parser_codec_compression_codec_per_java() {
        // Java: testCompressionCodec.
        // 9 sub-assertions covering name lookup (4 cases incl. mixed-case)
        // and filename detection (5 cases for both Gzip and None patterns).
    }

    #[test]
    #[ignore = "feature gap: Codec::from_name('invalid') must error with 'Invalid codec name: invalid'"]
    fn test_table_metadata_parser_codec_invalid_codec_name_per_java() {
        // Java: testInvalidCodecName.
    }

    #[test]
    #[ignore = "feature gap: Codec::from_file_name('path/to/file.parquet') must error with '<path> is not a valid metadata file'"]
    fn test_table_metadata_parser_codec_invalid_file_name_per_java() {
        // Java: testInvalidFileName.
    }

    // --- TestCreateSnapshotEvent port --------------------------------------
    //
    // Java's `Listeners.register(listener, CreateSnapshotEvent.class)` wires
    // a callback that receives a `CreateSnapshotEvent { summary: Map<String,
    // String>, ... }` every time `table.newAppend()/newDelete()/etc.commit()`
    // produces a new snapshot. The event's `summary()` exposes the same
    // canonical metric keys that get written to the snapshot's summary:
    // added-records/added-data-files/total-records/total-data-files and
    // deleted-records/deleted-data-files on a delete commit.
    //
    // Rust has NO events module, no Listeners registry, and no commit-time
    // notification hook. Both @TestTemplate scenarios are pinned for an
    // eventual `events::Listener<CreateSnapshotEvent>` trait.

    #[test]
    #[ignore = "feature gap: no events::Listeners::register / CreateSnapshotEvent; append commit must notify listener with summary {added-records, added-data-files, total-records, total-data-files}"]
    fn test_create_snapshot_event_append_commit_event_per_java() {
        // Java: testAppendCommitEvent.
        // After 1st append(FILE_A): summary has added-records=1,
        // added-data-files=1, total-records=1, total-data-files=1.
        // After 2nd append(FILE_A): added-records=1, added-data-files=1,
        // total-records=2, total-data-files=2.
    }

    #[test]
    #[ignore = "feature gap: same listener flow but delete commit must also fire with deleted-records / deleted-data-files / total-records / total-data-files keys"]
    fn test_create_snapshot_event_append_and_delete_commit_event_per_java() {
        // Java: testAppendAndDeleteCommitEvent.
        // append(FILE_A, FILE_B) -> {added-records:2, added-data-files:2,
        //                            total-records:2, total-data-files:2}.
        // delete(FILE_A)         -> {deleted-records:1, deleted-data-files:1,
        //                            total-records:1, total-data-files:1}.
    }

    // --- TestSnapshotChanges port ------------------------------------------
    //
    // Java's `SnapshotChanges.builderFor(table).snapshot(s).build()` is a
    // diff-accessor over a snapshot that exposes:
    //   - `.addedDataFiles()` — Iterable<DataFile> added in `s`.
    //   - `.removedDataFiles()` — Iterable<DataFile> removed in `s`.
    // Both calls are cached (`isSameAs` equality on the second invocation).
    //
    // Rust has NO SnapshotChanges struct and no per-snapshot file diff
    // accessor (the data is implicit in manifest read paths but isn't
    // exposed as a builder-style API). All 3 @Test scenarios are pinned
    // for an eventual `snapshot_changes::SnapshotChanges` builder.

    #[test]
    #[ignore = "feature gap: no SnapshotChanges::builder_for(table).snapshot(s).build().added_data_files(); should return Iterable<DataFile> for the snapshot's additions"]
    fn test_snapshot_changes_added_data_files_per_java() {
        // Java: testAddedDataFiles.
        // newFastAppend.appendFile('/path/to/test-data.parquet').commit();
        // SnapshotChanges.added_data_files has 1 entry whose path matches.
    }

    #[test]
    #[ignore = "feature gap: no SnapshotChanges; .removed_data_files() must return Iterable<DataFile> for the snapshot's removals AND cache the iterable across repeated calls (isSameAs equality)"]
    fn test_snapshot_changes_removed_data_files_per_java() {
        // Java: testRemovedDataFiles.
        // Append (fileToRemove + fileToKeep). Delete (fileToRemove).
        // SnapshotChanges.removed_data_files has 1 entry matching
        // fileToRemove; two calls return the SAME Iterable reference.
    }

    #[test]
    #[ignore = "feature gap: SnapshotChanges must cache results — first and second calls to .removed_data_files() return the same Iterable reference"]
    fn test_snapshot_changes_caching_per_java() {
        // Java: testSnapshotChangesCaching.
        // Append (file1 + file2). Delete (file1).
        // changes.removed_data_files() (1st) == (2nd) by identity.
    }

    // --- TestMetricsModes port ---------------------------------------------
    //
    // Java's `org.apache.iceberg.MetricsModes` is an enum hierarchy
    // (None / Counts / Truncate(N) / Full) plus a parser
    // (`fromString(name)`) for the table-property string form. The
    // companion `MetricsConfig.forTable(table)` reads
    // `write.metadata.metrics.default` + per-column overrides and applies
    // these rules:
    //   - Invalid per-column modes default to the table default mode.
    //   - Invalid default mode falls back to `Truncate(16)` (library default).
    //   - Sort-key columns default to `Truncate(16)` even when the table
    //     default is `none`/`counts`.
    //   - `write.metadata.metrics.max-inferred-column-defaults` caps how
    //     many columns get the inferred default; the rest default to `None`.
    //   - V3 VariantType supports metrics modes; nested struct fields
    //     each get their own mode entry.
    //
    // Rust has NO `MetricsModes` / `MetricsConfig` / table-property
    // parser for this surface (grep finds zero matches). All 9 Java
    // @TestTemplate scenarios are pinned `#[ignore]` for an eventual
    // `metrics_modes::{from_string, MetricsConfig}` API.

    #[test]
    #[ignore = "feature gap: no MetricsModes::from_string; case-insensitive parse 'none'/'counts'/'truncate(N)'/'full' (8 sub-assertions covering mixed case)"]
    fn test_metrics_modes_parsing_per_java() {
        // Java: testMetricsModeParsing. 8 inputs all parse to expected variant.
    }

    #[test]
    #[ignore = "feature gap: MetricsModes::from_string('truncate(0)') must error 'Truncate length should be positive'"]
    fn test_metrics_modes_invalid_truncation_length_per_java() {
        // Java: testInvalidTruncationLength.
    }

    #[test]
    #[ignore = "feature gap: MetricsConfig must silently fall back invalid per-column mode to the table default (e.g. 'full')"]
    fn test_metrics_modes_invalid_column_mode_value_per_java() {
        // Java: testInvalidColumnModeValue.
        // default-mode=full, col=troncate(5). config.columnMode(col) == Full.
    }

    #[test]
    #[ignore = "feature gap: invalid default mode falls back to library default Truncate(16)"]
    fn test_metrics_modes_invalid_default_column_mode_value_per_java() {
        // Java: testInvalidDefaultColumnModeValue.
        // default-mode=fuull (sic). config.columnMode(*) == Truncate(16).
    }

    #[test]
    #[ignore = "feature gap: sort-key columns default to Truncate(16) when the table default is counts; per-column user override is respected (None) ignoring sort-key default"]
    fn test_metrics_modes_config_sorted_cols_default_per_java() {
        // Java: testMetricsConfigSortedColsDefault.
        // Schema (col1..col4). Sort: asc(col2).asc(col3). Properties:
        // default=counts; col1=counts; col2=none.
        // Expected: col1=Counts, col2=None (user override),
        //           col3=Truncate(16) (sort-default), col4=Counts.
    }

    #[test]
    #[ignore = "feature gap: invalid mode on a sort-key column falls back to the table default rather than the sort-key default Truncate(16)"]
    fn test_metrics_modes_config_sorted_cols_default_by_invalid_per_java() {
        // Java: testMetricsConfigSortedColsDefaultByInvalid.
        // Sort: asc(col2).asc(col3). Properties: default=counts;
        // col1=full; col2=invalid.
        // Expected: col1=Full, col2=Counts (fall back to table default).
    }

    #[test]
    #[ignore = "feature gap: METRICS_MAX_INFERRED_COLUMN_DEFAULTS caps how many columns get the inferred default; the rest default to None"]
    fn test_metrics_modes_config_inferred_default_mode_limit_per_java() {
        // Java: testMetricsConfigInferredDefaultModeLimit.
        // 3 columns; max-inferred=2. col1+col2=Truncate(16), col3=None.
    }

    #[test]
    #[ignore = "feature gap: V3 VariantType columns support metrics modes; assumes formatVersion >= 3"]
    fn test_metrics_modes_variant_supported_per_java() {
        // Java: testMetricsVariantSupported.
        // Schema (variant, int). max-inferred=1.
        // Expected: variant=Truncate(16), int=None.
    }

    #[test]
    #[ignore = "feature gap: nested struct fields get per-leaf entries in the MetricsConfig (col_struct, col_struct.a, col_struct.b, top)"]
    fn test_metrics_modes_config_nested_types_structs_per_java() {
        // Java: testMetricsConfigNestedTypesStructs.
        // Schema with col_struct(a, b) + top. max-inferred=2.
        // Expected: col_struct.a=Truncate(16), col_struct.b=None,
        //           top=Truncate(16).
    }

    // --- TestRowLineageMetadata port (V3 row-lineage) ----------------------
    //
    // Iceberg V3 adds row-lineage tracking: each Snapshot carries
    // `first_row_id` and `added_rows` fields, and TableMetadata tracks
    // `next_row_id` that increments by the snapshot's `added_rows` on
    // each new snapshot. Java's `BaseSnapshot` constructor performs
    // tight validation of these new fields.
    //
    // Rust state:
    //   - TableMetadata.next_row_id IS modelled (line 150 in this file).
    //   - Snapshot does NOT carry first_row_id / added_rows fields.
    //   - The TableMetadata builder does NOT auto-increment next_row_id
    //     when adding a snapshot; the constructor takes the value
    //     verbatim.
    //   - The validation rules Java pins on (first_row_id, added_rows)
    //     are not enforced.
    //
    // All 11 Java @Test/@TestTemplate scenarios are pinned `#[ignore]`
    // for an eventual extension of `Snapshot` with row-lineage fields
    // plus the matching validation in the constructor and a builder
    // step that increments TableMetadata::next_row_id on snapshot add.

    #[test]
    #[ignore = "feature gap: Snapshot has no first_row_id/added_rows fields. Java's BaseSnapshot constructor validates these (null first_row_id forces null added_rows; both can be 0; null added_rows with non-null first_row_id rejected; negative added_rows or first_row_id rejected with specific messages)"]
    fn test_row_lineage_snapshot_row_id_validation_per_java() {
        // Java: testSnapshotRowIDValidation. 6 sub-assertions:
        //   - first_row_id=null, added_rows=null -> both null.
        //   - first_row_id=null, added_rows=10 -> both null (added cleared).
        //   - first_row_id=0, added_rows=0 -> kept.
        //   - first_row_id=10, added_rows=null -> throws.
        //   - first_row_id=0, added_rows=-1 -> throws.
        //   - first_row_id=-1, added_rows=1 -> throws.
    }

    #[test]
    #[ignore = "feature gap: TableMetadata.addSnapshot must auto-increment next_row_id by the snapshot's added_rows; today the builder takes next_row_id verbatim"]
    fn test_row_lineage_snapshot_addition_per_java() {
        // Java: testSnapshotAddition.
        // base.next_row_id = 0; add snapshot(added_rows=30) ->
        //   first.next_row_id == 30.
        // add another (added_rows=30) -> second.next_row_id == 60.
    }

    #[test]
    #[ignore = "feature gap: TableMetadata.addSnapshot must reject snapshots whose first_row_id doesn't equal base.next_row_id, or whose first_row_id/added_rows are missing on a V3 table"]
    fn test_row_lineage_invalid_snapshot_addition_per_java() {
        // Java: testInvalidSnapshotAddition. Multiple sub-assertions
        // covering mismatch scenarios (first_row_id < base.next_row_id;
        // first_row_id > base.next_row_id; null added_rows on V3).
    }

    #[test]
    #[ignore = "feature gap: newFastAppend() must assign first_row_id = current next_row_id and added_rows from the data files"]
    fn test_row_lineage_fast_append_per_java() {
        // Java: testFastAppend. table.newFastAppend.appendFile(...).commit();
        // snapshot.first_row_id == 0 (initial); next_row_id == added.
    }

    #[test]
    #[ignore = "feature gap: newAppend() must assign first_row_id + added_rows"]
    fn test_row_lineage_append_per_java() {
        // Java: testAppend. Same flow with newAppend.
    }

    #[test]
    #[ignore = "feature gap: newAppend on a branch reference must still update next_row_id at the table level"]
    fn test_row_lineage_append_branch_per_java() {
        // Java: testAppendBranch. table.newAppend.toBranch("other").commit().
    }

    #[test]
    #[ignore = "feature gap: newDelete must NOT touch first_row_id/added_rows (deletes don't add rows)"]
    fn test_row_lineage_deletes_per_java() {
        // Java: testDeletes. delete-snapshot.added_rows == 0 (or null per spec).
    }

    #[test]
    #[ignore = "feature gap: position-delete files don't add rows; row-lineage stays unchanged"]
    fn test_row_lineage_position_deletes_per_java() {
        // Java: testPositionDeletes.
    }

    #[test]
    #[ignore = "feature gap: equality-delete files don't add rows; row-lineage stays unchanged"]
    fn test_row_lineage_equality_deletes_per_java() {
        // Java: testEqualityDeletes.
    }

    #[test]
    #[ignore = "feature gap: newReplace (overwrite/replacePartitions) must set first_row_id + added_rows for the replacement files"]
    fn test_row_lineage_replace_per_java() {
        // Java: testReplace.
    }

    #[test]
    #[ignore = "feature gap: rewriteManifests (metadata-only rewrite) must NOT touch first_row_id/added_rows or next_row_id"]
    fn test_row_lineage_metadata_rewrite_per_java() {
        // Java: testMetadataRewrite.
    }
}
