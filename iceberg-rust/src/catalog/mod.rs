/*!
Defines traits to communicate with an iceberg catalog.
*/

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub mod identifier;
pub mod namespace;

use iceberg_rust_spec::spec::partition::PartitionSpec;
use iceberg_rust_spec::spec::schema::Schema;
use iceberg_rust_spec::spec::snapshot::{Snapshot, SnapshotReference};
use iceberg_rust_spec::spec::sort::SortOrder;
use identifier::Identifier;
use object_store::ObjectStore;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;

use self::bucket::Bucket;
use self::namespace::Namespace;
use self::tabular::Tabular;

pub mod bucket;
pub mod requirements;
pub mod tabular;
pub mod updates;

/// Trait to create, replace and drop tables in an iceberg catalog.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + Debug {
    /// Lists all tables in the given namespace.
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<Identifier>, Error>;
    /// Lists all namespaces in the catalog.
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, Error>;
    /// Create a table from an identifier and a schema
    /// Check if a table exists
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool, Error>;
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Load a table.
    async fn load_table(self: Arc<Self>, identifier: &Identifier) -> Result<Tabular, Error>;
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_file_location: &str,
    ) -> Result<Tabular, Error>;
    /// Update a table by atomically changing the pointer to the metadata file
    async fn update_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_file_location: &str,
        previous_metadata_file_location: &str,
    ) -> Result<Tabular, Error>;
    /// Return the associated object store for a bucket
    fn object_store(&self, bucket: Bucket) -> Arc<dyn ObjectStore>;
}

/// Trait to obtain a catalog by name
#[async_trait::async_trait]
pub trait CatalogList: Send + Sync + Debug {
    /// Get catalog from list by name
    async fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>>;
    /// Get the list of available catalogs
    async fn list_catalogs(&self) -> Vec<String>;
}

/// Update metadata of a table
#[derive(Serialize, Deserialize)]
pub struct CommitTable {
    /// Table identifier
    pub identifier: Identifier,
    /// Assertions about the metadata that must be true to update the metadata
    pub requirements: Vec<TableRequirement>,
    /// Changes to the table metadata
    pub updates: Vec<TableUpdate>,
}

/// Update the metadata of a table in the catalog
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableUpdate {
    /// Assign new uuid
    AssignUUID {
        /// new uuid
        uuid: String,
    },
    /// Update format version
    UpgradeFormatVersion {
        /// New format version
        format_version: i32,
    },
    /// Add a new schema
    AddSchema {
        /// Schema to add
        schema: Schema,
        /// New last column id
        last_column_id: Option<i32>,
    },
    /// Set current schema
    SetCurrentSchema {
        /// New schema_id
        schema_id: i32,
    },
    /// Add new partition spec
    AddPartitionSpec {
        /// New partition spec
        spec: PartitionSpec,
    },
    /// Set the default partition spec
    SetDefaultSpec {
        /// Spec id to set
        spec_id: i32,
    },
    /// Add a new sort order
    AddSortOrder {
        /// New sort order
        sort_order: SortOrder,
    },
    /// Set the default sort order
    SetDefaultSortOrder {
        /// Sort order id to set
        sort_order_id: i32,
    },
    /// Add a new snapshot
    AddSnapshot {
        /// New snapshot
        snapshot: Snapshot,
    },
    /// Set the current snapshot reference
    SetSnapshotRef {
        /// Name of the snapshot refrence
        ref_name: String,
        /// Snapshot refernce to set
        snapshot_reference: SnapshotReference,
    },
    /// Remove snapshots with certain snapshot ids
    RemoveSnapshots {
        /// Ids of the snapshots to remove
        snapshot_ids: Vec<i64>,
    },
    /// Remove snapshot reference
    RemoveSnapshotRef {
        /// Name of the snapshot ref to remove
        ref_name: String,
    },
    /// Set a new location for the table
    SetLocation {
        /// New Location
        location: String,
    },
    /// Set table properties
    SetProperties {
        /// Properties to set
        updates: HashMap<String, String>,
    },
    /// Remove table properties
    RemoveProperties {
        /// Properties to remove
        removals: Vec<String>,
    },
}

/// Requirements on the table metadata to perform the updates
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableRequirement {
    /// The table must not already exist; used for create transactions
    AssertCreate,
    /// The table UUID must match the requirement's `uuid`
    AssertTableUuid {
        /// Uuid to assert
        uuid: Uuid,
    },
    /// The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`;
    /// if `snapshot-id` is `null` or missing, the ref must not already exist
    AssertRefSnapshotId {
        /// Name of ref
        r#ref: String,
        /// Snapshot id
        snapshot_id: i64,
    },
    /// The table's last assigned column id must match the requirement's `last-assigned-field-id`
    AssertLastAssignedFieldId {
        /// Id of last assigned id
        last_assigned_field_id: i32,
    },
    /// The table's current schema id must match the requirement's `current-schema-id`
    AssertCurrentSchemaId {
        /// Current schema id
        current_schema_id: i32,
    },
    ///The table's last assigned partition id must match the requirement's `last-assigned-partition-id`
    AssertLastAssignedPartitionId {
        /// id of last assigned partition
        last_assigned_partition_id: i32,
    },
    /// The table's default spec id must match the requirement's `default-spec-id`
    AssertDefaultSpecId {
        /// Default spec id
        default_spec_id: i32,
    },
    /// The table's default sort order id must match the requirement's `default-sort-order-id`
    AssertDefaultSortOrderId {
        /// Default sort order id
        default_sort_order_id: i32,
    },
}
