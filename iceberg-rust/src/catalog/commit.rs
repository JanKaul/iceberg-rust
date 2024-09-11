/*!
Struct to perform a [CommitTable] or [CommitView] operation
*/
use std::collections::HashMap;

use iceberg_rust_spec::{
    spec::{
        partition::PartitionSpec,
        schema::Schema,
        snapshot::{Snapshot, SnapshotReference},
        sort::SortOrder,
        table_metadata::TableMetadata,
        view_metadata::{GeneralViewMetadata, Version},
    },
    view_metadata::Materialization,
};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;

use super::identifier::Identifier;

/// Update metadata of a table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitTable {
    /// Table identifier
    pub identifier: Identifier,
    /// Assertions about the metadata that must be true to update the metadata
    pub requirements: Vec<TableRequirement>,
    /// Changes to the table metadata
    pub updates: Vec<TableUpdate>,
}

/// Update metadata of a table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitView<T: Materialization> {
    /// Table identifier
    pub identifier: Identifier,
    /// Assertions about the metadata that must be true to update the metadata
    pub requirements: Vec<ViewRequirement>,
    /// Changes to the table metadata
    pub updates: Vec<ViewUpdate<T>>,
}

/// Update the metadata of a table in the catalog
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableUpdate {
    /// Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
    AssignUUID {
        /// new uuid
        uuid: String,
    },
    /// Update format version
    UpgradeFormatVersion {
        /// New format version
        format_version: i32,
    },
    /// The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side.
    AddSchema {
        /// Schema to add
        schema: Schema,
        /// New last column id
        last_column_id: Option<i32>,
    },
    /// Schema ID to set as current, or -1 to set last added schema
    SetCurrentSchema {
        /// New schema_id
        schema_id: i32,
    },
    /// Add new partition spec
    AddPartitionSpec {
        /// New partition spec
        spec: PartitionSpec,
    },
    /// Partition spec ID to set as the default, or -1 to set last added spec
    SetDefaultSpec {
        /// Spec id to set
        spec_id: i32,
    },
    /// Add a new sort order
    AddSortOrder {
        /// New sort order
        sort_order: SortOrder,
    },
    /// Sort order ID to set as the default, or -1 to set last added sort order
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
        #[serde(flatten)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Update the metadata of a view in the catalog
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum ViewUpdate<T: Materialization> {
    /// Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
    AssignUUID {
        /// new uuid
        uuid: String,
    },
    /// Update format version
    UpgradeFormatVersion {
        /// New format version
        format_version: i32,
    },
    /// The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side.
    AddSchema {
        /// Schema to add
        schema: Schema,
        /// New last column id
        last_column_id: Option<i32>,
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
    /// Add a new version to the view
    AddViewVersion {
        /// Version to add
        view_version: Version<T>,
    },
    /// The view version id to set as current, or -1 to set last added view version id
    SetCurrentViewVersion {
        /// The id to set
        view_version_id: i64,
    },
}

/// Requirements on the table metadata to perform the updates
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum ViewRequirement {
    /// The view UUID must match the requirement's `uuid`
    AssertViewUuid {
        /// Uuid to assert
        uuid: Uuid,
    },
}
/// Check table update requirements
pub fn check_table_requirements(
    requirements: &[TableRequirement],
    metadata: &TableMetadata,
) -> bool {
    requirements.iter().all(|x| match x {
        // Assert create has to be check in another place
        TableRequirement::AssertCreate => true,
        TableRequirement::AssertTableUuid { uuid } => metadata.table_uuid == *uuid,
        TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => metadata
            .refs
            .get(r#ref)
            .map(|id| id.snapshot_id == *snapshot_id)
            .unwrap_or(false),
        TableRequirement::AssertLastAssignedFieldId {
            last_assigned_field_id,
        } => metadata.last_column_id == *last_assigned_field_id,
        TableRequirement::AssertCurrentSchemaId { current_schema_id } => {
            metadata.current_schema_id == *current_schema_id
        }
        TableRequirement::AssertLastAssignedPartitionId {
            last_assigned_partition_id,
        } => metadata.last_partition_id == *last_assigned_partition_id,
        TableRequirement::AssertDefaultSpecId { default_spec_id } => {
            metadata.default_spec_id == *default_spec_id
        }
        TableRequirement::AssertDefaultSortOrderId {
            default_sort_order_id,
        } => metadata.default_sort_order_id == *default_sort_order_id,
    })
}

/// Check table update requirements
pub fn check_view_requirements<T: Materialization + Eq + 'static>(
    requirements: &[ViewRequirement],
    metadata: &GeneralViewMetadata<T>,
) -> bool {
    requirements.iter().all(|x| match x {
        ViewRequirement::AssertViewUuid { uuid } => metadata.view_uuid == *uuid,
    })
}
/// Apply updates to metadata
pub fn apply_table_updates(
    metadata: &mut TableMetadata,
    updates: Vec<TableUpdate>,
) -> Result<(), Error> {
    for update in updates {
        match update {
            TableUpdate::UpgradeFormatVersion { format_version: _ } => {
                unimplemented!();
            }
            TableUpdate::AssignUUID { uuid } => {
                metadata.table_uuid = Uuid::parse_str(&uuid)?;
            }
            TableUpdate::AddSchema {
                schema,
                last_column_id,
            } => {
                metadata.schemas.insert(*schema.schema_id(), schema);
                if let Some(last_column_id) = last_column_id {
                    metadata.last_column_id = last_column_id;
                }
            }
            TableUpdate::SetCurrentSchema { schema_id } => {
                metadata.current_schema_id = schema_id;
            }
            TableUpdate::AddPartitionSpec { spec } => {
                metadata.partition_specs.insert(*spec.spec_id(), spec);
            }
            TableUpdate::SetDefaultSpec { spec_id } => {
                metadata.default_spec_id = spec_id;
            }
            TableUpdate::AddSortOrder { sort_order } => {
                metadata.sort_orders.insert(sort_order.order_id, sort_order);
            }
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                metadata.default_sort_order_id = sort_order_id;
            }
            TableUpdate::AddSnapshot { snapshot } => {
                metadata.snapshots.insert(*snapshot.snapshot_id(), snapshot);
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                snapshot_reference,
            } => {
                if ref_name == "main" {
                    metadata.current_snapshot_id = Some(snapshot_reference.snapshot_id);
                }
                metadata.refs.insert(ref_name, snapshot_reference);
            }
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                for id in snapshot_ids {
                    metadata.snapshots.remove(&id);
                }
            }
            TableUpdate::RemoveSnapshotRef { ref_name } => {
                metadata.refs.remove(&ref_name);
            }
            TableUpdate::SetLocation { location } => {
                metadata.location = location;
            }
            TableUpdate::SetProperties { updates } => {
                metadata.properties.extend(updates);
            }
            TableUpdate::RemoveProperties { removals } => {
                for rem in removals {
                    metadata.properties.remove(&rem);
                }
            }
        };
    }
    Ok(())
}

/// Apply updates to metadata
pub fn apply_view_updates<T: Materialization + 'static>(
    metadata: &mut GeneralViewMetadata<T>,
    updates: Vec<ViewUpdate<T>>,
) -> Result<(), Error> {
    for update in updates {
        match update {
            ViewUpdate::UpgradeFormatVersion { format_version: _ } => {
                unimplemented!();
            }
            ViewUpdate::AssignUUID { uuid } => {
                metadata.view_uuid = Uuid::parse_str(&uuid)?;
            }
            ViewUpdate::AddSchema {
                schema,
                last_column_id: _,
            } => {
                metadata.schemas.insert(*schema.schema_id(), schema);
            }
            ViewUpdate::SetLocation { location } => {
                metadata.location = location;
            }
            ViewUpdate::SetProperties { updates } => {
                metadata.properties.extend(updates);
            }
            ViewUpdate::RemoveProperties { removals } => {
                for rem in removals {
                    metadata.properties.remove(&rem);
                }
            }
            ViewUpdate::AddViewVersion { view_version } => {
                metadata
                    .versions
                    .insert(view_version.version_id, view_version);
            }
            ViewUpdate::SetCurrentViewVersion { view_version_id } => {
                metadata.current_version_id = view_version_id;
            }
        };
    }
    Ok(())
}
