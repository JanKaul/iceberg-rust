//! Commit operations for atomic metadata updates in Iceberg catalogs
//!
//! This module provides the core functionality for atomic commits to Iceberg tables and views:
//! * Commit structures for tables and views
//! * Update operations that can be applied
//! * Requirements that must be satisfied
//! * Functions to validate requirements and apply updates
//!
//! All changes are made atomically - either all updates succeed or none are applied.
//! Requirements are checked first to ensure concurrent modifications don't corrupt state.

use iceberg_rust_spec::{
    spec::{
        partition::PartitionSpec,
        schema::Schema,
        snapshot::{Snapshot, SnapshotReference},
        sort::SortOrder,
        table_metadata::TableMetadata,
        view_metadata::{GeneralViewMetadata, Version},
    },
    table_metadata::SnapshotLog,
    view_metadata::Materialization,
};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::error::Error;

use super::identifier::Identifier;

/// A commit operation to update table metadata in an Iceberg catalog
///
/// This struct represents an atomic commit operation that can:
/// * Update table metadata
/// * Add or remove snapshots
/// * Modify schema, partition specs, and sort orders
///
/// The commit includes both requirements that must be satisfied and
/// a list of updates to apply atomically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitTable {
    /// Table identifier
    pub identifier: Identifier,
    /// Assertions about the metadata that must be true to update the metadata
    pub requirements: Vec<TableRequirement>,
    /// Changes to the table metadata
    pub updates: Vec<TableUpdate>,
}

/// A commit operation to update view metadata in an Iceberg catalog
///
/// This struct represents an atomic commit operation that can:
/// * Update view metadata
/// * Add or modify schemas
/// * Update view versions
/// * Modify location and properties
///
/// The commit includes both requirements that must be satisfied and
/// a list of updates to apply atomically.
///
/// # Type Parameters
/// * `T` - The materialization type for the view, typically `Option<()>` for regular views
///   or custom types for materialized views
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitView<T: Materialization> {
    /// Table identifier
    pub identifier: Identifier,
    /// Assertions about the metadata that must be true to update the metadata
    pub requirements: Vec<ViewRequirement>,
    /// Changes to the table metadata
    pub updates: Vec<ViewUpdate<T>>,
}

/// Updates that can be applied to table metadata in a commit operation
///
/// This enum represents all possible modifications that can be made to table metadata:
/// * UUID assignment (only during table creation)
/// * Format version updates
/// * Schema modifications
/// * Partition spec and sort order changes
/// * Snapshot management (add, remove, set references)
/// * Location and property updates
///
/// Each variant includes the necessary data for that specific update type.
/// Updates are applied atomically as part of a commit operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableUpdate {
    /// Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
    AssignUuid {
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
    AddSpec {
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

/// Requirements that must be met before applying updates to table metadata
///
/// This enum defines preconditions that must be satisfied before a table update
/// can be committed. Requirements are checked atomically to prevent concurrent
/// modifications from corrupting table state.
///
/// # Requirements Types
/// * Table existence checks
/// * UUID validation
/// * Reference state validation
/// * Schema and partition spec version checks
/// * Sort order validation
/// * Column and partition ID validation
///
/// Each variant includes the specific values that must match the current table state.
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

/// Updates that can be applied to view metadata in a commit operation
///
/// This enum represents all possible modifications that can be made to view metadata:
/// * UUID assignment (only during view creation)
/// * Format version updates
/// * Schema modifications
/// * Location and property updates
/// * Version management (add versions, set current version)
///
/// # Type Parameters
/// * `T` - The materialization type for the view, typically `Option<()>` for regular views
///   or `FullIdentifier` for materialized views
///
/// Each variant includes the necessary data for that specific update type.
/// Updates are applied atomically as part of a commit operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum ViewUpdate<T: Materialization> {
    /// Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
    AssignUuid {
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

/// Requirements that must be met before applying updates to view metadata
///
/// This enum defines preconditions that must be satisfied before a view update
/// can be committed. Requirements are checked atomically to prevent concurrent
/// modifications from corrupting view state.
///
/// # Requirements Types
/// * UUID validation - Ensures view UUID matches expected value
///
/// Each variant includes the specific values that must match the current view state.
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
/// Validates that table metadata meets all specified requirements
///
/// This function checks if the current table metadata satisfies all the requirements
/// specified for a commit operation. It ensures atomic updates by verifying preconditions
/// like UUID matches, snapshot references, schema versions etc.
///
/// # Arguments
/// * `requirements` - List of requirements that must be satisfied
/// * `metadata` - Current table metadata to validate against
///
/// # Returns
/// * `true` if all requirements are met
/// * `false` if any requirement is not satisfied
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

/// Validates that view metadata meets all specified requirements
///
/// This function checks if the current view metadata satisfies all the requirements
/// specified for a commit operation. It ensures atomic updates by verifying preconditions
/// like UUID matches.
///
/// # Type Parameters
/// * `T` - The materialization type for the view, must implement Materialization, Eq and 'static
///
/// # Arguments
/// * `requirements` - List of requirements that must be satisfied
/// * `metadata` - Current view metadata to validate against
///
/// # Returns
/// * `true` if all requirements are met
/// * `false` if any requirement is not satisfied
pub fn check_view_requirements<T: Materialization + Eq + 'static>(
    requirements: &[ViewRequirement],
    metadata: &GeneralViewMetadata<T>,
) -> bool {
    requirements.iter().all(|x| match x {
        ViewRequirement::AssertViewUuid { uuid } => metadata.view_uuid == *uuid,
    })
}
/// Applies a sequence of updates to table metadata
///
/// This function processes each update in order, modifying the table metadata accordingly.
/// Updates can include:
/// * Format version changes
/// * UUID assignments
/// * Schema modifications
/// * Partition spec and sort order changes
/// * Snapshot management
/// * Location and property updates
///
/// # Arguments
/// * `metadata` - Mutable reference to table metadata to modify
/// * `updates` - Vector of updates to apply
///
/// # Returns
/// * `Ok(())` if all updates were applied successfully
/// * `Err(Error)` if any update failed to apply
pub fn apply_table_updates(
    metadata: &mut TableMetadata,
    updates: Vec<TableUpdate>,
) -> Result<(), Error> {
    let mut added_schema_id = None;
    let mut added_spec_id = None;
    let mut added_sort_order_id = None;
    for update in updates {
        match update {
            TableUpdate::UpgradeFormatVersion { format_version } => {
                if i32::from(metadata.format_version) != format_version {
                    unimplemented!("Table format upgrade");
                }
            }
            TableUpdate::AssignUuid { uuid } => {
                metadata.table_uuid = Uuid::parse_str(&uuid)?;
            }
            TableUpdate::AddSchema {
                schema,
                last_column_id,
            } => {
                let schema_id = *schema.schema_id();
                metadata.schemas.insert(schema_id, schema);
                added_schema_id = Some(schema_id);
                if let Some(last_column_id) = last_column_id {
                    metadata.last_column_id = last_column_id;
                }
            }
            TableUpdate::SetCurrentSchema { schema_id } => {
                if schema_id == -1 {
                    if let Some(added_schema_id) = added_schema_id {
                        metadata.current_schema_id = added_schema_id;
                    } else {
                        return Err(Error::InvalidFormat(
                            "Cannot set current schema to -1 without adding a schema first"
                                .to_string(),
                        ));
                    }
                } else {
                    metadata.current_schema_id = schema_id;
                }
            }
            TableUpdate::AddSpec { spec } => {
                let spec_id = *spec.spec_id();
                metadata.partition_specs.insert(spec_id, spec);
                added_spec_id = Some(spec_id);
            }
            TableUpdate::SetDefaultSpec { spec_id } => {
                if spec_id == -1 {
                    if let Some(added_spec_id) = added_spec_id {
                        metadata.default_spec_id = added_spec_id;
                    } else {
                        return Err(Error::InvalidFormat(
                            "Cannot set default spec to -1 without adding a spec first".to_string(),
                        ));
                    }
                } else {
                    metadata.default_spec_id = spec_id;
                }
            }
            TableUpdate::AddSortOrder { sort_order } => {
                let sort_order_id = sort_order.order_id;
                metadata.sort_orders.insert(sort_order_id, sort_order);
                added_sort_order_id = Some(sort_order_id);
            }
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                if sort_order_id == -1 {
                    if let Some(added_sort_order_id) = added_sort_order_id {
                        metadata.default_sort_order_id = added_sort_order_id;
                    } else {
                        return Err(Error::InvalidFormat(
                            "Cannot set default sort order to -1 without adding a sort order first"
                                .to_string(),
                        ));
                    }
                } else {
                    metadata.default_sort_order_id = sort_order_id;
                }
            }
            TableUpdate::AddSnapshot { snapshot } => {
                metadata.snapshot_log.push(SnapshotLog {
                    snapshot_id: *snapshot.snapshot_id(),
                    timestamp_ms: *snapshot.timestamp_ms(),
                });
                metadata.last_sequence_number = *snapshot.sequence_number();
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

                metadata
                    .snapshot_log
                    .retain(|e| metadata.snapshots.contains_key(&e.snapshot_id));
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

    // Lastly make sure `last-updated-ms` field is up-to-date
    metadata.last_updated_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    Ok(())
}

/// Applies a sequence of updates to view metadata
///
/// This function processes each update in order, modifying the view metadata accordingly.
/// Updates can include:
/// * Format version changes
/// * UUID assignments
/// * Schema modifications
/// * Location and property updates
/// * Version management
///
/// # Type Parameters
/// * `T` - The materialization type for the view, must implement Materialization + 'static
///
/// # Arguments
/// * `metadata` - Mutable reference to view metadata to modify
/// * `updates` - Vector of updates to apply
///
/// # Returns
/// * `Ok(())` if all updates were applied successfully
/// * `Err(Error)` if any update failed to apply
pub fn apply_view_updates<T: Materialization + 'static>(
    metadata: &mut GeneralViewMetadata<T>,
    updates: Vec<ViewUpdate<T>>,
) -> Result<(), Error> {
    for update in updates {
        match update {
            ViewUpdate::UpgradeFormatVersion { format_version } => {
                if i32::from(metadata.format_version.clone()) != format_version {
                    unimplemented!("Upgrade of format version");
                }
            }
            ViewUpdate::AssignUuid { uuid } => {
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
