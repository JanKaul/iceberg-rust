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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg_rust_spec::spec::{
        partition::PartitionSpec,
        schema::SchemaBuilder,
        snapshot::{SnapshotReference, SnapshotRetention},
        table_metadata::TableMetadataBuilder,
        types::{PrimitiveType, StructField, Type},
    };
    use uuid::Uuid;

    use super::*;

    // --- TestUpdateRequirements: check_table_requirements ------------------
    //
    // `check_table_requirements` is a pure function — every variant of
    // TableRequirement maps to a field on TableMetadata. These tests pin
    // the pass/fail behaviour per variant + a couple of multi-requirement
    // boundary cases. The helper below builds the smallest metadata that
    // is valid for the builder.

    fn fixture(uuid: Uuid, refs: &[(&str, i64)]) -> TableMetadata {
        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let refs = refs
            .iter()
            .map(|(name, snapshot_id)| {
                (
                    (*name).to_string(),
                    SnapshotReference {
                        snapshot_id: *snapshot_id,
                        retention: SnapshotRetention::default(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        TableMetadataBuilder::default()
            .table_uuid(uuid)
            .location("s3://tests/table".to_owned())
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(0, schema)]))
            .partition_specs(HashMap::from_iter(vec![(0, PartitionSpec::default())]))
            .default_spec_id(0)
            .last_partition_id(999)
            .last_column_id(7)
            .default_sort_order_id(0)
            .refs(refs)
            .build()
            .unwrap()
    }

    #[test]
    fn test_check_table_requirements_empty_list_passes() {
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(check_table_requirements(&[], &metadata));
    }

    #[test]
    fn test_check_table_requirements_assert_create_treated_as_satisfied() {
        // The actual existence check happens elsewhere in the create path;
        // the requirement walker treats AssertCreate as a no-op.
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertCreate],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_table_uuid() {
        let uuid = Uuid::from_u128(0x42);
        let metadata = fixture(uuid, &[]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertTableUuid { uuid }],
            &metadata,
        ));
        assert!(!check_table_requirements(
            &[TableRequirement::AssertTableUuid {
                uuid: Uuid::from_u128(0x43),
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_ref_snapshot_id_matches_existing_ref() {
        let metadata = fixture(Uuid::nil(), &[("main", 1)]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertRefSnapshotId {
                r#ref: "main".to_string(),
                snapshot_id: 1,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_ref_snapshot_id_fails_on_wrong_id() {
        let metadata = fixture(Uuid::nil(), &[("main", 1)]);
        assert!(!check_table_requirements(
            &[TableRequirement::AssertRefSnapshotId {
                r#ref: "main".to_string(),
                snapshot_id: 9999,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_ref_snapshot_id_fails_when_ref_missing() {
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(!check_table_requirements(
            &[TableRequirement::AssertRefSnapshotId {
                r#ref: "absent".to_string(),
                snapshot_id: 1,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_current_schema_id() {
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertCurrentSchemaId {
                current_schema_id: 0,
            }],
            &metadata,
        ));
        assert!(!check_table_requirements(
            &[TableRequirement::AssertCurrentSchemaId {
                current_schema_id: 7,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_default_spec_id() {
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertDefaultSpecId { default_spec_id: 0 }],
            &metadata,
        ));
        assert!(!check_table_requirements(
            &[TableRequirement::AssertDefaultSpecId {
                default_spec_id: 42,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_default_sort_order_id() {
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id: 0,
            }],
            &metadata,
        ));
        assert!(!check_table_requirements(
            &[TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id: 99,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_last_assigned_field_id() {
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id: 7,
            }],
            &metadata,
        ));
        assert!(!check_table_requirements(
            &[TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id: 8,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_assert_last_assigned_partition_id() {
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(check_table_requirements(
            &[TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id: 999,
            }],
            &metadata,
        ));
        assert!(!check_table_requirements(
            &[TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id: 1000,
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_check_table_requirements_short_circuits_when_any_fails() {
        // Several pass-then-one-fail combination: a single failure must
        // sink the whole list (`Iterator::all` semantics).
        let uuid = Uuid::from_u128(0x55);
        let metadata = fixture(uuid, &[]);
        let reqs = vec![
            TableRequirement::AssertCurrentSchemaId {
                current_schema_id: 0,
            },
            TableRequirement::AssertTableUuid { uuid },
            TableRequirement::AssertDefaultSpecId {
                default_spec_id: 42, // wrong
            },
        ];
        assert!(!check_table_requirements(&reqs, &metadata));
    }

    // --- TestMetadataUpdateParser: apply_table_updates ----------------------

    use iceberg_rust_spec::spec::{
        partition::{PartitionField, Transform},
        snapshot::SnapshotBuilder,
        sort::{NullOrder, SortDirection, SortField, SortOrder},
    };

    fn add_default_snapshot(metadata: &mut TableMetadata, id: i64, seq: i64) {
        let snapshot = SnapshotBuilder::default()
            .with_snapshot_id(id)
            .with_sequence_number(seq)
            .with_timestamp_ms(seq * 1_000)
            .with_manifest_list(format!("manifest-{id}.avro"))
            .with_summary(iceberg_rust_spec::spec::snapshot::Summary {
                operation: iceberg_rust_spec::spec::snapshot::Operation::Append,
                other: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
            .unwrap();
        metadata.snapshots.insert(id, snapshot);
    }

    #[test]
    fn test_apply_table_updates_assign_uuid_parses_string() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let new_uuid = Uuid::from_u128(0xDEAD_BEEF);
        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::AssignUuid {
                uuid: new_uuid.to_string(),
            }],
        )
        .unwrap();
        assert_eq!(metadata.table_uuid, new_uuid);
    }

    #[test]
    fn test_apply_table_updates_add_schema_inserts_and_optionally_advances_last_column_id() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let new_schema = SchemaBuilder::default()
            .with_schema_id(5)
            .with_struct_field(StructField {
                id: 17,
                name: "label".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::AddSchema {
                schema: new_schema.clone(),
                last_column_id: Some(17),
            }],
        )
        .unwrap();
        assert!(metadata.schemas.contains_key(&5));
        assert_eq!(metadata.last_column_id, 17);
    }

    #[test]
    fn test_apply_table_updates_set_current_schema_by_explicit_id() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        // Add a schema with id 9, then SetCurrentSchema 9.
        let new_schema = SchemaBuilder::default()
            .with_schema_id(9)
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();
        apply_table_updates(
            &mut metadata,
            vec![
                TableUpdate::AddSchema {
                    schema: new_schema,
                    last_column_id: None,
                },
                TableUpdate::SetCurrentSchema { schema_id: 9 },
            ],
        )
        .unwrap();
        assert_eq!(metadata.current_schema_id, 9);
    }

    #[test]
    fn test_apply_table_updates_set_current_schema_minus_one_uses_last_added() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let new_schema = SchemaBuilder::default()
            .with_schema_id(11)
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();
        apply_table_updates(
            &mut metadata,
            vec![
                TableUpdate::AddSchema {
                    schema: new_schema,
                    last_column_id: None,
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
        )
        .unwrap();
        assert_eq!(metadata.current_schema_id, 11);
    }

    #[test]
    fn test_apply_table_updates_set_current_schema_minus_one_without_add_errors() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let err = apply_table_updates(
            &mut metadata,
            vec![TableUpdate::SetCurrentSchema { schema_id: -1 }],
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)));
    }

    #[test]
    fn test_apply_table_updates_add_spec_and_set_default_spec_minus_one() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let spec = PartitionSpec::builder()
            .with_spec_id(3)
            .with_partition_field(PartitionField::new(1, 1000, "id_id", Transform::Identity))
            .build()
            .unwrap();
        apply_table_updates(
            &mut metadata,
            vec![
                TableUpdate::AddSpec { spec },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ],
        )
        .unwrap();
        assert_eq!(metadata.default_spec_id, 3);
        assert!(metadata.partition_specs.contains_key(&3));
    }

    #[test]
    fn test_apply_table_updates_add_sort_order_and_set_default_minus_one() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let sort_order = SortOrder {
            order_id: 4,
            fields: vec![SortField {
                source_id: 1,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }],
        };
        apply_table_updates(
            &mut metadata,
            vec![
                TableUpdate::AddSortOrder { sort_order },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
            ],
        )
        .unwrap();
        assert_eq!(metadata.default_sort_order_id, 4);
        assert!(metadata.sort_orders.contains_key(&4));
    }

    #[test]
    fn test_apply_table_updates_add_snapshot_appends_log_and_advances_sequence() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let snapshot = SnapshotBuilder::default()
            .with_snapshot_id(101)
            .with_sequence_number(5)
            .with_timestamp_ms(5_000)
            .with_manifest_list("manifest-101.avro".to_string())
            .with_summary(iceberg_rust_spec::spec::snapshot::Summary {
                operation: iceberg_rust_spec::spec::snapshot::Operation::Append,
                other: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
            .unwrap();

        apply_table_updates(&mut metadata, vec![TableUpdate::AddSnapshot { snapshot }]).unwrap();
        assert!(metadata.snapshots.contains_key(&101));
        assert_eq!(metadata.last_sequence_number, 5);
        assert_eq!(metadata.snapshot_log.len(), 1);
        assert_eq!(metadata.snapshot_log[0].snapshot_id, 101);
        assert_eq!(metadata.snapshot_log[0].timestamp_ms, 5_000);
    }

    #[test]
    fn test_apply_table_updates_set_snapshot_ref_main_updates_current_snapshot_id() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_reference: SnapshotReference {
                    snapshot_id: 42,
                    retention: SnapshotRetention::default(),
                },
            }],
        )
        .unwrap();
        assert_eq!(metadata.current_snapshot_id, Some(42));
        assert!(metadata.refs.contains_key("main"));
    }

    #[test]
    fn test_apply_table_updates_set_snapshot_ref_non_main_does_not_touch_current_snapshot_id() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let before = metadata.current_snapshot_id;
        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::SetSnapshotRef {
                ref_name: "release".to_string(),
                snapshot_reference: SnapshotReference {
                    snapshot_id: 99,
                    retention: SnapshotRetention::default(),
                },
            }],
        )
        .unwrap();
        assert_eq!(metadata.current_snapshot_id, before);
        assert_eq!(metadata.refs.get("release").unwrap().snapshot_id, 99,);
    }

    #[test]
    fn test_apply_table_updates_remove_snapshots_prunes_snapshot_log() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        add_default_snapshot(&mut metadata, 1, 1);
        add_default_snapshot(&mut metadata, 2, 2);
        metadata.snapshot_log = vec![
            iceberg_rust_spec::table_metadata::SnapshotLog {
                snapshot_id: 1,
                timestamp_ms: 1_000,
            },
            iceberg_rust_spec::table_metadata::SnapshotLog {
                snapshot_id: 2,
                timestamp_ms: 2_000,
            },
        ];
        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![1],
            }],
        )
        .unwrap();
        assert!(!metadata.snapshots.contains_key(&1));
        assert!(metadata.snapshots.contains_key(&2));
        // snapshot_log entries pointing at removed snapshots are pruned.
        assert_eq!(metadata.snapshot_log.len(), 1);
        assert_eq!(metadata.snapshot_log[0].snapshot_id, 2);
    }

    #[test]
    fn test_apply_table_updates_set_location_and_properties_round_trip() {
        let mut metadata = fixture(Uuid::nil(), &[]);
        let mut new_props = HashMap::new();
        new_props.insert("write.format.default".to_string(), "parquet".to_string());
        new_props.insert("retention.days".to_string(), "30".to_string());

        apply_table_updates(
            &mut metadata,
            vec![
                TableUpdate::SetLocation {
                    location: "s3://bucket/new/location".to_string(),
                },
                TableUpdate::SetProperties { updates: new_props },
                TableUpdate::RemoveProperties {
                    removals: vec!["retention.days".to_string()],
                },
            ],
        )
        .unwrap();
        assert_eq!(metadata.location, "s3://bucket/new/location");
        assert_eq!(
            metadata
                .properties
                .get("write.format.default")
                .map(String::as_str),
            Some("parquet"),
        );
        assert!(!metadata.properties.contains_key("retention.days"));
    }

    // --- TestUpdateRequirementParser / TestUpdateTableRequestParser --------
    //
    // The TableRequirement / TableUpdate enums use serde with a kebab-case
    // tag (`"type"` / `"action"`) and kebab-case field names. These tests
    // pin the exact wire shape so a REST catalog talking to the Java
    // reference implementation sees the same payload.

    #[test]
    fn test_table_requirement_assert_create_serialises_with_kebab_case_type() {
        let json = serde_json::to_value(TableRequirement::AssertCreate).unwrap();
        assert_eq!(json["type"], "assert-create");
    }

    #[test]
    fn test_table_requirement_round_trips_every_variant_via_json() {
        let uuid = Uuid::from_u128(0x42);
        let cases = vec![
            TableRequirement::AssertCreate,
            TableRequirement::AssertTableUuid { uuid },
            TableRequirement::AssertRefSnapshotId {
                r#ref: "main".to_string(),
                snapshot_id: 17,
            },
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id: 7,
            },
            TableRequirement::AssertCurrentSchemaId {
                current_schema_id: 3,
            },
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id: 1001,
            },
            TableRequirement::AssertDefaultSpecId { default_spec_id: 2 },
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id: 4,
            },
        ];
        for req in cases {
            let json = serde_json::to_string(&req).unwrap();
            let back: TableRequirement = serde_json::from_str(&json).unwrap();
            assert_eq!(back, req, "round-trip mismatch via {json}");
        }
    }

    #[test]
    fn test_table_requirement_assert_ref_snapshot_id_uses_kebab_case_field_names() {
        // `r#ref` should serialise as `ref` (Rust keyword escape) and
        // `snapshot_id` as `snapshot-id` thanks to rename_all_fields.
        let req = TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_string(),
            snapshot_id: 17,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["type"], "assert-ref-snapshot-id");
        assert_eq!(json["ref"], "main");
        assert_eq!(json["snapshot-id"], 17);
    }

    #[test]
    fn test_table_requirement_rejects_unknown_type_tag() {
        let json = r#"{"type": "assert-frobnicated", "extra": 42}"#;
        assert!(serde_json::from_str::<TableRequirement>(json).is_err());
    }

    #[test]
    fn test_table_update_round_trips_every_variant_via_json() {
        let schema = SchemaBuilder::default()
            .with_schema_id(8)
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap();

        let snapshot = SnapshotBuilder::default()
            .with_snapshot_id(101)
            .with_sequence_number(5)
            .with_timestamp_ms(5_000)
            .with_manifest_list("manifest-101.avro".to_string())
            .with_summary(iceberg_rust_spec::spec::snapshot::Summary {
                operation: iceberg_rust_spec::spec::snapshot::Operation::Append,
                other: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
            .unwrap();

        let mut props = HashMap::new();
        props.insert("k".to_string(), "v".to_string());

        let cases = vec![
            TableUpdate::AssignUuid {
                uuid: Uuid::from_u128(0xA).to_string(),
            },
            TableUpdate::UpgradeFormatVersion { format_version: 2 },
            TableUpdate::AddSchema {
                schema,
                last_column_id: Some(7),
            },
            TableUpdate::SetCurrentSchema { schema_id: 8 },
            TableUpdate::AddSpec {
                spec: PartitionSpec::builder()
                    .with_spec_id(3)
                    .with_partition_field(PartitionField::new(1, 1000, "x", Transform::Identity))
                    .build()
                    .unwrap(),
            },
            TableUpdate::SetDefaultSpec { spec_id: 3 },
            TableUpdate::AddSortOrder {
                sort_order: SortOrder {
                    order_id: 4,
                    fields: vec![SortField {
                        source_id: 1,
                        transform: Transform::Identity,
                        direction: SortDirection::Ascending,
                        null_order: NullOrder::First,
                    }],
                },
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: 4 },
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_reference: SnapshotReference {
                    snapshot_id: 17,
                    retention: SnapshotRetention::default(),
                },
            },
            TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![1, 2],
            },
            TableUpdate::RemoveSnapshotRef {
                ref_name: "release".to_string(),
            },
            TableUpdate::SetLocation {
                location: "s3://b/p".to_string(),
            },
            TableUpdate::SetProperties { updates: props },
            TableUpdate::RemoveProperties {
                removals: vec!["k".to_string()],
            },
        ];
        for update in cases {
            let json = serde_json::to_string(&update).unwrap();
            let back: TableUpdate = serde_json::from_str(&json).unwrap();
            assert_eq!(back, update, "round-trip mismatch via {json}");
        }
    }

    #[test]
    fn test_table_update_uses_kebab_case_action_tag() {
        let json = serde_json::to_value(TableUpdate::AssignUuid {
            uuid: Uuid::from_u128(0xA).to_string(),
        })
        .unwrap();
        assert_eq!(json["action"], "assign-uuid");

        let json = serde_json::to_value(TableUpdate::SetCurrentSchema { schema_id: 1 }).unwrap();
        assert_eq!(json["action"], "set-current-schema");
        assert_eq!(json["schema-id"], 1);

        let json =
            serde_json::to_value(TableUpdate::UpgradeFormatVersion { format_version: 2 }).unwrap();
        assert_eq!(json["action"], "upgrade-format-version");
        assert_eq!(json["format-version"], 2);
    }

    #[test]
    fn test_table_update_rejects_unknown_action_tag() {
        let json = r#"{"action": "rewrite-the-universe"}"#;
        assert!(serde_json::from_str::<TableUpdate>(json).is_err());
    }
}
