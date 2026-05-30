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

    // --- Feature gaps (Java has the operation, Rust does not) --------------
    //
    // These tests pin places where Rust's apply_table_updates / requirements
    // model is narrower than the Java reference. Removing the marker (or
    // adjusting the assertion) is the natural last step of implementing the
    // missing feature in Rust.

    #[test]
    #[should_panic(expected = "Table format upgrade")]
    fn test_apply_table_updates_upgrade_format_version_to_different_value_panics() {
        // The Rust impl of apply_table_updates for UpgradeFormatVersion only
        // accepts the no-op case (target == current). Any genuine upgrade
        // surfaces as `unimplemented!("Table format upgrade")`. Java's
        // TestFormatVersions covers upgrade-to-latest, downgrade rejection
        // (with a specific message), and unsupported-version rejection.
        // None of that exists in Rust today; this #[should_panic] pin makes
        // the gap explicit.
        let mut metadata = fixture(Uuid::nil(), &[]);
        // The fixture defaults to V2; ask for V3 to trigger the panic.
        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::UpgradeFormatVersion { format_version: 3 }],
        )
        .unwrap();
    }

    // --- Port: TestFormatVersions (Apache Iceberg Java, 5 @TestTemplate) ---
    //
    // Java parametrises by formatVersion (V1, V2 — V3 excluded as latest).
    // The Rust port is non-parametrised and runs one test per scenario per
    // version. Default-version assertions pass; the upgrade-rule scenarios
    // are #[ignore] because Rust's apply_table_updates panics with
    // `unimplemented!("Table format upgrade")` for any non-no-op upgrade
    // and has no equivalent of Java's `upgradeToFormatVersion` validator
    // that distinguishes downgrade / unsupported-version / valid-upgrade.

    use iceberg_rust_spec::spec::table_metadata::FormatVersion;

    fn fixture_with_format_version(version: FormatVersion) -> TableMetadata {
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
        TableMetadataBuilder::default()
            .format_version(version)
            .table_uuid(Uuid::nil())
            .location("s3://tests/table".to_owned())
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(0, schema)]))
            .partition_specs(HashMap::from_iter(vec![(0, PartitionSpec::default())]))
            .default_spec_id(0)
            .last_partition_id(999)
            .last_column_id(7)
            .default_sort_order_id(0)
            .refs(HashMap::new())
            .build()
            .unwrap()
    }

    #[test]
    fn test_default_format_version_v1_per_java() {
        // Java: testDefaultFormatVersion @ formatVersion = 1.
        let metadata = fixture_with_format_version(FormatVersion::V1);
        assert_eq!(metadata.format_version, FormatVersion::V1);
    }

    #[test]
    fn test_default_format_version_v2_per_java() {
        // Java: testDefaultFormatVersion @ formatVersion = 2.
        let metadata = fixture_with_format_version(FormatVersion::V2);
        assert_eq!(metadata.format_version, FormatVersion::V2);
    }

    #[test]
    fn test_default_format_version_v3_per_java() {
        // Java's parametrisation excludes the latest supported version, so V3
        // isn't directly covered there; included here for completeness since
        // Rust's FormatVersion exposes V3 the same way.
        let metadata = fixture_with_format_version(FormatVersion::V3);
        assert_eq!(metadata.format_version, FormatVersion::V3);
    }

    #[test]
    #[ignore = "feature gap: Rust's apply_table_updates panics on any non-no-op UpgradeFormatVersion (no upgradeToFormatVersion validator); Java's testFormatVersionUpgrade asserts v -> v+1 succeeds and the changes() log records exactly one UpgradeFormatVersion(v+1)"]
    fn test_format_version_upgrade_to_next_per_java() {
        let mut metadata = fixture_with_format_version(FormatVersion::V1);
        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::UpgradeFormatVersion { format_version: 2 }],
        )
        .unwrap();
        assert_eq!(metadata.format_version, FormatVersion::V2);
    }

    #[test]
    #[ignore = "feature gap: same upgrade-rule gap as test_format_version_upgrade_to_next_per_java; Java's testFormatVersionUpgradeToLatest asserts a v1 table can upgrade directly to SUPPORTED_TABLE_FORMAT_VERSION (v3)"]
    fn test_format_version_upgrade_to_latest_supported_per_java() {
        let mut metadata = fixture_with_format_version(FormatVersion::V1);
        apply_table_updates(
            &mut metadata,
            vec![TableUpdate::UpgradeFormatVersion { format_version: 3 }],
        )
        .unwrap();
        assert_eq!(metadata.format_version, FormatVersion::V3);
    }

    #[test]
    #[ignore = "feature gap: Rust's apply_table_updates panics with `Table format upgrade` for any version mismatch; Java's testFormatVersionDowngrade asserts a specific IllegalArgumentException with message `Cannot downgrade v{N} table to v{M}`"]
    fn test_format_version_downgrade_rejected_per_java() {
        let mut metadata = fixture_with_format_version(FormatVersion::V2);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            apply_table_updates(
                &mut metadata,
                vec![TableUpdate::UpgradeFormatVersion { format_version: 1 }],
            )
        }));
        // Java pins the exact error message; Rust's panic includes only
        // `Table format upgrade` regardless of direction.
        assert!(result.is_err(), "expected downgrade to be rejected");
    }

    #[test]
    #[ignore = "feature gap: Rust has no SUPPORTED_TABLE_FORMAT_VERSION ceiling check; Java's testFormatVersionUpgradeNotSupported asserts upgrade to vmax+1 is rejected with message `Cannot upgrade table to unsupported format version: v{N} (supported: v{M})`"]
    fn test_format_version_upgrade_to_unsupported_rejected_per_java() {
        let mut metadata = fixture_with_format_version(FormatVersion::V3);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            apply_table_updates(
                &mut metadata,
                vec![TableUpdate::UpgradeFormatVersion { format_version: 4 }],
            )
        }));
        assert!(result.is_err(), "expected v4 to be rejected as unsupported");
    }

    #[test]
    #[ignore = "feature gap: AssertCreate is always satisfied; Java's AssertTableDoesNotExist.validate(metadata) fails when the metadata exists"]
    fn test_check_table_requirements_assert_create_fails_when_metadata_exists_per_java() {
        // Java's AssertTableDoesNotExist requirement is generated by
        // UpdateRequirements.forCreateTable() and its validate() throws
        // CommitFailedException with "table already exists" when the
        // provided metadata is not null. Rust's check_table_requirements
        // treats AssertCreate as a no-op, deferring the existence check to
        // the catalog implementation. This test pins the spec contract
        // (rejection) waiting on a Rust-side fix.
        let metadata = fixture(Uuid::nil(), &[]);
        assert!(!check_table_requirements(
            &[TableRequirement::AssertCreate],
            &metadata,
        ));
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

    // --- ViewRequirement / ViewUpdate JSON parser + apply_view_updates ----

    use iceberg_rust_spec::spec::view_metadata::{
        Version, ViewMetadata, ViewMetadataBuilder, ViewRepresentation,
    };

    fn view_fixture(uuid: Uuid) -> ViewMetadata {
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

        let version: Version<Option<()>> = Version::builder()
            .version_id(1)
            .schema_id(0)
            .timestamp_ms(0)
            .default_namespace(vec!["ns".to_string()])
            .build()
            .unwrap();

        ViewMetadataBuilder::default()
            .view_uuid(uuid)
            .location("s3://tests/view".to_string())
            .current_version_id(1)
            .with_version((1, version))
            .with_schema((0, schema))
            .build()
            .unwrap()
    }

    #[test]
    fn test_view_requirement_assert_view_uuid_round_trips() {
        let uuid = Uuid::from_u128(0x1234);
        let req = ViewRequirement::AssertViewUuid { uuid };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["type"], "assert-view-uuid");
        let back: ViewRequirement = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn test_check_view_requirements_pass_and_fail() {
        let uuid = Uuid::from_u128(0xAA);
        let metadata = view_fixture(uuid);
        assert!(check_view_requirements(
            &[ViewRequirement::AssertViewUuid { uuid }],
            &metadata,
        ));
        assert!(!check_view_requirements(
            &[ViewRequirement::AssertViewUuid {
                uuid: Uuid::from_u128(0xBB),
            }],
            &metadata,
        ));
    }

    #[test]
    fn test_view_update_round_trips_every_variant_via_json() {
        let schema = SchemaBuilder::default()
            .with_schema_id(7)
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

        let version: Version<Option<()>> = Version::builder()
            .version_id(2)
            .schema_id(7)
            .timestamp_ms(1_000)
            .default_namespace(vec!["ns".to_string()])
            .with_representation(ViewRepresentation::sql("select 1", None))
            .build()
            .unwrap();

        let mut props = HashMap::new();
        props.insert("comment".to_string(), "hi".to_string());

        let cases: Vec<ViewUpdate<Option<()>>> = vec![
            ViewUpdate::AssignUuid {
                uuid: Uuid::from_u128(0xC).to_string(),
            },
            ViewUpdate::UpgradeFormatVersion { format_version: 1 },
            ViewUpdate::AddSchema {
                schema,
                last_column_id: Some(7),
            },
            ViewUpdate::SetLocation {
                location: "s3://b/p".to_string(),
            },
            ViewUpdate::SetProperties { updates: props },
            ViewUpdate::RemoveProperties {
                removals: vec!["comment".to_string()],
            },
            ViewUpdate::AddViewVersion {
                view_version: version,
            },
            ViewUpdate::SetCurrentViewVersion { view_version_id: 2 },
        ];
        for update in cases {
            let json = serde_json::to_string(&update).unwrap();
            let back: ViewUpdate<Option<()>> = serde_json::from_str(&json).unwrap();
            assert_eq!(back, update, "round-trip mismatch via {json}");
        }
    }

    #[test]
    fn test_view_update_uses_kebab_case_action_tag() {
        let update: ViewUpdate<Option<()>> =
            ViewUpdate::SetCurrentViewVersion { view_version_id: 9 };
        let json = serde_json::to_value(&update).unwrap();
        assert_eq!(json["action"], "set-current-view-version");
        assert_eq!(json["view-version-id"], 9);
    }

    #[test]
    fn test_apply_view_updates_assign_uuid_and_set_location_and_properties() {
        let mut metadata = view_fixture(Uuid::nil());
        let new_uuid = Uuid::from_u128(0xFEED);
        let mut props = HashMap::new();
        props.insert("k".to_string(), "v".to_string());
        apply_view_updates(
            &mut metadata,
            vec![
                ViewUpdate::AssignUuid {
                    uuid: new_uuid.to_string(),
                },
                ViewUpdate::SetLocation {
                    location: "s3://other/loc".to_string(),
                },
                ViewUpdate::SetProperties {
                    updates: props.clone(),
                },
                ViewUpdate::RemoveProperties {
                    removals: vec!["k".to_string()],
                },
            ],
        )
        .unwrap();
        assert_eq!(metadata.view_uuid, new_uuid);
        assert_eq!(metadata.location, "s3://other/loc");
        assert!(!metadata.properties.contains_key("k"));
    }

    #[test]
    fn test_apply_view_updates_add_schema_and_add_view_version_and_set_current() {
        let mut metadata = view_fixture(Uuid::nil());
        let schema = SchemaBuilder::default()
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

        let version: Version<Option<()>> = Version::builder()
            .version_id(99)
            .schema_id(11)
            .timestamp_ms(1_000)
            .default_namespace(vec!["ns".to_string()])
            .build()
            .unwrap();

        apply_view_updates(
            &mut metadata,
            vec![
                ViewUpdate::AddSchema {
                    schema,
                    last_column_id: None,
                },
                ViewUpdate::AddViewVersion {
                    view_version: version,
                },
                ViewUpdate::SetCurrentViewVersion {
                    view_version_id: 99,
                },
            ],
        )
        .unwrap();
        assert!(metadata.schemas.contains_key(&11));
        assert!(metadata.versions.contains_key(&99));
        assert_eq!(metadata.current_version_id, 99);
    }

    // --- Port: TestUpdateTableRequestParser (Apache Iceberg Java) ----------
    //
    // Java's TestUpdateTableRequestParser has 7 @Test methods. The Rust
    // CommitTable struct mirrors most of the Java UpdateTableRequest shape
    // but differs on identifier nullability: Rust requires identifier;
    // Java treats it as optional.

    #[test]
    fn test_commit_table_parser_rejects_invalid_identifier_missing_name() {
        // Java: fromJson("{identifier: {}}") -> "missing string: name".
        // Rust serde rejects with the same intent (missing required field).
        let json = r#"{
            "identifier": {},
            "requirements": [],
            "updates": []
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_parser_rejects_invalid_identifier_non_string_name() {
        // Java: fromJson("{identifier: {name: 23}}") ->
        //   "Cannot parse to a string value: name: 23".
        let json = r#"{
            "identifier": {"namespace": ["ns1"], "name": 23},
            "requirements": [],
            "updates": []
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_parser_rejects_invalid_requirement_non_object() {
        // Java: requirements:[23] -> "non-object value: 23".
        let json = r#"{
            "identifier": {"namespace": ["ns1"], "name": "table1"},
            "requirements": [23],
            "updates": []
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_parser_rejects_requirement_with_missing_type_tag() {
        // Java: requirements:[{}] -> "Missing field: type".
        let json = r#"{
            "identifier": {"namespace": ["ns1"], "name": "table1"},
            "requirements": [{}],
            "updates": []
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_parser_rejects_assert_table_uuid_without_uuid() {
        // Java: requirements:[{type:"assert-table-uuid"}] -> "missing string: uuid".
        let json = r#"{
            "identifier": {"namespace": ["ns1"], "name": "table1"},
            "requirements": [{"type": "assert-table-uuid"}],
            "updates": []
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_parser_rejects_invalid_metadata_update_non_object() {
        // Java: updates:[23] -> "non-object value: 23".
        let json = r#"{
            "identifier": {"namespace": ["ns1"], "name": "table1"},
            "requirements": [],
            "updates": [23]
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_parser_rejects_update_with_missing_action_tag() {
        // Java: updates:[{}] -> "Missing field: action".
        let json = r#"{
            "identifier": {"namespace": ["ns1"], "name": "table1"},
            "requirements": [],
            "updates": [{}]
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_parser_rejects_assign_uuid_without_uuid() {
        // Java: updates:[{action:"assign-uuid"}] -> "missing string: uuid".
        let json = r#"{
            "identifier": {"namespace": ["ns1"], "name": "table1"},
            "requirements": [],
            "updates": [{"action": "assign-uuid"}]
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_table_round_trip_pinned_wire_shape() {
        // Port of Java's roundTripSerde with the same uuid + two
        // requirements + two updates. Rust pins the per-key wire shape via
        // serde_json::Value comparison.
        let uuid_str = "2cc52516-5e73-41f2-b139-545d41a4e151";
        let uuid = Uuid::parse_str(uuid_str).unwrap();
        let request = CommitTable {
            identifier: Identifier::new(&["ns1".to_string()], "table1"),
            requirements: vec![
                TableRequirement::AssertTableUuid { uuid },
                TableRequirement::AssertCreate,
            ],
            updates: vec![
                TableUpdate::AssignUuid {
                    uuid: uuid.to_string(),
                },
                TableUpdate::SetCurrentSchema { schema_id: 23 },
            ],
        };

        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["identifier"]["name"], "table1");
        assert_eq!(value["identifier"]["namespace"], serde_json::json!(["ns1"]));
        assert_eq!(value["requirements"][0]["type"], "assert-table-uuid");
        assert_eq!(value["requirements"][0]["uuid"], uuid_str);
        assert_eq!(value["requirements"][1]["type"], "assert-create");
        assert_eq!(value["updates"][0]["action"], "assign-uuid");
        assert_eq!(value["updates"][0]["uuid"], uuid_str);
        assert_eq!(value["updates"][1]["action"], "set-current-schema");
        assert_eq!(value["updates"][1]["schema-id"], 23);

        let parsed: CommitTable = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, request);
    }

    #[test]
    fn test_commit_table_round_trip_with_empty_requirements_and_updates() {
        // Port of Java's emptyRequirementsAndUpdates. Both vecs empty
        // round-trip via JSON.
        let request = CommitTable {
            identifier: Identifier::new(&["ns1".to_string()], "table1"),
            requirements: vec![],
            updates: vec![],
        };
        let json = serde_json::to_string(&request).unwrap();
        let parsed: CommitTable = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, request);
    }

    #[test]
    #[ignore = "feature gap: Rust CommitTable.identifier is required (not Option); Java's UpdateTableRequest treats identifier as optional, allowing fromJson(\"{}\") to return a request with null identifier + empty vecs"]
    fn test_commit_table_parser_accepts_empty_object_per_java() {
        // Java: fromJson("{}") returns UpdateTableRequest with
        //   identifier=null, updates=[], requirements=[].
        // Rust's CommitTable requires identifier; an empty object fails to
        // parse. Removing the marker after making identifier optional
        // will flip this test to passing.
        let result: Result<CommitTable, _> = serde_json::from_str("{}");
        assert!(result.is_ok());
    }

    #[test]
    #[ignore = "feature gap: Rust CommitTable cannot omit identifier; Java's UpdateTableRequest accepts {requirements:[...], updates:[...]} without identifier"]
    fn test_commit_table_parser_accepts_request_without_identifier_per_java() {
        // Java: roundTripSerdeWithoutIdentifier — full request body with
        // requirements + updates but no identifier field. Once the Rust
        // struct uses Option<Identifier>, this should parse and round-trip.
        let json = r#"{
            "requirements": [{"type":"assert-create"}],
            "updates": []
        }"#;
        let result: Result<CommitTable, _> = serde_json::from_str(json);
        assert!(result.is_ok());
    }

    // --- Port: TestUpdateRequirementParser (Apache Iceberg Java) -----------
    //
    // Java has explicit per-variant ToJson + FromJson tests for every
    // UpdateRequirement variant. Cycle 17 already exercised round-trip via
    // a loop; this section adds the explicit per-variant assertions Java
    // does, so each variant has its own pin.

    const SAMPLE_UUID_STR: &str = "2cc52516-5e73-41f2-b139-545d41a4e151";

    fn sample_uuid() -> Uuid {
        Uuid::parse_str(SAMPLE_UUID_STR).unwrap()
    }

    #[test]
    fn test_update_requirement_parser_rejects_missing_type_tag() {
        // Java: testUpdateRequirementWithoutRequirementTypeCannotParse —
        // both `{type:null, uuid:...}` and `{uuid:...}` are rejected with
        // "Missing field: type".
        let null_type = r#"{"type":null,"uuid":"2cc52516-5e73-41f2-b139-545d41a4e151"}"#;
        let no_type = r#"{"uuid":"2cc52516-5e73-41f2-b139-545d41a4e151"}"#;
        assert!(serde_json::from_str::<TableRequirement>(null_type).is_err());
        assert!(serde_json::from_str::<TableRequirement>(no_type).is_err());
    }

    #[test]
    fn test_assert_table_uuid_to_json_per_java() {
        // Java: testAssertUUIDToJson — exact JSON shape.
        let req = TableRequirement::AssertTableUuid {
            uuid: sample_uuid(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            format!(r#"{{"type":"assert-table-uuid","uuid":"{SAMPLE_UUID_STR}"}}"#),
        );
    }

    #[test]
    fn test_assert_table_uuid_from_json_per_java() {
        // Java: testAssertUUIDFromJson — parse the canonical payload.
        let json = format!(r#"{{"type":"assert-table-uuid","uuid":"{SAMPLE_UUID_STR}"}}"#);
        let parsed: TableRequirement = serde_json::from_str(&json).unwrap();
        match parsed {
            TableRequirement::AssertTableUuid { uuid } => assert_eq!(uuid, sample_uuid()),
            other => panic!("expected AssertTableUuid, got {other:?}"),
        }
    }

    #[test]
    fn test_assert_view_uuid_to_json_per_java() {
        // Java: testAssertViewUUIDToJson.
        let req = ViewRequirement::AssertViewUuid {
            uuid: sample_uuid(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            format!(r#"{{"type":"assert-view-uuid","uuid":"{SAMPLE_UUID_STR}"}}"#),
        );
    }

    #[test]
    fn test_assert_view_uuid_from_json_per_java() {
        // Java: testAssertViewUUIDFromJson.
        let json = format!(r#"{{"type":"assert-view-uuid","uuid":"{SAMPLE_UUID_STR}"}}"#);
        let parsed: ViewRequirement = serde_json::from_str(&json).unwrap();
        match parsed {
            ViewRequirement::AssertViewUuid { uuid } => assert_eq!(uuid, sample_uuid()),
        }
    }

    #[test]
    fn test_assert_table_does_not_exist_to_json_per_java() {
        // Java: testAssertTableDoesNotExistToJson — bare tag form.
        // Rust spells the variant AssertCreate.
        let req = TableRequirement::AssertCreate;
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"assert-create"}"#);
    }

    #[test]
    fn test_assert_table_does_not_exist_from_json_per_java() {
        // Java: testAssertTableDoesNotExistFromJson.
        let json = r#"{"type":"assert-create"}"#;
        let parsed: TableRequirement = serde_json::from_str(json).unwrap();
        assert!(matches!(parsed, TableRequirement::AssertCreate));
    }

    #[test]
    fn test_assert_ref_snapshot_id_to_json_per_java() {
        // Java: testAssertRefSnapshotIdFromJson (Java labels are swapped vs
        // Rust convention; this test exercises the toJson path).
        let req = TableRequirement::AssertRefSnapshotId {
            r#ref: "snapshot-name".to_string(),
            snapshot_id: 1,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"assert-ref-snapshot-id","ref":"snapshot-name","snapshot-id":1}"#,
        );
    }

    #[test]
    fn test_assert_ref_snapshot_id_from_json_per_java() {
        // Java: testAssertRefSnapshotIdToJson (label-swapped, see above).
        let json = r#"{"type":"assert-ref-snapshot-id","ref":"snapshot-name","snapshot-id":1}"#;
        let parsed: TableRequirement = serde_json::from_str(json).unwrap();
        match parsed {
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                assert_eq!(r#ref, "snapshot-name");
                assert_eq!(snapshot_id, 1);
            }
            other => panic!("expected AssertRefSnapshotId, got {other:?}"),
        }
    }

    #[test]
    #[ignore = "feature gap: Rust AssertRefSnapshotId.snapshot_id is i64 (not Option<i64>); Java models snapshot-id as nullable and tests the null case in testAssertRefSnapshotId{To,From}JsonWithNullSnapshotId"]
    fn test_assert_ref_snapshot_id_with_null_snapshot_id_per_java() {
        // Java: testAssertRefSnapshotIdToJsonWithNullSnapshotId — Java
        // formats snapshot-id as the string "null" when it's null and the
        // FromJson path accepts null. Rust models snapshot_id as i64, so
        // the null payload cannot round-trip.
        let json = r#"{"type":"assert-ref-snapshot-id","ref":"snapshot-name","snapshot-id":null}"#;
        let result: Result<TableRequirement, _> = serde_json::from_str(json);
        assert!(
            result.is_ok(),
            "expected null snapshot-id to parse: {result:?}"
        );
    }

    #[test]
    fn test_assert_last_assigned_field_id_to_json_per_java() {
        // Java: testAssertLastAssignedFieldIdToJson.
        let req = TableRequirement::AssertLastAssignedFieldId {
            last_assigned_field_id: 12,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"assert-last-assigned-field-id","last-assigned-field-id":12}"#,
        );
    }

    #[test]
    fn test_assert_last_assigned_field_id_from_json_per_java() {
        // Java: testAssertLastAssignedFieldIdFromJson.
        let json = r#"{"type":"assert-last-assigned-field-id","last-assigned-field-id":12}"#;
        let parsed: TableRequirement = serde_json::from_str(json).unwrap();
        match parsed {
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => assert_eq!(last_assigned_field_id, 12),
            other => panic!("expected AssertLastAssignedFieldId, got {other:?}"),
        }
    }

    #[test]
    fn test_assert_current_schema_id_to_json_per_java() {
        // Java: testAssertCurrentSchemaIdToJson.
        let req = TableRequirement::AssertCurrentSchemaId {
            current_schema_id: 4,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"assert-current-schema-id","current-schema-id":4}"#,
        );
    }

    #[test]
    fn test_assert_current_schema_id_from_json_per_java() {
        // Java: testAssertCurrentSchemaIdFromJson.
        let json = r#"{"type":"assert-current-schema-id","current-schema-id":4}"#;
        let parsed: TableRequirement = serde_json::from_str(json).unwrap();
        match parsed {
            TableRequirement::AssertCurrentSchemaId { current_schema_id } => {
                assert_eq!(current_schema_id, 4)
            }
            other => panic!("expected AssertCurrentSchemaId, got {other:?}"),
        }
    }

    #[test]
    fn test_assert_last_assigned_partition_id_to_json_per_java() {
        // Java: testAssertLastAssignedPartitionIdToJson.
        let req = TableRequirement::AssertLastAssignedPartitionId {
            last_assigned_partition_id: 1004,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1004}"#,
        );
    }

    #[test]
    fn test_assert_last_assigned_partition_id_from_json_per_java() {
        // Java: testAssertLastAssignedPartitionIdFromJson.
        let json =
            r#"{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1004}"#;
        let parsed: TableRequirement = serde_json::from_str(json).unwrap();
        match parsed {
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => assert_eq!(last_assigned_partition_id, 1004),
            other => panic!("expected AssertLastAssignedPartitionId, got {other:?}"),
        }
    }

    #[test]
    fn test_assert_default_spec_id_to_json_per_java() {
        // Java: testAssertDefaultSpecIdToJson.
        let req = TableRequirement::AssertDefaultSpecId { default_spec_id: 5 };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"assert-default-spec-id","default-spec-id":5}"#,
        );
    }

    #[test]
    fn test_assert_default_spec_id_from_json_per_java() {
        // Java: testAssertDefaultSpecIdFromJson.
        let json = r#"{"type":"assert-default-spec-id","default-spec-id":5}"#;
        let parsed: TableRequirement = serde_json::from_str(json).unwrap();
        match parsed {
            TableRequirement::AssertDefaultSpecId { default_spec_id } => {
                assert_eq!(default_spec_id, 5)
            }
            other => panic!("expected AssertDefaultSpecId, got {other:?}"),
        }
    }

    #[test]
    fn test_assert_default_sort_order_id_to_json_per_java() {
        // Java: testAssertDefaultSortOrderIdToJson.
        let req = TableRequirement::AssertDefaultSortOrderId {
            default_sort_order_id: 10,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"type":"assert-default-sort-order-id","default-sort-order-id":10}"#,
        );
    }

    #[test]
    fn test_assert_default_sort_order_id_from_json_per_java() {
        // Java: testAssertDefaultSortOrderIdFromJson.
        let json = r#"{"type":"assert-default-sort-order-id","default-sort-order-id":10}"#;
        let parsed: TableRequirement = serde_json::from_str(json).unwrap();
        match parsed {
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id,
            } => assert_eq!(default_sort_order_id, 10),
            other => panic!("expected AssertDefaultSortOrderId, got {other:?}"),
        }
    }

    // --- Port: TestMetadataUpdateParser (Apache Iceberg Java, 56 @Test) ----
    //
    // Java's MetadataUpdate is a single union; Rust splits it between
    // `TableUpdate` (table-only actions) and `ViewUpdate` (view-only actions).
    // Each Java per-variant ToJson / FromJson test is ported below as one
    // Rust test that pins the wire shape via `serde_json::to_value` (kebab-case
    // keys + the `action` tag) or parses the canonical Java payload.
    //
    // Variants Rust doesn't model yet are pinned with `#[ignore]`:
    //   - SetStatistics, RemoveStatistics
    //   - SetPartitionStatistics, RemovePartitionStatistics
    //   - RemovePartitionSpecs, RemoveSchemas
    //   - AddEncryptionKey, RemoveEncryptionKey
    // Plus a few wire-shape divergences (Java's "updated"/"removed" aliases,
    // case-insensitive snapshot-ref type, AddViewVersion summary as free-form
    // map, AddPartitionSpec auto-assigned field ids, Iceberg v3 snapshot
    // fields first-row-id / added-rows / key-id).

    const METADATA_UPDATE_UUID: &str = "9510c070-5e6d-4b40-bf40-a8915bb76e5d";

    fn id_data_schema_for_metadata_update_parser() -> Schema {
        SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Int),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "data".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap()
    }

    // --- 1. testMetadataUpdateWithoutActionCannotDeserialize ---------------

    #[test]
    fn test_table_update_rejects_object_missing_action_per_java() {
        // Java: invalidJson includes `{"format-version":2}` — parser raises
        // "Missing field: action". Rust's tag=action enum rejects too.
        let result: Result<TableUpdate, _> = serde_json::from_str(r#"{"format-version":2}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_table_update_rejects_null_action_per_java() {
        // Java: invalidJson includes `{"action":null,"format-version":2}`.
        let result: Result<TableUpdate, _> =
            serde_json::from_str(r#"{"action":null,"format-version":2}"#);
        assert!(result.is_err());
    }

    // --- 2-3. testAssignUUID{To,From}Json ----------------------------------

    #[test]
    fn test_assign_uuid_to_json_per_java() {
        let update = TableUpdate::AssignUuid {
            uuid: METADATA_UPDATE_UUID.to_string(),
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "assign-uuid");
        assert_eq!(value["uuid"], METADATA_UPDATE_UUID);
    }

    #[test]
    fn test_assign_uuid_from_json_per_java() {
        let json = format!(r#"{{"action":"assign-uuid","uuid":"{METADATA_UPDATE_UUID}"}}"#);
        let parsed: TableUpdate = serde_json::from_str(&json).unwrap();
        match parsed {
            TableUpdate::AssignUuid { uuid } => assert_eq!(uuid, METADATA_UPDATE_UUID),
            other => panic!("expected AssignUuid, got {other:?}"),
        }
    }

    // --- 4-5. testUpgradeFormatVersion{To,From}Json ------------------------

    #[test]
    fn test_upgrade_format_version_to_json_per_java() {
        let update = TableUpdate::UpgradeFormatVersion { format_version: 2 };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "upgrade-format-version");
        assert_eq!(value["format-version"], 2);
    }

    #[test]
    fn test_upgrade_format_version_from_json_per_java() {
        let json = r#"{"action":"upgrade-format-version","format-version":2}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::UpgradeFormatVersion { format_version } => {
                assert_eq!(format_version, 2)
            }
            other => panic!("expected UpgradeFormatVersion, got {other:?}"),
        }
    }

    // --- 6-7. testAddSchema{From,To}Json -----------------------------------

    #[test]
    fn test_add_schema_from_json_per_java() {
        // Java sends "{action:add-schema,schema:{...}}" without last-column-id.
        // Rust's last_column_id is Option<i32>; missing key deserialises to None.
        let schema = id_data_schema_for_metadata_update_parser();
        let schema_json = serde_json::to_string(&schema).unwrap();
        let json = format!(r#"{{"action":"add-schema","schema":{schema_json}}}"#);
        let parsed: TableUpdate = serde_json::from_str(&json).unwrap();
        match parsed {
            TableUpdate::AddSchema {
                schema: parsed_schema,
                last_column_id,
            } => {
                assert_eq!(parsed_schema, schema);
                assert_eq!(last_column_id, None);
            }
            other => panic!("expected AddSchema, got {other:?}"),
        }
    }

    #[test]
    fn test_add_schema_to_json_per_java() {
        // Java sends "last-column-id" alongside the schema. Rust does too when
        // the constructor sets `last_column_id`.
        let schema = id_data_schema_for_metadata_update_parser();
        let update = TableUpdate::AddSchema {
            schema,
            last_column_id: Some(2),
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "add-schema");
        assert_eq!(value["last-column-id"], 2);
        assert!(value["schema"].is_object(), "got {value}");
    }

    // --- 8-9. testSetCurrentSchema{From,To}Json ----------------------------

    #[test]
    fn test_set_current_schema_from_json_per_java() {
        let json = r#"{"action":"set-current-schema","schema-id":6}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetCurrentSchema { schema_id } => assert_eq!(schema_id, 6),
            other => panic!("expected SetCurrentSchema, got {other:?}"),
        }
    }

    #[test]
    fn test_set_current_schema_to_json_per_java() {
        let update = TableUpdate::SetCurrentSchema { schema_id: 6 };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-current-schema");
        assert_eq!(value["schema-id"], 6);
    }

    // --- 10. testAddPartitionSpecFromJsonWithFieldId -----------------------

    #[test]
    fn test_add_partition_spec_from_json_with_field_id_per_java() {
        // Each PartitionField carries an explicit field-id in the wire.
        let json = r#"{
            "action":"add-spec",
            "spec":{
                "spec-id":1,
                "fields":[
                    {"name":"id_bucket","transform":"bucket[8]","source-id":1,"field-id":1000},
                    {"name":"data_bucket","transform":"bucket[16]","source-id":2,"field-id":1001}
                ]
            }
        }"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::AddSpec { spec } => {
                assert_eq!(*spec.spec_id(), 1);
                assert_eq!(spec.fields().len(), 2);
                assert_eq!(*spec.fields()[0].field_id(), 1000);
                assert_eq!(*spec.fields()[1].field_id(), 1001);
            }
            other => panic!("expected AddSpec, got {other:?}"),
        }
    }

    // --- 11. testAddPartitionSpecFromJsonWithoutFieldId --------------------

    #[test]
    #[ignore = "feature gap: Rust PartitionField requires `field-id` on the wire; Java auto-assigns field-ids starting at 1000 when omitted"]
    fn test_add_partition_spec_from_json_without_field_id_per_java() {
        let json = r#"{
            "action":"add-spec",
            "spec":{
                "spec-id":1,
                "fields":[
                    {"name":"id_bucket","transform":"bucket[8]","source-id":1},
                    {"name":"data_bucket","transform":"bucket[16]","source-id":2}
                ]
            }
        }"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::AddSpec { spec } => {
                assert_eq!(*spec.fields()[0].field_id(), 1000);
                assert_eq!(*spec.fields()[1].field_id(), 1001);
            }
            other => panic!("expected AddSpec, got {other:?}"),
        }
    }

    // --- 12. testAddPartitionSpecToJson ------------------------------------

    #[test]
    fn test_add_partition_spec_to_json_per_java() {
        let spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_partition_field(PartitionField::new(
                1,
                1000,
                "id_bucket",
                Transform::Bucket(8),
            ))
            .with_partition_field(PartitionField::new(
                2,
                1001,
                "data_bucket",
                Transform::Bucket(16),
            ))
            .build()
            .unwrap();
        let update = TableUpdate::AddSpec { spec };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "add-spec");
        assert_eq!(value["spec"]["spec-id"], 1);
        assert_eq!(value["spec"]["fields"][0]["field-id"], 1000);
        assert_eq!(value["spec"]["fields"][1]["field-id"], 1001);
    }

    // --- 13-14. testSetDefaultPartitionSpec{To,From}Json -------------------

    #[test]
    fn test_set_default_partition_spec_to_json_per_java() {
        let update = TableUpdate::SetDefaultSpec { spec_id: 4 };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-default-spec");
        assert_eq!(value["spec-id"], 4);
    }

    #[test]
    fn test_set_default_partition_spec_from_json_per_java() {
        let json = r#"{"action":"set-default-spec","spec-id":4}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetDefaultSpec { spec_id } => assert_eq!(spec_id, 4),
            other => panic!("expected SetDefaultSpec, got {other:?}"),
        }
    }

    // --- 15-16. testAddSortOrder{To,From}Json ------------------------------

    fn sample_metadata_update_sort_order() -> SortOrder {
        SortOrder {
            order_id: 3,
            fields: vec![
                SortField {
                    source_id: 1,
                    transform: Transform::Identity,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::First,
                },
                SortField {
                    source_id: 2,
                    transform: Transform::Identity,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::Last,
                },
            ],
        }
    }

    #[test]
    fn test_add_sort_order_to_json_per_java() {
        let update = TableUpdate::AddSortOrder {
            sort_order: sample_metadata_update_sort_order(),
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "add-sort-order");
        assert_eq!(value["sort-order"]["order-id"], 3);
        assert_eq!(value["sort-order"]["fields"][0]["source-id"], 1);
        assert_eq!(value["sort-order"]["fields"][1]["direction"], "desc");
    }

    #[test]
    fn test_add_sort_order_from_json_per_java() {
        let expected = TableUpdate::AddSortOrder {
            sort_order: sample_metadata_update_sort_order(),
        };
        let json = serde_json::to_string(&expected).unwrap();
        let parsed: TableUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, expected);
    }

    // --- 17-18. testSetDefaultSortOrder{To,From}Json -----------------------

    #[test]
    fn test_set_default_sort_order_to_json_per_java() {
        let update = TableUpdate::SetDefaultSortOrder { sort_order_id: 2 };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-default-sort-order");
        assert_eq!(value["sort-order-id"], 2);
    }

    #[test]
    fn test_set_default_sort_order_from_json_per_java() {
        let json = r#"{"action":"set-default-sort-order","sort-order-id":2}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                assert_eq!(sort_order_id, 2)
            }
            other => panic!("expected SetDefaultSortOrder, got {other:?}"),
        }
    }

    // --- 19-20. testAddSnapshot{To,From}Json -------------------------------

    fn sample_metadata_update_snapshot() -> Snapshot {
        SnapshotBuilder::default()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(1_i64)
            .with_sequence_number(0)
            .with_timestamp_ms(1_700_000_000_000)
            .with_manifest_list("s3://bucket/manifest-list.avro".to_string())
            .with_summary(iceberg_rust_spec::spec::snapshot::Summary {
                operation: iceberg_rust_spec::spec::snapshot::Operation::Replace,
                other: HashMap::from([
                    ("files-added".to_string(), "4".to_string()),
                    ("files-deleted".to_string(), "100".to_string()),
                ]),
            })
            .with_schema_id(3_i32)
            .build()
            .unwrap()
    }

    #[test]
    fn test_add_snapshot_to_json_per_java() {
        let update = TableUpdate::AddSnapshot {
            snapshot: sample_metadata_update_snapshot(),
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "add-snapshot");
        assert_eq!(value["snapshot"]["snapshot-id"], 2);
        assert_eq!(value["snapshot"]["parent-snapshot-id"], 1);
        assert_eq!(value["snapshot"]["schema-id"], 3);
        assert_eq!(
            value["snapshot"]["manifest-list"],
            "s3://bucket/manifest-list.avro"
        );
    }

    #[test]
    fn test_add_snapshot_from_json_per_java() {
        let update = TableUpdate::AddSnapshot {
            snapshot: sample_metadata_update_snapshot(),
        };
        let json = serde_json::to_string(&update).unwrap();
        let parsed: TableUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, update);
    }

    #[test]
    #[ignore = "feature gap: Rust Snapshot lacks Iceberg v3 fields first-row-id, added-rows, key-id; Java's testAddSnapshot{To,From}Json BaseSnapshot constructor pins those keys on the wire"]
    fn test_add_snapshot_includes_v3_fields_per_java() {
        let json = r#"{
            "action":"add-snapshot",
            "snapshot":{
                "snapshot-id":2,
                "parent-snapshot-id":1,
                "sequence-number":0,
                "timestamp-ms":1700000000000,
                "manifest-list":"x",
                "summary":{"operation":"replace"},
                "schema-id":3,
                "first-row-id":4,
                "added-rows":10,
                "key-id":"key-1"
            }
        }"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["snapshot"]["first-row-id"], 4);
        assert_eq!(value["snapshot"]["added-rows"], 10);
        assert_eq!(value["snapshot"]["key-id"], "key-1");
    }

    // --- 21-22. testRemoveSnapshot{From,To}Json (single id) ----------------

    #[test]
    fn test_remove_snapshot_single_from_json_per_java() {
        let json = r#"{"action":"remove-snapshots","snapshot-ids":[2]}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                assert_eq!(snapshot_ids, vec![2]);
            }
            other => panic!("expected RemoveSnapshots, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_snapshot_single_to_json_per_java() {
        let update = TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![2],
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "remove-snapshots");
        assert_eq!(value["snapshot-ids"], serde_json::json!([2]));
    }

    // --- 23-24. testRemoveSnapshots{From,To}Json (multi) -------------------

    #[test]
    fn test_remove_snapshots_multi_from_json_per_java() {
        let json = r#"{"action":"remove-snapshots","snapshot-ids":[2,3]}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                let mut sorted = snapshot_ids.clone();
                sorted.sort();
                assert_eq!(sorted, vec![2, 3]);
            }
            other => panic!("expected RemoveSnapshots, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_snapshots_multi_to_json_per_java() {
        let update = TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![2, 3],
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "remove-snapshots");
        assert_eq!(value["snapshot-ids"], serde_json::json!([2, 3]));
    }

    // --- 25-26. testRemoveSnapshotRef{From,To}Json -------------------------

    #[test]
    fn test_remove_snapshot_ref_from_json_per_java() {
        let json = r#"{"action":"remove-snapshot-ref","ref-name":"snapshot-ref"}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::RemoveSnapshotRef { ref_name } => {
                assert_eq!(ref_name, "snapshot-ref")
            }
            other => panic!("expected RemoveSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_snapshot_ref_to_json_per_java() {
        let update = TableUpdate::RemoveSnapshotRef {
            ref_name: "snapshot-ref".to_string(),
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "remove-snapshot-ref");
        assert_eq!(value["ref-name"], "snapshot-ref");
    }

    // --- 27-30. testSetSnapshotRefTag {default,allFields} x {missing,null} -

    #[test]
    fn test_set_snapshot_ref_tag_default_from_json_per_java() {
        // Java: testSetSnapshotRefTagFromJsonDefault_NullValuesMissing.
        let json =
            r#"{"action":"set-snapshot-ref","ref-name":"hank","snapshot-id":1,"type":"tag"}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetSnapshotRef {
                ref_name,
                snapshot_reference,
            } => {
                assert_eq!(ref_name, "hank");
                assert_eq!(snapshot_reference.snapshot_id, 1);
                assert!(matches!(
                    snapshot_reference.retention,
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None
                    },
                ));
            }
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    fn test_set_snapshot_ref_tag_default_explicit_nulls_from_json_per_java() {
        // Java: testSetSnapshotRefTagFromJsonDefault_ExplicitNullValues. Java's
        // tag JSON has min-snapshots-to-keep / max-snapshot-age-ms / max-ref-age-ms
        // set to null. For a Tag those branch-only keys are unknown — serde
        // skips unknown fields by default, so this still parses.
        let json = r#"{"action":"set-snapshot-ref","ref-name":"hank","snapshot-id":1,"type":"tag","min-snapshots-to-keep":null,"max-snapshot-age-ms":null,"max-ref-age-ms":null}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetSnapshotRef {
                snapshot_reference, ..
            } => {
                assert!(matches!(
                    snapshot_reference.retention,
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None
                    },
                ));
            }
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    fn test_set_snapshot_ref_tag_all_fields_from_json_per_java() {
        // Java: testSetSnapshotRefTagFromJsonAllFields_NullValuesMissing.
        let json = r#"{"action":"set-snapshot-ref","ref-name":"hank","snapshot-id":1,"type":"tag","max-ref-age-ms":1}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetSnapshotRef {
                snapshot_reference, ..
            } => match snapshot_reference.retention {
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(age),
                } => assert_eq!(age, 1),
                other => panic!("expected Tag retention, got {other:?}"),
            },
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    fn test_set_snapshot_ref_tag_all_fields_explicit_nulls_from_json_per_java() {
        // Java: testSetSnapshotRefTagFromJsonAllFields_ExplicitNullValues.
        let json = r#"{"action":"set-snapshot-ref","ref-name":"hank","snapshot-id":1,"type":"tag","max-ref-age-ms":1,"min-snapshots-to-keep":null,"max-snapshot-age-ms":null}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetSnapshotRef {
                snapshot_reference, ..
            } => match snapshot_reference.retention {
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(1),
                } => {}
                other => panic!("expected Tag(max_ref_age_ms=1), got {other:?}"),
            },
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    // --- 31-33. testSetSnapshotRefBranch tests -----------------------------

    #[test]
    #[ignore = "feature gap: Rust SnapshotRetention tag matcher is strictly lowercase; Java accepts \"bRaNch\" via case-insensitive enum parse in testSetSnapshotRefBranchFromJsonDefault_NullValuesMissing"]
    fn test_set_snapshot_ref_branch_mixed_case_from_json_per_java() {
        let json =
            r#"{"action":"set-snapshot-ref","ref-name":"hank","snapshot-id":1,"type":"bRaNch"}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetSnapshotRef {
                snapshot_reference, ..
            } => assert!(matches!(
                snapshot_reference.retention,
                SnapshotRetention::Branch { .. },
            )),
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    #[ignore = "feature gap: same case-insensitive enum parse gap as above; Java's testSetSnapshotRefBranchFromJsonDefault_ExplicitNullValues additionally sets the three retention keys to null"]
    fn test_set_snapshot_ref_branch_mixed_case_explicit_nulls_from_json_per_java() {
        let json = r#"{"action":"set-snapshot-ref","ref-name":"hank","snapshot-id":1,"type":"bRaNch","max-ref-age-ms":null,"min-snapshots-to-keep":null,"max-snapshot-age-ms":null}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetSnapshotRef {
                snapshot_reference, ..
            } => assert!(matches!(
                snapshot_reference.retention,
                SnapshotRetention::Branch { .. },
            )),
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    fn test_set_snapshot_ref_branch_all_fields_from_json_per_java() {
        // Java: testBranchFromJsonAllFields.
        let json = r#"{"action":"set-snapshot-ref","ref-name":"hank","snapshot-id":1,"type":"branch","min-snapshots-to-keep":2,"max-snapshot-age-ms":3,"max-ref-age-ms":4}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetSnapshotRef {
                ref_name,
                snapshot_reference,
            } => {
                assert_eq!(ref_name, "hank");
                assert_eq!(snapshot_reference.snapshot_id, 1);
                match snapshot_reference.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep,
                        max_snapshot_age_ms,
                        max_ref_age_ms,
                    } => {
                        assert_eq!(min_snapshots_to_keep, Some(2));
                        assert_eq!(max_snapshot_age_ms, Some(3));
                        assert_eq!(max_ref_age_ms, Some(4));
                    }
                    other => panic!("expected Branch retention, got {other:?}"),
                }
            }
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    // --- 34-37. testSetSnapshotRef{Tag,Branch}ToJson{Default,AllFields} ----

    #[test]
    fn test_set_snapshot_ref_tag_default_to_json_per_java() {
        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            snapshot_reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            },
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-snapshot-ref");
        assert_eq!(value["ref-name"], "hank");
        assert_eq!(value["snapshot-id"], 1);
        assert_eq!(value["type"], "tag");
        assert!(value.get("max-ref-age-ms").is_none(), "got {value}");
    }

    #[test]
    fn test_set_snapshot_ref_tag_all_fields_to_json_per_java() {
        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            snapshot_reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: Some(1),
                },
            },
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["type"], "tag");
        assert_eq!(value["max-ref-age-ms"], 1);
    }

    #[test]
    fn test_set_snapshot_ref_branch_default_to_json_per_java() {
        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            snapshot_reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-snapshot-ref");
        assert_eq!(value["type"], "branch");
        assert!(value.get("min-snapshots-to-keep").is_none(), "got {value}");
        assert!(value.get("max-snapshot-age-ms").is_none(), "got {value}");
        assert!(value.get("max-ref-age-ms").is_none(), "got {value}");
    }

    #[test]
    fn test_set_snapshot_ref_branch_all_fields_to_json_per_java() {
        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            snapshot_reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(2),
                    max_snapshot_age_ms: Some(3),
                    max_ref_age_ms: Some(4),
                },
            },
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["min-snapshots-to-keep"], 2);
        assert_eq!(value["max-snapshot-age-ms"], 3);
        assert_eq!(value["max-ref-age-ms"], 4);
    }

    // --- 38-39. testSetProperties{From,Failure}Json ------------------------

    #[test]
    fn test_set_properties_from_json_per_java() {
        // Java's testSetPropertiesFromJson asserts the "updates" key parses.
        let json = r#"{"action":"set-properties","updates":{"prop1":"val1","prop2":"val2"}}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetProperties { updates } => {
                assert_eq!(updates.get("prop1").map(String::as_str), Some("val1"));
                assert_eq!(updates.get("prop2").map(String::as_str), Some("val2"));
            }
            other => panic!("expected SetProperties, got {other:?}"),
        }
    }

    #[test]
    #[ignore = "feature gap: Rust SetProperties only accepts the canonical `updates` key; Java also accepts the deprecated `updated` alias and resolves `updates` taking precedence when both are present"]
    fn test_set_properties_accepts_deprecated_updated_alias_per_java() {
        let json = r#"{"action":"set-properties","updated":{"prop1":"val1"}}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetProperties { updates } => {
                assert_eq!(updates.get("prop1").map(String::as_str), Some("val1"));
            }
            other => panic!("expected SetProperties, got {other:?}"),
        }
    }

    #[test]
    fn test_set_properties_rejects_null_property_value_per_java() {
        // Java: testSetPropertiesFromJsonFailsWhenDeserializingNullValues.
        let json = r#"{"action":"set-properties","updates":{"prop1":"val1","prop2":null}}"#;
        let result: Result<TableUpdate, _> = serde_json::from_str(json);
        assert!(result.is_err(), "expected null value to be rejected");
    }

    // --- 40. testSetPropertiesToJson ---------------------------------------

    #[test]
    fn test_set_properties_to_json_per_java() {
        let mut updates = HashMap::new();
        updates.insert("prop1".to_string(), "val1".to_string());
        updates.insert("prop2".to_string(), "val2".to_string());
        let update = TableUpdate::SetProperties { updates };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-properties");
        assert_eq!(value["updates"]["prop1"], "val1");
        assert_eq!(value["updates"]["prop2"], "val2");
    }

    // --- 41-42. testRemoveProperties{From,To}Json --------------------------

    #[test]
    fn test_remove_properties_from_json_per_java() {
        let json = r#"{"action":"remove-properties","removals":["prop1","prop2"]}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::RemoveProperties { mut removals } => {
                removals.sort();
                assert_eq!(removals, vec!["prop1", "prop2"]);
            }
            other => panic!("expected RemoveProperties, got {other:?}"),
        }
    }

    #[test]
    #[ignore = "feature gap: Rust RemoveProperties only accepts the canonical `removals` key; Java also accepts the deprecated `removed` alias and resolves `removals` taking precedence when both are present"]
    fn test_remove_properties_accepts_deprecated_removed_alias_per_java() {
        let json = r#"{"action":"remove-properties","removed":["prop1","prop2"]}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::RemoveProperties { removals } => {
                assert_eq!(removals.len(), 2);
            }
            other => panic!("expected RemoveProperties, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_properties_to_json_per_java() {
        let update = TableUpdate::RemoveProperties {
            removals: vec!["prop1".to_string(), "prop2".to_string()],
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "remove-properties");
        assert_eq!(value["removals"], serde_json::json!(["prop1", "prop2"]));
    }

    // --- 43-44. testSetLocation{From,To}Json -------------------------------

    #[test]
    fn test_set_location_from_json_per_java() {
        let json = r#"{"action":"set-location","location":"s3://bucket/warehouse/tbl_location"}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        match parsed {
            TableUpdate::SetLocation { location } => {
                assert_eq!(location, "s3://bucket/warehouse/tbl_location");
            }
            other => panic!("expected SetLocation, got {other:?}"),
        }
    }

    #[test]
    fn test_set_location_to_json_per_java() {
        let update = TableUpdate::SetLocation {
            location: "s3://bucket/warehouse/tbl_location".to_string(),
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-location");
        assert_eq!(value["location"], "s3://bucket/warehouse/tbl_location");
    }

    // --- 45-46. testSetStatistics / testRemoveStatistics (Rust gap) --------

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no SetStatistics variant; Java models this action carrying a StatisticsFile with puffin path/size/blob-metadata"]
    fn test_set_statistics_per_java() {
        let json = r#"{"action":"set-statistics","snapshot-id":1940541653261589030,"statistics":{"snapshot-id":1940541653261589030,"statistics-path":"s3://bucket/warehouse/stats.puffin","file-size-in-bytes":124,"file-footer-size-in-bytes":27,"blob-metadata":[{"type":"boring-type","snapshot-id":1940541653261589030,"sequence-number":2,"fields":[1],"properties":{"prop-key":"prop-value"}}]}}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "set-statistics");
    }

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no RemoveStatistics variant; Java's testRemoveStatistics pins {action:remove-statistics, snapshot-id} on the wire"]
    fn test_remove_statistics_per_java() {
        let json = r#"{"action":"remove-statistics","snapshot-id":1940541653261589030}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "remove-statistics");
    }

    // --- 47-48. testAddViewVersion{From,To}Json (ViewUpdate side) ---------

    fn sample_metadata_update_view_version() -> Version<Option<()>> {
        Version::builder()
            .version_id(23)
            .timestamp_ms(123_456_789)
            .schema_id(4)
            .summary(iceberg_rust_spec::spec::view_metadata::Summary::default())
            .representations(vec![])
            .default_namespace(vec!["ns".to_string()])
            .build()
            .unwrap()
    }

    #[test]
    fn test_add_view_version_to_json_per_java() {
        // Java pins {action:add-view-version, view-version:{version-id, ...}}.
        // Rust matches the outer action tag and the view-version inner shape.
        let update: ViewUpdate<Option<()>> = ViewUpdate::AddViewVersion {
            view_version: sample_metadata_update_view_version(),
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "add-view-version");
        assert_eq!(value["view-version"]["version-id"], 23);
        assert_eq!(value["view-version"]["timestamp-ms"], 123_456_789);
        assert_eq!(value["view-version"]["schema-id"], 4);
        assert_eq!(
            value["view-version"]["default-namespace"],
            serde_json::json!(["ns"]),
        );
    }

    #[test]
    fn test_add_view_version_from_json_per_java() {
        let expected: ViewUpdate<Option<()>> = ViewUpdate::AddViewVersion {
            view_version: sample_metadata_update_view_version(),
        };
        let json = serde_json::to_string(&expected).unwrap();
        let parsed: ViewUpdate<Option<()>> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    #[ignore = "feature gap: Rust view-version `summary` is a fixed struct (operation/engine-name/engine-version); Java's ViewVersion.summary is a free-form Map<String,String> and testAddViewVersion{To,From}Json pins {\"user\":\"some-user\"}"]
    fn test_add_view_version_summary_is_free_form_map_per_java() {
        let json = r#"{"action":"add-view-version","view-version":{"version-id":23,"timestamp-ms":123456789,"schema-id":4,"summary":{"user":"some-user"},"default-namespace":["ns"],"representations":[]}}"#;
        let parsed: ViewUpdate<Option<()>> = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["view-version"]["summary"]["user"], "some-user");
    }

    // --- 49-50. testSetCurrentViewVersion{From,To}Json ---------------------

    #[test]
    fn test_set_current_view_version_from_json_per_java() {
        let json = r#"{"action":"set-current-view-version","view-version-id":23}"#;
        let parsed: ViewUpdate<Option<()>> = serde_json::from_str(json).unwrap();
        match parsed {
            ViewUpdate::SetCurrentViewVersion { view_version_id } => {
                assert_eq!(view_version_id, 23);
            }
            other => panic!("expected SetCurrentViewVersion, got {other:?}"),
        }
    }

    #[test]
    fn test_set_current_view_version_to_json_per_java() {
        let update: ViewUpdate<Option<()>> = ViewUpdate::SetCurrentViewVersion {
            view_version_id: 23,
        };
        let value = serde_json::to_value(&update).unwrap();
        assert_eq!(value["action"], "set-current-view-version");
        assert_eq!(value["view-version-id"], 23);
    }

    // --- 51-52. testSetPartitionStatistics / testRemovePartitionStatistics -

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no SetPartitionStatistics variant; Java pins {action:set-partition-statistics, partition-statistics:{snapshot-id, statistics-path, file-size-in-bytes}}"]
    fn test_set_partition_statistics_per_java() {
        let json = r#"{"action":"set-partition-statistics","partition-statistics":{"snapshot-id":1940541653261589030,"statistics-path":"s3://bucket/warehouse/stats1.parquet","file-size-in-bytes":43}}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "set-partition-statistics");
    }

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no RemovePartitionStatistics variant; Java pins {action:remove-partition-statistics, snapshot-id}"]
    fn test_remove_partition_statistics_per_java() {
        let json = r#"{"action":"remove-partition-statistics","snapshot-id":1940541653261589030}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "remove-partition-statistics");
    }

    // --- 53. testRemovePartitionSpec ---------------------------------------

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no RemovePartitionSpecs variant; Java pins {action:remove-partition-specs, spec-ids:[...]}"]
    fn test_remove_partition_specs_per_java() {
        let json = r#"{"action":"remove-partition-specs","spec-ids":[1,2,3]}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "remove-partition-specs");
    }

    // --- 54. testRemoveSchemas ---------------------------------------------

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no RemoveSchemas variant; Java pins {action:remove-schemas, schema-ids:[...]}"]
    fn test_remove_schemas_per_java() {
        let json = r#"{"action":"remove-schemas","schema-ids":[1,2,3]}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "remove-schemas");
    }

    // --- 55. testAddEncryptionKey ------------------------------------------

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no AddEncryptionKey variant; Java pins {action:add-encryption-key, encryption-key:{key-id, encrypted-key-metadata, encrypted-by-id}}"]
    fn test_add_encryption_key_per_java() {
        let json = r#"{"action":"add-encryption-key","encryption-key":{"key-id":"a","encrypted-key-metadata":"a2V5","encrypted-by-id":"b"}}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "add-encryption-key");
    }

    // --- 56. testRemoveEncryptionKey ---------------------------------------

    #[test]
    #[ignore = "feature gap: Rust TableUpdate has no RemoveEncryptionKey variant; Java pins {action:remove-encryption-key, key-id}"]
    fn test_remove_encryption_key_per_java() {
        let json = r#"{"action":"remove-encryption-key","key-id":"a"}"#;
        let parsed: TableUpdate = serde_json::from_str(json).unwrap();
        let value = serde_json::to_value(&parsed).unwrap();
        assert_eq!(value["action"], "remove-encryption-key");
    }

    // --- Port: TestUpdateRequirements (Apache Iceberg Java, 41 @Test) ------
    //
    // Java's `UpdateRequirements` is a static helper with four entry points:
    //   - `forCreateTable(updates) -> List<UpdateRequirement>`
    //   - `forUpdateTable(metadata, updates) -> List<UpdateRequirement>`
    //   - `forReplaceTable(metadata, updates) -> List<UpdateRequirement>`
    //   - `forReplaceView(viewMetadata, updates) -> List<UpdateRequirement>`
    // Each entry point walks the proposed `MetadataUpdate` list and derives
    // the preconditions a client must include in its commit body so the
    // server can detect concurrent modification. This is the CLIENT-side
    // generator that pairs with Rust's existing `check_table_requirements`
    // (the server-side validator).
    //
    // Rust has no centralised generator. Per-operation requirement creation
    // is scattered across `iceberg-rust/src/table/transaction/operation.rs`
    // (e.g. `Operation::NewSnapshot` emits `AssertRefSnapshotId`) but no
    // function reduces an arbitrary `Vec<TableUpdate>` into the matching
    // `Vec<TableRequirement>`. Each Java `@Test` below is pinned with
    // `#[ignore]` documenting:
    //   - The Java scenario
    //   - The expected requirement-list shape (the contract that the
    //     missing Rust `update_requirements::for_update_table(...)` etc.
    //     must satisfy to make the test pass).

    #[test]
    #[ignore = "feature gap: Rust has no UpdateRequirements::for_{create,update,replace}_table / for_replace_view generators; Java's nullCheck asserts each of the 4 entry points throws NullPointerException when given a null metadata or null updates argument (7 assertions in one test)"]
    fn test_update_requirements_null_check_per_java() {
        // Java: nullCheck.
    }

    #[test]
    #[ignore = "feature gap: Rust has no UpdateRequirements::for_create_table; Java's emptyUpdatesForCreateTable asserts forCreateTable([]) yields a single [AssertTableDoesNotExist] requirement"]
    fn test_update_requirements_for_create_table_empty_updates_per_java() {
        // Java: emptyUpdatesForCreateTable.
    }

    #[test]
    #[ignore = "feature gap: Rust has no UpdateRequirements::for_{update,replace}_table; Java's emptyUpdatesForUpdateAndReplaceTable asserts forReplaceTable([]) and forUpdateTable([]) yield [AssertTableUUID(metadata.uuid)]"]
    fn test_update_requirements_for_update_and_replace_table_empty_updates_per_java() {
        // Java: emptyUpdatesForUpdateAndReplaceTable.
    }

    #[test]
    #[ignore = "feature gap: Rust has no UpdateRequirements::for_replace_view; Java's emptyUpdatesForReplaceView asserts forReplaceView([]) yields [AssertViewUUID(viewMetadata.uuid)]"]
    fn test_update_requirements_for_replace_view_empty_updates_per_java() {
        // Java: emptyUpdatesForReplaceView.
    }

    #[test]
    #[ignore = "feature gap: Java's tableAlreadyExists asserts forCreateTable([AssignUUID(...), UpgradeFormatVersion(2)]) emits exactly one AssertTableDoesNotExist (no duplicate even though the schema/format-version paths would normally add their own assertions)"]
    fn test_update_requirements_table_already_exists_per_java() {
        // Java: tableAlreadyExists.
    }

    #[test]
    #[ignore = "feature gap: Java's assignUUID asserts forUpdateTable(meta, [AssignUUID(meta.uuid)]) emits only [AssertTableUUID(meta.uuid)] (no extra AssignUUID requirement) when the action matches existing UUID"]
    fn test_update_requirements_assign_uuid_success_per_java() {
        // Java: assignUUID.
    }

    #[test]
    #[ignore = "feature gap: Java's assignUUIDFailure asserts forUpdateTable(meta, [AssignUUID(other-uuid)]) throws CommitFailedException(\"Cannot reassign uuid\") when the proposed uuid differs from metadata.uuid"]
    fn test_update_requirements_assign_uuid_failure_per_java() {
        // Java: assignUUIDFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's assignUUIDToView mirrors assignUUID for forReplaceView with the same matching-UUID precondition"]
    fn test_update_requirements_assign_uuid_to_view_success_per_java() {
        // Java: assignUUIDToView.
    }

    #[test]
    #[ignore = "feature gap: Java's assignUUIDToViewFailure mirrors assignUUIDFailure for forReplaceView"]
    fn test_update_requirements_assign_uuid_to_view_failure_per_java() {
        // Java: assignUUIDToViewFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's upgradeFormatVersion(2) and (3) ValueSource cases assert forUpdateTable(meta, [UpgradeFormatVersion(N)]) emits [AssertTableUUID(meta.uuid)] only (no per-version requirement)"]
    fn test_update_requirements_upgrade_format_version_per_java() {
        // Java: upgradeFormatVersion @ ValueSource(ints = {2, 3}).
    }

    #[test]
    #[ignore = "feature gap: Java's upgradeFormatVersionForView mirrors upgradeFormatVersion for forReplaceView"]
    fn test_update_requirements_upgrade_format_version_for_view_per_java() {
        // Java: upgradeFormatVersionForView.
    }

    #[test]
    #[ignore = "feature gap: Java's addSchema asserts forUpdateTable(meta, [AddSchema(schema)]) emits [AssertTableUUID, AssertLastAssignedFieldId(meta.last_column_id)]"]
    fn test_update_requirements_add_schema_per_java() {
        // Java: addSchema.
    }

    #[test]
    #[ignore = "feature gap: Java's addSchemaFailure simulates concurrent last_column_id drift and asserts forUpdateTable still emits the AssertLastAssignedFieldId(orig) requirement so the commit fails server-side"]
    fn test_update_requirements_add_schema_failure_per_java() {
        // Java: addSchemaFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's addSchemaForView mirrors addSchema for forReplaceView; result is [AssertViewUUID, AssertLastAssignedFieldId(meta.last_column_id)]"]
    fn test_update_requirements_add_schema_for_view_per_java() {
        // Java: addSchemaForView.
    }

    #[test]
    #[ignore = "feature gap: Java's setCurrentSchema asserts forUpdateTable(meta, [SetCurrentSchema(id)]) emits [AssertTableUUID, AssertCurrentSchemaID(meta.current_schema_id)]"]
    fn test_update_requirements_set_current_schema_per_java() {
        // Java: setCurrentSchema.
    }

    #[test]
    #[ignore = "feature gap: Java's setCurrentSchemaFailure pins concurrent schema drift; the requirement list must include the AssertCurrentSchemaID precondition that fails on the diverged state"]
    fn test_update_requirements_set_current_schema_failure_per_java() {
        // Java: setCurrentSchemaFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's addPartitionSpec asserts forUpdateTable(meta, [AddPartitionSpec(spec)]) emits [AssertTableUUID, AssertLastAssignedPartitionId(meta.last_partition_id)]"]
    fn test_update_requirements_add_partition_spec_per_java() {
        // Java: addPartitionSpec.
    }

    #[test]
    #[ignore = "feature gap: Java's addPartitionSpecFailure pins concurrent last_partition_id drift; the AssertLastAssignedPartitionId precondition must trigger commit failure"]
    fn test_update_requirements_add_partition_spec_failure_per_java() {
        // Java: addPartitionSpecFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's setDefaultPartitionSpec asserts forUpdateTable(meta, [SetDefaultPartitionSpec(id)]) emits [AssertTableUUID, AssertDefaultSpecID(meta.default_spec_id)]"]
    fn test_update_requirements_set_default_partition_spec_per_java() {
        // Java: setDefaultPartitionSpec.
    }

    #[test]
    #[ignore = "feature gap: Java's setDefaultPartitionSpecFailure pins concurrent default_spec_id drift"]
    fn test_update_requirements_set_default_partition_spec_failure_per_java() {
        // Java: setDefaultPartitionSpecFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's removePartitionSpec asserts forUpdateTable(meta, [RemovePartitionSpecs(spec_ids)]) emits [AssertTableUUID, AssertDefaultSpecID, AssertLastAssignedPartitionId]"]
    fn test_update_requirements_remove_partition_specs_per_java() {
        // Java: removePartitionSpec.
    }

    #[test]
    #[ignore = "feature gap: Java's testRemovePartitionSpecsWithBranch pins the branch-aware variant; an AssertRefSnapshotId is added for each branch the removed spec was referenced from"]
    fn test_update_requirements_remove_partition_specs_with_branch_per_java() {
        // Java: testRemovePartitionSpecsWithBranch.
    }

    #[test]
    #[ignore = "feature gap: Java's testRemovePartitionSpecsWithSpecChangedFailure pins concurrent default_spec_id drift while RemovePartitionSpecs is in flight"]
    fn test_update_requirements_remove_partition_specs_spec_changed_failure_per_java() {
        // Java: testRemovePartitionSpecsWithSpecChangedFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's testRemovePartitionSpecsWithBranchChangedFailure pins concurrent branch head drift while RemovePartitionSpecs is in flight"]
    fn test_update_requirements_remove_partition_specs_branch_changed_failure_per_java() {
        // Java: testRemovePartitionSpecsWithBranchChangedFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's removeSchemas asserts forUpdateTable(meta, [RemoveSchemas(ids)]) emits [AssertTableUUID, AssertCurrentSchemaID, AssertLastAssignedFieldId]"]
    fn test_update_requirements_remove_schemas_per_java() {
        // Java: removeSchemas.
    }

    #[test]
    #[ignore = "feature gap: Java's testRemoveSchemasWithBranch pins the branch-aware variant for RemoveSchemas"]
    fn test_update_requirements_remove_schemas_with_branch_per_java() {
        // Java: testRemoveSchemasWithBranch.
    }

    #[test]
    #[ignore = "feature gap: Java's testRemoveSchemasWithSchemaChangedFailure pins concurrent current_schema_id drift while RemoveSchemas is in flight"]
    fn test_update_requirements_remove_schemas_schema_changed_failure_per_java() {
        // Java: testRemoveSchemasWithSchemaChangedFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's testRemoveSchemasWithBranchChangedFailure pins concurrent branch head drift while RemoveSchemas is in flight"]
    fn test_update_requirements_remove_schemas_branch_changed_failure_per_java() {
        // Java: testRemoveSchemasWithBranchChangedFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's addSortOrder asserts forUpdateTable(meta, [AddSortOrder(...)]) emits [AssertTableUUID] (sort order adds no extra precondition since sort_order_id is server-assigned)"]
    fn test_update_requirements_add_sort_order_per_java() {
        // Java: addSortOrder.
    }

    #[test]
    #[ignore = "feature gap: Java's setDefaultSortOrder asserts forUpdateTable(meta, [SetDefaultSortOrder(id)]) emits [AssertTableUUID, AssertDefaultSortOrderID(meta.default_sort_order_id)]"]
    fn test_update_requirements_set_default_sort_order_per_java() {
        // Java: setDefaultSortOrder.
    }

    #[test]
    #[ignore = "feature gap: Java's setDefaultSortOrderFailure pins concurrent default_sort_order_id drift"]
    fn test_update_requirements_set_default_sort_order_failure_per_java() {
        // Java: setDefaultSortOrderFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's setAndRemoveStatistics asserts forUpdateTable(meta, [SetStatistics, RemoveStatistics]) emits [AssertTableUUID]; the statistics actions don't add per-snapshot preconditions"]
    fn test_update_requirements_set_and_remove_statistics_per_java() {
        // Java: setAndRemoveStatistics.
    }

    #[test]
    #[ignore = "feature gap: Java's addAndRemoveSnapshot asserts forUpdateTable(meta, [AddSnapshot, RemoveSnapshots([id])]) emits [AssertTableUUID, AssertRefSnapshotId(\"main\", current)]"]
    fn test_update_requirements_add_and_remove_snapshot_per_java() {
        // Java: addAndRemoveSnapshot.
    }

    #[test]
    #[ignore = "feature gap: Java's addAndRemoveSnapshots pins the bulk variant with multiple snapshot ids; same AssertRefSnapshotId precondition for the affected refs"]
    fn test_update_requirements_add_and_remove_snapshots_bulk_per_java() {
        // Java: addAndRemoveSnapshots.
    }

    #[test]
    #[ignore = "feature gap: Java's setAndRemoveSnapshotRef asserts forUpdateTable(meta, [SetSnapshotRef(ref,...), RemoveSnapshotRef(ref)]) emits [AssertTableUUID, AssertRefSnapshotId(ref, ...)]"]
    fn test_update_requirements_set_and_remove_snapshot_ref_per_java() {
        // Java: setAndRemoveSnapshotRef.
    }

    #[test]
    #[ignore = "feature gap: Java's setSnapshotRefFailure pins concurrent ref drift; the AssertRefSnapshotId precondition must trigger commit failure"]
    fn test_update_requirements_set_snapshot_ref_failure_per_java() {
        // Java: setSnapshotRefFailure.
    }

    #[test]
    #[ignore = "feature gap: Java's setAndRemoveProperties asserts forUpdateTable(meta, [SetProperties, RemoveProperties]) emits [AssertTableUUID] only — property changes have no extra precondition"]
    fn test_update_requirements_set_and_remove_properties_per_java() {
        // Java: setAndRemoveProperties.
    }

    #[test]
    #[ignore = "feature gap: Java's setAndRemovePropertiesForView mirrors setAndRemoveProperties for forReplaceView with [AssertViewUUID]"]
    fn test_update_requirements_set_and_remove_properties_for_view_per_java() {
        // Java: setAndRemovePropertiesForView.
    }

    #[test]
    #[ignore = "feature gap: Java's setLocation asserts forUpdateTable(meta, [SetLocation(path)]) emits [AssertTableUUID]"]
    fn test_update_requirements_set_location_per_java() {
        // Java: setLocation.
    }

    #[test]
    #[ignore = "feature gap: Java's setLocationForView mirrors setLocation for forReplaceView with [AssertViewUUID]"]
    fn test_update_requirements_set_location_for_view_per_java() {
        // Java: setLocationForView.
    }

    #[test]
    #[ignore = "feature gap: Java's addViewVersion asserts forReplaceView(viewMeta, [AddViewVersion(...)]) emits [AssertViewUUID]"]
    fn test_update_requirements_add_view_version_per_java() {
        // Java: addViewVersion.
    }

    #[test]
    #[ignore = "feature gap: Java's setCurrentViewVersion asserts forReplaceView(viewMeta, [SetCurrentViewVersion(id)]) emits [AssertViewUUID]"]
    fn test_update_requirements_set_current_view_version_per_java() {
        // Java: setCurrentViewVersion.
    }
}
