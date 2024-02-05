/*!
check if requirements are valid
*/
use iceberg_rust_spec::spec::table_metadata::TableMetadata;

use super::TableRequirement;

/// Check table update requirements
pub fn check_requirements(requirements: &[TableRequirement], metadata: &TableMetadata) -> bool {
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
