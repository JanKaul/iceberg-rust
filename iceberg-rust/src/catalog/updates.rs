/*!
Apply updates to table metadata
*/
use iceberg_rust_spec::spec::table_metadata::TableMetadata;
use uuid::Uuid;

use crate::error::Error;

use super::TableUpdate;

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
                metadata.schemas.insert(schema.schema_id, schema);
                if let Some(last_column_id) = last_column_id {
                    metadata.last_column_id = last_column_id;
                }
            }
            TableUpdate::SetCurrentSchema { schema_id } => {
                metadata.current_schema_id = schema_id;
            }
            TableUpdate::AddPartitionSpec { spec } => {
                metadata.partition_specs.insert(spec.spec_id, spec);
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
                metadata.snapshots.insert(snapshot.snapshot_id, snapshot);
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                snapshot_reference,
            } => {
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
