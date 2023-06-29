/*!
Defining the [TableBuilder] struct for creating catalog tables and starting create/replace transactions
*/

use std::sync::Arc;
use std::time::SystemTime;

use object_store::path::Path;
use object_store::ObjectStore;
use uuid::Uuid;

use crate::catalog::identifier::Identifier;
use crate::catalog::relation::Relation;
use crate::model::partition::{PartitionField, Transform};
use crate::model::sort::{NullOrder, SortDirection, SortField, SortOrder};
use crate::model::table_metadata::VersionNumber;
use crate::model::{partition::PartitionSpec, schema::SchemaV2, table_metadata::TableMetadataV2};
use crate::table::Table;
use anyhow::{anyhow, Result};

use super::{Catalog, TableType};

///Builder pattern to create a table
pub struct TableBuilder {
    table_type: TableType,
    metadata: TableMetadataV2,
}

impl TableBuilder {
    /// Creates a new [TableBuilder] to create a Metastore Table with some default metadata entries already set.
    pub fn new_metastore_table(
        base_path: &str,
        schema: SchemaV2,
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self> {
        let partition_spec = PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                name: "default".to_string(),
                field_id: 1,
                source_id: 1,
                transform: Transform::Void,
            }],
        };
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 1,
                transform: Transform::Void,
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            }],
        };
        let metadata = TableMetadataV2 {
            format_version: VersionNumber,
            table_uuid: Uuid::new_v4(),
            location: base_path.to_owned() + &identifier.to_string().replace('.', "/"),
            last_sequence_number: 1,
            last_updated_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|err| anyhow!(err.to_string()))?
                .as_millis() as i64,
            last_column_id: schema.fields.fields.len() as i32,
            schemas: vec![schema],
            current_schema_id: 1,
            partition_specs: vec![partition_spec],
            default_spec_id: 1,
            last_partition_id: 1,
            properties: None,
            current_snapshot_id: None,
            snapshots: None,
            snapshot_log: None,
            metadata_log: None,
            sort_orders: vec![sort_order],
            default_sort_order_id: 0,
            refs: None,
        };
        Ok(TableBuilder {
            metadata,
            table_type: TableType::Metastore(identifier, catalog),
        })
    }
    /// Creates a new [TableBuilder] to create a FileSystem Table with some default metadata entries already set.
    pub fn new_filesystem_table(
        location: &str,
        schema: SchemaV2,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let partition_spec = PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                name: "default".to_string(),
                field_id: 1,
                source_id: 1,
                transform: Transform::Void,
            }],
        };
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 1,
                transform: Transform::Void,
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            }],
        };
        let metadata = TableMetadataV2 {
            format_version: VersionNumber,
            table_uuid: Uuid::new_v4(),
            location: location.to_string(),
            last_sequence_number: 1,
            last_updated_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|err| anyhow!(err.to_string()))?
                .as_millis() as i64,
            last_column_id: schema.fields.fields.len() as i32,
            schemas: vec![schema],
            current_schema_id: 1,
            partition_specs: vec![partition_spec],
            default_spec_id: 1,
            last_partition_id: 1,
            properties: None,
            current_snapshot_id: None,
            snapshots: None,
            snapshot_log: None,
            metadata_log: None,
            sort_orders: vec![sort_order],
            default_sort_order_id: 0,
            refs: None,
        };
        Ok(TableBuilder {
            metadata,
            table_type: TableType::FileSystem(object_store),
        })
    }
    /// Building a table writes the metadata file and commits the table to either the metastore or the filesystem
    pub async fn commit(self) -> Result<Table> {
        match self.table_type {
            TableType::Metastore(identifier, catalog) => {
                let object_store = catalog.object_store();
                let location = &self.metadata.location;
                let uuid = Uuid::new_v4();
                let version = &self.metadata.last_sequence_number;
                let metadata_json = serde_json::to_string(&self.metadata)
                    .map_err(|err| anyhow!(err.to_string()))?;
                let path: Path = (location.to_string()
                    + "/metadata/"
                    + &version.to_string()
                    + "-"
                    + &uuid.to_string()
                    + ".metadata.json")
                    .into();
                object_store
                    .put(&path, metadata_json.into())
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                if let Relation::Table(table) =
                    catalog.register_table(identifier, path.as_ref()).await?
                {
                    Ok(table)
                } else {
                    Err(anyhow!("Building the table failed because registering the table in the catalog didn't return a table."))
                }
            }
            TableType::FileSystem(object_store) => {
                let location = &self.metadata.location;
                let uuid = Uuid::new_v4();
                let version = &self.metadata.last_sequence_number;
                let metadata_json = serde_json::to_string(&self.metadata)
                    .map_err(|err| anyhow!(err.to_string()))?;
                let temp_path: Path =
                    (location.to_string() + "/metadata/" + &uuid.to_string() + ".metadata.json")
                        .into();
                let final_path: Path = (location.to_string()
                    + "/metadata/v"
                    + &version.to_string()
                    + ".metadata.json")
                    .into();
                object_store
                    .put(&temp_path, metadata_json.into())
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                object_store
                    .copy_if_not_exists(&temp_path, &final_path)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                object_store
                    .delete(&temp_path)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))?;
                let table = Table::load_file_system_table(location, &object_store).await?;
                Ok(table)
            }
        }
    }
    /// Sets a partition spec for the table.
    pub fn with_partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.metadata.partition_specs.push(partition_spec);
        self
    }
}
