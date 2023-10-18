/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use anyhow::anyhow;
use object_store::ObjectStore;

use crate::{
    catalog::{identifier::Identifier, Catalog},
    model::{
        manifest_list::ManifestFileEntry,
        schema::Schema,
        snapshot::{Operation, Snapshot, Summary},
        table_metadata::TableMetadata,
    },
    table::transaction::TableTransaction,
    util::strip_prefix,
};

pub mod files;
pub mod table_builder;
pub mod transaction;

/// Iceberg table
pub struct Table {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: TableMetadata,
    metadata_location: String,
}

/// Public interface of the table.
impl Table {
    /// Create a new metastore Table
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadata,
        metadata_location: &str,
    ) -> Result<Self, anyhow::Error> {
        Ok(Table {
            identifier,
            catalog,
            metadata,
            metadata_location: metadata_location.to_string(),
        })
    }
    /// Get the table identifier in the catalog. Returns None of it is a filesystem table.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    /// Get the catalog associated to the table. Returns None if the table is a filesystem table
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    /// Get the object_store associated to the table
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog.object_store()
    }
    /// Get the metadata of the table
    pub fn schema(&self) -> Result<&Schema, anyhow::Error> {
        self.metadata.current_schema()
    }
    /// Get the metadata of the table
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Get the location of the current metadata file
    pub async fn manifests(&self) -> Result<Vec<ManifestFileEntry>, anyhow::Error> {
        let metadata = self.metadata();
        metadata
            .current_snapshot()?
            .ok_or(anyhow!("There is no snapshot for table."))?
            .manifests(metadata, self.object_store().clone())
            .await?
            .collect()
    }
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self) -> TableTransaction {
        TableTransaction::new(self)
    }
}

/// Private interface of the table.
impl Table {
    /// Increment the sequence number of the table. Is typically used when commiting a new table transaction.
    pub(crate) fn increment_sequence_number(&mut self) {
        self.metadata.last_sequence_number += 1;
    }

    /// Create a new table snapshot based on the manifest_list file of the previous snapshot.
    pub(crate) async fn new_snapshot(&mut self) -> Result<(), anyhow::Error> {
        let mut bytes: [u8; 8] = [0u8; 8];
        getrandom::getrandom(&mut bytes).unwrap();
        let snapshot_id = i64::from_le_bytes(bytes);
        let object_store = self.object_store();
        let metadata = &mut self.metadata;
        let old_manifest_list_location = metadata.current_snapshot()?.map(|x| &x.manifest_list);
        let new_manifest_list_location = metadata.location.to_string()
            + "/metadata/snap-"
            + &snapshot_id.to_string()
            + &uuid::Uuid::new_v4().to_string()
            + ".avro";
        // If there is a previous snapshot with a manifest_list file, that file gets copied for the new snapshot. If not, a new empty file is created.
        match old_manifest_list_location {
            Some(old_manifest_list_location) => {
                object_store
                    .copy(
                        &strip_prefix(old_manifest_list_location).as_str().into(),
                        &strip_prefix(&new_manifest_list_location).as_str().into(),
                    )
                    .await?
            }
            None => {
                object_store
                    .put(
                        &strip_prefix(&new_manifest_list_location).as_str().into(),
                        "null".as_bytes().into(),
                    )
                    .await?;
            }
        };
        let snapshot = Snapshot {
            snapshot_id,
            parent_snapshot_id: metadata.current_snapshot_id,
            sequence_number: metadata.last_sequence_number + 1,
            timestamp_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            manifest_list: new_manifest_list_location,
            summary: Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            },
            schema_id: Some(metadata.current_schema_id as i64),
        };
        if let Some(snapshots) = &mut metadata.snapshots {
            snapshots.insert(snapshot_id, snapshot);
            metadata.current_snapshot_id = Some(snapshot_id)
        } else {
            metadata.snapshots = Some(HashMap::from_iter(vec![(snapshot_id, snapshot)]));
            metadata.current_snapshot_id = Some(snapshot_id)
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        catalog::{identifier::Identifier, memory::MemoryCatalog, Catalog},
        model::{
            schema::SchemaV2,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_increment_sequence_number() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemoryCatalog::new("test", object_store).unwrap());
        let identifier = Identifier::parse("test.table1").unwrap();
        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            fields: StructType {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "one".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "two".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                ],
            },
        };
        let mut table = TableBuilder::new("/", schema, identifier.clone(), catalog.clone())
            .expect("Failed to create table builder.")
            .commit()
            .await
            .expect("Failed to create table.");

        let metadata_location1 = table.metadata_location().to_string();

        let transaction = table.new_transaction();
        transaction.commit().await.unwrap();
        let metadata_location2 = table.metadata_location();
        assert_ne!(metadata_location1, metadata_location2);
    }
}
