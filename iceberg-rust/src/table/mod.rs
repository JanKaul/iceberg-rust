/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{collections::HashMap, io::Cursor, sync::Arc, time::SystemTime};

use anyhow::Result;
use apache_avro::types::Value as AvroValue;
use object_store::ObjectStore;

use crate::{
    catalog::{identifier::Identifier, Catalog},
    model::{
        manifest_list::{ManifestFile, ManifestFileV1, ManifestFileV2},
        snapshot::{Operation, SnapshotV2, Summary},
        table_metadata::{FormatVersion, TableMetadata},
        types::StructType,
    },
    table::transaction::TableTransaction,
    util::{self, strip_prefix},
};

pub mod files;
pub mod table_builder;
pub mod transaction;

/// Tables can be either one of following types:
/// - FileSystem(https://iceberg.apache.org/spec/#file-system-tables)
/// - Metastore(https://iceberg.apache.org/spec/#metastore-tables)
pub enum TableType {
    /// Metastore table
    Metastore(Identifier, Arc<dyn Catalog>),
}

/// Iceberg table
pub struct Table {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: TableMetadata,
    metadata_location: String,
    manifests: Vec<ManifestFile>,
}

/// Public interface of the table.
impl Table {
    /// Create a new metastore Table
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadata,
        metadata_location: &str,
    ) -> Result<Self> {
        let manifests = get_manifests(&metadata, catalog.object_store()).await?;
        Ok(Table {
            identifier,
            catalog,
            metadata,
            metadata_location: metadata_location.to_string(),
            manifests,
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
    pub fn schema(&self) -> &StructType {
        self.metadata
            .schemas
            .get(&self.metadata.current_schema_id)
            .unwrap()
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
    pub fn manifests(&self) -> &[ManifestFile] {
        &self.manifests
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
    pub(crate) async fn new_snapshot(&mut self) -> Result<()> {
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
                        &strip_prefix(&old_manifest_list_location).as_str().into(),
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
        let snapshot = SnapshotV2 {
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

// Return all manifest files associated to the latest table snapshot. Reads the related manifest_list file and returns its entries.
// If the manifest list file is empty returns an empty vector.
pub(crate) async fn get_manifests(
    metadata: &TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<ManifestFile>> {
    match metadata.current_snapshot()?.map(|x| &x.manifest_list) {
        Some(manifest_list) => {
            let bytes: Cursor<Vec<u8>> = Cursor::new(
                object_store
                    .get(&util::strip_prefix(manifest_list).into())
                    .await
                    .map_err(anyhow::Error::msg)?
                    .bytes()
                    .await?
                    .into(),
            );
            // Read the file content only if the bytes are not empty otherwise return an empty vector
            if !bytes.get_ref().is_empty() {
                let reader = apache_avro::Reader::new(bytes)?;
                reader
                    .map(|record| {
                        avro_value_to_manifest_file(record, metadata.format_version.clone())
                    })
                    .collect()
            } else {
                Ok(Vec::new())
            }
        }
        None => Ok(Vec::new()),
    }
}

/// Convert an avro value to a [ManifestFile] according to the provided format version
fn avro_value_to_manifest_file(
    entry: Result<AvroValue, apache_avro::Error>,
    format_version: FormatVersion,
) -> Result<ManifestFile, anyhow::Error> {
    entry
        .and_then(|value| match format_version {
            FormatVersion::V1 => {
                apache_avro::from_value::<ManifestFileV1>(&value).map(ManifestFile::V1)
            }
            FormatVersion::V2 => {
                apache_avro::from_value::<ManifestFileV2>(&value).map(ManifestFile::V2)
            }
        })
        .map_err(anyhow::Error::msg)
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
