/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{collections::HashMap, io::Cursor, sync::Arc, time::SystemTime};

use anyhow::{anyhow, Result};
use apache_avro::types::Value as AvroValue;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore};

use crate::{
    catalog::{identifier::Identifier, Catalog},
    model::{
        data_types::StructType,
        manifest_list::{ManifestFile, ManifestFileV1, ManifestFileV2},
        snapshot::{Operation, SnapshotV1, SnapshotV2, Summary},
        table_metadata::{FormatVersion, TableMetadata},
    },
    table::transaction::TableTransaction,
    util,
};

pub mod files;
pub mod table_builder;
pub mod transaction;

/// Tables can be either one of following types:
/// - FileSystem(https://iceberg.apache.org/spec/#file-system-tables)
/// - Metastore(https://iceberg.apache.org/spec/#metastore-tables)
pub enum TableType {
    /// Filesystem table
    FileSystem(Arc<dyn ObjectStore>),
    /// Metastore table
    Metastore(Identifier, Arc<dyn Catalog>),
}

/// Iceberg table
pub struct Table {
    table_type: TableType,
    metadata: TableMetadata,
    metadata_location: String,
    manifests: Vec<ManifestFile>,
}

/// Public interface of the table.
impl Table {
    /// Create a new metastore Table
    pub async fn new_metastore_table(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadata,
        metadata_location: &str,
    ) -> Result<Self> {
        let manifests = get_manifests(&metadata, catalog.object_store()).await?;
        Ok(Table {
            table_type: TableType::Metastore(identifier, catalog),
            metadata,
            metadata_location: metadata_location.to_string(),
            manifests,
        })
    }
    /// Load a filesystem table from an objectstore
    pub async fn load_file_system_table(
        location: &str,
        object_store: &Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let path: Path = (location.to_string() + "/metadata/").into();
        let files = object_store
            .list(Some(&path))
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let version = files
            .fold(Ok::<i64, anyhow::Error>(0), |acc, x| async move {
                match (acc, x) {
                    (Ok(acc), Ok(object_meta)) => {
                        let name = object_meta
                            .location
                            .parts()
                            .last()
                            .ok_or_else(|| anyhow!("Metadata location path is empty."))?;
                        if name.as_ref().ends_with(".metadata.json") {
                            let version: i64 = name
                                .as_ref()
                                .trim_start_matches('v')
                                .trim_end_matches(".metadata.json")
                                .parse()?;
                            if version > acc {
                                Ok(version)
                            } else {
                                Ok(acc)
                            }
                        } else {
                            Ok(acc)
                        }
                    }
                    (Err(err), _) => Err(anyhow!(err.to_string())),
                    (_, Err(err)) => Err(anyhow!(err.to_string())),
                }
            })
            .await?;
        let metadata_location = path.to_string() + "/v" + &version.to_string() + ".metadata.json";
        let bytes = &object_store
            .get(&metadata_location.clone().into())
            .await
            .map_err(|err| anyhow!(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let metadata: TableMetadata = serde_json::from_str(
            std::str::from_utf8(bytes).map_err(|err| anyhow!(err.to_string()))?,
        )
        .map_err(|err| anyhow!(err.to_string()))?;
        let manifests = get_manifests(&metadata, Arc::clone(object_store)).await?;
        Ok(Table {
            metadata,
            table_type: TableType::FileSystem(Arc::clone(object_store)),
            metadata_location,
            manifests,
        })
    }
    /// Get the table identifier in the catalog. Returns None of it is a filesystem table.
    pub fn identifier(&self) -> Option<&Identifier> {
        match &self.table_type {
            TableType::FileSystem(_) => None,
            TableType::Metastore(identifier, _) => Some(identifier),
        }
    }
    /// Get the catalog associated to the table. Returns None if the table is a filesystem table
    pub fn catalog(&self) -> Option<&Arc<dyn Catalog>> {
        match &self.table_type {
            TableType::FileSystem(_) => None,
            TableType::Metastore(_, catalog) => Some(catalog),
        }
    }
    /// Get the object_store associated to the table
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match &self.table_type {
            TableType::FileSystem(object_store) => Arc::clone(object_store),
            TableType::Metastore(_, catalog) => catalog.object_store(),
        }
    }
    /// Get the metadata of the table
    pub fn schema(&self) -> &StructType {
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
        match &mut self.metadata {
            TableMetadata::V1(_) => (),
            TableMetadata::V2(metadata) => {
                metadata.last_sequence_number += 1;
            }
        }
    }

    /// Create a new table snapshot based on the manifest_list file of the previous snapshot.
    pub(crate) async fn new_snapshot(&mut self) -> Result<()> {
        let mut bytes: [u8; 8] = [0u8; 8];
        getrandom::getrandom(&mut bytes).unwrap();
        let snapshot_id = i64::from_le_bytes(bytes);
        let object_store = self.object_store();
        let old_manifest_list_location = self.metadata.manifest_list().map(|st| st.to_string());
        match &mut self.metadata {
            TableMetadata::V1(metadata) => {
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
                                &old_manifest_list_location.into(),
                                &new_manifest_list_location.clone().into(),
                            )
                            .await?
                    }
                    None => {
                        object_store
                            .put(
                                &new_manifest_list_location.clone().into(),
                                Vec::new().into(),
                            )
                            .await?;
                    }
                };
                let snapshot = SnapshotV1 {
                    snapshot_id,
                    parent_snapshot_id: metadata.current_snapshot_id,
                    timestamp_ms: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                    manifest_list: Some(new_manifest_list_location),
                    manifests: None,
                    summary: None,
                    schema_id: metadata.current_schema_id.map(|id| id as i64),
                };
                if let Some(snapshots) = &mut metadata.snapshots {
                    snapshots.push(snapshot);
                    metadata.current_snapshot_id = Some(snapshot_id)
                } else {
                    metadata.snapshots = Some(vec![snapshot]);
                    metadata.current_snapshot_id = Some(snapshot_id)
                };
                Ok(())
            }
            TableMetadata::V2(metadata) => {
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
                                &old_manifest_list_location.into(),
                                &new_manifest_list_location.clone().into(),
                            )
                            .await?
                    }
                    None => {
                        object_store
                            .put(
                                &new_manifest_list_location.clone().into(),
                                Vec::new().into(),
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
                    snapshots.push(snapshot);
                    metadata.current_snapshot_id = Some(snapshot_id)
                } else {
                    metadata.snapshots = Some(vec![snapshot]);
                    metadata.current_snapshot_id = Some(snapshot_id)
                };
                Ok(())
            }
        }
    }
}

// Return all manifest files associated to the latest table snapshot. Reads the related manifest_list file and returns its entries.
// If the manifest list file is empty returns an empty vector.
pub(crate) async fn get_manifests(
    metadata: &TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Vec<ManifestFile>> {
    match metadata.manifest_list() {
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
                    .map(|record| avro_value_to_manifest_file(record, metadata.format_version()))
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
        model::{
            data_types::{PrimitiveType, StructField, StructType, Type},
            schema::SchemaV2,
        },
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_increment_sequence_number() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
        let mut table =
            TableBuilder::new_filesystem_table("test/table1", schema, Arc::clone(&object_store))
                .unwrap()
                .commit()
                .await
                .unwrap();

        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/table1/metadata/v1.metadata.json");

        let transaction = table.new_transaction();
        transaction.commit().await.unwrap();
        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/table1/metadata/v2.metadata.json");
    }
}
