/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{collections::HashMap, io::Cursor, iter::repeat, sync::Arc, time::SystemTime};

use anyhow::anyhow;
use object_store::{path::Path, ObjectStore};

use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};

use crate::{
    catalog::{identifier::Identifier, Catalog},
    model::{
        manifest::{ManifestEntry, ManifestReader},
        manifest_list::ManifestListEntry,
        schema::Schema,
        snapshot::{Operation, Snapshot, Summary},
        table_metadata::TableMetadata,
    },
    table::transaction::TableTransaction,
    util::{self, strip_prefix},
};

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
    /// Get list of current manifest files
    pub async fn manifests(&self) -> Result<Vec<ManifestListEntry>, anyhow::Error> {
        let metadata = self.metadata();
        metadata
            .current_snapshot()?
            .ok_or(anyhow!("There is no snapshot for table."))?
            .manifests(metadata, self.object_store().clone())
            .await?
            .collect()
    }
    /// Get list of datafiles corresponding to the given manifest files
    pub async fn data_files(
        &self,
        manifests: &[ManifestListEntry],
        filter: Option<Vec<bool>>,
    ) -> Result<Vec<ManifestEntry>, anyhow::Error> {
        // filter manifest files according to filter vector
        let iter = match filter {
            Some(predicate) => {
                manifests
                    .iter()
                    .zip(Box::new(predicate.into_iter())
                        as Box<dyn Iterator<Item = bool> + Send + Sync>)
                    .filter_map(
                        filter_manifest
                            as fn((&ManifestListEntry, bool)) -> Option<&ManifestListEntry>,
                    )
            }
            None => manifests
                .iter()
                .zip(Box::new(repeat(true)) as Box<dyn Iterator<Item = bool> + Send + Sync>)
                .filter_map(
                    filter_manifest as fn((&ManifestListEntry, bool)) -> Option<&ManifestListEntry>,
                ),
        };
        // Collect a vector of data files by creating a stream over the manifst files, fetch their content and return a flatten stream over their entries.
        stream::iter(iter)
            .map(|file| async move {
                let object_store = Arc::clone(&self.object_store());
                let path: Path = util::strip_prefix(file.manifest_path()).into();
                let bytes = Cursor::new(Vec::from(
                    object_store
                        .get(&path)
                        .and_then(|file| file.bytes())
                        .await?,
                ));
                let reader = ManifestReader::new(bytes, self.metadata())?;
                Ok(stream::iter(reader))
            })
            .flat_map(|reader| reader.try_flatten_stream())
            .try_collect()
            .await
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

// Filter manifest files according to predicate. Returns Some(&ManifestFile) of the predicate is true and None if it is false.
fn filter_manifest(
    (manifest, predicate): (&ManifestListEntry, bool),
) -> Option<&ManifestListEntry> {
    if predicate {
        Some(manifest)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use anyhow::anyhow;
    use itertools::Itertools;
    use object_store::{local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore};
    use parquet::{
        arrow::async_reader::fetch_parquet_metadata, errors::ParquetError, format::FileMetaData,
        schema::types,
    };

    use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};

    use crate::{
        catalog::{identifier::Identifier, memory::MemoryCatalog, relation::Relation, Catalog},
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

    #[tokio::test]
    async fn test_files_stream() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix("../iceberg-tests/nyc_taxis_append").unwrap(),
        );

        let catalog: Arc<dyn Catalog> =
            Arc::new(MemoryCatalog::new("test", object_store.clone()).unwrap());
        let identifier = Identifier::parse("test.table1").unwrap();

        catalog.clone().register_table(identifier.clone(), "/home/iceberg/warehouse/nyc/taxis/metadata/fb072c92-a02b-11e9-ae9c-1bb7bc9eca94.metadata.json").await.expect("Failed to register table.");

        let mut table = if let Relation::Table(table) = catalog
            .load_table(&identifier)
            .await
            .expect("Failed to load table")
        {
            Ok(table)
        } else {
            Err(anyhow!("Relation must be a table"))
        }
        .unwrap();

        let parquet_files = vec!["/home/iceberg/warehouse/nyc/taxis/data/vendor_id=1/00000-0-03c9b632-a796-4f56-97b0-a638a6f6d6f4-00001.parquet",
            "/home/iceberg/warehouse/nyc/taxis/data/vendor_id=1/00003-3-ae86257a-5d0b-4c42-9782-f08ec510637e-00001.parquet",
            "/home/iceberg/warehouse/nyc/taxis/data/vendor_id=2/00001-1-2a1bfa65-21d8-4302-ad47-85c00b092e8b-00001.parquet",
            "/home/iceberg/warehouse/nyc/taxis/data/vendor_id=2/00002-2-22cded08-1e3c-4905-a73c-5e0ea8ed268f-00001.parquet"];

        let file_metadata = stream::iter(parquet_files.clone().into_iter())
            .then(|file| {
                let object_store = object_store.clone();
                async move {
                    let file = file.to_string();
                    let path: Path = file.as_str().into();
                    let file_metadata =
                        object_store.head(&path).await.map_err(anyhow::Error::msg)?;
                    let parquet_metadata = fetch_parquet_metadata(
                        |range| {
                            object_store
                                .get_range(&path, range)
                                .map_err(|err| ParquetError::General(err.to_string()))
                        },
                        file_metadata.size,
                        None,
                    )
                    .await
                    .map_err(anyhow::Error::msg)?;
                    let schema_elements =
                        types::to_thrift(parquet_metadata.file_metadata().schema())?;
                    let row_groups = parquet_metadata
                        .row_groups()
                        .iter()
                        .map(|x| x.to_thrift())
                        .collect::<Vec<_>>();
                    let metadata = FileMetaData::new(
                        parquet_metadata.file_metadata().version(),
                        schema_elements,
                        parquet_metadata.file_metadata().num_rows(),
                        row_groups,
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                    Ok::<_, anyhow::Error>((file, metadata))
                }
            })
            .try_collect()
            .await
            .expect("Failed to get file metadata.");

        table
            .new_transaction()
            .append(file_metadata)
            .commit()
            .await
            .unwrap();
        let manifests = table.manifests().await.unwrap();
        let mut files = table
            .data_files(&manifests, None)
            .await
            .unwrap()
            .into_iter()
            .map(|manifest_entry| manifest_entry.file_path().to_string());
        assert!(parquet_files
            .iter()
            .contains(&files.next().unwrap().as_str()));
        assert!(parquet_files
            .iter()
            .contains(&files.next().unwrap().as_str()));
        assert!(parquet_files
            .iter()
            .contains(&files.next().unwrap().as_str()));
        assert!(parquet_files
            .iter()
            .contains(&files.next().unwrap().as_str()));
        object_store
            .delete(&table.metadata_location().into())
            .await
            .unwrap();
    }
}
