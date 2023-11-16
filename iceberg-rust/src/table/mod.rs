/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{collections::HashMap, io::Cursor, iter::repeat, sync::Arc, time::SystemTime};

use object_store::{path::Path, ObjectStore};

use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use uuid::Uuid;

use crate::{
    catalog::{identifier::Identifier, Catalog},
    error::Error,
    spec::{
        manifest::{Content, ManifestEntry, ManifestReader},
        manifest_list::ManifestListEntry,
        schema::Schema,
        snapshot::{Operation, Reference, Retention, Snapshot, Summary},
        table_metadata::{TableMetadata, MAIN_BRANCH},
    },
    table::transaction::TableTransaction,
    util::{self, strip_prefix},
};

pub mod table_builder;
pub mod transaction;

#[derive(Debug)]
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
    ) -> Result<Self, Error> {
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
    /// Get the schema of the table for a given branch. Defaults to main.
    pub fn schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch)
    }
    /// Get the metadata of the table
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Get list of current manifest files within an optional snapshot range. The start snapshot is excluded from the range.
    pub async fn manifests(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<Vec<ManifestListEntry>, Error> {
        let metadata = self.metadata();
        let current_snapshot = if let Some(current) = metadata.current_snapshot(None)? {
            current
        } else {
            return Ok(vec![]);
        };
        let end_snapshot = end
            .and_then(|id| metadata.snapshot(id))
            .unwrap_or(current_snapshot);
        let start_sequence_number =
            start
                .and_then(|id| metadata.snapshot(id))
                .and_then(|snapshot| {
                    let sequence_number = snapshot.sequence_number;
                    if sequence_number == 0 {
                        None
                    } else {
                        Some(sequence_number)
                    }
                });
        let iter = end_snapshot
            .manifests(metadata, self.object_store().clone())
            .await?;
        match start_sequence_number {
            Some(start) => iter
                .filter(|manifest| {
                    if let Ok(manifest) = manifest {
                        manifest.sequence_number > start
                    } else {
                        true
                    }
                })
                .collect(),
            None => iter.collect(),
        }
    }
    /// Get list of datafiles corresponding to the given manifest files
    pub async fn datafiles(
        &self,
        manifests: &[ManifestListEntry],
        filter: Option<Vec<bool>>,
    ) -> Result<Vec<ManifestEntry>, Error> {
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
                let path: Path = util::strip_prefix(&file.manifest_path).into();
                let bytes = Cursor::new(Vec::from(
                    object_store
                        .get(&path)
                        .and_then(|file| file.bytes())
                        .await?,
                ));
                let reader = ManifestReader::new(bytes)?;
                Ok(stream::iter(reader))
            })
            .flat_map(|reader| reader.try_flatten_stream())
            .try_collect()
            .await
    }
    /// Check if datafiles contain deletes
    pub async fn datafiles_contains_delete(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<bool, Error> {
        let manifests = self.manifests(start, end).await?;
        let datafiles = self.datafiles(&manifests, None).await?;
        Ok(datafiles
            .iter()
            .any(|entry| !matches!(entry.data_file.content, Content::Data)))
    }
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self, branch: Option<&str>) -> TableTransaction {
        TableTransaction::new(self, branch)
    }

    /// delete all datafiles, manifests and metadata files, does not remove table from catalog
    pub async fn drop(self) -> Result<(), Error> {
        let object_store = self.object_store();
        let manifests = self.manifests(None, None).await?;
        let datafiles = self.datafiles(&manifests, None).await?;
        let snapshots = &self.metadata().snapshots;

        stream::iter(datafiles.into_iter())
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(None, |datafile| {
                let object_store = object_store.clone();
                async move {
                    object_store
                        .delete(&datafile.data_file.file_path.into())
                        .await?;
                    Ok(())
                }
            })
            .await?;

        stream::iter(manifests.into_iter())
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(None, |manifest| {
                let object_store = object_store.clone();
                async move {
                    object_store.delete(&manifest.manifest_path.into()).await?;
                    Ok(())
                }
            })
            .await?;

        stream::iter(snapshots.values())
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(None, |snapshot| {
                let object_store = object_store.clone();
                async move {
                    object_store
                        .delete(&snapshot.manifest_list.as_str().into())
                        .await?;
                    Ok(())
                }
            })
            .await?;

        object_store
            .delete(&self.metadata_location().into())
            .await?;

        Ok(())
    }
}

impl Table {
    pub(crate) fn new_metadata_location(&self) -> Result<String, Error> {
        let transaction_uuid = Uuid::new_v4();
        let version = self.metadata().last_sequence_number;
        Ok(self.metadata().location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &transaction_uuid.to_string()
            + ".metadata.json")
    }
}

/// Private interface of the table.
impl Table {
    /// Increment the sequence number of the table. Is typically used when commiting a new table transaction.
    pub(crate) fn increment_sequence_number(&mut self) {
        self.metadata.last_sequence_number += 1;
    }

    /// Create a new table snapshot based on the manifest_list file of the previous snapshot.
    pub(crate) async fn new_snapshot(
        &mut self,
        branch: Option<String>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let mut bytes: [u8; 8] = [0u8; 8];
        getrandom::getrandom(&mut bytes).unwrap();
        let snapshot_id = i64::from_le_bytes(bytes);
        let object_store = self.object_store();
        let metadata = &mut self.metadata;
        let old_manifest_list_location = metadata
            .current_snapshot(branch.as_deref())?
            .map(|x| &x.manifest_list)
            .cloned();
        let new_manifest_list_location = metadata.location.to_string()
            + "/metadata/snap-"
            + &snapshot_id.to_string()
            + &uuid::Uuid::new_v4().to_string()
            + ".avro";
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
            schema_id: Some(metadata.current_schema_id),
        };

        let branch_name = branch.unwrap_or("main".to_string());

        metadata.snapshots.insert(snapshot_id, snapshot);
        if &branch_name == MAIN_BRANCH {
            metadata.current_snapshot_id = Some(snapshot_id);
        }
        metadata
            .refs
            .entry(branch_name)
            .and_modify(|x| x.snapshot_id = snapshot_id)
            .or_insert(Reference {
                snapshot_id,
                retention: Retention::default(),
            });
        match old_manifest_list_location {
            Some(old_manifest_list_location) => Ok(Some(
                object_store
                    .get(&strip_prefix(&old_manifest_list_location).as_str().into())
                    .await?
                    .bytes()
                    .await?
                    .into(),
            )),
            None => Ok(None),
        }
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

    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        catalog::{identifier::Identifier, memory::MemoryCatalog, Catalog},
        spec::{
            schema::Schema,
            types::{PrimitiveType, StructField, StructType, Type},
        },
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_increment_sequence_number() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemoryCatalog::new("test", object_store).unwrap());
        let identifier = Identifier::parse("test.table1").unwrap();
        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            fields: StructType::new(vec![
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
            ]),
        };
        let mut builder = TableBuilder::new(&identifier, catalog.clone())
            .expect("Failed to create table builder.");
        builder
            .location("/")
            .with_schema((1, schema))
            .current_schema_id(1);
        let mut table = builder.build().await.expect("Failed to create table.");

        let metadata_location1 = table.metadata_location().to_string();

        let transaction = table.new_transaction(None);
        transaction.commit().await.unwrap();
        let metadata_location2 = table.metadata_location();
        assert_ne!(metadata_location1, metadata_location2);
    }
}
