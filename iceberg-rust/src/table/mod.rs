/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{io::Cursor, iter::repeat, sync::Arc};

use object_store::{path::Path, ObjectStore};

use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use iceberg_rust_spec::spec::{
    manifest::{Content, ManifestEntry, ManifestReader},
    manifest_list::ManifestListEntry,
    schema::Schema,
    table_metadata::TableMetadata,
};
use iceberg_rust_spec::util::{self};

use crate::{
    catalog::{bucket::Bucket, create::CreateTableBuilder, identifier::Identifier, Catalog},
    error::Error,
    table::transaction::TableTransaction,
};

pub mod transaction;

#[derive(Debug)]
/// Iceberg table
pub struct Table {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: TableMetadata,
}

/// Public interface of the table.
impl Table {
    /// Build a new table
    pub fn builder() -> CreateTableBuilder {
        CreateTableBuilder::default()
    }

    /// Create a new metastore Table
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadata,
    ) -> Result<Self, Error> {
        Ok(Table {
            identifier,
            catalog,
            metadata,
        })
    }
    #[inline]
    /// Get the table identifier in the catalog. Returns None of it is a filesystem table.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    #[inline]
    /// Get the catalog associated to the table. Returns None if the table is a filesystem table
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    #[inline]
    /// Get the object_store associated to the table
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog
            .object_store(Bucket::from_path(&self.metadata.location).unwrap())
    }
    #[inline]
    /// Get the schema of the table for a given branch. Defaults to main.
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    #[inline]
    /// Get the metadata of the table
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
    #[inline]
    /// Get the metadata of the table
    pub fn into_metadata(self) -> TableMetadata {
        self.metadata
    }
    /// Get list of current manifest files within an optional snapshot range. The start snapshot is excluded from the range.
    pub async fn manifests(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<Vec<ManifestListEntry>, Error> {
        let metadata = self.metadata();
        let end_snapshot = match end.and_then(|id| metadata.snapshots.get(&id)) {
            Some(snapshot) => snapshot,
            None => {
                if let Some(current) = metadata.current_snapshot(None)? {
                    current
                } else {
                    return Ok(vec![]);
                }
            }
        };
        let start_sequence_number =
            start
                .and_then(|id| metadata.snapshots.get(&id))
                .and_then(|snapshot| {
                    let sequence_number = *snapshot.sequence_number();
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
                .collect::<Result<_, iceberg_rust_spec::error::Error>>()
                .map_err(Error::from),
            None => iter
                .collect::<Result<_, iceberg_rust_spec::error::Error>>()
                .map_err(Error::from),
        }
    }
    /// Get list of datafiles corresponding to the given manifest files
    #[inline]
    pub async fn datafiles(
        &self,
        manifests: &[ManifestListEntry],
        filter: Option<Vec<bool>>,
    ) -> Result<Vec<ManifestEntry>, Error> {
        datafiles(self.object_store(), manifests, filter).await
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
            .any(|entry| !matches!(entry.data_file().content(), Content::Data)))
    }
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self, branch: Option<&str>) -> TableTransaction {
        TableTransaction::new(self, branch)
    }
}

async fn datafiles(
    object_store: Arc<dyn ObjectStore>,
    manifests: &[ManifestListEntry],
    filter: Option<Vec<bool>>,
) -> Result<Vec<ManifestEntry>, Error> {
    // filter manifest files according to filter vector
    let iter = match filter {
        Some(predicate) => manifests
            .iter()
            .zip(Box::new(predicate.into_iter()) as Box<dyn Iterator<Item = bool> + Send + Sync>)
            .filter_map(
                filter_manifest as fn((&ManifestListEntry, bool)) -> Option<&ManifestListEntry>,
            ),
        None => manifests
            .iter()
            .zip(Box::new(repeat(true)) as Box<dyn Iterator<Item = bool> + Send + Sync>)
            .filter_map(
                filter_manifest as fn((&ManifestListEntry, bool)) -> Option<&ManifestListEntry>,
            ),
    };
    // Collect a vector of data files by creating a stream over the manifst files, fetch their content and return a flatten stream over their entries.
    stream::iter(iter)
        .map(|file| {
            let object_store = object_store.clone();
            async move {
                let path: Path = util::strip_prefix(&file.manifest_path).into();
                let bytes = Cursor::new(Vec::from(
                    object_store
                        .get(&path)
                        .and_then(|file| file.bytes())
                        .await?,
                ));
                let reader = ManifestReader::new(bytes)?;
                Ok(stream::iter(reader))
            }
        })
        .flat_map(|reader| reader.try_flatten_stream())
        .try_collect()
        .await
        .map_err(Error::from)
}

/// delete all datafiles, manifests and metadata files, does not remove table from catalog
pub(crate) async fn delete_files(
    metadata: &TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Error> {
    let Some(snapshot) = metadata.current_snapshot(None)? else {
        return Ok(());
    };
    let manifests = snapshot
        .manifests(metadata, object_store.clone())
        .await?
        .collect::<Result<Vec<_>, iceberg_rust_spec::error::Error>>()?;
    let datafiles = datafiles(object_store.clone(), &manifests, None).await?;
    let snapshots = &metadata.snapshots;

    stream::iter(datafiles.into_iter())
        .map(Ok::<_, Error>)
        .try_for_each_concurrent(None, |datafile| {
            let object_store = object_store.clone();
            async move {
                object_store
                    .delete(&datafile.data_file().file_path().as_str().into())
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
                    .delete(&snapshot.manifest_list().as_str().into())
                    .await?;
                Ok(())
            }
        })
        .await?;

    Ok(())
}

#[inline]
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
