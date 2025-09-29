//! Table module provides the core functionality for working with Iceberg tables
//!
//! The main type in this module is [`Table`], which represents an Iceberg table and provides
//! methods for:
//! * Reading table data and metadata
//! * Modifying table structure (schema, partitioning, etc.)
//! * Managing table snapshots and branches
//! * Performing atomic transactions
//!
//! Tables can be created using [`Table::builder()`] and modified using transactions
//! created by [`Table::new_transaction()`].

use std::{io::Cursor, sync::Arc};

use futures::future::try_join_all;
use itertools::Itertools;
use manifest::ManifestReader;
use manifest_list::read_snapshot;
use object_store::{path::Path, ObjectStore};

use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use iceberg_rust_spec::util::{self};
use iceberg_rust_spec::{
    spec::{
        manifest::{Content, ManifestEntry},
        manifest_list::ManifestListEntry,
        schema::Schema,
        table_metadata::TableMetadata,
    },
    table_metadata::{
        WRITE_OBJECT_STORAGE_ENABLED, WRITE_PARQUET_COMPRESSION_CODEC,
        WRITE_PARQUET_COMPRESSION_LEVEL,
    },
};

use tracing::{instrument, Instrument};

use crate::{
    catalog::{create::CreateTableBuilder, identifier::Identifier, Catalog},
    error::Error,
    table::transaction::TableTransaction,
};

pub mod manifest;
pub mod manifest_list;
pub mod transaction;

/// Builder for configuring and executing snapshot expiration operations
///
/// This builder provides a fluent API for configuring how snapshots should be expired:
/// * [`expire_older_than`](ExpireSnapshotsBuilder::expire_older_than) - Remove snapshots older than a timestamp
/// * [`retain_last`](ExpireSnapshotsBuilder::retain_last) - Keep only the most recent N snapshots
/// * [`clean_orphan_files`](ExpireSnapshotsBuilder::clean_orphan_files) - Also remove unreferenced data files
/// * [`dry_run`](ExpireSnapshotsBuilder::dry_run) - Preview what would be deleted without actually deleting
pub struct ExpireSnapshotsBuilder<'a> {
    table: &'a mut Table,
    older_than: Option<i64>,
    retain_last: Option<usize>,
    clean_orphan_files: bool,
    retain_ref_snapshots: bool,
    dry_run: bool,
}

impl<'a> ExpireSnapshotsBuilder<'a> {
    /// Create a new snapshot expiration builder for the given table
    fn new(table: &'a mut Table) -> Self {
        Self {
            table,
            older_than: None,
            retain_last: None,
            clean_orphan_files: false,
            retain_ref_snapshots: true,
            dry_run: false,
        }
    }

    /// Expire snapshots older than the given timestamp (in milliseconds since Unix epoch)
    pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self {
        self.older_than = Some(timestamp_ms);
        self
    }

    /// Retain only the most recent N snapshots, expiring all others
    pub fn retain_last(mut self, count: usize) -> Self {
        self.retain_last = Some(count);
        self
    }

    /// Enable or disable cleanup of orphaned data files
    pub fn clean_orphan_files(mut self, enabled: bool) -> Self {
        self.clean_orphan_files = enabled;
        self
    }

    /// Control whether snapshots referenced by branches/tags should be preserved
    pub fn retain_ref_snapshots(mut self, enabled: bool) -> Self {
        self.retain_ref_snapshots = enabled;
        self
    }

    /// Enable dry run mode to preview what would be deleted without actually deleting
    pub fn dry_run(mut self, enabled: bool) -> Self {
        self.dry_run = enabled;
        self
    }

    /// Execute the snapshot expiration operation
    pub async fn execute(self) -> Result<Vec<i64>, Error> {
        let _result = self.table.new_transaction(None)
            .expire_snapshots(
                self.older_than,
                self.retain_last,
                self.clean_orphan_files,
                self.retain_ref_snapshots,
                self.dry_run,
            )
            .commit()
            .await?;

        // Extract the expired snapshot IDs from the commit result
        // For now, we'll need to return empty vec since the transaction commit
        // doesn't directly return the expired snapshot IDs
        // TODO: Enhance transaction result to include operation-specific details
        Ok(vec![])
    }
}

#[derive(Debug, Clone)]
/// Iceberg table
pub struct Table {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<dyn ObjectStore>,
    metadata: TableMetadata,
}

/// Public interface of the table.
impl Table {
    /// Creates a new table builder with default configuration
    ///
    /// Returns a `CreateTableBuilder` initialized with default properties:
    /// * WRITE_PARQUET_COMPRESSION_CODEC: "zstd"
    /// * WRITE_PARQUET_COMPRESSION_LEVEL: "3"
    /// * WRITE_OBJECT_STORAGE_ENABLED: "false"
    ///
    /// # Returns
    /// * `CreateTableBuilder` - A builder for configuring and creating a new table
    ///
    /// # Example
    /// ```
    /// use iceberg_rust::table::Table;
    ///
    /// let builder = Table::builder()
    ///     .with_name("my_table")
    ///     .with_schema(schema);
    /// ```
    pub fn builder() -> CreateTableBuilder {
        let mut builder = CreateTableBuilder::default();
        builder
            .with_property((
                WRITE_PARQUET_COMPRESSION_CODEC.to_owned(),
                "zstd".to_owned(),
            ))
            .with_property((WRITE_PARQUET_COMPRESSION_LEVEL.to_owned(), 3.to_string()))
            .with_property((WRITE_OBJECT_STORAGE_ENABLED.to_owned(), "false".to_owned()));
        builder
    }

    /// Creates a new table instance with the given identifier, catalog and metadata
    ///
    /// # Arguments
    /// * `identifier` - The unique identifier for this table in the catalog
    /// * `catalog` - The catalog that this table belongs to
    /// * `metadata` - The table's metadata containing schema, partitioning, etc.
    ///
    /// # Returns
    /// * `Result<Table, Error>` - The newly created table instance or an error
    ///
    /// This is typically called by catalog implementations rather than directly by users.
    /// For creating new tables, use [`Table::builder()`] instead.
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<dyn ObjectStore>,
        metadata: TableMetadata,
    ) -> Result<Self, Error> {
        Ok(Table {
            identifier,
            catalog,
            object_store,
            metadata,
        })
    }
    #[inline]
    /// Returns the unique identifier for this table in the catalog
    ///
    /// The identifier contains both the namespace and name that uniquely identify
    /// this table within its catalog.
    ///
    /// # Returns
    /// * `&Identifier` - A reference to this table's identifier
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    #[inline]
    /// Returns a reference to the catalog containing this table
    ///
    /// The returned catalog reference is wrapped in an Arc to allow shared ownership
    /// and thread-safe access to the catalog implementation.
    ///
    /// # Returns
    /// * `Arc<dyn Catalog>` - A thread-safe reference to the table's catalog
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    #[inline]
    /// Returns the object store for this table's location
    ///
    /// The object store is determined by the table's location and is used for
    /// reading and writing table data files. The returned store is wrapped in
    /// an Arc to allow shared ownership and thread-safe access.
    ///
    /// # Returns
    /// * `Arc<dyn ObjectStore>` - A thread-safe reference to the table's object store
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }
    #[inline]
    /// Returns the current schema for this table, optionally for a specific branch
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to get the schema for. If None, returns the main branch schema
    ///
    /// # Returns
    /// * `Result<&Schema, Error>` - The current schema if found, or an error if the schema cannot be found
    ///
    /// # Errors
    /// Returns an error if the schema ID cannot be found in the table metadata
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    #[inline]
    /// Returns a reference to this table's metadata
    ///
    /// The metadata contains all table information including:
    /// * Schema definitions
    /// * Partition specifications
    /// * Snapshots
    /// * Sort orders
    /// * Table properties
    ///
    /// # Returns
    /// * `&TableMetadata` - A reference to the table's metadata
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
    #[inline]
    /// Consumes the table and returns its metadata
    ///
    /// This method takes ownership of the table instance and returns just the
    /// underlying TableMetadata. This is useful when you no longer need the
    /// table instance but want to retain its metadata.
    ///
    /// # Returns
    /// * `TableMetadata` - The owned metadata from this table
    pub fn into_metadata(self) -> TableMetadata {
        self.metadata
    }
    /// Returns manifest list entries for snapshots within the given sequence range
    ///
    /// # Arguments
    /// * `start` - Optional starting snapshot ID (exclusive). If None, includes from the beginning
    /// * `end` - Optional ending snapshot ID (inclusive). If None, uses the current snapshot
    ///
    /// # Returns
    /// * `Result<Vec<ManifestListEntry>, Error>` - Vector of manifest entries in the range,
    ///   or an empty vector if no current snapshot exists
    ///
    /// # Errors
    /// Returns an error if:
    /// * The end snapshot ID is invalid
    /// * Reading the manifest list fails
    #[instrument(name = "iceberg_rust::table::manifests", level = "debug", skip(self), fields(
        table_identifier = %self.identifier,
        start = ?start,
        end = ?end
    ))]
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
        let iter = read_snapshot(end_snapshot, metadata, self.object_store().clone()).await?;
        match start_sequence_number {
            Some(start) => iter
                .filter_ok(|manifest| manifest.sequence_number > start)
                .collect(),
            None => iter.collect(),
        }
    }
    /// Returns a stream of manifest entries for the given manifest list entries
    ///
    /// # Arguments
    /// * `manifests` - List of manifest entries to read data files from
    /// * `filter` - Optional vector of boolean predicates to filter manifest entries
    /// * `sequence_number_range` - Tuple of (start, end) sequence numbers to filter entries by
    ///
    /// # Returns
    /// * `Result<impl Stream<Item = Result<ManifestEntry, Error>>, Error>` - Stream of manifest entries
    ///   that match the given filters
    ///
    /// # Type Parameters
    /// * `'a` - Lifetime of the manifest list entries reference
    ///
    /// # Errors
    /// Returns an error if reading any manifest file fails
    #[inline]
    pub async fn datafiles<'a>(
        &self,
        manifests: &'a [ManifestListEntry],
        filter: Option<Vec<bool>>,
        sequence_number_range: (Option<i64>, Option<i64>),
    ) -> Result<impl Iterator<Item = Result<(ManifestPath, ManifestEntry), Error>> + 'a, Error>
    {
        datafiles(
            self.object_store(),
            manifests,
            filter,
            sequence_number_range,
        )
        .await
    }
    /// Check if datafiles contain deletes
    pub async fn datafiles_contains_delete(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<bool, Error> {
        let manifests = self.manifests(start, end).await?;
        let datafiles = self.datafiles(&manifests, None, (None, None)).await?;
        stream::iter(datafiles)
            .try_any(|entry| async move { !matches!(entry.1.data_file().content(), Content::Data) })
            .await
    }
    /// Creates a new transaction for atomic modifications to this table
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to create the transaction for. If None, uses the main branch
    ///
    /// # Returns
    /// * `TableTransaction` - A new transaction that can be used to atomically modify this table
    ///
    /// The transaction must be committed for any changes to take effect.
    /// Multiple operations can be chained within a single transaction.
    pub fn new_transaction(&mut self, branch: Option<&str>) -> TableTransaction<'_> {
        TableTransaction::new(self, branch)
    }

    /// Configures snapshot expiration for this table
    ///
    /// Returns a builder that allows configuring snapshot expiration policies:
    /// * Time-based expiration: Remove snapshots older than a timestamp
    /// * Count-based retention: Keep only the most recent N snapshots  
    /// * Orphan file cleanup: Remove data files no longer referenced by any snapshot
    /// * Reference preservation: Protect snapshots referenced by branches/tags
    /// * Dry run mode: Preview what would be deleted without actually deleting
    ///
    /// The operation is executed through the table's transaction system, ensuring
    /// atomicity and consistency with other table operations.
    ///
    /// # Returns
    /// * `ExpireSnapshotsBuilder` - A builder for configuring expiration parameters
    ///
    /// # Examples
    /// ```rust,no_run
    /// # async fn example(table: &mut Table) -> Result<(), Box<dyn std::error::Error>> {
    /// // Expire snapshots older than 7 days but keep at least 5 snapshots
    /// let result = table.expire_snapshots()
    ///     .expire_older_than(chrono::Utc::now().timestamp_millis() - 7 * 24 * 60 * 60 * 1000)
    ///     .retain_last(5)
    ///     .clean_orphan_files(true)
    ///     .execute()
    ///     .await?;
    /// 
    /// println!("Expired {} snapshots", result.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn expire_snapshots(&mut self) -> ExpireSnapshotsBuilder<'_> {
        ExpireSnapshotsBuilder::new(self)
    }
}

/// Path of a Manifest file
pub type ManifestPath = String;

#[instrument(name = "iceberg_rust::table::datafiles", level = "debug", skip(object_store, manifests), fields(
    manifest_count = manifests.len(),
    filter_provided = filter.is_some(),
    sequence_range = ?sequence_number_range
))]
async fn datafiles(
    object_store: Arc<dyn ObjectStore>,
    manifests: &'_ [ManifestListEntry],
    filter: Option<Vec<bool>>,
    sequence_number_range: (Option<i64>, Option<i64>),
) -> Result<impl Iterator<Item = Result<(ManifestPath, ManifestEntry), Error>> + '_, Error> {
    // filter manifest files according to filter vector
    let iter: Box<dyn Iterator<Item = &ManifestListEntry> + Send + Sync> = match filter {
        Some(predicate) => {
            let iter = manifests
                .iter()
                .zip(predicate.into_iter())
                .filter(|(_, predicate)| *predicate)
                .map(|(manifest, _)| manifest);
            Box::new(iter)
        }
        None => Box::new(manifests.iter()),
    };

    let futures: Vec<_> = iter
        .map(move |file| {
            let object_store = object_store.clone();
            async move {
                let manifest_path = &file.manifest_path;
                let path: Path = util::strip_prefix(manifest_path).into();
                let bytes = Cursor::new(Vec::from(
                    object_store
                        .get(&path)
                        .and_then(|file| file.bytes())
                        .instrument(tracing::trace_span!("iceberg_rust::get_manifest"))
                        .await?,
                ));
                Ok::<_, Error>((bytes, manifest_path, file.sequence_number))
            }
        })
        .collect();

    let results = try_join_all(futures).await?;

    Ok(results.into_iter().flat_map(move |result| {
        let (bytes, path, sequence_number) = result;

        let reader = ManifestReader::new(bytes).unwrap();
        reader.filter_map(move |x| {
            let mut x = match x {
                Ok(entry) => entry,
                Err(_) => return None,
            };

            let sequence_number = if let Some(sequence_number) = x.sequence_number() {
                *sequence_number
            } else {
                *x.sequence_number_mut() = Some(sequence_number);
                sequence_number
            };

            let filter = match sequence_number_range {
                (Some(start), Some(end)) => start < sequence_number && sequence_number <= end,
                (Some(start), None) => start < sequence_number,
                (None, Some(end)) => sequence_number <= end,
                _ => true,
            };
            if filter {
                Some(Ok((path.to_owned(), x)))
            } else {
                None
            }
        })
    }))
}

/// delete all datafiles, manifests and metadata files, does not remove table from catalog
pub(crate) async fn delete_all_table_files(
    metadata: &TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Error> {
    let Some(snapshot) = metadata.current_snapshot(None)? else {
        return Ok(());
    };
    let manifests: Vec<ManifestListEntry> = read_snapshot(snapshot, metadata, object_store.clone())
        .await?
        .collect::<Result<_, _>>()?;

    let datafiles = datafiles(object_store.clone(), &manifests, None, (None, None)).await?;
    let snapshots = &metadata.snapshots;

    // stream::iter(datafiles.into_iter())
    stream::iter(datafiles)
        .try_for_each_concurrent(None, |datafile| {
            let object_store = object_store.clone();
            async move {
                object_store
                    .delete(&datafile.1.data_file().file_path().as_str().into())
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
