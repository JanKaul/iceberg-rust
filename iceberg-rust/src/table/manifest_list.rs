/*!
 * Helpers to deal with manifest lists and files
*/

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io::{Cursor, Read},
    iter::{repeat, Map, Repeat, Zip},
    sync::Arc,
};

use apache_avro::{
    types::Value as AvroValue, Reader as AvroReader, Schema as AvroSchema, Writer as AvroWriter,
};
use futures::{future::join_all, stream, TryFutureExt, TryStreamExt};
use iceberg_rust_spec::{
    manifest::{partition_value_schema, DataFile, ManifestEntry, Status},
    manifest_list::{
        avro_value_to_manifest_list_entry, manifest_list_schema_v1, manifest_list_schema_v2,
        Content, ManifestListEntry,
    },
    snapshot::Snapshot,
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
};
use object_store::ObjectStore;
use smallvec::SmallVec;

use crate::{
    error::Error,
    table::datafiles,
    util::{summary_to_rectangle, Rectangle, Vec4},
};

use super::{
    manifest::{ManifestReader, ManifestWriter},
    transaction::{
        append::{
            select_manifest_partitioned, select_manifest_unpartitioned, split_datafiles,
            SelectedManifest,
        },
        operation::{
            bounding_partition_values, compute_n_splits, new_manifest_list_location,
            new_manifest_location, prefetch_manifest,
        },
        overwrite::{
            select_manifest_without_overwrites_partitioned,
            select_manifest_without_overwrites_unpartitioned, OverwriteManifest,
        },
    },
};

type ReaderZip<'a, 'metadata, R> = Zip<AvroReader<'a, R>, Repeat<&'metadata TableMetadata>>;
type ReaderMap<'a, 'metadata, R> = Map<
    ReaderZip<'a, 'metadata, R>,
    fn((Result<AvroValue, apache_avro::Error>, &TableMetadata)) -> Result<ManifestListEntry, Error>,
>;

/// A reader for Iceberg manifest list files that provides an iterator over manifest list entries.
///
/// ManifestListReader parses manifest list files according to the table's format version (V1/V2)
/// and provides access to the manifest entries that describe the table's data files.
///
/// # Type Parameters
/// * `'a` - The lifetime of the underlying Avro reader
/// * `'metadata` - The lifetime of the table metadata reference
/// * `R` - The type implementing `Read` that provides the manifest list data
pub(crate) struct ManifestListReader<'a, 'metadata, R: Read> {
    reader: ReaderMap<'a, 'metadata, R>,
}

impl<R: Read> Iterator for ManifestListReader<'_, '_, R> {
    type Item = Result<ManifestListEntry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl<'metadata, R: Read> ManifestListReader<'_, 'metadata, R> {
    /// Creates a new ManifestListReader from a reader and table metadata.
    ///
    /// This method initializes a reader that can parse manifest list files according to
    /// the table's format version (V1/V2). It uses the appropriate Avro schema based on
    /// the format version from the table metadata.
    ///
    /// # Arguments
    /// * `reader` - A type implementing the `Read` trait that provides the manifest list data
    /// * `table_metadata` - Reference to the table metadata containing format version info
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestListReader instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The Avro reader cannot be created with the schema
    /// * The manifest list format is invalid
    pub(crate) fn new(reader: R, table_metadata: &'metadata TableMetadata) -> Result<Self, Error> {
        let schema: &AvroSchema = match table_metadata.format_version {
            FormatVersion::V1 => manifest_list_schema_v1(),
            FormatVersion::V2 => manifest_list_schema_v2(),
        };
        Ok(Self {
            reader: AvroReader::with_schema(schema, reader)?
                .zip(repeat(table_metadata))
                .map(|(avro_value_res, meta)| {
                    avro_value_to_manifest_list_entry(avro_value_res, meta).map_err(Error::from)
                }),
        })
    }
}

/// Reads a snapshot's manifest list file and returns an iterator over its manifest list entries.
///
/// This function:
/// 1. Fetches the manifest list file from object storage
/// 2. Creates a reader for the appropriate format version
/// 3. Returns an iterator that will yield each manifest list entry
///
/// # Arguments
/// * `snapshot` - The snapshot containing the manifest list location
/// * `table_metadata` - Reference to the table metadata for format version info
/// * `object_store` - The object store to read the manifest list file from
///
/// # Returns
/// * `Result<impl Iterator<...>, Error>` - An iterator over manifest list entries or an error
///
/// # Errors
/// Returns an error if:
/// * The manifest list file cannot be read from storage
/// * The manifest list format is invalid
/// * The Avro reader cannot be created
pub(crate) async fn read_snapshot<'metadata>(
    snapshot: &Snapshot,
    table_metadata: &'metadata TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<impl Iterator<Item = Result<ManifestListEntry, Error>> + 'metadata, Error> {
    let bytes: Cursor<Vec<u8>> = Cursor::new(
        object_store
            .get(&strip_prefix(snapshot.manifest_list()).into())
            .await?
            .bytes()
            .await?
            .into(),
    );
    ManifestListReader::new(bytes, table_metadata)
}

/// Computes the overall partition bounds for all data files in a snapshot.
///
/// This function reads the manifest list for a given snapshot and computes the
/// bounding rectangle that encompasses all partition values across all manifests
/// in the snapshot. It's useful for understanding the overall data distribution
/// and for query optimization by determining which partitions contain data.
///
/// The function:
/// 1. Fetches the manifest list file from object storage
/// 2. Iterates through all manifest entries in the manifest list
/// 3. For each manifest that has partition information, converts the partition
///    summary to a rectangle and expands the overall bounds
/// 4. Returns the combined bounding rectangle or None if no partitions are found
///
/// # Arguments
/// * `snapshot` - The snapshot containing the manifest list location
/// * `table_metadata` - Reference to the table metadata for format version info
/// * `object_store` - The object store to read the manifest list file from
///
/// # Returns
/// * `Result<Option<Rectangle>, Error>` - The bounding rectangle encompassing all
///   partition values, or None if no partitions are found, or an error if the
///   operation fails
///
/// # Errors
/// Returns an error if:
/// * The manifest list file cannot be read from storage
/// * The manifest list format is invalid
/// * The Avro reader cannot be created
/// * Partition summary conversion fails
///
/// # Example Usage
/// ```ignore
/// let bounds = snapshot_partition_bounds(&snapshot, &table_metadata, object_store).await?;
/// if let Some(rectangle) = bounds {
///     println!("Partition bounds: {:?}", rectangle);
/// } else {
///     println!("No partition bounds found");
/// }
/// ```
pub async fn snapshot_partition_bounds(
    snapshot: &Snapshot,
    table_metadata: &TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Option<Rectangle>, Error> {
    let bytes: Cursor<Vec<u8>> = Cursor::new(
        object_store
            .get(&strip_prefix(snapshot.manifest_list()).into())
            .await?
            .bytes()
            .await?
            .into(),
    );

    ManifestListReader::new(bytes, table_metadata)?.try_fold(None::<Rectangle>, |acc, x| {
        if let Some(partitions) = x?.partitions {
            let rect = summary_to_rectangle(&partitions)?;
            if let Some(mut acc) = acc {
                acc.expand(&rect);
                Ok(Some(acc))
            } else {
                Ok(Some(rect))
            }
        } else {
            Ok(acc)
        }
    })
}

/// Computes the column bounds (minimum and maximum values) for all primitive fields
/// across all data files in a snapshot.
///
/// This function reads all manifests in the snapshot, extracts data files from them,
/// and computes a bounding rectangle that encompasses the lower and upper bounds
/// of all primitive columns across all data files.
///
/// # Arguments
///
/// * `snapshot` - The snapshot to compute column bounds for
/// * `table_metadata` - Metadata of the table containing schema information
/// * `object_store` - Object store implementation for reading manifest files
///
/// # Returns
///
/// Returns `Ok(Some(Rectangle))` containing the computed bounds, or `Ok(None)` if
/// no data files are found. Returns an error if:
/// - Schema cannot be resolved for the snapshot
/// - Manifest files cannot be read
/// - Column bounds are missing for any primitive field in any data file
///
/// # Errors
///
/// * `Error::NotFound` - When column bounds are missing for a primitive field
/// * Other I/O errors from reading manifest or data files
pub async fn snapshot_column_bounds(
    snapshot: &Snapshot,
    table_metadata: &TableMetadata,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Option<Rectangle>, Error> {
    let schema = table_metadata
        .schema(*snapshot.snapshot_id())
        .or(table_metadata.current_schema(None))?;
    let manifests = read_snapshot(snapshot, table_metadata, object_store.clone())
        .await?
        .collect::<Result<Vec<_>, _>>()?;

    // Use the schema as fallback for manifests with empty schemas
    let datafiles = datafiles(
        object_store,
        &manifests,
        None,
        (None, None),
        Some(schema.clone()),
    )
    .await?;

    let primitive_field_ids = schema.primitive_field_ids().collect::<Vec<_>>();
    let n = primitive_field_ids.len();
    stream::iter(datafiles)
        .try_fold(None::<Rectangle>, |acc, (_, manifest)| {
            let primitive_field_ids = &primitive_field_ids;
            async move {
                let mut mins = Vec4::with_capacity(n);
                let mut maxs = Vec4::with_capacity(n);
                for (i, id) in primitive_field_ids.iter().enumerate() {
                    let min = manifest
                        .data_file()
                        .lower_bounds()
                        .as_ref()
                        .and_then(|x| x.get(id));
                    let max = manifest
                        .data_file()
                        .upper_bounds()
                        .as_ref()
                        .and_then(|x| x.get(id));
                    let (Some(min), Some(max)) = (min, max) else {
                        return Err(Error::NotFound("column bounds".to_string()));
                    };
                    mins[i] = min.clone();
                    maxs[i] = max.clone();
                }
                let rect = Rectangle::new(mins, maxs);
                if let Some(mut acc) = acc {
                    acc.expand(&rect);
                    Ok(Some(acc))
                } else {
                    Ok(Some(rect))
                }
            }
        })
        .await
}

/// A writer for Iceberg manifest list files that manages the creation and updating of manifest lists.
///
/// The ManifestListWriter is responsible for:
/// - Creating new manifest list files from scratch or updating existing ones
/// - Managing manifest entries and their metadata
/// - Optimizing data file organization through splitting and partitioning
/// - Writing the final manifest list to object storage
///
/// This writer can operate in two modes:
/// 1. **New manifest list**: Creates a completely new manifest list from data files
/// 2. **Append to existing**: Reuses compatible manifests from an existing manifest list
///
/// The writer automatically handles:
/// - Partition boundary calculations
/// - Manifest splitting for optimal performance
/// - Schema compatibility between format versions
/// - Concurrent manifest writing operations
///
/// # Type Parameters
/// * `'schema` - The lifetime of the Avro schema used for serialization
/// * `'metadata` - The lifetime of the table metadata reference
///
/// # Fields
/// * `table_metadata` - Reference to the table metadata for schema and configuration
/// * `writer` - The underlying Avro writer for manifest list serialization
/// * `selected_manifest` - Optional existing manifest that can be reused for appends
/// * `bounding_partition_values` - Computed partition boundaries for the data files
/// * `n_existing_files` - Count of existing files for split calculations
/// * `branch` - Optional branch name for multi-branch table operations
pub(crate) struct ManifestListWriter<'schema, 'metadata> {
    table_metadata: &'metadata TableMetadata,
    writer: AvroWriter<'schema, Vec<u8>>,
    selected_data_manifest: Option<ManifestListEntry>,
    selected_delete_manifest: Option<ManifestListEntry>,
    bounding_partition_values: Rectangle,
    n_existing_files: usize,
    commit_uuid: String,
    manifest_count: usize,
    branch: Option<String>,
}

impl<'schema, 'metadata> ManifestListWriter<'schema, 'metadata> {
    /// Creates a new ManifestListWriter for building a manifest list from scratch.
    ///
    /// This constructor initializes a writer that will create a completely new manifest list
    /// without reusing any existing manifests. It computes partition boundaries from the
    /// provided data files and sets up the Avro writer with the appropriate schema.
    ///
    /// # Arguments
    /// * `data_files` - Iterator over data files to compute partition boundaries from
    /// * `schema` - The Avro schema to use for manifest list serialization
    /// * `table_metadata` - Reference to the table metadata for partition field information
    /// * `branch` - Optional branch name for multi-branch table operations
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestListWriter instance or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * The partition fields cannot be retrieved from table metadata
    /// * Partition boundary computation fails
    /// * The Avro writer cannot be initialized
    ///
    /// # Example Usage
    /// ```ignore
    /// let writer = ManifestListWriter::new(
    ///     data_files.iter(),
    ///     &manifest_list_schema,
    ///     &table_metadata,
    ///     Some("main"),
    /// )?;
    /// ```
    pub(crate) fn new<'datafiles>(
        data_files: impl Iterator<Item = &'datafiles DataFile>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        branch: Option<&str>,
    ) -> Result<Self, Error> {
        let partition_fields = table_metadata.current_partition_fields(branch)?;

        let partition_column_names = partition_fields
            .iter()
            .map(|x| x.name())
            .collect::<SmallVec<[_; 4]>>();

        let bounding_partition_values =
            bounding_partition_values(data_files, &partition_column_names)?;

        let commit_uuid = uuid::Uuid::new_v4().to_string();

        let writer = AvroWriter::new(schema, Vec::new());

        Ok(Self {
            table_metadata,
            writer,
            selected_data_manifest: None,
            selected_delete_manifest: None,
            bounding_partition_values,
            n_existing_files: 0,
            commit_uuid,
            manifest_count: 0,
            branch: branch.map(ToOwned::to_owned),
        })
    }

    /// Creates a new ManifestListWriter from an existing manifest list, optimizing for append operations.
    ///
    /// This constructor analyzes an existing manifest list to determine which manifests can be
    /// reused for the new operation. It selects compatible manifests based on partition boundaries
    /// and copies other manifests to the new manifest list. This approach optimizes append
    /// operations by avoiding unnecessary manifest rewrites.
    ///
    /// The method:
    /// 1. Reads the existing manifest list to understand current manifests
    /// 2. Computes partition boundaries for the new data files
    /// 3. Selects manifests that can be reused (partitioned vs unpartitioned logic)
    /// 4. Copies non-selected manifests to the new manifest list
    /// 5. Prepares to append new data to the selected manifest
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes of the existing manifest list file
    /// * `data_files` - Iterator over new data files to be appended
    /// * `schema` - The Avro schema to use for manifest list serialization
    /// * `table_metadata` - Reference to the table metadata for partition field information
    /// * `branch` - Optional branch name for multi-branch table operations
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestListWriter instance with selected manifest or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * The existing manifest list cannot be parsed
    /// * Partition fields cannot be retrieved from table metadata
    /// * Partition boundary computation fails
    /// * Manifest selection logic fails
    /// * The Avro writer cannot be initialized
    ///
    /// # Example Usage
    /// ```ignore
    /// let writer = ManifestListWriter::from_existing(
    ///     &existing_manifest_list_bytes,
    ///     new_data_files.iter(),
    ///     &manifest_list_schema,
    ///     &table_metadata,
    ///     Some("main"),
    /// )?;
    /// ```
    pub(crate) fn from_existing<'datafiles>(
        bytes: &[u8],
        data_files: impl Iterator<Item = &'datafiles DataFile>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        branch: Option<&str>,
    ) -> Result<Self, Error> {
        let partition_fields = table_metadata.current_partition_fields(branch)?;

        let partition_column_names = partition_fields
            .iter()
            .map(|x| x.name())
            .collect::<SmallVec<[_; 4]>>();

        let bounding_partition_values =
            bounding_partition_values(data_files, &partition_column_names)?;

        let manifest_list_reader = ManifestListReader::new(bytes, table_metadata)?;

        let commit_uuid = uuid::Uuid::new_v4().to_string();

        let mut writer = AvroWriter::new(schema, Vec::new());

        let SelectedManifest {
            data_manifest,
            delete_manifest,
            file_count_all_entries,
        } = if partition_column_names.is_empty() {
            select_manifest_unpartitioned(manifest_list_reader, &mut writer)?
        } else {
            select_manifest_partitioned(
                manifest_list_reader,
                &mut writer,
                &bounding_partition_values,
            )?
        };

        Ok(Self {
            table_metadata,
            writer,
            selected_data_manifest: Some(data_manifest),
            selected_delete_manifest: delete_manifest,
            bounding_partition_values,
            n_existing_files: file_count_all_entries,
            commit_uuid,
            manifest_count: 0,
            branch: branch.map(ToOwned::to_owned),
        })
    }

    /// Creates a ManifestListWriter from an existing manifest list, excluding manifests scheduled for overwriting.
    ///
    /// This constructor is specifically designed for overwrite operations where certain manifests
    /// need to be replaced while preserving others. It analyzes an existing manifest list and:
    /// 1. Identifies manifests that should be overwritten (excluded from the new manifest list)
    /// 2. Selects compatible manifests that can be reused for appending new data
    /// 3. Copies non-selected, non-overwritten manifests to the new manifest list
    /// 4. Returns both the writer and the list of manifests that will be overwritten
    ///
    /// This approach optimizes overwrite operations by:
    /// - Avoiding unnecessary rewrites of unaffected manifests
    /// - Providing efficient append capabilities for new data
    /// - Returning metadata about what will be overwritten for cleanup operations
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes of the existing manifest list file
    /// * `data_files` - Iterator over new data files to be appended
    /// * `manifests_to_overwrite` - Set of manifest paths that should be excluded/overwritten
    /// * `schema` - The Avro schema to use for manifest list serialization
    /// * `table_metadata` - Reference to the table metadata for partition field information
    /// * `branch` - Optional branch name for multi-branch table operations
    ///
    /// # Returns
    /// * `Result<(Self, Vec<ManifestListEntry>), Error>` - A tuple containing:
    ///   - A new ManifestListWriter instance with selected manifest for appends
    ///   - A vector of ManifestListEntry objects that will be overwritten
    ///
    /// # Errors
    /// Returns an error if:
    /// * The existing manifest list cannot be parsed
    /// * Partition fields cannot be retrieved from table metadata
    /// * Partition boundary computation fails
    /// * Manifest selection logic fails
    /// * The Avro writer cannot be initialized
    ///
    /// # Example Usage
    /// ```ignore
    /// let manifests_to_overwrite = HashSet::from(["manifest1.avro", "manifest2.avro"]);
    /// let (writer, overwritten_manifests) = ManifestListWriter::from_existing_without_overwrites(
    ///     &existing_manifest_list_bytes,
    ///     new_data_files.iter(),
    ///     &manifests_to_overwrite,
    ///     &manifest_list_schema,
    ///     &table_metadata,
    ///     Some("main"),
    /// )?;
    /// ```
    pub(crate) fn from_existing_without_overwrites<'datafiles>(
        bytes: &[u8],
        data_files: impl Iterator<Item = &'datafiles DataFile>,
        manifests_to_overwrite: &HashSet<String>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        branch: Option<&str>,
    ) -> Result<(Self, Vec<ManifestListEntry>), Error> {
        let partition_fields = table_metadata.current_partition_fields(branch)?;

        let partition_column_names = partition_fields
            .iter()
            .map(|x| x.name())
            .collect::<SmallVec<[_; 4]>>();

        let bounding_partition_values =
            bounding_partition_values(data_files, &partition_column_names)?;

        let manifest_list_reader = ManifestListReader::new(bytes, table_metadata)?;

        let commit_uuid = uuid::Uuid::new_v4().to_string();

        let mut writer = AvroWriter::new(schema, Vec::new());

        let OverwriteManifest {
            manifest,
            file_count_all_entries,
            manifests_to_overwrite: manifests,
        } = if partition_column_names.is_empty() {
            select_manifest_without_overwrites_unpartitioned(
                manifest_list_reader,
                &mut writer,
                manifests_to_overwrite,
            )?
        } else {
            select_manifest_without_overwrites_partitioned(
                manifest_list_reader,
                &mut writer,
                &bounding_partition_values,
                manifests_to_overwrite,
            )?
        };

        Ok((
            Self {
                table_metadata,
                writer,
                selected_data_manifest: Some(manifest),
                selected_delete_manifest: None,
                bounding_partition_values,
                n_existing_files: file_count_all_entries,
                commit_uuid,
                manifest_count: 0,
                branch: branch.map(ToOwned::to_owned),
            },
            manifests,
        ))
    }

    /// Calculates the optimal number of manifest splits for the given number of data files.
    ///
    /// This method determines how many manifest files should be created to optimize
    /// query performance and manage file sizes. The calculation considers:
    /// - The number of existing files in the table
    /// - The number of new data files being added
    /// - The number of files in any selected (reusable) manifest
    ///
    /// The splitting strategy helps maintain optimal manifest sizes for efficient
    /// query planning and metadata operations.
    ///
    /// # Arguments
    /// * `n_data_files` - The number of new data files being added
    ///
    /// # Returns
    /// * `u32` - The recommended number of manifest splits
    ///
    /// # Example Usage
    /// ```ignore
    /// let splits = writer.n_splits(1000); // Calculate splits for 1000 new files
    /// ```
    pub(crate) fn n_splits(&self, n_data_files: usize, content: Content) -> u32 {
        let selected_manifest = match content {
            Content::Data => &self.selected_data_manifest,
            Content::Deletes => &self.selected_delete_manifest,
        };
        let selected_manifest_file_count = selected_manifest
            .as_ref()
            .and_then(|selected_manifest| {
                match (
                    selected_manifest.existing_files_count,
                    selected_manifest.added_files_count,
                ) {
                    (Some(x), Some(y)) => Some(x + y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }
            })
            .unwrap_or(0) as usize;

        compute_n_splits(
            self.n_existing_files,
            n_data_files,
            selected_manifest_file_count,
        )
    }

    /// Appends data files to a single manifest and finalizes the manifest list.
    ///
    /// This method creates a single manifest file containing all the provided data files,
    /// either by appending to an existing reusable manifest or creating a new one.
    /// It then writes the complete manifest list to object storage.
    ///
    /// This approach is optimal for:
    /// - Small to medium append operations
    /// - Cases where manifest splitting is not required
    /// - Simple append operations without complex partitioning needs
    ///
    /// The process:
    /// 1. Determines whether to reuse an existing manifest or create new one
    /// 2. Creates/updates a manifest writer with the selected manifest
    /// 3. Appends all provided data files to the manifest
    /// 4. Finalizes the manifest and writes it to storage
    /// 5. Adds the manifest entry to the manifest list
    /// 6. Writes the complete manifest list to storage
    ///
    /// # Arguments
    /// * `data_files` - Iterator over manifest entries to append
    /// * `snapshot_id` - The snapshot ID for the new manifest
    /// * `object_store` - The object store for writing files
    ///
    /// # Returns
    /// * `Result<String, Error>` - The location of the new manifest list file or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * Manifest schema creation fails
    /// * Manifest writer creation or operation fails
    /// * Object storage operations fail
    /// * Avro serialization fails
    ///
    /// # Example Usage
    /// ```ignore
    /// let manifest_list_location = writer.append(
    ///     data_files_iter,
    ///     snapshot_id,
    ///     object_store,
    /// ).await?;
    /// ```
    #[inline]
    pub(crate) async fn append(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<(), Error> {
        self.append_filtered(
            data_files,
            snapshot_id,
            None::<fn(&Result<ManifestEntry, Error>) -> bool>,
            object_store,
            content,
        )
        .await
    }

    #[inline]
    pub(crate) async fn append_concurrently(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        self.append_filtered_concurrently(
            data_files,
            snapshot_id,
            None::<fn(&Result<ManifestEntry, Error>) -> bool>,
            object_store,
            content,
        )
        .await
    }

    /// Appends data files to a single manifest with optional filtering and finalizes the manifest list.
    ///
    /// This method extends the basic `append` functionality by providing the ability to
    /// filter data files during the append process. It creates a single manifest file containing
    /// the provided data files (after filtering), either by appending to an existing reusable
    /// manifest or creating a new one.
    ///
    /// The filtering capability is particularly useful for:
    /// - Excluding certain files from being included in the manifest
    /// - Conditional processing based on file properties or metadata
    /// - Implementing custom business logic during manifest creation
    /// - Selective processing of existing manifest entries when reusing manifests
    ///
    /// This approach is optimal for:
    /// - Small to medium append operations with conditional logic
    /// - Cases where certain files need to be excluded or processed differently
    /// - Operations requiring custom filtering logic during manifest creation
    ///
    /// The process:
    /// 1. Determines whether to reuse an existing manifest or create a new one
    /// 2. If reusing, applies the filter when reading existing manifest entries
    /// 3. Creates/updates a manifest writer with the selected manifest
    /// 4. Appends all provided data files to the manifest
    /// 5. Finalizes the manifest and writes it to storage
    /// 6. Adds the manifest entry to the manifest list
    /// 7. Writes the complete manifest list to storage
    ///
    /// # Arguments
    /// * `data_files` - Iterator over manifest entries to append
    /// * `snapshot_id` - The snapshot ID for the new manifest
    /// * `filter` - Optional filter function to apply to existing manifest entries when reusing
    /// * `object_store` - The object store for writing files
    ///
    /// # Returns
    /// * `Result<String, Error>` - The location of the new manifest list file or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * Partition field retrieval fails
    /// * Manifest schema creation fails
    /// * Manifest writer creation or operation fails
    /// * Object storage operations fail
    /// * Avro serialization fails
    /// * Filter function encounters an error
    ///
    /// # Example Usage
    /// ```ignore
    /// let manifest_list_location = writer.append_filtered(
    ///     data_files_iter,
    ///     snapshot_id,
    ///     Some(|entry| entry.as_ref().map(|e| e.status() == &Status::Added).unwrap_or(false)),
    ///     object_store,
    /// ).await?;
    /// ```
    #[inline]
    pub(crate) async fn append_filtered(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        filter: Option<impl Fn(&Result<ManifestEntry, Error>) -> bool>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<(), Error> {
        self.append_filtered_concurrently(data_files, snapshot_id, filter, object_store, content)
            .await?
            .await?;
        Ok(())
    }

    pub(crate) async fn append_filtered_concurrently(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        filter: Option<impl Fn(&Result<ManifestEntry, Error>) -> bool>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let selected_manifest = match content {
            Content::Data => self.selected_data_manifest.take(),
            Content::Deletes => self.selected_delete_manifest.take(),
        };
        let selected_manifest_bytes_opt = prefetch_manifest(&selected_manifest, &object_store);

        let partition_fields = self
            .table_metadata
            .current_partition_fields(self.branch.as_deref())?;

        let manifest_schema = ManifestEntry::schema(
            &partition_value_schema(&partition_fields)?,
            &self.table_metadata.format_version,
        )?;

        let mut manifest_writer = if let (Some(mut manifest), Some(manifest_bytes)) =
            (selected_manifest, selected_manifest_bytes_opt)
        {
            let manifest_bytes = manifest_bytes.await??;

            manifest.manifest_path = self.next_manifest_location();

            let manifest_reader = ManifestReader::new(manifest_bytes.as_ref())?;

            if let Some(filter) = filter {
                ManifestWriter::from_existing(
                    manifest_reader.filter(filter),
                    manifest,
                    &manifest_schema,
                    self.table_metadata,
                    self.branch.as_deref(),
                )?
            } else {
                ManifestWriter::from_existing(
                    manifest_reader,
                    manifest,
                    &manifest_schema,
                    self.table_metadata,
                    self.branch.as_deref(),
                )?
            }
        } else {
            let manifest_location = self.next_manifest_location();

            ManifestWriter::new(
                &manifest_location,
                snapshot_id,
                &manifest_schema,
                self.table_metadata,
                content,
                self.branch.as_deref(),
            )?
        };

        for manifest_entry in data_files {
            manifest_writer.append(manifest_entry?)?;
        }

        let (manifest, future) = manifest_writer.finish_concurrently(object_store.clone())?;

        self.writer.append_ser(manifest)?;

        Ok(future)
    }

    /// Appends data files by splitting them across multiple manifests and finalizes the manifest list.
    ///
    /// This method is designed for large append operations where splitting data files across
    /// multiple manifest files provides better query performance and parallelism. It distributes
    /// the data files across the specified number of splits based on partition boundaries.
    ///
    /// This approach is optimal for:
    /// - Large append operations with hundreds or thousands of files
    /// - Partitioned tables where files can be split by partition boundaries
    /// - Cases requiring high query parallelism and performance
    ///
    /// The process:
    /// 1. Computes optimal partition boundaries for splitting
    /// 2. Merges new data files with existing files from selected manifest (if any)
    /// 3. Splits all files across the specified number of manifest files
    /// 4. Creates and writes multiple manifest files concurrently
    /// 5. Adds all manifest entries to the manifest list
    /// 6. Writes the complete manifest list to storage
    ///
    /// # Arguments
    /// * `data_files` - Iterator over manifest entries to append and split
    /// * `snapshot_id` - The snapshot ID for the new manifests
    /// * `n_splits` - The number of manifest files to create (should match `n_splits()` result)
    /// * `object_store` - The object store for writing files
    ///
    /// # Returns
    /// * `Result<String, Error>` - The location of the new manifest list file or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * Partition field retrieval fails
    /// * Manifest schema creation fails
    /// * File splitting logic fails
    /// * Manifest writer creation or operation fails
    /// * Concurrent manifest writing fails
    /// * Object storage operations fail
    /// * Avro serialization fails
    ///
    /// # Example Usage
    /// ```ignore
    /// let n_splits = writer.n_splits(data_files.len());
    /// let manifest_list_location = writer.append_split(
    ///     data_files_iter,
    ///     snapshot_id,
    ///     n_splits,
    ///     object_store,
    /// ).await?;
    /// ```
    pub(crate) async fn append_multiple_concurrently(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        n_splits: u32,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        self.append_multiple_filtered_concurrently(
            data_files,
            snapshot_id,
            n_splits,
            None::<fn(&Result<ManifestEntry, Error>) -> bool>,
            object_store,
            content,
        )
        .await
    }

    /// Appends data files across multiple manifests with optional filtering and finalizes the manifest list.
    ///
    /// This method extends the `append_multiple` functionality by providing the ability to
    /// filter data files during the append and splitting process. It distributes the data files
    /// (after filtering) across the specified number of splits based on partition boundaries,
    /// optimizing for large operations that require conditional processing.
    ///
    /// The filtering capability is particularly useful for:
    /// - Excluding certain files from being included in any manifest
    /// - Conditional processing based on file properties, status, or metadata
    /// - Implementing custom business logic during large-scale manifest operations
    /// - Selective processing of existing manifest entries when reusing manifests
    /// - Complex overwrite scenarios where certain entries need special handling
    ///
    /// This approach is optimal for:
    /// - Large append operations with hundreds or thousands of files requiring filtering
    /// - Partitioned tables where files need both splitting and filtering
    /// - Complex operations combining append, overwrite, and conditional logic
    /// - Cases requiring high query parallelism with selective data inclusion
    ///
    /// The process:
    /// 1. Computes optimal partition boundaries for splitting
    /// 2. If reusing an existing manifest, applies filter when reading existing entries
    /// 3. Merges new data files with filtered existing files from selected manifest
    /// 4. Splits all files across the specified number of manifest files
    /// 5. Creates and writes multiple manifest files concurrently
    /// 6. Adds all manifest entries to the manifest list
    /// 7. Writes the complete manifest list to storage
    ///
    /// # Arguments
    /// * `data_files` - Iterator over manifest entries to append and split
    /// * `snapshot_id` - The snapshot ID for the new manifests
    /// * `n_splits` - The number of manifest files to create (should match `n_splits()` result)
    /// * `filter` - Optional filter function to apply to existing manifest entries when reusing
    /// * `object_store` - The object store for writing files
    ///
    /// # Returns
    /// * `Result<String, Error>` - The location of the new manifest list file or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * Partition field retrieval fails
    /// * Manifest schema creation fails
    /// * File splitting logic fails
    /// * Manifest writer creation or operation fails
    /// * Concurrent manifest writing fails
    /// * Object storage operations fail
    /// * Avro serialization fails
    /// * Filter function encounters an error
    ///
    /// # Example Usage
    /// ```ignore
    /// let n_splits = writer.n_splits(data_files.len());
    /// let manifest_list_location = writer.append_multiple_filtered(
    ///     data_files_iter,
    ///     snapshot_id,
    ///     n_splits,
    ///     Some(|entry| entry.as_ref().map(|e| e.status() != &Status::Deleted).unwrap_or(false)),
    ///     object_store,
    /// ).await?;
    /// ```
    #[inline]
    pub(crate) async fn append_multiple_filtered(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        n_splits: u32,
        filter: Option<impl Fn(&Result<ManifestEntry, Error>) -> bool>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<(), Error> {
        self.append_multiple_filtered_concurrently(
            data_files,
            snapshot_id,
            n_splits,
            filter,
            object_store,
            content,
        )
        .await?
        .await?;
        Ok(())
    }

    pub(crate) async fn append_multiple_filtered_concurrently(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        n_splits: u32,
        filter: Option<impl Fn(&Result<ManifestEntry, Error>) -> bool>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let partition_fields = self
            .table_metadata
            .current_partition_fields(self.branch.as_deref())?;

        let partition_column_names = partition_fields
            .iter()
            .map(|x| x.name())
            .collect::<SmallVec<[_; 4]>>();

        let manifest_schema = ManifestEntry::schema(
            &partition_value_schema(&partition_fields)?,
            &self.table_metadata.format_version,
        )?;

        let selected_manifest = match content {
            Content::Data => self.selected_data_manifest.take(),
            Content::Deletes => self.selected_delete_manifest.take(),
        };

        let bounds = selected_manifest
            .as_ref()
            .and_then(|x| x.partitions.as_deref())
            .map(summary_to_rectangle)
            .transpose()?
            .map(|mut x| {
                x.expand(&self.bounding_partition_values);
                x
            })
            .unwrap_or(self.bounding_partition_values.clone());

        let selected_manifest_bytes_opt = prefetch_manifest(&selected_manifest, &object_store);

        // Split datafiles
        let splits = if let (Some(manifest), Some(manifest_bytes)) =
            (selected_manifest, selected_manifest_bytes_opt)
        {
            let manifest_bytes = manifest_bytes.await??;
            let manifest_reader = ManifestReader::new(&*manifest_bytes)?.map(|entry| {
                let mut entry = entry?;
                *entry.status_mut() = Status::Existing;
                if entry.sequence_number().is_none() {
                    *entry.sequence_number_mut() = Some(manifest.sequence_number);
                }
                if entry.snapshot_id().is_none() {
                    *entry.snapshot_id_mut() = Some(manifest.added_snapshot_id);
                }
                Ok(entry)
            });

            if let Some(filter) = filter {
                split_datafiles(
                    data_files.chain(manifest_reader.filter(filter)),
                    bounds,
                    &partition_column_names,
                    n_splits,
                )?
            } else {
                split_datafiles(
                    data_files.chain(manifest_reader),
                    bounds,
                    &partition_column_names,
                    n_splits,
                )?
            }
        } else {
            split_datafiles(data_files, bounds, &partition_column_names, n_splits)?
        };

        let (manifests, manifest_futures) = splits
            .into_iter()
            .map(|entries| {
                let manifest_location = self.next_manifest_location();

                let mut manifest_writer = ManifestWriter::new(
                    &manifest_location,
                    snapshot_id,
                    &manifest_schema,
                    self.table_metadata,
                    content,
                    self.branch.as_deref(),
                )?;

                for manifest_entry in entries {
                    manifest_writer.append(manifest_entry)?;
                }

                manifest_writer.finish_concurrently(object_store.clone())
            })
            .collect::<Result<(Vec<_>, Vec<_>), _>>()?;

        for manifest in manifests {
            self.writer.append_ser(manifest)?;
        }

        let future = futures::future::try_join_all(manifest_futures).map_ok(|_| ());

        Ok(future)
    }

    pub(crate) async fn finish(
        mut self,
        snapshot_id: i64,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<String, Error> {
        if let Some(selected_data_manifest) = self.selected_data_manifest.take() {
            self.writer.append_ser(selected_data_manifest)?;
        }

        if let Some(selected_delete_manifest) = self.selected_delete_manifest.take() {
            self.writer.append_ser(selected_delete_manifest)?;
        }

        let new_manifest_list_location = new_manifest_list_location(
            &self.table_metadata.location,
            snapshot_id,
            0,
            &self.commit_uuid,
        );

        let manifest_list_bytes = self.writer.into_inner()?;

        object_store
            .put(
                &strip_prefix(&new_manifest_list_location).into(),
                manifest_list_bytes.into(),
            )
            .await?;

        Ok(new_manifest_list_location)
    }

    /// Processes manifests for overwrite operations by filtering out specific data files.
    ///
    /// This method is specifically designed for complex overwrite scenarios where certain data files
    /// within existing manifests need to be removed while preserving others. It processes a list of
    /// manifests, filters out specified data files from each one, and adds the filtered manifests
    /// to the manifest list being constructed.
    ///
    /// This operation is essential for:
    /// - **Overwrite operations**: Removing specific files that are being replaced by new data
    /// - **Partial table updates**: Selectively removing files while keeping others
    /// - **Data deduplication**: Filtering out duplicate or obsolete data files
    /// - **Complex merge operations**: Managing file-level changes during table merges
    ///
    /// The method operates at the manifest level rather than the manifest list level, providing
    /// fine-grained control over which data files are included in the final table state.
    ///
    /// The process:
    /// 1. Processes each manifest in the provided list concurrently
    /// 2. For each manifest, retrieves the list of data files to filter out
    /// 3. Loads the manifest content from object storage
    /// 4. Creates a new manifest location and updates the manifest path
    /// 5. Uses `ManifestWriter::from_existing_with_filter` to exclude specified files
    /// 6. Writes the filtered manifest to storage with a new location
    /// 7. Adds the new manifest entry to the manifest list being constructed
    ///
    /// # Arguments
    /// * `manifests_to_overwrite` - Vector of manifest list entries to process and filter
    /// * `data_files_to_filter` - Map from manifest path to list of data file paths to exclude
    /// * `object_store` - The object store for reading existing and writing new manifest files
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if all manifests were successfully processed and filtered
    ///
    /// # Errors
    /// Returns an error if:
    /// * A manifest path is not found in the `data_files_to_filter` map
    /// * Object storage operations fail (reading existing or writing new manifests)
    /// * Manifest parsing or writing operations fail
    /// * Avro serialization fails
    /// * Concurrent processing encounters errors
    ///
    /// # Example Usage
    /// ```ignore
    /// let mut manifest_list_writer = ManifestListWriter::new(...)?;
    /// let data_files_to_filter = HashMap::from([
    ///     ("manifest1.avro".to_string(), vec!["file1.parquet".to_string(), "file2.parquet".to_string()]),
    ///     ("manifest2.avro".to_string(), vec!["file3.parquet".to_string()]),
    /// ]);
    ///
    /// manifest_list_writer.append_and_filter(
    ///     manifests_to_overwrite,
    ///     &data_files_to_filter,
    ///     object_store,
    /// ).await?;
    /// ```
    ///
    /// # Implementation Notes
    /// - Manifests are processed concurrently for optimal performance
    /// - Each filtered manifest gets a new location to avoid conflicts
    /// - The method modifies the manifest list writer's internal state by adding filtered manifests
    /// - This method is typically called as part of a larger overwrite operation workflow
    pub(crate) async fn append_and_filter(
        &mut self,
        manifests_to_overwrite: Vec<ManifestListEntry>,
        data_files_to_filter: &HashMap<String, Vec<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(), Error> {
        let partition_fields = self
            .table_metadata
            .current_partition_fields(self.branch.as_deref())?;

        let manifest_schema = Arc::new(ManifestEntry::schema(
            &partition_value_schema(&partition_fields)?,
            &self.table_metadata.format_version,
        )?);

        let futures = manifests_to_overwrite.into_iter().map(|mut manifest| {
            let object_store = object_store.clone();
            let manifest_schema = manifest_schema.clone();
            let branch = self.branch.clone();
            let manifest_location = self.next_manifest_location();
            let table_metadata = self.table_metadata;
            async move {
                let data_files_to_filter: HashSet<String> = data_files_to_filter
                    .get(&manifest.manifest_path)
                    .ok_or(Error::NotFound("Datafiles for manifest".to_owned()))?
                    .iter()
                    .map(ToOwned::to_owned)
                    .collect();

                let bytes = object_store
                    .clone()
                    .get(&strip_prefix(&manifest.manifest_path).into())
                    .await?
                    .bytes()
                    .await?;

                manifest.manifest_path = manifest_location;

                let manifest_writer = ManifestWriter::from_existing_with_filter(
                    &bytes,
                    manifest,
                    &data_files_to_filter,
                    &manifest_schema,
                    table_metadata,
                    branch.as_deref(),
                )?;

                let new_manifest = manifest_writer.finish(object_store.clone()).await?;

                Ok::<_, Error>(new_manifest)
            }
        });
        for manifest_res in join_all(futures).await {
            let manifest = manifest_res?;
            self.writer.append_ser(manifest)?;
        }
        Ok(())
    }

    pub(crate) fn selected_data_manifest(&self) -> Option<&ManifestListEntry> {
        self.selected_data_manifest.as_ref()
    }

    /// Get the next manifest location, tracking and numbering preceding manifests written by this
    /// writer.
    fn next_manifest_location(&mut self) -> String {
        let next_id = self.manifest_count;

        self.manifest_count += 1;

        new_manifest_location(&self.table_metadata.location, &self.commit_uuid, next_id)
    }
}
