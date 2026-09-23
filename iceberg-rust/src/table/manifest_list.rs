/*!
 * Helpers to deal with manifest lists and files
*/

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io::{Cursor, Read},
    sync::Arc,
};

use apache_avro::{Reader as AvroReader, Schema as AvroSchema, Writer as AvroWriter};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use iceberg_rust_spec::{
    manifest::{partition_value_schema, DataFile, FirstRowIdInheritance, ManifestEntry, Status},
    manifest_list::{Content, ManifestListEntry, ManifestListEntryDecoder},
    snapshot::Snapshot,
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
};
use object_store::{ObjectStore, ObjectStoreExt};
use smallvec::SmallVec;

use crate::{
    error::Error,
    table::datafiles,
    util::{summary_to_rectangle, Rectangle, Vec4},
};

use super::{
    manifest::{FilteredManifestStats, ManifestReader, ManifestWriter},
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

const MANIFEST_REWRITE_CONCURRENCY: usize = 8;

fn manifest_schema_for_spec(
    table_metadata: &TableMetadata,
    partition_spec_id: i32,
    added_snapshot_id: Option<i64>,
) -> Result<AvroSchema, Error> {
    let fields = table_metadata.partition_fields_for_spec(partition_spec_id, added_snapshot_id)?;
    ManifestEntry::schema(
        &partition_value_schema(&fields)?,
        &table_metadata.format_version,
    )
    .map_err(Error::from)
}

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
    reader: AvroReader<'a, R>,
    decoder: ManifestListEntryDecoder<'metadata>,
    writer_format_version: FormatVersion,
}

impl<R: Read> Iterator for ManifestListReader<'_, '_, R> {
    type Item = Result<ManifestListEntry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next().map(|value| {
            self.decoder
                .decode(value, self.writer_format_version)
                .map_err(Error::from)
        })
    }
}

fn manifest_list_format_version(
    schema: &AvroSchema,
    table_format_version: FormatVersion,
) -> Result<FormatVersion, Error> {
    let AvroSchema::Record(record) = schema else {
        return Err(Error::InvalidFormat(
            "manifest list writer schema must be a record".to_string(),
        ));
    };

    let detected = if record.lookup.contains_key("first_row_id") {
        FormatVersion::V3
    } else if record.lookup.contains_key("content") {
        FormatVersion::V2
    } else {
        FormatVersion::V1
    };
    if table_format_version == FormatVersion::V2 && detected == FormatVersion::V3 {
        // Older Embucket builds emitted a V2 schema containing the future V3 field.
        Ok(FormatVersion::V2)
    } else {
        Ok(detected)
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
        // We intentionally read without a reader schema so that the embedded writer schema is used.
        // Some query engines (e.g. AWS Athena) write legacy field names such as
        // `added_data_files_count` instead of the spec-correct `added_files_count` (see
        // https://github.com/apache/iceberg/issues/8684). Supplying a reader schema causes
        // apache_avro to perform Avro-level field resolution by name, which fails for those
        // files because avro-rs has no alias support.  Without a reader schema the raw field
        // names from the file reach the serde layer, where `#[serde(alias)]` can map both the
        // legacy and the canonical names.
        //
        // TODO: switch back to `AvroReader::with_schema` once all major query engines write
        // the spec-correct field names.
        let reader = AvroReader::new(reader)?;
        let writer_format_version =
            manifest_list_format_version(reader.writer_schema(), table_metadata.format_version)?;
        Ok(Self {
            reader,
            decoder: ManifestListEntryDecoder::new(table_metadata),
            writer_format_version,
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
        .or(table_metadata.current_schema())?;
    let manifests = read_snapshot(snapshot, table_metadata, object_store.clone())
        .await?
        .collect::<Result<Vec<_>, _>>()?;
    let datafiles = datafiles(object_store, &manifests, None, (None, None)).await?;

    let primitive_field_ids = schema.primitive_field_ids().collect::<Vec<_>>();
    let n = primitive_field_ids.len();
    datafiles
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowIdAssigner {
    next_row_id: i64,
}

impl RowIdAssigner {
    fn new(next_row_id: i64) -> Self {
        Self { next_row_id }
    }

    /// Applies Iceberg's first-row-ID assignment rule to one manifest-list entry.
    fn assign(&mut self, manifest: &mut ManifestListEntry) -> Result<(), Error> {
        if manifest.content == Content::Deletes {
            manifest.first_row_id = None;
            return Ok(());
        }
        if manifest.first_row_id.is_some() {
            return Ok(());
        }

        let added_rows = required_non_negative_row_count(
            manifest.added_rows_count,
            "added_rows_count",
            &manifest.manifest_path,
        )?;
        let existing_rows = required_non_negative_row_count(
            manifest.existing_rows_count,
            "existing_rows_count",
            &manifest.manifest_path,
        )?;
        let assigned_first_row_id = self.next_row_id;
        self.next_row_id = assigned_first_row_id
            .checked_add(added_rows)
            .and_then(|next| next.checked_add(existing_rows))
            .ok_or_else(|| Error::InvalidFormat("next row id overflow".to_string()))?;
        manifest.first_row_id = Some(assigned_first_row_id);
        Ok(())
    }
}

fn required_non_negative_row_count(
    row_count: Option<i64>,
    field: &str,
    manifest_path: &str,
) -> Result<i64, Error> {
    let row_count = row_count.ok_or_else(|| {
        Error::InvalidFormat(format!(
            "manifest {manifest_path} is missing required {field}"
        ))
    })?;
    if row_count < 0 {
        return Err(Error::InvalidFormat(format!(
            "manifest {manifest_path} has negative {field}"
        )));
    }
    Ok(row_count)
}

pub(crate) fn append_manifest(
    writer: &mut AvroWriter<'_, Vec<u8>>,
    row_id_assigner: Option<&mut RowIdAssigner>,
    mut manifest: ManifestListEntry,
) -> Result<(), Error> {
    if let Some(row_id_assigner) = row_id_assigner {
        manifest.format_version = FormatVersion::V3;
        row_id_assigner.assign(&mut manifest)?;
        if manifest.content == Content::Data && manifest.first_row_id.is_none() {
            return Err(Error::InvalidFormat(format!(
                "v3 data manifest {} has no first row id",
                manifest.manifest_path
            )));
        }
    } else if manifest.format_version == FormatVersion::V3 {
        return Err(Error::InvalidFormat(
            "v3 manifest list requires row id assignment".to_string(),
        ));
    }
    writer.append_ser(manifest)?;
    Ok(())
}

pub(crate) struct ManifestListWriter<'schema, 'metadata> {
    table_metadata: &'metadata TableMetadata,
    writer: AvroWriter<'schema, Vec<u8>>,
    selected_data_manifest: Option<ManifestListEntry>,
    selected_delete_manifest: Option<ManifestListEntry>,
    bounding_partition_values: Rectangle,
    n_existing_files: usize,
    commit_uuid: String,
    manifest_count: usize,
    row_id_assigner: Option<RowIdAssigner>,
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
    /// )?;
    /// ```
    pub(crate) fn new<'datafiles>(
        data_files: impl Iterator<Item = &'datafiles DataFile>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
    ) -> Result<Self, Error> {
        let partition_fields = table_metadata.current_partition_fields()?;

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
            row_id_assigner: (table_metadata.format_version == FormatVersion::V3)
                .then(|| RowIdAssigner::new(table_metadata.next_row_id)),
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
    /// )?;
    /// ```
    pub(crate) fn from_existing<'datafiles>(
        bytes: &[u8],
        data_files: impl Iterator<Item = &'datafiles DataFile>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
    ) -> Result<Self, Error> {
        let partition_fields = table_metadata.current_partition_fields()?;

        let partition_column_names = partition_fields
            .iter()
            .map(|x| x.name())
            .collect::<SmallVec<[_; 4]>>();

        let bounding_partition_values =
            bounding_partition_values(data_files, &partition_column_names)?;

        let manifest_list_reader = ManifestListReader::new(bytes, table_metadata)?;

        let commit_uuid = uuid::Uuid::new_v4().to_string();

        let mut writer = AvroWriter::new(schema, Vec::new());

        // Rewriting a v3 manifest would lose inherited row IDs for existing files.
        // Preserve old manifests and place appended rows in new manifests instead.
        if table_metadata.format_version == FormatVersion::V3 {
            let mut file_count_all_entries = 0usize;
            let mut row_id_assigner = RowIdAssigner::new(table_metadata.next_row_id);
            for manifest in manifest_list_reader {
                let manifest = manifest?;
                let file_count = manifest
                    .added_files_count
                    .unwrap_or(0)
                    .checked_add(manifest.existing_files_count.unwrap_or(0))
                    .ok_or_else(|| Error::InvalidFormat("manifest file count".to_string()))?;
                file_count_all_entries = file_count_all_entries
                    .checked_add(file_count.try_into()?)
                    .ok_or_else(|| Error::InvalidFormat("manifest file count".to_string()))?;
                append_manifest(&mut writer, Some(&mut row_id_assigner), manifest)?;
            }

            return Ok(Self {
                table_metadata,
                writer,
                selected_data_manifest: None,
                selected_delete_manifest: None,
                bounding_partition_values,
                n_existing_files: file_count_all_entries,
                commit_uuid,
                manifest_count: 0,
                row_id_assigner: Some(row_id_assigner),
            });
        }

        let SelectedManifest {
            data_manifest,
            delete_manifest,
            file_count_all_entries,
        } = if partition_column_names.is_empty() {
            select_manifest_unpartitioned(manifest_list_reader, &mut writer, None, table_metadata)?
        } else {
            select_manifest_partitioned(
                manifest_list_reader,
                &mut writer,
                None,
                &bounding_partition_values,
                table_metadata,
            )?
        };

        Ok(Self {
            table_metadata,
            writer,
            selected_data_manifest: data_manifest,
            selected_delete_manifest: delete_manifest,
            bounding_partition_values,
            n_existing_files: file_count_all_entries,
            commit_uuid,
            manifest_count: 0,
            row_id_assigner: None,
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
    /// )?;
    /// ```
    pub(crate) fn from_existing_without_overwrites<'datafiles>(
        bytes: &[u8],
        data_files: impl Iterator<Item = &'datafiles DataFile>,
        manifests_to_overwrite: &HashSet<String>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
    ) -> Result<(Self, Vec<ManifestListEntry>), Error> {
        let partition_fields = table_metadata.current_partition_fields()?;

        let partition_column_names = partition_fields
            .iter()
            .map(|x| x.name())
            .collect::<SmallVec<[_; 4]>>();

        let bounding_partition_values =
            bounding_partition_values(data_files, &partition_column_names)?;

        let manifest_list_reader = ManifestListReader::new(bytes, table_metadata)?;

        let commit_uuid = uuid::Uuid::new_v4().to_string();

        let mut writer = AvroWriter::new(schema, Vec::new());

        let mut row_id_assigner = (table_metadata.format_version == FormatVersion::V3)
            .then(|| RowIdAssigner::new(table_metadata.next_row_id));

        // V3 manifests carry row-ID inheritance state. Preserve unaffected
        // manifests by path and materialize inherited IDs only in the affected
        // manifests rewritten below.
        if table_metadata.format_version == FormatVersion::V3 {
            let mut manifests = Vec::new();
            let mut file_count_all_entries = 0usize;
            for manifest in manifest_list_reader {
                let manifest = manifest?;
                let file_count = manifest
                    .added_files_count
                    .unwrap_or(0)
                    .checked_add(manifest.existing_files_count.unwrap_or(0))
                    .ok_or_else(|| Error::InvalidFormat("manifest file count".to_string()))?;
                file_count_all_entries = file_count_all_entries
                    .checked_add(file_count.try_into()?)
                    .ok_or_else(|| Error::InvalidFormat("manifest file count".to_string()))?;

                if manifests_to_overwrite.contains(&manifest.manifest_path) {
                    manifests.push(manifest);
                } else {
                    append_manifest(&mut writer, row_id_assigner.as_mut(), manifest)?;
                }
            }

            return Ok((
                Self {
                    table_metadata,
                    writer,
                    selected_data_manifest: None,
                    selected_delete_manifest: None,
                    bounding_partition_values,
                    n_existing_files: file_count_all_entries,
                    commit_uuid,
                    manifest_count: 0,
                    row_id_assigner,
                },
                manifests,
            ));
        }

        let OverwriteManifest {
            manifest,
            file_count_all_entries,
            manifests_to_overwrite: manifests,
        } = if partition_column_names.is_empty() {
            select_manifest_without_overwrites_unpartitioned(
                manifest_list_reader,
                &mut writer,
                row_id_assigner.as_mut(),
                manifests_to_overwrite,
                table_metadata,
            )?
        } else {
            select_manifest_without_overwrites_partitioned(
                manifest_list_reader,
                &mut writer,
                row_id_assigner.as_mut(),
                &bounding_partition_values,
                manifests_to_overwrite,
                table_metadata,
            )?
        };

        Ok((
            Self {
                table_metadata,
                writer,
                selected_data_manifest: manifest,
                selected_delete_manifest: None,
                bounding_partition_values,
                n_existing_files: file_count_all_entries,
                commit_uuid,
                manifest_count: 0,
                row_id_assigner,
            },
            manifests,
        ))
    }

    /// Creates a manifest-list writer for an overwrite that only removes data files.
    ///
    /// Unlike [`Self::from_existing_without_overwrites`], this path does not select a
    /// manifest for new files and therefore does not require partition bounds from an
    /// added data file. Manifests containing files to remove are returned for filtering;
    /// every other manifest is copied to the new manifest list unchanged.
    pub(crate) fn from_existing_for_deletion(
        bytes: &[u8],
        manifests_to_overwrite: &HashSet<String>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
    ) -> Result<(Self, Vec<ManifestListEntry>), Error> {
        let manifest_list_reader = ManifestListReader::new(bytes, table_metadata)?;
        let mut writer = AvroWriter::new(schema, Vec::new());
        let mut row_id_assigner = (table_metadata.format_version == FormatVersion::V3)
            .then(|| RowIdAssigner::new(table_metadata.next_row_id));
        let mut manifests = Vec::new();
        let mut file_count_all_entries = 0usize;

        for manifest in manifest_list_reader {
            let manifest = manifest?;
            let file_count = manifest
                .added_files_count
                .unwrap_or(0)
                .checked_add(manifest.existing_files_count.unwrap_or(0))
                .ok_or_else(|| Error::InvalidFormat("manifest file count".to_string()))?;
            file_count_all_entries = file_count_all_entries
                .checked_add(file_count.try_into()?)
                .ok_or_else(|| Error::InvalidFormat("manifest file count".to_string()))?;

            if manifests_to_overwrite.contains(&manifest.manifest_path) {
                manifests.push(manifest);
            } else {
                append_manifest(&mut writer, row_id_assigner.as_mut(), manifest)?;
            }
        }

        Ok((
            Self {
                table_metadata,
                writer,
                selected_data_manifest: None,
                selected_delete_manifest: None,
                bounding_partition_values: Rectangle::new(Vec4::new(), Vec4::new()),
                n_existing_files: file_count_all_entries,
                commit_uuid: uuid::Uuid::new_v4().to_string(),
                manifest_count: 0,
                row_id_assigner,
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
            None::<HashSet<String>>,
            object_store,
            content,
        )
        .await
        .map(|_| ())
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
            None::<HashSet<String>>,
            object_store,
            content,
        )
        .await
        .map(|(future, _)| future)
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
    /// * `filter` - Optional set of file paths to exclude when reusing an existing manifest
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
        filter: Option<HashSet<String>>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<Option<FilteredManifestStats>, Error> {
        let (future, stats) = self
            .append_filtered_concurrently(data_files, snapshot_id, filter, object_store, content)
            .await?;
        future.await?;
        Ok(stats)
    }

    pub(crate) async fn append_filtered_concurrently(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        filter: Option<HashSet<String>>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<
        (
            impl Future<Output = Result<(), Error>>,
            Option<FilteredManifestStats>,
        ),
        Error,
    > {
        let selected_manifest = match content {
            Content::Data => self.selected_data_manifest.take(),
            Content::Deletes => self.selected_delete_manifest.take(),
        };
        let selected_manifest_bytes_opt = prefetch_manifest(&selected_manifest, &object_store);

        let partition_spec_id = selected_manifest
            .as_ref()
            .map_or(self.table_metadata.default_spec_id, |manifest| {
                manifest.partition_spec_id
            });
        let added_snapshot_id = selected_manifest
            .as_ref()
            .map(|manifest| manifest.added_snapshot_id);
        let manifest_schema =
            manifest_schema_for_spec(self.table_metadata, partition_spec_id, added_snapshot_id)?;

        let (mut manifest_writer, filtered_stats) =
            if let (Some(mut manifest), Some(manifest_bytes)) =
                (selected_manifest, selected_manifest_bytes_opt)
            {
                let manifest_bytes = manifest_bytes.await??;

                manifest.manifest_path = self.next_manifest_location();

                if let Some(filter) = filter {
                    let (manifest_writer, filtered_stats) =
                        ManifestWriter::from_existing_with_filter(
                            manifest_bytes.as_ref(),
                            manifest,
                            &filter,
                            snapshot_id,
                            &manifest_schema,
                            self.table_metadata,
                        )?;
                    (manifest_writer, Some(filtered_stats))
                } else {
                    let manifest_reader = ManifestReader::new(manifest_bytes.as_ref())?;
                    let manifest_writer = ManifestWriter::from_existing(
                        manifest_reader,
                        manifest,
                        snapshot_id,
                        &manifest_schema,
                        self.table_metadata,
                    )?;
                    (manifest_writer, None)
                }
            } else {
                let manifest_location = self.next_manifest_location();

                let manifest_writer = ManifestWriter::new(
                    &manifest_location,
                    snapshot_id,
                    &manifest_schema,
                    self.table_metadata,
                    content,
                )?;
                (manifest_writer, None)
            };

        for manifest_entry in data_files {
            manifest_writer.append(manifest_entry?)?;
        }

        let (manifest, future) = manifest_writer.finish_concurrently(object_store.clone())?;

        self.append_new_manifest(manifest)?;

        Ok((future, filtered_stats))
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
            None::<HashSet<String>>,
            object_store,
            content,
        )
        .await
        .map(|(future, _)| future)
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
    /// * `filter` - Optional set of file paths to exclude when reusing an existing manifest
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
        filter: Option<HashSet<String>>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<Option<FilteredManifestStats>, Error> {
        let (future, stats) = self
            .append_multiple_filtered_concurrently(
                data_files,
                snapshot_id,
                n_splits,
                filter,
                object_store,
                content,
            )
            .await?;
        future.await?;
        Ok(stats)
    }

    pub(crate) async fn append_multiple_filtered_concurrently(
        &mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        n_splits: u32,
        filter: Option<HashSet<String>>,
        object_store: Arc<dyn ObjectStore>,
        content: Content,
    ) -> Result<
        (
            impl Future<Output = Result<(), Error>>,
            Option<FilteredManifestStats>,
        ),
        Error,
    > {
        let mut removed_stats = if filter.is_some() {
            Some(FilteredManifestStats::default())
        } else {
            None
        };
        let partition_fields = self.table_metadata.current_partition_fields()?;

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
            let mut unmatched_files = filter.clone().unwrap_or_default();
            let mut row_id_inheritance =
                FirstRowIdInheritance::for_committed_manifest(manifest.first_row_id);
            let manifest_reader = ManifestReader::new(&*manifest_bytes)?.filter_map(|entry| {
                let mut entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => return Some(Err(err)),
                };

                if *entry.status() == Status::Deleted {
                    return None;
                }
                if let Err(error) = row_id_inheritance.apply(&mut entry) {
                    return Some(Err(error.into()));
                }
                if entry.sequence_number().is_none() {
                    *entry.sequence_number_mut() = Some(manifest.sequence_number);
                }
                if entry.snapshot_id().is_none() {
                    *entry.snapshot_id_mut() = Some(manifest.added_snapshot_id);
                }

                if let (Some(files_to_filter), Some(removed_stats)) =
                    (filter.as_ref(), &mut removed_stats)
                {
                    if files_to_filter.contains(entry.data_file().file_path()) {
                        if *entry.data_file().content()
                            != iceberg_rust_spec::manifest::Content::Data
                        {
                            return Some(Err(Error::InvalidFormat(
                                "overwrite can only remove data files".to_string(),
                            )));
                        }
                        unmatched_files.remove(entry.data_file().file_path());
                        removed_stats.removed_records += entry.data_file().record_count();
                        removed_stats.removed_file_size_bytes +=
                            entry.data_file().file_size_in_bytes();
                        removed_stats.removed_data_files += 1;
                        *entry.status_mut() = Status::Deleted;
                        *entry.snapshot_id_mut() = Some(snapshot_id);
                        return Some(Ok(entry));
                    }
                }
                *entry.status_mut() = Status::Existing;
                Some(Ok(entry))
            });

            let splits = split_datafiles(
                data_files.chain(manifest_reader),
                bounds,
                &partition_column_names,
                n_splits,
            )?;
            if !unmatched_files.is_empty() {
                let mut unmatched_files = unmatched_files.into_iter().collect::<Vec<_>>();
                unmatched_files.sort_unstable();
                return Err(Error::NotFound(format!(
                    "Live data files to overwrite were not found in manifest: {unmatched_files:?}"
                )));
            }
            splits
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
                )?;

                for manifest_entry in entries {
                    manifest_writer.append(manifest_entry)?;
                }

                manifest_writer.finish_concurrently(object_store.clone())
            })
            .collect::<Result<(Vec<_>, Vec<_>), _>>()?;

        for manifest in manifests {
            self.append_new_manifest(manifest)?;
        }

        let future = futures::future::try_join_all(manifest_futures).map_ok(|_| ());

        Ok((future, removed_stats))
    }

    pub(crate) async fn finish(
        mut self,
        snapshot_id: i64,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(String, Option<i64>), Error> {
        if let Some(selected_data_manifest) = self.selected_data_manifest.take() {
            append_manifest(
                &mut self.writer,
                self.row_id_assigner.as_mut(),
                selected_data_manifest,
            )?;
        }

        if let Some(selected_delete_manifest) = self.selected_delete_manifest.take() {
            append_manifest(
                &mut self.writer,
                self.row_id_assigner.as_mut(),
                selected_delete_manifest,
            )?;
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

        Ok((
            new_manifest_list_location,
            self.row_id_assigner.map(|assigner| assigner.next_row_id),
        ))
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
    /// * `snapshot_id` - Snapshot ID recorded on rewritten manifest entries
    ///
    /// # Returns
    /// * Statistics and deleted entries collected from the filtered manifests
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
    ///     snapshot_id,
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
        snapshot_id: i64,
    ) -> Result<FilteredManifestStats, Error> {
        let first_manifest_id = self.manifest_count;
        self.manifest_count = self
            .manifest_count
            .checked_add(manifests_to_overwrite.len())
            .ok_or_else(|| Error::InvalidFormat("manifest count overflow".to_string()))?;
        let table_location = self.table_metadata.location.clone();
        let commit_uuid = self.commit_uuid.clone();
        let table_metadata = self.table_metadata;

        let mut manifest_results = stream::iter(manifests_to_overwrite.into_iter().enumerate())
            .map(|(index, mut manifest)| {
                let object_store = object_store.clone();
                let manifest_location =
                    new_manifest_location(&table_location, &commit_uuid, first_manifest_id + index);
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
                    let source_snapshot_id = (manifest.partition_spec_id
                        != table_metadata.default_spec_id)
                        .then_some(manifest.added_snapshot_id);
                    let manifest_schema = manifest_schema_for_spec(
                        table_metadata,
                        manifest.partition_spec_id,
                        source_snapshot_id,
                    )?;

                    let (manifest_writer, filtered_stats) =
                        ManifestWriter::from_existing_with_filter(
                            &bytes,
                            manifest,
                            &data_files_to_filter,
                            snapshot_id,
                            &manifest_schema,
                            table_metadata,
                        )?;

                    let new_manifest = manifest_writer.finish(object_store.clone()).await?;

                    Ok::<_, Error>((new_manifest, filtered_stats))
                }
            })
            .buffered(MANIFEST_REWRITE_CONCURRENCY);
        let mut removed_stats = FilteredManifestStats::default();
        while let Some((manifest, filtered_stats)) = manifest_results.try_next().await? {
            removed_stats.append(filtered_stats);

            if manifest.added_files_count.unwrap_or(0) > 0
                || manifest.existing_files_count.unwrap_or(0) > 0
                || manifest.deleted_files_count.unwrap_or(0) > 0
            {
                append_manifest(&mut self.writer, self.row_id_assigner.as_mut(), manifest)?;
            }
        }
        Ok(removed_stats)
    }

    pub(crate) fn selected_data_manifest(&self) -> Option<&ManifestListEntry> {
        self.selected_data_manifest.as_ref()
    }

    fn append_new_manifest(&mut self, manifest: ManifestListEntry) -> Result<(), Error> {
        append_manifest(&mut self.writer, self.row_id_assigner.as_mut(), manifest)
    }

    /// Get the next manifest location, tracking and numbering preceding manifests written by this
    /// writer.
    fn next_manifest_location(&mut self) -> String {
        let next_id = self.manifest_count;

        self.manifest_count += 1;

        new_manifest_location(&self.table_metadata.location, &self.commit_uuid, next_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(
        path: &str,
        content: Content,
        first_row_id: Option<i64>,
        added_rows_count: Option<i64>,
        existing_rows_count: Option<i64>,
    ) -> ManifestListEntry {
        ManifestListEntry {
            format_version: FormatVersion::V3,
            manifest_path: path.to_string(),
            manifest_length: 1,
            partition_spec_id: 0,
            content,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(0),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count,
            existing_rows_count,
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id,
        }
    }

    #[test]
    fn row_id_assigner_follows_spec_worked_example() {
        let mut assigner = RowIdAssigner::new(1000);
        let mut existing = manifest("existing.avro", Content::Data, Some(925), Some(0), Some(75));
        let mut mixed = manifest("mixed.avro", Content::Data, None, Some(100), Some(25));
        let mut added = manifest("added.avro", Content::Data, None, Some(100), Some(0));
        let mut next = manifest("next.avro", Content::Data, None, Some(25), Some(0));

        assigner.assign(&mut existing).unwrap();
        assigner.assign(&mut mixed).unwrap();
        assigner.assign(&mut added).unwrap();
        assigner.assign(&mut next).unwrap();

        assert_eq!(existing.first_row_id, Some(925));
        assert_eq!(mixed.first_row_id, Some(1000));
        assert_eq!(added.first_row_id, Some(1125));
        assert_eq!(next.first_row_id, Some(1225));
        assert_eq!(assigner.next_row_id, 1250);
    }

    #[test]
    fn row_id_assigner_assigns_upgraded_existing_rows_and_skips_deletes() {
        let mut assigner = RowIdAssigner::new(1000);
        let mut upgraded = manifest("upgraded.avro", Content::Data, None, Some(0), Some(30));
        let mut deletes = manifest(
            "deletes.avro",
            Content::Deletes,
            Some(99),
            Some(10),
            Some(20),
        );

        assigner.assign(&mut upgraded).unwrap();
        assigner.assign(&mut deletes).unwrap();

        assert_eq!(upgraded.first_row_id, Some(1000));
        assert_eq!(deletes.first_row_id, None);
        assert_eq!(assigner.next_row_id, 1030);
    }

    #[test]
    fn row_id_assigner_requires_both_row_counts() {
        let mut assigner = RowIdAssigner::new(1000);
        let mut missing_existing = manifest("missing.avro", Content::Data, None, Some(1), None);

        assert!(matches!(
            assigner.assign(&mut missing_existing),
            Err(Error::InvalidFormat(_))
        ));
        assert_eq!(missing_existing.first_row_id, None);
        assert_eq!(assigner.next_row_id, 1000);
    }
}
