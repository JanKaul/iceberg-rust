//! Provides functionality for reading and writing Iceberg manifest files.
//!
//! This module implements the core manifest handling capabilities:
//! - Reading manifest files via [`ManifestReader`]
//! - Writing new manifest files via [`ManifestWriter`]
//! - Converting between manifest formats (V1/V2)
//! - Managing manifest entries and their metadata
//! - Tracking partition statistics and file counts
//!
//! Manifest files are a key part of Iceberg tables, containing:
//! - Data file locations and metadata
//! - Partition information
//! - File statistics and metrics
//! - Schema and partition spec references
//!
//! The module handles both V1 and V2 manifest formats transparently.

use std::{
    collections::HashSet,
    future::Future,
    io::Read,
    iter::{repeat, Map, Repeat, Zip},
    sync::Arc,
};

use apache_avro::{
    to_value, types::Value as AvroValue, Reader as AvroReader, Schema as AvroSchema,
    Writer as AvroWriter,
};
use futures::TryFutureExt;
use iceberg_rust_spec::{
    manifest::{Content, ManifestEntry, ManifestEntryV1, ManifestEntryV2, Status},
    manifest_list::{self, FieldSummary, ManifestListEntry},
    partition::{PartitionField, PartitionSpec},
    schema::{Schema, SchemaV1, SchemaV2},
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
    values::{Struct, Value},
};
use object_store::ObjectStore;

use crate::error::Error;

type ReaderZip<'a, R> = Zip<AvroReader<'a, R>, Repeat<Arc<(Schema, PartitionSpec, FormatVersion)>>>;
type ReaderMap<'a, R> = Map<
    ReaderZip<'a, R>,
    fn(
        (
            Result<AvroValue, apache_avro::Error>,
            Arc<(Schema, PartitionSpec, FormatVersion)>,
        ),
    ) -> Result<ManifestEntry, Error>,
>;

/// A reader for Iceberg manifest files that provides an iterator over manifest entries.
///
/// The reader handles both V1 and V2 manifest formats and automatically converts entries
/// to the appropriate version based on the manifest metadata.
///
/// # Type Parameters
/// * `'a` - The lifetime of the underlying reader
/// * `R` - The type implementing `Read` that provides the manifest data
pub(crate) struct ManifestReader<'a, R: Read> {
    reader: ReaderMap<'a, R>,
}

impl<R: Read> Iterator for ManifestReader<'_, R> {
    type Item = Result<ManifestEntry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl<R: Read> ManifestReader<'_, R> {
    /// Creates a new ManifestReader from a reader implementing the Read trait.
    ///
    /// This method initializes a reader that can parse both V1 and V2 manifest formats.
    /// It extracts metadata from the Avro file including format version, schema, and partition spec information.
    ///
    /// # Arguments
    /// * `reader` - A type implementing the `Read` trait that provides access to the manifest file data
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestReader instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The Avro reader cannot be created
    /// * Required metadata fields are missing
    /// * Format version is invalid
    /// * Schema or partition spec information cannot be parsed
    pub(crate) fn new(reader: R) -> Result<Self, Error> {
        let reader = AvroReader::new(reader)?;
        let metadata = reader.user_metadata();

        let format_version: FormatVersion = match metadata
            .get("format-version")
            .map(|bytes| String::from_utf8(bytes.clone()))
            .transpose()?
            .unwrap_or("1".to_string())
            .as_str()
        {
            "1" => Ok(FormatVersion::V1),
            "2" => Ok(FormatVersion::V2),
            _ => Err(Error::InvalidFormat("format version".to_string())),
        }?;

        let schema: Schema = match format_version {
            FormatVersion::V1 => TryFrom::<SchemaV1>::try_from(serde_json::from_slice(
                metadata
                    .get("schema")
                    .ok_or(Error::InvalidFormat("manifest metadata".to_string()))?,
            )?)?,
            FormatVersion::V2 => TryFrom::<SchemaV2>::try_from(serde_json::from_slice(
                metadata
                    .get("schema")
                    .ok_or(Error::InvalidFormat("manifest metadata".to_string()))?,
            )?)?,
        };

        let partition_fields: Vec<PartitionField> = serde_json::from_slice(
            metadata
                .get("partition-spec")
                .ok_or(Error::InvalidFormat("manifest metadata".to_string()))?,
        )?;
        let spec_id: i32 = metadata
            .get("partition-spec-id")
            .map(|x| String::from_utf8(x.clone()))
            .transpose()?
            .unwrap_or("0".to_string())
            .parse()?;
        let partition_spec = PartitionSpec::builder()
            .with_spec_id(spec_id)
            .with_fields(partition_fields)
            .build()?;
        Ok(Self {
            reader: reader
                .zip(repeat(Arc::new((schema, partition_spec, format_version))))
                .map(avro_value_to_manifest_entry),
        })
    }
}

/// A writer for Iceberg manifest files that handles creating and updating manifest entries.
///
/// ManifestWriter manages both creating new manifests and updating existing ones, handling
/// the complexities of manifest metadata, entry tracking, and partition summaries.
///
/// # Type Parameters
/// * `'schema` - The lifetime of the Avro schema used for writing entries
/// * `'metadata` - The lifetime of the table metadata reference
///
/// # Fields
/// * `table_metadata` - Reference to the table's metadata containing schema and partition information
/// * `manifest` - The manifest list entry being built or modified
/// * `writer` - The underlying Avro writer for serializing manifest entries
pub(crate) struct ManifestWriter<'schema, 'metadata> {
    table_metadata: &'metadata TableMetadata,
    manifest: ManifestListEntry,
    writer: AvroWriter<'schema, Vec<u8>>,
}

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct FilteredManifestStats {
    pub removed_data_files: i32,
    pub removed_records: i64,
    pub removed_file_size_bytes: i64,
}

impl FilteredManifestStats {
    pub(crate) fn append(&mut self, stats: FilteredManifestStats) {
        self.removed_file_size_bytes += stats.removed_file_size_bytes;
        self.removed_records += stats.removed_records;
        self.removed_data_files += stats.removed_data_files;
    }
}

impl<'schema, 'metadata> ManifestWriter<'schema, 'metadata> {
    /// Creates a new ManifestWriter for writing manifest entries to a new manifest file.
    ///
    /// # Arguments
    /// * `manifest_location` - The location where the manifest file will be written
    /// * `snapshot_id` - The ID of the snapshot this manifest belongs to
    /// * `schema` - The Avro schema used for serializing manifest entries
    /// * `table_metadata` - The table metadata containing schema and partition information
    /// * `branch` - Optional branch name to get the current schema from
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestWriter instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The Avro writer cannot be created
    /// * Required metadata fields cannot be serialized
    /// * The partition spec ID is not found in table metadata
    pub(crate) fn new(
        manifest_location: &str,
        snapshot_id: i64,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        content: manifest_list::Content,
        branch: Option<&str>,
    ) -> Result<Self, Error> {
        let mut writer = AvroWriter::new(schema, Vec::new());

        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
                FormatVersion::V2 => serde_json::to_string(&Into::<SchemaV2>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema(branch)?.schema_id())?,
        )?;

        let spec_id = table_metadata.default_spec_id;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(
                &table_metadata
                    .partition_specs
                    .get(&spec_id)
                    .ok_or(Error::NotFound(format!("Partition spec with id {spec_id}")))?
                    .fields(),
            )?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&spec_id)?,
        )?;

        writer.add_user_metadata(
            "content".to_string(),
            match content {
                manifest_list::Content::Data => "data",
                manifest_list::Content::Deletes => "deletes",
            },
        )?;

        let manifest = ManifestListEntry {
            format_version: table_metadata.format_version,
            manifest_path: manifest_location.to_owned(),
            manifest_length: 0,
            partition_spec_id: table_metadata.default_spec_id,
            content,
            sequence_number: table_metadata.last_sequence_number + 1,
            min_sequence_number: table_metadata.last_sequence_number + 1,
            added_snapshot_id: snapshot_id,
            added_files_count: Some(0),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
        };

        Ok(ManifestWriter {
            manifest,
            writer,
            table_metadata,
        })
    }

    /// Creates a ManifestWriter from an existing manifest file, preserving its entries.
    ///
    /// This method reads an existing manifest file and creates a new writer that includes
    /// all the existing entries with their status updated to "Existing". It also updates
    /// sequence numbers and snapshot IDs as needed.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes of the existing manifest file
    /// * `manifest` - The manifest list entry describing the existing manifest
    /// * `schema` - The Avro schema used for serializing manifest entries
    /// * `table_metadata` - The table metadata containing schema and partition information
    /// * `branch` - Optional branch name to get the current schema from
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestWriter instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The existing manifest cannot be read
    /// * The Avro writer cannot be created
    /// * Required metadata fields cannot be serialized
    /// * The partition spec ID is not found in table metadata
    pub(crate) fn from_existing(
        manifest_reader: impl Iterator<Item = Result<ManifestEntry, Error>>,
        mut manifest: ManifestListEntry,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        branch: Option<&str>,
    ) -> Result<Self, Error> {
        let mut writer = AvroWriter::new(schema, Vec::new());
        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
                FormatVersion::V2 => serde_json::to_string(&Into::<SchemaV2>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema(branch)?.schema_id())?,
        )?;

        let spec_id = table_metadata.default_spec_id;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(
                &table_metadata
                    .partition_specs
                    .get(&spec_id)
                    .ok_or(Error::NotFound(format!("Partition spec with id {spec_id}")))?
                    .fields(),
            )?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&spec_id)?,
        )?;

        writer.add_user_metadata(
            "content".to_string(),
            match manifest.content {
                manifest_list::Content::Data => "data",
                manifest_list::Content::Deletes => "deletes",
            },
        )?;

        writer.extend(
            manifest_reader
                .map(|entry| {
                    let mut entry = entry.map_err(|err| {
                        apache_avro::Error::new(apache_avro::error::Details::DeserializeValue(
                            err.to_string(),
                        ))
                    })?;
                    *entry.status_mut() = Status::Existing;
                    if entry.sequence_number().is_none() {
                        *entry.sequence_number_mut() = Some(manifest.sequence_number);
                    }
                    if entry.snapshot_id().is_none() {
                        *entry.snapshot_id_mut() = Some(manifest.added_snapshot_id);
                    }
                    to_value(entry)
                })
                .filter_map(Result::ok),
        )?;

        manifest.sequence_number = table_metadata.last_sequence_number + 1;

        manifest.existing_files_count = Some(
            manifest.existing_files_count.unwrap_or(0) + manifest.added_files_count.unwrap_or(0),
        );
        manifest.existing_rows_count = Some(
            manifest.existing_rows_count.unwrap_or(0) + manifest.added_rows_count.unwrap_or(0),
        );

        manifest.added_files_count = None;
        manifest.added_rows_count = None;

        Ok(ManifestWriter {
            manifest,
            writer,
            table_metadata,
        })
    }

    /// Creates a ManifestWriter from an existing manifest file with selective filtering of entries.
    ///
    /// This method reads an existing manifest file and creates a new writer that includes
    /// only the entries whose file paths are NOT in the provided filter set. Entries that
    /// pass the filter have their status updated to "Existing" and their sequence numbers
    /// and snapshot IDs updated as needed.
    ///
    /// This is particularly useful for overwrite operations where specific files need to be
    /// excluded from the new manifest while preserving other existing entries.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes of the existing manifest file
    /// * `manifest` - The manifest list entry describing the existing manifest
    /// * `filter` - A set of file paths to exclude from the new manifest
    /// * `schema` - The Avro schema used for serializing manifest entries
    /// * `table_metadata` - The table metadata containing schema and partition information
    /// * `branch` - Optional branch name to get the current schema from
    ///
    /// # Returns
    /// * `Result<Self, Error>` - A new ManifestWriter instance or an error if initialization fails
    ///
    /// # Errors
    /// Returns an error if:
    /// * The existing manifest cannot be read
    /// * The Avro writer cannot be created
    /// * Required metadata fields cannot be serialized
    /// * The partition spec ID is not found in table metadata
    ///
    /// # Behavior
    /// - Entries whose file paths are in the `filter` set are excluded from the new manifest
    /// - Remaining entries have their status set to `Status::Existing`
    /// - Sequence numbers are updated for entries that don't have them
    /// - Snapshot IDs are updated for entries that don't have them
    /// - The manifest's sequence number is incremented
    /// - File counts are updated to reflect the filtered entries
    pub(crate) fn from_existing_with_filter(
        bytes: &[u8],
        mut manifest: ManifestListEntry,
        filter: &HashSet<String>,
        schema: &'schema AvroSchema,
        table_metadata: &'metadata TableMetadata,
        branch: Option<&str>,
    ) -> Result<(Self, FilteredManifestStats), Error> {
        let manifest_reader = ManifestReader::new(bytes)?;

        let mut writer = AvroWriter::new(schema, Vec::new());
        let mut filtered_stats = FilteredManifestStats::default();

        writer.add_user_metadata(
            "format-version".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => "1".as_bytes(),
                FormatVersion::V2 => "2".as_bytes(),
            },
        )?;

        writer.add_user_metadata(
            "schema".to_string(),
            match table_metadata.format_version {
                FormatVersion::V1 => serde_json::to_string(&Into::<SchemaV1>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
                FormatVersion::V2 => serde_json::to_string(&Into::<SchemaV2>::into(
                    table_metadata.current_schema(branch)?.clone(),
                ))?,
            },
        )?;

        writer.add_user_metadata(
            "schema-id".to_string(),
            serde_json::to_string(&table_metadata.current_schema(branch)?.schema_id())?,
        )?;

        let spec_id = table_metadata.default_spec_id;

        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(
                &table_metadata
                    .partition_specs
                    .get(&spec_id)
                    .ok_or(Error::NotFound(format!("Partition spec with id {spec_id}")))?
                    .fields(),
            )?,
        )?;

        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            serde_json::to_string(&spec_id)?,
        )?;

        writer.add_user_metadata(
            "content".to_string(),
            match manifest.content {
                manifest_list::Content::Data => "data",
                manifest_list::Content::Deletes => "deletes",
            },
        )?;

        writer.extend(manifest_reader.filter_map(|entry| {
            let mut entry = entry
                .map_err(|err| {
                    apache_avro::Error::new(apache_avro::error::Details::DeserializeValue(
                        err.to_string(),
                    ))
                })
                .unwrap();
            if !filter.contains(entry.data_file().file_path()) {
                *entry.status_mut() = Status::Existing;
                if entry.sequence_number().is_none() {
                    *entry.sequence_number_mut() = Some(manifest.sequence_number);
                }
                if entry.snapshot_id().is_none() {
                    *entry.snapshot_id_mut() = Some(manifest.added_snapshot_id);
                }
                Some(to_value(entry).unwrap())
            } else {
                if *entry.data_file().content() == Content::Data {
                    filtered_stats.removed_records += entry.data_file().record_count();
                }
                filtered_stats.removed_file_size_bytes += entry.data_file().file_size_in_bytes();
                filtered_stats.removed_data_files += 1;
                None
            }
        }))?;

        manifest.sequence_number = table_metadata.last_sequence_number + 1;

        manifest.existing_files_count = Some(
            manifest.existing_files_count.unwrap_or(0) + manifest.added_files_count.unwrap_or(0)
                - filtered_stats.removed_data_files,
        );
        manifest.existing_rows_count = Some(
            manifest.existing_rows_count.unwrap_or(0) + manifest.added_rows_count.unwrap_or(0)
                - filtered_stats.removed_records,
        );

        manifest.added_files_count = None;
        manifest.added_rows_count = None;

        Ok((
            ManifestWriter {
                manifest,
                writer,
                table_metadata,
            },
            filtered_stats,
        ))
    }

    /// Appends a manifest entry to the manifest file and updates summary statistics.
    ///
    /// This method adds a new manifest entry while maintaining:
    /// - Partition statistics (null values, bounds)
    /// - File counts by status (added, existing, deleted)
    /// - Row counts (added, deleted)
    /// - Sequence number tracking
    ///
    /// # Arguments
    /// * `manifest_entry` - The manifest entry to append
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if the entry was successfully appended, Error otherwise
    ///
    /// # Errors
    /// Returns an error if:
    /// * The entry cannot be serialized
    /// * Partition statistics cannot be updated
    /// * The default partition spec is not found
    pub(crate) fn append(&mut self, manifest_entry: ManifestEntry) -> Result<(), Error> {
        let mut added_rows_count = 0;
        let mut deleted_rows_count = 0;

        if self.manifest.partitions.is_none() {
            self.manifest.partitions = Some(
                self.table_metadata
                    .default_partition_spec()?
                    .fields()
                    .iter()
                    .map(|_| FieldSummary {
                        contains_null: false,
                        contains_nan: None,
                        lower_bound: None,
                        upper_bound: None,
                    })
                    .collect::<Vec<FieldSummary>>(),
            );
        }

        match manifest_entry.data_file().content() {
            Content::Data => {
                added_rows_count += manifest_entry.data_file().record_count();
            }
            Content::EqualityDeletes => {
                deleted_rows_count += manifest_entry.data_file().record_count();
            }
            _ => (),
        }
        let status = *manifest_entry.status();

        update_partitions(
            self.manifest.partitions.as_mut().unwrap(),
            manifest_entry.data_file().partition(),
            self.table_metadata.default_partition_spec()?.fields(),
        )?;

        if let Some(sequence_number) = manifest_entry.sequence_number() {
            if self.manifest.min_sequence_number > *sequence_number {
                self.manifest.min_sequence_number = *sequence_number;
            }
        };

        self.writer.append_ser(manifest_entry)?;

        match status {
            Status::Added => {
                self.manifest.added_files_count = match self.manifest.added_files_count {
                    Some(count) => Some(count + 1),
                    None => Some(1),
                };
            }
            Status::Existing => {
                self.manifest.existing_files_count = match self.manifest.existing_files_count {
                    Some(count) => Some(count + 1),
                    None => Some(1),
                };
            }
            Status::Deleted => {
                self.manifest.deleted_files_count = match self.manifest.deleted_files_count {
                    Some(count) => Some(count + 1),
                    None => Some(1),
                };
            }
        }

        self.manifest.added_rows_count = match self.manifest.added_rows_count {
            Some(count) => Some(count + added_rows_count),
            None => Some(added_rows_count),
        };

        self.manifest.deleted_rows_count = match self.manifest.deleted_rows_count {
            Some(count) => Some(count + deleted_rows_count),
            None => Some(deleted_rows_count),
        };

        Ok(())
    }

    /// Finalizes the manifest writer and writes the manifest file to storage.
    ///
    /// This method:
    /// 1. Completes writing all entries
    /// 2. Updates the manifest length
    /// 3. Writes the manifest file to the object store
    ///
    /// # Arguments
    /// * `object_store` - The object store to write the manifest file to
    ///
    /// # Returns
    /// * `Result<ManifestListEntry, Error>` - The completed manifest list entry or an error
    ///
    /// # Errors
    /// Returns an error if:
    /// * The writer cannot be finalized
    /// * The manifest file cannot be written to storage
    pub(crate) async fn finish(
        mut self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<ManifestListEntry, Error> {
        let manifest_bytes = self.writer.into_inner()?;

        let manifest_length: i64 = manifest_bytes.len() as i64;

        self.manifest.manifest_length += manifest_length;

        object_store
            .put(
                &strip_prefix(&self.manifest.manifest_path).as_str().into(),
                manifest_bytes.into(),
            )
            .await?;
        Ok(self.manifest)
    }

    /// Finishes writing the manifest file concurrently.
    ///
    /// This method completes the manifest writing process by finalizing the writer
    /// and returning both the manifest list entry and a future for the actual file upload.
    /// The upload operation can be awaited separately, allowing for concurrent processing
    /// of multiple manifest writes.
    ///
    /// # Arguments
    ///
    /// * `object_store` - The object store implementation used to persist the manifest file
    ///
    /// # Returns
    ///
    /// Returns a tuple containing:
    /// - `ManifestListEntry`: The completed manifest entry with updated metadata
    /// - `impl Future<Output = Result<PutResult, Error>>`: A future that performs the actual file upload
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The writer cannot be finalized
    /// - There are issues preparing the upload operation
    pub(crate) fn finish_concurrently(
        mut self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(ManifestListEntry, impl Future<Output = Result<(), Error>>), Error> {
        let manifest_bytes = self.writer.into_inner()?;

        let manifest_length: i64 = manifest_bytes.len() as i64;

        self.manifest.manifest_length += manifest_length;

        let path = strip_prefix(&self.manifest.manifest_path).as_str().into();
        let future = async move {
            object_store
                .put(&path, manifest_bytes.into())
                .map_ok(|_| ())
                .map_err(Error::from)
                .await
        };
        Ok((self.manifest, future))
    }

    pub(crate) fn apply_filtered_stats(&mut self, filtered_stats: &FilteredManifestStats) {
        let removed_files = filtered_stats.removed_data_files;
        if removed_files > 0 {
            self.manifest.deleted_files_count = match self.manifest.deleted_files_count {
                Some(count) => Some(count + removed_files),
                None => Some(removed_files),
            };
        }

        if filtered_stats.removed_records > 0 {
            self.manifest.deleted_rows_count = match self.manifest.deleted_rows_count {
                Some(count) => Some(count + filtered_stats.removed_records),
                None => Some(filtered_stats.removed_records),
            };
        }
    }
}

#[allow(clippy::type_complexity)]
/// Convert avro value to ManifestEntry based on the format version of the table.
fn avro_value_to_manifest_entry(
    value: (
        Result<AvroValue, apache_avro::Error>,
        Arc<(Schema, PartitionSpec, FormatVersion)>,
    ),
) -> Result<ManifestEntry, Error> {
    let entry = value.0?;
    let schema = &value.1 .0;
    let partition_spec = &value.1 .1;
    let format_version = &value.1 .2;
    match format_version {
        FormatVersion::V2 => ManifestEntry::try_from_v2(
            apache_avro::from_value::<ManifestEntryV2>(&entry)?,
            schema,
            partition_spec,
        )
        .map_err(Error::from),
        FormatVersion::V1 => ManifestEntry::try_from_v1(
            apache_avro::from_value::<ManifestEntryV1>(&entry)?,
            schema,
            partition_spec,
        )
        .map_err(Error::from),
    }
}

fn update_partitions(
    partitions: &mut [FieldSummary],
    partition_values: &Struct,
    partition_columns: &[PartitionField],
) -> Result<(), Error> {
    for (field, summary) in partition_columns.iter().zip(partitions.iter_mut()) {
        let value = partition_values.get(field.name()).and_then(|x| x.as_ref());
        if let Some(value) = value {
            if summary.lower_bound.is_none() {
                summary.lower_bound = Some(value.clone());
            } else if let Some(lower_bound) = &mut summary.lower_bound {
                match (value, lower_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
            if summary.upper_bound.is_none() {
                summary.upper_bound = Some(value.clone());
            } else if let Some(upper_bound) = &mut summary.upper_bound {
                match (value, upper_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

/// TODO
#[cfg(test)]
mod tests {}
