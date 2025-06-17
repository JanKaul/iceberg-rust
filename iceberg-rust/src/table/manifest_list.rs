/*!
 * Helpers to deal with manifest lists and files
*/

use std::{
    io::{Cursor, Read},
    iter::{repeat, Map, Repeat, Zip},
    sync::Arc,
};

use apache_avro::{
    types::Value as AvroValue, Reader as AvroReader, Schema as AvroSchema, Writer as AvroWriter,
};
use iceberg_rust_spec::{
    manifest::{partition_value_schema, DataFile, ManifestEntry, Status},
    manifest_list::{
        avro_value_to_manifest_list_entry, manifest_list_schema_v1, manifest_list_schema_v2,
        ManifestListEntry,
    },
    snapshot::Snapshot,
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
};
use object_store::ObjectStore;
use smallvec::SmallVec;

use crate::{
    error::Error,
    util::{summary_to_rectangle, Rectangle},
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

pub(crate) struct ManifestListWriter<'schema, 'metadata> {
    table_metadata: &'metadata TableMetadata,
    writer: AvroWriter<'schema, Vec<u8>>,
    selected_manifest: Option<ManifestListEntry>,
    bounding_partition_values: Rectangle,
    n_existing_files: usize,
    branch: Option<String>,
}

impl<'schema, 'metadata> ManifestListWriter<'schema, 'metadata> {
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

        let writer = AvroWriter::new(schema, Vec::new());

        Ok(Self {
            table_metadata,
            writer,
            selected_manifest: None,
            bounding_partition_values,
            n_existing_files: 0,
            branch: branch.map(ToOwned::to_owned),
        })
    }

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

        let mut writer = AvroWriter::new(schema, Vec::new());

        let SelectedManifest {
            manifest,
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
            selected_manifest: Some(manifest),
            bounding_partition_values,
            n_existing_files: file_count_all_entries,
            branch: branch.map(ToOwned::to_owned),
        })
    }

    pub(crate) fn n_splits(&self, n_data_files: usize) -> u32 {
        let selected_manifest_file_count = self
            .selected_manifest
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

    pub(crate) async fn append_and_finish(
        mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<String, Error> {
        let selected_manifest_bytes_opt = prefetch_manifest(&self.selected_manifest, &object_store);

        let partition_fields = self
            .table_metadata
            .current_partition_fields(self.branch.as_deref())?;

        let manifest_schema = ManifestEntry::schema(
            &partition_value_schema(&partition_fields)?,
            &self.table_metadata.format_version,
        )?;

        let commit_uuid = &uuid::Uuid::new_v4().to_string();

        let mut manifest_writer = if let (Some(mut manifest), Some(manifest_bytes)) =
            (self.selected_manifest, selected_manifest_bytes_opt)
        {
            let manifest_bytes = manifest_bytes.await??;

            manifest.manifest_path =
                new_manifest_location(&self.table_metadata.location, commit_uuid, 0);

            ManifestWriter::from_existing(
                &manifest_bytes,
                manifest,
                &manifest_schema,
                self.table_metadata,
                self.branch.as_deref(),
            )?
        } else {
            let manifest_location =
                new_manifest_location(&self.table_metadata.location, commit_uuid, 0);

            ManifestWriter::new(
                &manifest_location,
                snapshot_id,
                &manifest_schema,
                self.table_metadata,
                self.branch.as_deref(),
            )?
        };

        for manifest_entry in data_files {
            manifest_writer.append(manifest_entry?)?;
        }

        let manifest = manifest_writer.finish(object_store.clone()).await?;

        self.writer.append_ser(manifest)?;

        let new_manifest_list_location =
            new_manifest_list_location(&self.table_metadata.location, snapshot_id, 0, commit_uuid);

        let manifest_list_bytes = self.writer.into_inner()?;

        object_store
            .put(
                &strip_prefix(&new_manifest_list_location).into(),
                manifest_list_bytes.into(),
            )
            .await?;

        Ok(new_manifest_list_location)
    }

    pub(crate) async fn append_split_and_finish(
        mut self,
        data_files: impl Iterator<Item = Result<ManifestEntry, Error>>,
        snapshot_id: i64,
        n_splits: u32,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<String, Error> {
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

        let bounds = self
            .selected_manifest
            .as_ref()
            .and_then(|x| x.partitions.as_deref())
            .map(summary_to_rectangle)
            .transpose()?
            .map(|mut x| {
                x.expand(&self.bounding_partition_values);
                x
            })
            .unwrap_or(self.bounding_partition_values);

        let selected_manifest_bytes_opt = prefetch_manifest(&self.selected_manifest, &object_store);

        let commit_uuid = &uuid::Uuid::new_v4().to_string();
        // Split datafiles
        let splits = if let (Some(manifest), Some(manifest_bytes)) =
            (self.selected_manifest, selected_manifest_bytes_opt)
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

            split_datafiles(
                data_files.chain(manifest_reader),
                bounds,
                &partition_column_names,
                n_splits,
            )?
        } else {
            split_datafiles(data_files, bounds, &partition_column_names, n_splits)?
        };

        let manifest_futures = splits
            .into_iter()
            .enumerate()
            .map(|(i, entries)| {
                let manifest_location =
                    new_manifest_location(&self.table_metadata.location, commit_uuid, i);

                let mut manifest_writer = ManifestWriter::new(
                    &manifest_location,
                    snapshot_id,
                    &manifest_schema,
                    self.table_metadata,
                    self.branch.as_deref(),
                )?;

                for manifest_entry in entries {
                    manifest_writer.append(manifest_entry)?;
                }

                Ok::<_, Error>(manifest_writer.finish(object_store.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let manifests = futures::future::try_join_all(manifest_futures).await?;

        for manifest in manifests {
            self.writer.append_ser(manifest)?;
        }

        let new_manifest_list_location =
            new_manifest_list_location(&self.table_metadata.location, snapshot_id, 0, commit_uuid);

        let manifest_list_bytes = self.writer.into_inner()?;

        object_store
            .put(
                &strip_prefix(&new_manifest_list_location).into(),
                manifest_list_bytes.into(),
            )
            .await?;

        Ok(new_manifest_list_location)
    }
}
