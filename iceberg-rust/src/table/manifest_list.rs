/*!
 * Helpers to deal with manifest lists and files
*/

use std::{
    io::{Cursor, Read},
    iter::{repeat, Map, Repeat, Zip},
    sync::Arc,
};

use apache_avro::{types::Value as AvroValue, Reader as AvroReader, Schema as AvroSchema};
use iceberg_rust_spec::{
    manifest_list::{
        avro_value_to_manifest_list_entry, manifest_list_schema_v1, manifest_list_schema_v2,
        ManifestListEntry,
    },
    snapshot::Snapshot,
    table_metadata::{FormatVersion, TableMetadata},
    util::strip_prefix,
};
use object_store::ObjectStore;

use crate::error::Error;

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
    ManifestListReader::new(bytes, table_metadata)}
