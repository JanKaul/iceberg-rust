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
        avro_value_to_manifest_file, manifest_list_schema_v1, manifest_list_schema_v2,
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

/// Iterator of ManifestFileEntries
pub struct ManifestListReader<'a, 'metadata, R: Read> {
    reader: ReaderMap<'a, 'metadata, R>,
}

impl<'a, 'metadata, R: Read> Iterator for ManifestListReader<'a, 'metadata, R> {
    type Item = Result<ManifestListEntry, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl<'a, 'metadata, R: Read> ManifestListReader<'a, 'metadata, R> {
    /// Create a new ManifestFile reader
    pub fn new(reader: R, table_metadata: &'metadata TableMetadata) -> Result<Self, Error> {
        let schema: &AvroSchema = match table_metadata.format_version {
            FormatVersion::V1 => manifest_list_schema_v1(),
            FormatVersion::V2 => manifest_list_schema_v2(),
        };
        Ok(Self {
            reader: AvroReader::with_schema(schema, reader)?
                .zip(repeat(table_metadata))
                .map(|avro_res| avro_value_to_manifest_file(avro_res).map_err(Error::from)),
        })
    }
}

/// Return all manifest files associated to the latest table snapshot. Reads the related manifest_list file and returns its entries.
/// If the manifest list file is empty returns an empty vector.
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
    ManifestListReader::new(bytes, table_metadata).map_err(Into::into)
}
