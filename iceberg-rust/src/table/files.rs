/*!
 * Helper for iterating over files in a table.
*/
use std::{io::Cursor, iter::repeat, sync::Arc};

use anyhow::Result;
use apache_avro::types::Value as AvroValue;
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use object_store::path::Path;

use crate::{
    model::{
        manifest::{ManifestEntry, ManifestEntryV1, ManifestEntryV2},
        manifest_list::ManifestFile,
        table_metadata::FormatVersion,
    },
    util,
};

use super::Table;

impl Table {
    /// Get the data_files associated to a table. The files are returned based on the list of manifest files associated to that table.
    /// The included manifest files can be filtered based on an filter vector. The filter vector has the length equal to the number of manifest files
    /// and contains a true entry everywhere the manifest file is to be included in the output.
    pub async fn files(&self, filter: Option<Vec<bool>>) -> Result<Vec<ManifestEntry>> {
        // filter manifest files according to filter vector
        let iter = match filter {
            Some(predicate) => {
                self.manifests()
                    .iter()
                    .zip(Box::new(predicate.into_iter())
                        as Box<dyn Iterator<Item = bool> + Send + Sync>)
                    .filter_map(
                        filter_manifest as fn((&ManifestFile, bool)) -> Option<&ManifestFile>,
                    )
            }
            None => self
                .manifests()
                .iter()
                .zip(Box::new(repeat(true)) as Box<dyn Iterator<Item = bool> + Send + Sync>)
                .filter_map(filter_manifest as fn((&ManifestFile, bool)) -> Option<&ManifestFile>),
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
                let reader = apache_avro::Reader::new(bytes)?;
                Ok(stream::iter(reader.map(|record| {
                    avro_value_to_manifest_entry(record, &self.metadata().format_version)
                })))
            })
            .flat_map(|reader| reader.try_flatten_stream())
            .try_collect()
            .await
    }
}

// Filter manifest files according to predicate. Returns Some(&ManifestFile) of the predicate is true and None if it is false.
fn filter_manifest((manifest, predicate): (&ManifestFile, bool)) -> Option<&ManifestFile> {
    if predicate {
        Some(manifest)
    } else {
        None
    }
}

// Convert avro value to ManifestEntry based on the format version of the table.
fn avro_value_to_manifest_entry(
    entry: Result<AvroValue, apache_avro::Error>,
    format_version: &FormatVersion,
) -> Result<ManifestEntry, anyhow::Error> {
    match format_version {
        FormatVersion::V1 => entry
            .and_then(|value| apache_avro::from_value::<ManifestEntryV1>(&value))
            .map(ManifestEntry::V1)
            .map_err(anyhow::Error::msg),
        FormatVersion::V2 => entry
            .and_then(|value| apache_avro::from_value::<ManifestEntryV2>(&value))
            .map(ManifestEntry::V2)
            .map_err(anyhow::Error::msg),
    }
}

#[cfg(test)]
mod tests {

    use anyhow::anyhow;
    use futures::stream::{self, StreamExt};
    use futures::{TryFutureExt, TryStreamExt};
    use itertools::Itertools;
    use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
    use parquet::format::FileMetaData;
    use parquet::schema::types;
    use parquet::{arrow::async_reader::fetch_parquet_metadata, errors::ParquetError};
    use std::sync::Arc;

    use crate::catalog::identifier::Identifier;
    use crate::catalog::memory::MemoryCatalog;
    use crate::catalog::relation::Relation;
    use crate::catalog::Catalog;

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
        let mut files = table
            .files(None)
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
